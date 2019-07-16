package storage

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/klauspost/pgzip"

	"io"
	"os"
	"path"
	"regexp"
	"sync"

	"time"

	storagecache "github.com/docker/distribution/registry/storage/cache"

	"github.com/panjf2000/ants"

	//"github.com/docker/distribution/registry/handlers"
	digest "github.com/opencontainers/go-digest"
)

// TODO(stevvooe): This should configurable in the future.
const blobCacheControlMaxAge = 365 * 24 * time.Hour

// blobServer simply serves blobs from a driver instance using a path function
// to identify paths and a descriptor service to fill in metadata.
type blobServer struct {
	driver  driver.StorageDriver
	statter distribution.BlobStatter
	//ring                        	roundrobin.RoundRobin
	metdataService storagecache.DedupMetadataServiceCacheProvider //NANNAN: add a metdataService for restore
	pathFn         func(dgst digest.Digest) (string, error)
	redirect       bool // allows disabling URLFor redirects
}

type registriesAPIResponse struct {
	Registries []string
}

func (bs *blobServer) URLWriter(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	var registries []string
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if bs.driver.Name() == "distributed" {
		registriesstr, _ := bs.driver.URLFor(ctx, "/dev/nil", nil)
		registries = strings.Split(registriesstr, ",")
	} else {
		registries = make([]string, 0)
	}
	enc := json.NewEncoder(w)
	if err := enc.Encode(registriesAPIResponse{
		Registries: registries,
	}); err != nil {
		return err
	}
	return nil
}

func (bs *blobServer) ServeHeadBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return err
	}

	path, err := bs.pathFn(desc.Digest)
	if err != nil {
		return err
	}

	if bs.redirect {
		redirectURL, err := bs.driver.URLFor(ctx, path, map[string]interface{}{"method": r.Method})
		switch err.(type) {
		case nil:
			// Redirect to storage URL.
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return err

		case driver.ErrUnsupportedMethod:
			// Fallback to serving the content directly.
		default:
			// Some unexpected error.
			return err
		}
	}

	br, err := newFileReader(ctx, bs.driver, path, desc.Size)
	if err != nil {
		return err
	}
	defer br.Close()

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, desc.Digest)) // If-None-Match handled by ServeContent
	w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))

	if w.Header().Get("Docker-Content-Digest") == "" {
		w.Header().Set("Docker-Content-Digest", desc.Digest.String())
	}

	if w.Header().Get("Content-Type") == "" {
		// Set the content type if not already set.
		w.Header().Set("Content-Type", desc.MediaType)
	}

	if w.Header().Get("Content-Length") == "" {
		// Set the content length if not already set.
		w.Header().Set("Content-Length", fmt.Sprint(desc.Size))
	}

	http.ServeContent(w, r, desc.Digest.String(), time.Time{}, br)
	return nil
}

type TarFile struct {
	Lm sync.Mutex
	Tw *tar.Writer
}

type PgzipFile struct {
	Lm sync.Mutex
	Pw *pgzip.Writer
}

type Task struct {
	Src   string
	Desc  string
	Reg   *Registry
	Tf    *TarFile
	Ctype string
}

func addToTarFile(tf *TarFile, path string, contents []byte) (int, error) {

	hdr := &tar.Header{
		Name: path,
		Mode: 0600,
		Size: int64(len(contents)),
	}

	tf.Lm.Lock()
	if err := tf.Tw.WriteHeader(hdr); err != nil {
		fmt.Printf("NANNAN: cannot write file header to tar file for %s\n", path)
		tf.Lm.Unlock()
		return 0, err
	}

	size, err := tf.Tw.Write(contents)
	if err != nil {
		fmt.Printf("NANNAN: cannot write file contents to tar file for %s\n", path)
		tf.Lm.Unlock()
		return 0, err
	}

	tf.Lm.Unlock()
	return size, nil
}

func pgzipconcatTarFile(compressbufp *bytes.Buffer, pw *PgzipFile) error {
	rdr, err := pgzip.NewReader(compressbufp)
	if err != nil {
		fmt.Printf("NANNAN: pgzipconcatTarFile: cannot create reader: %v \n", err)
		return err
	}
	bs, err := ioutil.ReadAll(rdr)
	if err != nil {
		fmt.Printf("NANNAN: pgzipconcatTarFile: cannot read from reader: %v \n", err)
		return err
	}

	pw.Lm.Lock()
	pw.Pw.Write(bs)
	pw.Lm.Unlock()

	return nil
}

func pgzipTarFile(bufp *bytes.Buffer, compressbufp *bytes.Buffer, compr_level int) []byte {
	w, _ := pgzip.NewWriterLevel(compressbufp, compr_level)
	io.Copy(w, bufp)
	w.Close()
	return compressbufp.Bytes()
}

func packFile(i interface{}) {

	task, ok := i.(*Task)
	if !ok {
		fmt.Println(ok)
		return
	}
	newsrc := task.Src
	desc := task.Desc
	reg := task.Reg
	tf := task.Tf
	ctype := task.Ctype

	var contents *[]byte

	start := time.Now()
	//check if newsrc is in file cache
	bfss, err := reg.blobcache.GetFile(newsrc)
	if err == nil {
		fmt.Printf("NANNAN: file cache hit\n")
		contents = &bfss
	} else {
		fmt.Printf("NANNAN: mvfile: file cache error: %v: %s", err, newsrc)
		fmt.Printf("NANNAN: file cache miss\n")

		//check src file exists or not
		var _, err = os.Stat(newsrc)
		if os.IsNotExist(err) {
			fmt.Printf("NANNAN: src file %v: %v", newsrc, err)
			return
		}

		bfss, err := ioutil.ReadFile(newsrc)
		if err != nil {
			fmt.Printf("NANNAN: read file %s generated error: %v", desc, err)
			return
		} else {
			contents = &bfss
			//put in cache
			fmt.Printf("NANNAN: file cache put: %v B for %s", len(bfss), newsrc)
			if len(bfss) > 0 {
				err = reg.blobcache.SetFile(newsrc, bfss, ctype)
				if err != nil {
					fmt.Printf("NANNAN: file cache cannot write to digest: %v: %v ", newsrc, err)
				}
			}
		}
	}

	size, err := addToTarFile(tf, desc, *contents)
	if err != nil {
		fmt.Printf("NANNAN: desc file %s generated error: %v", desc, err)
		return
	}

	DurationFCP := time.Since(start).Seconds()
	fmt.Printf("NANNAN: wrote %d bytes to file %s duration: %v", size, desc, DurationFCP)
	return
}

func (bs *blobServer) serveManifest(ctx context.Context, _desc distribution.Descriptor, w http.ResponseWriter, r *http.Request) (float64, error) {
	// get from traditional registry, this is a manifest
	path, err := bs.pathFn(_desc.Digest)
	if err != nil {
		return 0.0, err
	}

	if bs.redirect {
		redirectURL, err := bs.driver.URLFor(ctx, path, map[string]interface{}{"method": r.Method})
		switch err.(type) {
		case nil:
			// Redirect to storage URL.
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return 0.0, err

		case driver.ErrUnsupportedMethod:
			// Fallback to serving the content directly.
		default:
			// Some unexpected error.
			return 0.0, err
		}
	}

	br, err := newFileReader(ctx, bs.driver, path, _desc.Size) //stat.Size())
	if err != nil {
		return 0.0, err
	}
	defer br.Close()

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, _desc.Digest)) // If-None-Match handled by ServeContent
	w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))

	if w.Header().Get("Docker-Content-Digest") == "" {
		w.Header().Set("Docker-Content-Digest", _desc.Digest.String())
	}

	if w.Header().Get("Content-Type") == "" {
		// Set the content type if not already set.
		w.Header().Set("Content-Type", _desc.MediaType)
	}

	if w.Header().Get("Content-Length") == "" {
		// Set the content length if not already set.
		w.Header().Set("Content-Length", fmt.Sprint(_desc.Size))
	}

	start := time.Now()
	http.ServeContent(w, r, _desc.Digest.String(), time.Time{}, br)
	DurationNTT := time.Since(start).Seconds()

	return DurationNTT, nil
}

func (bs *blobServer) packAllFiles(ctx context.Context, desc distribution.SliceRecipeDescriptor, bufp *bytes.Buffer, reg *registry, constructtype string) float64 {

	fcntno := 0.0
	fcnt := 0
	for _, sfdescriptor := range desc.Files {
		if sfdescriptor.HostServerIp != reg.hostserverIp {
			context.GetLogger(ctx).Debugf("NANNAN: this is not a locally available file, %v", sfdescriptor.HostServerIp) // not locally available
			continue
		}
		fcnt += 1
	}

	if fcnt > 2000 {
		fcnt = 2000
	}

	var wg sync.WaitGroup
	antp, _ := ants.NewPoolWithFunc(fcnt, func(i interface{}) {
		packFile(i)
		wg.Done()
	})
	defer antp.Release()

	regx, err := regexp.Compile("[^a-zA-Z0-9/.-]+")
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s, ", err)
		return 0.0
	}

	tw := tar.NewWriter(bufp)

	tf := &TarFile{
		Tw: tw,
	}

	if constructtype == "PRECONSTRUCTSLICE" {
		constructtype == "PREFETCHFILE"
	}

	start := time.Now()
	for _, sfdescriptor := range desc.Files {

		if sfdescriptor.ServerIp != reg.hostserverIp {
			context.GetLogger(ctx).Debugf("NANNAN: this is not a locally available file, %v", sfdescriptor.HostServerIp) // not locally available
			continue
		}
		//in case there are same files inside the layer dir
		fcntno = fcntno + 1.0
		random_dir := fmt.Sprintf("%f", fcntno)

		tarfpath := regx.ReplaceAllString(strings.SplitN(sfdescriptor.FilePath, "diff", 2)[1], "") // replace alphanumeric
		destfpath := path.Join(tarfpath + "-" + random_dir)
		wg.Add(1)
		antp.Invoke(&Task{
			Src:   sfdescriptor.FilePath, //strings.TrimPrefix(bfdescriptor.BlobFilePath, "/var/lib/registry"),
			Desc:  destfpath,
			Reg:   reg,
			Tf:    tf,
			Ctype: constructtype,
		})
	}
	wg.Wait()

	if err := tw.Close(); err != nil {
		context.GetLogger(ctx).Debugf("NANNAN: cannot close tar file for %v", desc.BlobDigest.String())
		return 0.0
	}
	DurationCP := time.Since(start).Seconds()
	return DurationCP
}

func (bs *blobServer) TransferBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, _desc distribution.Descriptor,
	cprssrder *bytes.Reader) (float64, error) {

	path_old, err := bs.pathFn(_desc.Digest)
	if err != nil {
		return 0.0, err
	}

	if bs.redirect {
		redirectURL, err := bs.driver.URLFor(ctx, path_old, map[string]interface{}{"method": r.Method})
		switch err.(type) {
		case nil:
			// Redirect to storage URL.
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return 0.0, err

		case driver.ErrUnsupportedMethod:
			// Fallback to serving the content directly.
		default:
			// Some unexpected error.
			return 0.0, err
		}
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, _desc.Digest)) // If-None-Match handled by ServeContent
	w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))

	if w.Header().Get("Docker-Content-Digest") == "" {
		w.Header().Set("Docker-Content-Digest", _desc.Digest.String())
	}

	if w.Header().Get("Content-Type") == "" {
		// Set the content type if not already set.
		w.Header().Set("Content-Type", _desc.MediaType)
	}

	if w.Header().Get("Content-Length") == "" {
		// Set the content length if not already set.
		w.Header().Set("Content-Length", fmt.Sprint(size))
	}

	start = time.Now()
	http.ServeContent(w, r, _desc.Digest.String(), time.Time{}, cprssrder) //packFile)
	DurationNTT := time.Since(start).Seconds()

	return DurationNTT, nil

}

type Restoringbuffer struct {
	cnd  sync.Cond
	bufp *bytes.Buffer
}

/*
//TYPE XXX USRADDR XXX REPONAME XXX
MANIFEST
LAYER
SLICE
PRECONSTRUCTLAYER
*/

func (bs *blobServer) NotifyPeerPreconstructLayer(ctx context.Context, dgst digest.Digest, reg *registry, wg *sync.WaitGroup) bool {

	defer wg.Done()
	tp := "TYPEPRECONSTRUCTLAYER"

	dgststring := dgst.String()
	var regipbuffer bytes.Buffer
	reponame := handlers.getRepoName(ctx)
	usrname := handlers.getUsrAddr(ctx)

	desc, err := bs.metdataService.StatLayerRecipe(ctx, dgst)
	if err != nil {
		context.GetLogger(ctx).Warnf("NANNAN: COULDN'T FIND LAYER RECIPE: %v or Empty layer \n", err)
		return nil
	}
	regip := desc.MasterIp

	regipbuffer.WriteString(regip)
	regipbuffer.WriteString(":5000")
	regip = regipbuffer.String()
	context.GetLogger(ctx).Debugf("NANNAN: NotifyPeerPreconstructLayer from %s, dgst: %s \n", regip, dgststring)

	//GET /v2/<name>/blobs/<digest>
	var urlbuffer bytes.Buffer
	urlbuffer.WriteString("http://")
	urlbuffer.WriteString(regip)
	urlbuffer.WriteString("/v2/")
	urlbuffer.WriteString(tp + "USRADDR" + usrname + "REPONAME" + reponame)
	urlbuffer.WriteString("/blobs/")

	urlbuffer.WriteString(dgststring)
	url := urlbuffer.String()
	context.GetLogger(ctx).Debugf("NANNAN: NotifyPeerPreconstructLayer URL %s \n", url)

	//let's skip head request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: NotifyPeerPreconstructLayer GET URL %s, err %s", url, err)
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: NotifyPeerPreconstructLayer Do GET URL %s, err %s", url, err)
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		context.GetLogger(ctx).Errorf("%s returned status code %d", regname, resp.StatusCode)
		return errors.New("NotifyPeerPreconstructLayer to other servers, failed")
	}
	return nil
}

/*
//TYPE XXX USRADDR XXX REPONAME XXX
MANIFEST
LAYER
SLICE*/

func (bs *blobServer) GetSliceFromRegistry(ctx context.Context, dgst digest.Digest, regip string, pw *PgzipFile, wg *sync.WaitGroup, constructtype string) error {

	defer wg.Done()

	dgststring := dgst.String()
	var regipbuffer bytes.Buffer
	reponame := handlers.getRepoName(ctx)
	usrname := handlers.getUsrAddr(ctx)

	regipbuffer.WriteString(regip)
	regipbuffer.WriteString(":5000")
	regip = regipbuffer.String()
	context.GetLogger(ctx).Debugf("NANNAN: GetSliceFromRegistry from %s, dgst: %s \n", regip, dgststring)

	//GET /v2/<name>/blobs/<digest>
	var urlbuffer bytes.Buffer
	urlbuffer.WriteString("http://")
	urlbuffer.WriteString(regip)
	urlbuffer.WriteString("/v2/")
	if constructtype == "OnMiss" {
		constructtype = "SLICE"
	}
	urlbuffer.WriteString("TYPE" + constructtype + "USRADDR" + usrname + "REPONAME" + reponame)
	urlbuffer.WriteString("/blobs/")

	urlbuffer.WriteString(dgststring)
	url := urlbuffer.String()
	context.GetLogger(ctx).Debugf("NANNAN: GetSliceFromRegistry URL %s \n", url)

	//let's skip head request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: ForwardToRegistry GET URL %s, err %s", url, err)
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: GetSliceFromRegistry Do GET URL %s, err %s", url, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		context.GetLogger(ctx).Errorf("%s returned status code %d", regname, resp.StatusCode)
		return errors.New("get slices from other servers, failed")
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: cannot read from resp.body: %s\n", err)
		return err
	}

	buf := bytes.NewBuffer(body)
	err = pgzipconcatTarFile(buf, pw)
	return err
}

func (bs *blobServer) ConstructSlice(ctx context.Context, desc distribution.SliceRecipeDescriptor, dgst digest.Digest, reg *registry, constructtype string) ([]byte, float64, string) {

	var buf bytes.Buffer
	var comprssbuf bytes.Buffer

	rbuf := &Restoringbuffer{
		bufp: &comprssbuf,
	}
	start := time.Now()
	rsbuf, ok := reg.restoringslicermap.LoadOrStore(dgst.String(), rbuf)
	if ok {
		rsbuf.cnd.L.Lock()
		rsbuf.cnd.Wait()
		context.GetLogger(ctx).Debugf("NANNAN: slice construct finish waiting for digest: %v", dgst.String())
		rsbuf.cnd.L.Unlock()
		DurationWSCT := time.Since(start).Seconds()

		tp := "WAITSLICECONSTRUCT"
		bss := rsbuf.bufp.Bytes()
		return bss, DurationWSCT, tp
	} else {
		rbuf.cnd.L.Lock()
		start := time.Now()
		DurationCP := bs.packAllFiles(ctx, desc, &buf, constructtype)

		start := time.Now()
		bss := pgzipTarFile(bufp, &compressbufp, reg.compr_level)
		DurationCMP := time.Since(start).Seconds()

		rbuf.cnd.L.Broadcast()
		rbuf.cnd.L.Unlock()
		DurationSCT := time.Since(start).Seconds()

		bss := compressbufp.Bytes()

		return bss, DurationSCT, tp
	}
}

func (bs *blobServer) ConstructLayer(ctx context.Context, desc distribution.LayerRecipeDescriptor, dgst digest.Digest, reg *registry, constructtype string) ([]byte, float64, string) {

	var wg sync.WaitGroup
	var comprssbuf bytes.Buffer
	pw, _ := pgzip.NewWriterLevel(&compressbufp, reg.compr_level)
	pf := &PgzipFile{
		Pw: pw,
	}

	rbuf := &Restoringbuffer{
		bufp: &comprssbuf,
	}
	start := time.Now()
	rsbuf, ok := reg.restoringlayermap.LoadOrStore(dgst.String(), rbuf)
	if ok {
		rsbuf.cnd.L.Lock()
		rsbuf.cnd.Wait()
		context.GetLogger(ctx).Debugf("NANNAN: layer construct finish waiting for digest: %v", dgst.String())
		rsbuf.cnd.L.Unlock()
		DurationWLCT := time.Since(start).Seconds()

		tp := "WAITLAYERCONSTRUCT"
		bss := rsbuf.bufp.Bytes()
		return bss, DurationWLCT, tp
	} else {
		rbuf.cnd.L.Lock()

		start := time.Now()
		for _, hserver := range desc.HostServerIps {
			wg.Add(1)
			go GetSliceFromRegistry(ctx, dgst, hserver, pf, &wg, constructtype)
		}
		wg.Wait()
		DurationLCT := time.Since(start).Seconds()

		rbuf.cnd.L.Broadcast()
		rbuf.cnd.L.Unlock()

		tp := "LAYERCONSTRUCT"
		bss := comprssbuf.Bytes()

		return bss, DurationLCT, tp
	}
}

func (bs *blobServer) Preconstructlayers(ctx context.Context, reg *registry) error {
	//image preconstruction master,
	reponame := handlers.getRepoName(ctx)
	usrname := handlers.getUsrAddr(ctx)
	context.GetLogger(ctx).Debugf("NANNAN: Preconstructlayers: for repo (%s) and usr (%s) for dgst (%s)", reponame, usrname, dgst.String())

	rlmapentry, err := bs.metdataService.StatRLMapEntry(ctx, reponame)
	ulmapentry, err := bs.metdataService.StatULMapEntry(ctx, usrname)

	repullratio := ulmapentry.repullcnt / ulmapentry.totalcnt * 1.0
	rlset := mapset.NewSetFromSlice(rlmap.dgsts)
	ulset := mapset.NewSetFromSlice(ulmap.dgsts)

	if repullratio < reg.repullratiothres {
		descdgstset = rlset.Difference(ulset)
	} else {
		descdgstset = rlset
	}

	context.GetLogger(ctx).Debugf("NANNAN: descdgstlst: %v \n", descdgstset)
	if len(descdgstlst) == 0 {
		return nil
	}
	var wg sync.WaitGroup
	it := descdgstset.Iterator()
	for dgst := range it.C {
		wg.Add(1)
		go NotifyPeerPreconstructLayer(ctx, dgst, reg, &wg)
	}
	wg.Wait()

	return nil
}

/*
//NANNAN:
	NoCompression       = flate.NoCompression
	BestSpeed           = flate.BestSpeed
	BestCompression     = flate.BestCompression
	DefaultCompression  = flate.DefaultCompression
	ConstantCompression = flate.ConstantCompression
	HuffmanOnly         = flate.HuffmanOnly
//TYPE XXX USRADDR XXX REPONAME XXX
MANIFEST
LAYER
SLICE
*/

func (bs *blobServer) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest, reg *registry) error {

	compre_level := reg.compr_level

	start := time.Now()
	_desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return err
	}
	DurationML := time.Since(start).Seconds()

	reqtype := handlers.getType(ctx)
	context.GetLogger(ctx).Debugf("NANNAN: ServeBlob: type: %s\n", reqtype)

	reponame := handlers.getRepoName(ctx)
	usrname := handlers.getUsrAddr(ctx)
	context.GetLogger(ctx).Debugf("NANNAN: Preconstructlayers: for repo (%s) and usr (%s) for dgst (%s)", reponame, usrname, dgst.String())

	if reqtype == "MANIFEST" {
		// Userlru lst change
		DurationML := time.Since(start).Seconds()
		context.GetLogger(ctx).Debugf("NANNAN: THIS IS A MANIFEST REQUEST, serve and preconstruct layers\n")

		go bs.Preconstructlayers(ctx, reg) // prefetch window

		DurationNTT, err := bs.serveManifest(ctx, _desc, w, r)
		if err != nil {
			return err
		}
		context.GetLogger(ctx).Debugf("NANNAN: manifest: metadata lookup time: %v, manifest transfer time: %v, manifest compressed size: %v",
			DurationML, DurationNTT, _desc.Size)
		return nil
	}

	var constructtype string
	var bytesreader *Bytes.Reader
	cachehit := false
	size := 0
	DurationML := 0.0
	DurationMAC := 0.0
	DurationLCT := 0.0
	var tp string
	DurationSCT := 0.0
	DurationNTT := 0.0

	if reqtype == "LAYER" || reqtype == "SLICE" {
		constructtype = "OnMiss"
	} else {
		constructtype = reqtype
	}

	if reqtype == "LAYER" || reqtype == "PRECONSTRUCTLAYER" {
		start := time.Now()
		bss, ok := reg.blobcache.GetLayer(dgst.String())
		if ok {
			cachehit = true
			if reqtype == "LAYER" {
				context.GetLogger(ctx).Debug("NANNAN: layer cache hit")
				bytesreader = bytes.NewReader(bss)
				DurationMAC = time.Since(start).Seconds()
			} else {
				bytesreader = bytes.NewReader([]byte("gotta!"))
			}
			goto out
		} else {
			desc, err := bs.metdataService.StatLayerRecipe(ctx, dgst)
			DurationML := time.Since(start).Seconds()

			if err != nil || (err == nil && len(desc.HostServerIps) == 0) {
				context.GetLogger(ctx).Warnf("NANNAN: COULDN'T FIND LAYER RECIPE: %v or Empty layer \n", err)
				goto Sendasmanifest
			}

			bss, DurationLCT, tp := ConstructLayer(ctx, desc, dgst, reg, constructtype)
			if reqtype == "LAYER" {
				bytesreader := bytes.NewReader(bss)
				size = cprssrder.Size()
			} else {
				bytesreader = bytes.NewReader([]byte("gotta!"))
			}
			goto out
		}
	}

	if reqtype == "SLICE" || reqtype == "PRECONSTRUCTSLICE" {
		start := time.Now()
		bss, ok := reg.blobcache.GetSlice(dgst.String())
		if ok {
			cachehit = true
			if reqtype == "SLICE" {
				context.GetLogger(ctx).Debug("NANNAN: slice cache hit")
				bytesreader = bytes.NewReader(bss)
				DurationMAC = time.Since(start).Seconds()
			} else {
				bytesreader = bytes.NewReader([]byte("gotta!"))
			}
			goto out
		} else {
			start := time.Now()
			desc, err := bs.metdataService.StatSliceRecipe(ctx, dgst)
			DurationML = time.Since(start).Seconds()

			if err != nil || (err == nil && len(desc.Files) == 0) {
				context.GetLogger(ctx).Warnf("NANNAN: COULDN'T FIND SLICE RECIPE: %v or Empty slice \n", err)
				goto Sendasmanifest
			}

			bss, DurationSCT, tp := ConstructSlice(ctx, desc, dgst, reg, constructtype)
			if reqtype == "SLICE" {
				bytesreader = bytes.NewReader(bss)
				size = cprssrder.Size()
			} else {
				bytesreader = bytes.NewReader([]byte("gotta!"))
			}
			goto out
		}
	}

	if reqtype != "SLICE" && reqtype != "PRECONSTRUCTSLICE" && reqtype != "LAYER" && reqtype != "PRECONSTRUCTLAYER" && reqtype != "MANIFEST" {
		context.GetLogger(ctx).Errorf("NANNAN: ServeBlob: No type found\n")
		return err
	}

Sendasmanifest:
	if reqtype == "LAYER" || reqtype == "SLICE" {
		DurationNTT, err := bs.serveManifest(ctx, _desc, w, r)
		if err != nil {
			return err
		}
		if reqtype == "LAYER" {
			context.GetLogger(ctx).Debugf("NANNAN: No layer recipe found: metadata lookup time: %v, layer transfer time: %v, layer compressed size: %v",
				DurationML, DurationNTT, _desc.Size)
		} else {
			context.GetLogger(ctx).Debugf("NANNAN: No slice recipe found: metadata lookup time: %v, slice transfer time: %v, slice compressed size: %v",
				DurationML, DurationNTT, _desc.Size)
		}
		return nil
	} else {
		bs.TransferBlob(ctx, w, r, _desc, bytes.NewReader([]byte("gotta!")))
		return nil
	}

out:
	DurationNTT, size, err := bs.TransferBlob(ctx, w, r, _desc, bytesreader)
	if err != nil {
		return err
	}

	//update ulmap
	if reqtype == "LAYER" {
		ulmapentry, err := bw.blobStore.registry.metdataService.StatULMapEntry(ctx, usrname)
		if err == nil {
			// exsist
			if val, ok := ulmapentry.Dgstmap[desc.Digest]; ok {
				//exsist
				ulmapentry.Dgstmap[desc.Digest] += 1
			} else {
				//not exsist
				ulmapentry.Dgstmap[desc.Digest] = 1
			}
		} else {
			//not exisit
			dgstmap := make(map[digest.Digest]int)
			dgstmap[desc.Digest] = 1
			ulmapentry = distribution.ULmapEntry{
				Dgstmap: dgstmap,
			}
		}
		err1 := bw.blobStore.registry.metdataService.SetULMapEntry(ctx, usrname, ulmapentry)
		if err1 != nil {
			return err1
		}

		//update rlmap
		rlmapentry, err := bw.blobStore.registry.metdataService.StatRLMapEntry(ctx, reponame)
		if err == nil {
			// exsist
			if val, ok := rlmapentry.Dgstmap[desc.Digest]; ok {
				//exsist
				rlmapentry.Dgstmap[desc.Digest] += 1
			} else {
				rlmapentry.Dgstmap[desc.Digest] = 1
			}
		} else {
			//not exisit
			dgstmap := make(map[digest.Digest]int)
			dgstmap[desc.Digest] = 1
			rlmapentry = distribution.RLmapEntry{
				Dgstmap: dgstmap,
			}
		}
		err1 := bw.blobStore.registry.metdataService.SetRLMapEntry(ctx, reponame, rlmapentry)
		if err1 != nil {
			return err1
		}
	}

	if cachehit {
		if reqtype == "LAYER" {
			context.GetLogger(ctx).Debugf("NANNAN: layer cache hit: metadata lookup time: %v, layer cache access time: %v, layer transfer time: %v, layer compressed size: %v",
				DurationML, DurationMAC, DurationNTT, size)
		} else if reqtype == "SLICE" {
			context.GetLogger(ctx).Debugf("NANNAN: slice cache hit: metadata lookup time: %v, slice cache access time: %v, slice transfer time: %v, slice compressed size: %v",
				DurationML, DurationMAC, DurationNTT, size)
		}
		return nil
	} else {
		if reqtype == "LAYER" || reqtype == "PRECONSTRUCTLAYER" {
			if reqtype == "LAYER" {
				context.GetLogger(ctx).Debug("NANNAN: layer cache miss")
			}
			context.GetLogger(ctx).Debugf(`NANNAN: layer construct: %s: metadata lookup time: %v, layer transfer and merge time: %v, 
																layer transfer time: %v, layer compressed size: %v, layer uncompressed size: %v`,
				tp, DurationML, DurationLCT, DurationNTT, size, desc.UncompressionSize)
			reg.blobcache.SetLayer(dgst.String(), bss, constructtype)
		} else if reqtype == "SLICE" || reqtype == "PRECONSTRUCTSLICE" {
			if reqtype == "SLICE" {
				context.GetLogger(ctx).Debug("NANNAN: slice cache miss")
			}
			context.GetLogger(ctx).Debugf(`NANNAN: slice construct: %s: metadata lookup time: %v, slice construct time: %v, \
													layer transfer time: %v, layer compressed size: %v, layer uncompressed size: %v`,
				tp, DurationML, DurationSCT, DurationNTT, size, desc.UncompressionSize)
			reg.blobcache.SetSlice(dgst.String(), bss, constructtype)
		}
		return nil
	}

	return nil
}
