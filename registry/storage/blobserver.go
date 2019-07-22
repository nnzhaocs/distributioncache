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

	//"bytes"
	"io"
	"os"
	"path"
	"regexp"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set"
	digest "github.com/opencontainers/go-digest"
	"github.com/panjf2000/ants"
)

// TODO(stevvooe): This should configurable in the future.
const blobCacheControlMaxAge = 365 * 24 * time.Hour

// blobServer simply serves blobs from a driver instance using a path function
// to identify paths and a descriptor service to fill in metadata.
type blobServer struct {
	driver  driver.StorageDriver
	statter distribution.BlobStatter
	reg     *registry
	//ring                        	roundrobin.RoundRobin
	//metadataService storagecache.DedupMetadataServiceCacheProvider //NANNAN: add a metadataService for restore
	pathFn   func(dgst digest.Digest) (string, error)
	redirect bool // allows disabling URLFor redirects
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
	Src  string
	Desc string
	Reg  *registry
	Tf   *TarFile
	//Ctype string
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
	//ctype := task.Ctype

	var contents *[]byte

	start := time.Now()
	//check if newsrc is in file cache
	bfss, ok := reg.blobcache.GetFile(newsrc)
	if ok {
		fmt.Printf("NANNAN: file cache hit\n")
		contents = &bfss
	} else {
		fmt.Printf("NANNAN: mvfile: file cache error: %v: %s", ok, newsrc)
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
				ok = reg.blobcache.SetFile(newsrc, bfss)
				if ok {
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
		if sfdescriptor.HostServerIp != bs.reg.hostserverIp {
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

	//	if constructtype == "PRECONSTRUCTSLICE" {
	//		constructtype == "PREFETCHFILE"
	//	}

	start := time.Now()
	for _, sfdescriptor := range desc.Files {

		if sfdescriptor.HostServerIp != bs.reg.hostserverIp {
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
			Src:  sfdescriptor.FilePath, //strings.TrimPrefix(bfdescriptor.BlobFilePath, "/var/lib/registry"),
			Desc: destfpath,
			Reg:  reg,
			Tf:   tf,
			//Ctype: constructtype,
		})
	}
	wg.Wait()

	if err := tw.Close(); err != nil {
		context.GetLogger(ctx).Debugf("NANNAN: cannot close tar file for %v", desc.Digest.String())
		return 0.0
	}
	DurationCP := time.Since(start).Seconds()
	return DurationCP
}

func (bs *blobServer) TransferBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, _desc distribution.Descriptor,
	cprssrder *bytes.Reader) (float64, error) {

	path_old, err := bs.pathFn(_desc.Digest)
	size := cprssrder.Size()

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

	start := time.Now()
	http.ServeContent(w, r, _desc.Digest.String(), time.Time{}, cprssrder) //packFile)
	DurationNTT := time.Since(start).Seconds()

	return DurationNTT, nil

}

type Restoringbuffer struct {
	sync.Mutex
	cnd  *sync.Cond
	bufp *bytes.Buffer
}

/*
//TYPE XXX USRADDR XXX REPONAME XXX
MANIFEST
LAYER
SLICE
PRECONSTRUCTLAYER
*/

func (bs *blobServer) notifyPeerPreconstructLayer(ctx context.Context, dgst digest.Digest, wg *sync.WaitGroup) bool {

	defer wg.Done()
	tp := "TYPEPRECONSTRUCTLAYER"

	dgststring := dgst.String()
	var regipbuffer bytes.Buffer
	reponame := context.GetRepoName(ctx)
	usrname := context.GetUsrAddr(ctx)

	desc, err := bs.reg.metadataService.StatLayerRecipe(ctx, dgst)
	if err != nil {
		context.GetLogger(ctx).Warnf("NANNAN: COULDN'T FIND LAYER RECIPE: %v or Empty layer for dgst", err, dgst)
		return false
	}
	regip := desc.MasterIp

	regipbuffer.WriteString(regip)
	regipbuffer.WriteString(":5000")
	regip = regipbuffer.String()
	context.GetLogger(ctx).Debugf("NANNAN: notifyPeerPreconstructLayer for %s, dgst: %s", regip, dgststring)

	//GET /v2/<name>/blobs/<digest>
	var urlbuffer bytes.Buffer
	urlbuffer.WriteString("http://")
	urlbuffer.WriteString(regip)
	urlbuffer.WriteString("/v2/")
	urlbuffer.WriteString(tp + "USRADDR" + usrname + "REPONAME" + reponame)
	urlbuffer.WriteString("/blobs/")

	urlbuffer.WriteString(dgststring)
	url := urlbuffer.String()
	url = trings.ToLower(url)
	context.GetLogger(ctx).Debugf("NANNAN: notifyPeerPreconstructLayer URL %s", url)

	//let's skip head request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: notifyPeerPreconstructLayer GET URL %s, err %s", url, err)
		return false
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: notifyPeerPreconstructLayer Do GET URL %s, err %s", url, err)
		return false
	}
	context.GetLogger(ctx).Debugf("NANNAN: %s returned status code %d", regip, resp.StatusCode)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {	
		return false //errors.New("notifyPeerPreconstructLayer to other servers, failed")
	}
	return true
}

/*
//TYPE XXX USRADDR XXX REPONAME XXX
MANIFEST
LAYER
SLICE*/

func (bs *blobServer) GetSliceFromRegistry(ctx context.Context, dgst digest.Digest, regip string, pw *PgzipFile, wg *sync.WaitGroup, constructtype string) error {

	defer wg.Done()
	dgststring := dgst.String()
	
//	if regip != bs.reg.hostserverIp{
		
		var regipbuffer bytes.Buffer
		reponame := context.GetRepoName(ctx)
		usrname := context.GetUsrAddr(ctx)
	
		regipbuffer.WriteString(regip)
		regipbuffer.WriteString(":5000")
		regip = regipbuffer.String()
		context.GetLogger(ctx).Debugf("NANNAN: GetSliceFromRegistry from %s, dgst: %s ", regip, dgststring)
	
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
		context.GetLogger(ctx).Debugf("NANNAN: GetSliceFromRegistry URL %s ", url)
	
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
			context.GetLogger(ctx).Errorf("%s returned status code %d", regip, resp.StatusCode)
			return errors.New("get slices from other servers, failed")
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: cannot read from resp.body: %s", err)
			return err
		}
	
		buf := bytes.NewBuffer(body)
		err = pgzipconcatTarFile(buf, pw)
		return err
//	}else{
//		start := time.Now()
//		desc, err := bs.reg.metadataService.StatSliceRecipe(ctx, dgst)
//		DurationML = time.Since(start).Seconds()
//
//		if err != nil || (err == nil && len(desc.Files) == 0) {
//			context.GetLogger(ctx).Warnf("NANNAN: COULDN'T FIND SLICE RECIPE: %v or Empty slice for dgst %v", err, dgst)
//			return err
//		}
//		bss, DurationSCT, tp = bs.constructSlice(ctx, desc, dgst, bs.reg, constructtype)
//		buf := bytes.NewBuffer(bss)
//		err = pgzipconcatTarFile(buf, pw)
//		return err
//	}
}

func (bs *blobServer) constructSlice(ctx context.Context, desc distribution.SliceRecipeDescriptor, dgst digest.Digest, reg *registry, constructtype string) ([]byte, float64, string) {

	var buf bytes.Buffer
	var comprssbuf bytes.Buffer

	rbuf := &Restoringbuffer{
		bufp: &comprssbuf,
	}
	rbuf.cnd = sync.NewCond(rbuf)

	start := time.Now()
	rsbufval, ok := bs.reg.restoringslicermap.LoadOrStore(dgst.String(), rbuf)
	if ok {
		// load true
		if rsbuf, ok := rsbufval.(*Restoringbuffer); ok {
			rsbuf.Lock()
			rsbuf.cnd.Wait()
			context.GetLogger(ctx).Debugf("NANNAN: slice construct finish waiting for digest: %v", dgst.String())
			rsbuf.Unlock()
			DurationWSCT := time.Since(start).Seconds()

			tp := "WAITSLICECONSTRUCT"
			bss := rsbuf.bufp.Bytes()
			return bss, DurationWSCT, tp
		} else {
			context.GetLogger(ctx).Debugf("NANNAN: bs.reg.restoringslicermap.LoadOrStore wrong digest: %v", dgst.String())
		}
	} else {
		rbuf.Lock()
		start := time.Now()
		_ = bs.packAllFiles(ctx, desc, &buf, reg, constructtype)
		//DurationCP
		//start = time.Now()
		bss := pgzipTarFile(&buf, &comprssbuf, bs.reg.compr_level)
		//DurationCMP := time.Since(start).Seconds()

		rbuf.cnd.Broadcast()
		rbuf.Unlock()
		DurationSCT := time.Since(start).Seconds()

		//bss = compressbufp.Bytes()
		tp := "SLICECONSTRUCT"
		return bss, DurationSCT, tp
	}
	return nil, 0.0, ""
}

func (bs *blobServer) constructLayer(ctx context.Context, desc distribution.LayerRecipeDescriptor, 
									dgst digest.Digest, constructtype string) ([]byte, float64, string) {

	var wg sync.WaitGroup
	var comprssbuf bytes.Buffer
	pw, _ := pgzip.NewWriterLevel(&comprssbuf, bs.reg.compr_level)
	pf := &PgzipFile{
		Pw: pw,
	}

	rbuf := &Restoringbuffer{
		bufp: &comprssbuf,
	}
	rbuf.cnd = sync.NewCond(rbuf)

	start := time.Now()
	rsbufval, ok := bs.reg.restoringlayermap.LoadOrStore(dgst.String(), rbuf)
	if ok {
		// load true
		if rsbuf, ok := rsbufval.(*Restoringbuffer); ok {
			rsbuf.Lock()
			rsbuf.cnd.Wait()
			context.GetLogger(ctx).Debugf("NANNAN: layer construct finish waiting for digest: %v", dgst.String())
			rsbuf.Unlock()
			DurationWLCT := time.Since(start).Seconds()

			tp := "WAITLAYERCONSTRUCT"
			bss := rsbuf.bufp.Bytes()
			return bss, DurationWLCT, tp
		} else {
			context.GetLogger(ctx).Debugf("NANNAN: bs.reg.restoringslicermap.LoadOrStore wrong digest: %v", dgst.String())
		}
	} else {
		rbuf.Lock()

		start := time.Now()
		for _, hserver := range desc.HostServerIps {
			wg.Add(1)
			go bs.GetSliceFromRegistry(ctx, dgst, hserver, pf, &wg, constructtype)
		}
		wg.Wait()
		DurationLCT := time.Since(start).Seconds()

		rbuf.cnd.Broadcast()
		rbuf.Unlock()

		tp := "LAYERCONSTRUCT"
		bss := comprssbuf.Bytes()

		return bss, DurationLCT, tp
	}
	return nil, 0.0, ""
}

func (bs *blobServer) Preconstructlayers(ctx context.Context, reg *registry) error {
	reponame := context.GetRepoName(ctx)
	usrname := context.GetUsrAddr(ctx)
	context.GetLogger(ctx).Debugf("NANNAN: Preconstructlayers: for repo (%s) and usr (%s)", reponame, usrname)

	rlmapentry, err := bs.reg.metadataService.StatRLMapEntry(ctx, reponame)
	if err != nil {
		context.GetLogger(ctx).Debugf("NANNAN: Preconstructlayers: cannot get rlmapentry for repo (%s)", reponame)
		return err
	}
	fmt.Println("NANNAN: PrecontstructionLayer: rlmapentry => %v", rlmapentry)
	ulmapentry, err := bs.reg.metadataService.StatULMapEntry(ctx, usrname)
	if err != nil {
		context.GetLogger(ctx).Debugf("NANNAN: Preconstructlayers: cannot get ulentry for usr (%s)", usrname)
		return err
	}
	fmt.Println("NANNAN: PrecontstructionLayer: ulmapentry => %v", ulmapentry)
	
	var rlgstlst []interface{}
	for k := range rlmapentry.Dgstmap {
		rlgstlst = append(rlgstlst, k)
	}
	fmt.Println("NANNAN: PrecontstructionLayer: rlmapentry dgstlst")
	rlset := mapset.NewSetFromSlice(rlgstlst)
	
	var ulgstlst []interface{}
	for k := range ulmapentry.Dgstmap {
		ulgstlst = append(ulgstlst, k)
	}
	fmt.Println("NANNAN: PrecontstructionLayer: ulmapentry dgstlst")
	
	ulset := mapset.NewSetFromSlice(ulgstlst)

	diffset := rlset.Difference(ulset)
	sameset := rlset.Intersect(ulset)
	
	fmt.Println("NANNAN: PrecontstructionLayer: diffset dgstlst: ", diffset)
	fmt.Println("NANNAN: PrecontstructionLayer: sameset dgstlst: ", sameset)
	
	var repulldgsts []interface{}
	it := sameset.Iterator()
	for elem := range it.C {
		id := elem.(digest.Digest)
		if ulmapentry.Dgstmap[id] > bs.reg.repullcntthres {
			repulldgsts = append(repulldgsts, id)
		}
	}

	repullset := mapset.NewSetFromSlice(repulldgsts)

	descdgstset := diffset.Union(repullset)
	context.GetLogger(ctx).Debugf("NANNAN: descdgstlst: %v ", descdgstset)
	
	if len(descdgstset.ToSlice()) == 0 {
		return nil
	}
	
	var wg sync.WaitGroup
	it = descdgstset.Iterator()
	for elem := range it.C {
		wg.Add(1)
		id := elem.(digest.Digest)
		go bs.notifyPeerPreconstructLayer(ctx, id, &wg)
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

func (bs *blobServer) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {

	//compre_level := bs.reg.compr_level

	start := time.Now()
	_desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return err
	}
	DurationML := time.Since(start).Seconds()

	reqtype := context.GetType(ctx)
	context.GetLogger(ctx).Debugf("NANNAN: ServeBlob: type: %s", reqtype)

	reponame := context.GetRepoName(ctx)
	usrname := context.GetUsrAddr(ctx)
	context.GetLogger(ctx).Debugf("NANNAN: Preconstructlayers: for repo (%s) and usr (%s) for dgst (%s)", reponame, usrname, dgst.String())

	if reqtype == "MANIFEST" {
		// Userlru lst change
		DurationML := time.Since(start).Seconds()
		context.GetLogger(ctx).Debugf("NANNAN: THIS IS A MANIFEST REQUEST, serve and preconstruct layers")

		go bs.Preconstructlayers(ctx, bs.reg) // prefetch window

		DurationNTT, err := bs.serveManifest(ctx, _desc, w, r)
		if err != nil {
			return err
		}
		context.GetLogger(ctx).Debugf("NANNAN: manifest: metadata lookup time: %v, manifest transfer time: %v, manifest compressed size: %v",
			DurationML, DurationNTT, _desc.Size)
		return nil
	}

	var constructtype string
	var bytesreader *bytes.Reader
	cachehit := false
	var size int64 = 0
	var Uncompressedsize int64 = 0
	DurationML = 0.0
	DurationMAC := 0.0
	DurationLCT := 0.0
	var tp string
	DurationSCT := 0.0
	DurationNTT := 0.0
	var bss []byte

	if reqtype == "LAYER" || reqtype == "SLICE" {
		constructtype = "OnMiss"
	} else {
		constructtype = reqtype
	}

	if reqtype == "LAYER" || reqtype == "PRECONSTRUCTLAYER" {
		start := time.Now()
		bss, ok := bs.reg.blobcache.GetLayer(dgst.String())
		if ok {
			cachehit = true
			if reqtype == "LAYER" {
				context.GetLogger(ctx).Debug("NANNAN: layer cache hit")
				bytesreader = bytes.NewReader(bss)
				DurationMAC = time.Since(start).Seconds()
				size = bytesreader.Size()
			} else {
				bytesreader = bytes.NewReader([]byte("gotta!"))
			}
			goto out
		} else {
			desc, err := bs.reg.metadataService.StatLayerRecipe(ctx, dgst)
			Uncompressedsize = desc.UncompressionSize
			DurationML = time.Since(start).Seconds()

			if err != nil || (err == nil && len(desc.HostServerIps) == 0) {
				context.GetLogger(ctx).Warnf("NANNAN: COULDN'T FIND LAYER RECIPE: %v or Empty layer ", err)
				goto Sendasmanifest
			}

			bss, DurationLCT, tp = bs.constructLayer(ctx, desc, dgst, constructtype)
			if reqtype == "LAYER" {
				bytesreader := bytes.NewReader(bss)
				size = bytesreader.Size()
			} else {
				bytesreader = bytes.NewReader([]byte("gotta!"))
			}
			goto out
		}
	}

	if reqtype == "SLICE" || reqtype == "PRECONSTRUCTSLICE" {
		start := time.Now()
		bss, ok := bs.reg.blobcache.GetSlice(dgst.String())
		if ok {
			cachehit = true
			if reqtype == "SLICE" {
				context.GetLogger(ctx).Debug("NANNAN: slice cache hit")
				bytesreader = bytes.NewReader(bss)
				DurationMAC = time.Since(start).Seconds()
				size = bytesreader.Size()
			} else {
				bytesreader = bytes.NewReader([]byte("gotta!"))
			}
			goto out
		} else {
			start := time.Now()
			desc, err := bs.reg.metadataService.StatSliceRecipe(ctx, dgst)
			DurationML = time.Since(start).Seconds()

			if err != nil || (err == nil && len(desc.Files) == 0) {
				context.GetLogger(ctx).Warnf("NANNAN: COULDN'T FIND SLICE RECIPE: %v or Empty slice ", err)
				goto Sendasmanifest
			}

			bss, DurationSCT, tp = bs.constructSlice(ctx, desc, dgst, bs.reg, constructtype)
			if reqtype == "SLICE" {
				bytesreader = bytes.NewReader(bss)
				size = bytesreader.Size()
			} else {
				bytesreader = bytes.NewReader([]byte("gotta!"))
			}
			goto out
		}
	}

	if reqtype != "SLICE" && reqtype != "PRECONSTRUCTSLICE" && reqtype != "LAYER" && reqtype != "PRECONSTRUCTLAYER" && reqtype != "MANIFEST" {
		context.GetLogger(ctx).Errorf("NANNAN: ServeBlob: No type found")
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
	DurationNTT, err = bs.TransferBlob(ctx, w, r, _desc, bytesreader)
	if err != nil {
		return err
	}

	//update ulmap
	if reqtype == "LAYER" {
		ulmapentry, err := bs.reg.metadataService.StatULMapEntry(ctx, usrname)
		if err == nil {
			// exsist
			if _, ok := ulmapentry.Dgstmap[dgst]; ok {
				//exsist
				ulmapentry.Dgstmap[dgst] += 1
			} else {
				//not exsist
				ulmapentry.Dgstmap[dgst] = 1
			}
		} else {
			//not exisit
			dgstmap := make(map[digest.Digest]int64)
			dgstmap[dgst] = 1
			ulmapentry = distribution.ULmapEntry{
				Dgstmap: dgstmap,
			}
		}
		err1 := bs.reg.metadataService.SetULMapEntry(ctx, usrname, ulmapentry)
		if err1 != nil {
			return err1
		}

		//update rlmap
		rlmapentry, err := bs.reg.metadataService.StatRLMapEntry(ctx, reponame)
		if err == nil {
			// exsist
			if _, ok := rlmapentry.Dgstmap[dgst]; ok {
				//exsist
				rlmapentry.Dgstmap[dgst] += 1
			} else {
				rlmapentry.Dgstmap[dgst] = 1
			}
		} else {
			//not exisit
			dgstmap := make(map[digest.Digest]int64)
			dgstmap[dgst] = 1
			rlmapentry = distribution.RLmapEntry{
				Dgstmap: dgstmap,
			}
		}
		err1 = bs.reg.metadataService.SetRLMapEntry(ctx, reponame, rlmapentry)
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
				tp, DurationML, DurationLCT, DurationNTT, size, Uncompressedsize)
			bs.reg.blobcache.SetLayer(dgst.String(), bss) //, constructtype)
		} else if reqtype == "SLICE" || reqtype == "PRECONSTRUCTSLICE" {
			if reqtype == "SLICE" {
				context.GetLogger(ctx).Debug("NANNAN: slice cache miss")
			}
			context.GetLogger(ctx).Debugf(`NANNAN: slice construct: %s: metadata lookup time: %v, slice construct time: %v, \
													layer transfer time: %v, layer compressed size: %v, layer uncompressed size: %v`,
				tp, DurationML, DurationSCT, DurationNTT, size, Uncompressedsize)
			bs.reg.blobcache.SetSlice(dgst.String(), bss) //, constructtype)
		}
		return nil
	}

	return nil
}
