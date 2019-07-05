package storage

import (
	//	"bytes"

	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	//	log "github.com/Sirupsen/logrus"
	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/cache"
	"github.com/klauspost/pgzip"
	//NANNAN
	"io"
	"os"
	"path"
	"regexp"
	//"runtime"
	"sync"

	storagecache "github.com/docker/distribution/registry/storage/cache"
	//	"github.com/docker/docker/pkg/archive"
	//"github.com/serialx/hashring"
	//	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"math/rand"
	//"strconv"
	"time"
	//roundrobin "github.com/hlts2/round-robin"
	"github.com/panjf2000/ants"
	//gzip "github.com/klauspost/pgzip"
	digest "github.com/opencontainers/go-digest"
)

// TODO(stevvooe): This should configurable in the future.
const blobCacheControlMaxAge = 365 * 24 * time.Hour

// blobServer simply serves blobs from a driver instance using a path function
// to identify paths and a descriptor service to fill in metadata.
type blobServer struct {
	driver  driver.StorageDriver
	statter distribution.BlobStatter

	//NANNAN: add a fileDescriptorCacheProvider for restore
	servers []*url.URL
	//	ring                        roundrobin.RoundRobin
	fileDescriptorCacheProvider storagecache.FileDescriptorCacheProvider
	serverIp                    string
	pathFn                      func(dgst digest.Digest) (string, error)
	redirect                    bool // allows disabling URLFor redirects
	cache                       *cache.MemCache
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

// add an id to avoid two threads conflict. make it thread safe.

func getGID() float64 {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return r1.Float64()
}

type TarFile struct {
	Lm sync.Mutex
	Tw *tar.Writer
}

type Task struct {
	Ctx  context.Context
	Src  string
	Desc string
	Bs   *blobServer
	Tf   *TarFile
	//	Reschan    chan float64
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

func packFile(i interface{}) {

	task, ok := i.(*Task)
	if !ok {
		fmt.Println(ok)
		return
	}
	ctx := task.Ctx
	newsrc := task.Src
	desc := task.Desc
	bs := task.Bs
	tf := task.Tf

	var contents *[]byte

	start := time.Now()
	//check if newsrc is in file cache
	bfss, err := bs.cache.Mc.Get(newsrc)
	if err == nil {
		fmt.Println("NANNAN: file cache hit\n")
		contents = &bfss
	} else {
		context.GetLogger(ctx).Errorf("NANNAN: mvfile: file cache error: %v: %s", err, newsrc)
		fmt.Printf("NANNAN: file cache miss\n")

		//check src file exists or not
		var _, err = os.Stat(newsrc)
		if os.IsNotExist(err) {
			context.GetLogger(ctx).Errorf("NANNAN: src file %v: %v", newsrc, err)
			return
		}

		bfss, err := ioutil.ReadFile(newsrc)
		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: read file %s generated error: %v", desc, err)
			return
		} else {
			contents = &bfss
			//put in cache
			context.GetLogger(ctx).Debugf("NANNAN: file cache put: %v B for %s", len(bfss), newsrc)
			if len(bfss) > 0 {
				err = bs.cache.Mc.Set(newsrc, bfss)
				if err != nil {
					context.GetLogger(ctx).Debugf("NANNAN: file cache cannot write to digest: %v: %v ", newsrc, err)
				}
			}
		}
	}

	size, err := addToTarFile(tf, desc, *contents)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: desc file %s generated error: %v", desc, err)
		return
	}

	DurationFCP := time.Since(start).Seconds()
	context.GetLogger(ctx).Debugf("NANNAN: wrote %d bytes to file %s duration: %v", size, desc, DurationFCP)
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

func (bs *blobServer) serveBlobCache(ctx context.Context, _desc distribution.Descriptor, w http.ResponseWriter, r *http.Request, bss []byte) (float64, int64, error) {
	bytesreader := bytes.NewReader(bss)
	//defer bytesreader.Close()
	path, err := bs.pathFn(_desc.Digest)
	if err != nil {
		return 0.0, 0, err
	}

	if bs.redirect {
		redirectURL, err := bs.driver.URLFor(ctx, path, map[string]interface{}{"method": r.Method})
		switch err.(type) {
		case nil:
			// Redirect to storage URL.
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return 0.0, 0, err

		case driver.ErrUnsupportedMethod:
			// Fallback to serving the content directly.
		default:
			// Some unexpected error.
			return 0.0, 0, err
		}
	}

	size := bytesreader.Size()

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
	http.ServeContent(w, r, _desc.Digest.String(), time.Time{}, bytesreader)
	DurationNTT := time.Since(start).Seconds()

	return DurationNTT, size, nil
}

func (bs *blobServer) packAllFiles(ctx context.Context, desc distribution.BSRecipeDescriptor, bufp *bytes.Buffer) (float64, error) {

	fcntno := 0.0
	fcnt := 0
	for _, bfdescriptor := range desc.BFs {
		if bfdescriptor.ServerIp != bs.serverIp {
			context.GetLogger(ctx).Debugf("NANNAN: this is not a locally available file, %v", bfdescriptor.ServerIp) // not locally available
			continue
		}
		fcnt += 1
	}

	if fcnt > 1000 {
		fcnt = 1000
	}

	var wg sync.WaitGroup
	antp, _ := ants.NewPoolWithFunc(fcnt, func(i interface{}) {
		packFile(i)
		wg.Done()
	})
	defer antp.Release()

	reg, err := regexp.Compile("[^a-zA-Z0-9/.-]+")
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s, ", err)
		return 0.0, err
	}

	tw := tar.NewWriter(bufp)

	tf := &TarFile{
		Tw: tw,
	}

	start := time.Now()
	for _, bfdescriptor := range desc.BFs {

		if bfdescriptor.ServerIp != bs.serverIp {
			context.GetLogger(ctx).Debugf("NANNAN: this is not a locally available file, %v", bfdescriptor.ServerIp) // not locally available
			continue
		}
		//in case there are same files inside the layer dir
		fcntno = fcntno + 1.0
		random_dir := fmt.Sprintf("%f", fcntno)

		tarfpath := reg.ReplaceAllString(strings.SplitN(bfdescriptor.BlobFilePath, "diff", 2)[1], "") // replace alphanumeric
		destfpath := path.Join(tarfpath + "-" + random_dir)
		//context.GetLogger(ctx).Debugf("NANNAN: dest path: %v", destfpath) // not locally available
		wg.Add(1)
		antp.Invoke(&Task{
			Ctx:  ctx,
			Src:  bfdescriptor.BlobFilePath, //strings.TrimPrefix(bfdescriptor.BlobFilePath, "/var/lib/registry"),
			Desc: destfpath,
			Bs:   bs,
			Tf:   tf,
		})
	}
	wg.Wait()

	if err := tw.Close(); err != nil {
		context.GetLogger(ctx).Debugf("NANNAN: cannot close tar file for %v", desc.BlobDigest.String())
		return 0.0, err
	}
	DurationCP := time.Since(start).Seconds()
	return DurationCP, nil
}

func pgzipTarFile(bufp *bytes.Buffer, compressbufp *bytes.Buffer, compr_level int) (*bytes.Reader, error) {

	w, _ := pgzip.NewWriterLevel(compressbufp, compr_level)
	io.Copy(w, bufp)
	w.Close()
	cprssrder := bytes.NewReader(compressbufp.Bytes())

	return cprssrder, nil
}

func (bs *blobServer) compressAndServe(ctx context.Context, w http.ResponseWriter, r *http.Request, _desc distribution.Descriptor, 
	bufp *bytes.Buffer, compressbufp *bytes.Buffer, compr_level int) (float64, float64, int64, error) {

	start := time.Now()

	cprssrder, err := pgzipTarFile(bufp, compressbufp, compr_level)
	if err != nil {
		return 0.0, 0.0, 0, err
	}
	size := cprssrder.Size()

	DurationCMP := time.Since(start).Seconds()

	path_old, err := bs.pathFn(_desc.Digest)
	if err != nil {
		return 0.0, 0.0, 0, err
	}

	if bs.redirect {
		redirectURL, err := bs.driver.URLFor(ctx, path_old, map[string]interface{}{"method": r.Method})
		switch err.(type) {
		case nil:
			// Redirect to storage URL.
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return 0.0, 0.0, 0, err

		case driver.ErrUnsupportedMethod:
			// Fallback to serving the content directly.
		default:
			// Some unexpected error.
			return 0.0, 0.0, 0, err
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

	return DurationCMP, DurationNTT, size, nil
}

//NANNAN: TODO: process manfiests
/*
check if it's manifest.
if it is, put all files in big cache.
if its layer, restore layer from disk+cache
delete and send to disk cache

	NoCompression       = flate.NoCompression
	BestSpeed           = flate.BestSpeed
	BestCompression     = flate.BestCompression
	DefaultCompression  = flate.DefaultCompression
	ConstantCompression = flate.ConstantCompression
	HuffmanOnly         = flate.HuffmanOnly

*/

func (bs *blobServer) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {

	compre_level := 4

	start := time.Now()
	_desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return err
	}
	//check if this is manifest
	
	//if it is, serve manifest and get repolayer map - usrmap, then do prefetching put in cache or other area
	// else serve layer.
	// make a global list for cache + disk cache
	// make a global list for now-restoring layer list

	desc, err := bs.fileDescriptorCacheProvider.StatBSRecipe(ctx, dgst)
	DurationML := time.Since(start).Seconds()

	if err != nil || (err == nil && len(desc.BFs) == 0) {
		context.GetLogger(ctx).Warnf("NANNAN: THIS IS A MANIFEST OR COMPRESSED TAR %v", err)
		DurationNTT, err := bs.serveManifest(ctx, _desc, w, r)
		if err != nil {
			return err
		}
		context.GetLogger(ctx).Debugf("NANNAN: manifest: metadata lookup time: %v, layer transfer time: %v, layer compressed size: %v",
			DurationML, DurationNTT, _desc.Size)
		return nil
	}

	//check if its in disk cache
	bss, err := bs.cache.Dc.Get(dgst.String())
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: serveblob: disk cache error: %v: %s", err, dgst.String())
	} else {
		//bytesreader := bytes.NewReader(bss)
		if bss != nil {
			context.GetLogger(ctx).Debug("NANNAN: slice cache hit")
			DurationNTT, size, err := bs.serveBlobCache(ctx, _desc, w, r, bss)
			if err != nil {
				return err
			}
			context.GetLogger(ctx).Debugf("NANNAN: slice cache hit: metadata lookup time: %v, layer transfer time: %v, layer compressed size: %v",
				DurationML, DurationNTT, size)
			return nil
		}
	}
	//otherwise restore slice
	context.GetLogger(ctx).Debug("NANNAN: slice cache miss")

	//WRITE ERROR CHANNEL AND CATCH ERRORS
	var buf bytes.Buffer
	var comprssbuf bytes.Buffer

	DurationCP, _ := bs.packAllFiles(ctx, desc, &buf)
	DurationCMP, DurationNTT, size, err := bs.compressAndServe(ctx, w, r, _desc, &buf, &comprssbuf, compre_level)
	if err != nil {
		return err
	}
	buf.Reset()

	//bfss, err := ioutil.ReadAll(comprssbuf.Bytes())
	bfss := comprssbuf.Bytes()
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s ", err)
	}
	context.GetLogger(ctx).Debugf("NANNAN: slice cache put: %v B for %s", len(bfss), dgst.String())

	if len(bfss) > 0 {
		err = bs.cache.Dc.Set(dgst.String(), bfss)
		if err != nil {
			context.GetLogger(ctx).Debugf("NANNAN: slice cache cannot write to digest: %v: %v ", dgst.String(), err)
		}
	}
	comprssbuf.Reset()
	
	context.GetLogger(ctx).Debugf("NANNAN: slice cache miss: metadata lookup time: %v, slice cp time: %v, slice compression time: %v, slice transfer time: %v, slice compressed size: %v, slice uncompressed size: %v",
		DurationML, DurationCP, DurationCMP, DurationNTT, size, desc.SliceSize)
	
	return nil
}
