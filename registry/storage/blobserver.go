package storage

import (
	//	"bytes"
	"crypto/sha256"
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
	"github.com/docker/docker/pkg/archive"
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

type Task struct {
	Ctx  context.Context
	Src  string
	Desc string
	Bs   *blobServer
}

func mvFile(i interface{}) {
	task, ok := i.(*Task)
	if !ok {
		fmt.Println(ok)
		return
	}
	ctx := task.Ctx
	src := task.Src
	desc := task.Desc
	bs := task.Bs
	//var contents *[]byte
	//	v, err := bs.cache.Mc.Get(src)
	//	if err != nil {
	//		context.GetLogger(ctx).Errorf("NANNAN: bs.cache error %s, ", err)
	//	}
	//	if v != nil { //read hit
	//		fmt.Println("NANNAN: file cache hit\n")
	//		contents = &v
	//	} else {
	fmt.Printf("NANNAN: file cache miss\n")
	data, err := bs.driver.GetContent(ctx, src)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: STILL SEND TAR %s, ", err)
		//		} else {
		//			//put in cache
		//			bs.cache.Mc.Set(src, data)
		//		}
		//contents = &data
	}
	err = bs.driver.PutContent(ctx, desc, data)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: STILL SEND TAR %s, ", err)
	}
	return
}

//NANNAN: TODO: process manfiests
/*
check if it's manifest.
if it is, put all files in big cache.
if its layer, restore layer from disk+cache
delete and send to disk cache
*/

func (bs *blobServer) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {

	start := time.Now()
	_desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return err
	}

	desc, err := bs.fileDescriptorCacheProvider.StatBFRecipe(ctx, dgst)
	DurationML := time.Since(start).Seconds()
	//	fmt.Println("NANNAN: metadata lookup time: %.3f, %v\n", DurationML, dgst)

	if err != nil {
		// get from traditional registry, this is a manifest
		context.GetLogger(ctx).Warnf("NANNAN: THIS IS A MANIFEST OR COMPRESSED TAR %s", err)

		path, err := bs.pathFn(_desc.Digest)
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

		br, err := newFileReader(ctx, bs.driver, path, _desc.Size) //stat.Size())
		if err != nil {
			return err
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

		start = time.Now()
		http.ServeContent(w, r, _desc.Digest.String(), time.Time{}, br)
		DurationNTT := time.Since(start).Seconds()
		context.GetLogger(ctx).Debugf("NANNAN: manifest: metadata lookup time: %v, layer transfer time: %v, layer size: %v",
			DurationML, DurationNTT, _desc.Size)

		return nil
	}

	//check disk cache
	bytesreader, err := bs.cache.Dc.Get(dgst.String())
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: serveblob: disk cache error: %v", err)
	}

	if bytesreader != nil {
		context.GetLogger(ctx).Debug("NANNAN: slice cache hit")
		path, err := bs.pathFn(_desc.Digest)
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

		storageDir := "/var/lib/registry/docker/registry/v2/pull_tars/diskcache"
		layerslicepath := storageDir + string(os.PathSeparator) + fmt.Sprintf("%x", sha256.Sum256([]byte(dgst.String()))) //(sha256.Sum256([]byte(dgst.String())))
		lf, err := os.Open(layerslicepath)
		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: cannot open disk cache file %v", err)
			return err
		}

		lfstat, err := lf.Stat()
		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: %s", err)
			return nil

		}

		//fsize := stat.Size()
		size := lfstat.Size()

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
		http.ServeContent(w, r, _desc.Digest.String(), time.Time{}, lf)
		DurationNTT := time.Since(start).Seconds()
		context.GetLogger(ctx).Debugf("NANNAN: slice cache hit: metadata lookup time: %v, layer transfer time: %v, layer size: %v",
			DurationML, DurationNTT, size)
		return nil

	}

	//else restore slice
	context.GetLogger(ctx).Debug("NANNAN: slice cache miss")

	gid := getGID()

	tmp_dir := fmt.Sprintf("%f", gid)
	context.GetLogger(ctx).Debugf("NANNAN: serveblob: the gid for this goroutine: =>%v", tmp_dir)

	packPath := path.Join("/docker/registry/v2/pull_tars/pull_tarfiles", tmp_dir)

	reg, err := regexp.Compile("[^a-zA-Z0-9/.-]+")
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s, ", err)
		return err
	}

	if len(desc.BSFDescriptors[bs.serverIp]) == 0 {
		context.GetLogger(ctx).Debugf("NANNAN: this server doesn't have any files for this layer, %v ", len(desc.BSFDescriptors[bs.serverIp]))
		return nil
	}

	var wg sync.WaitGroup
	antp, _ := ants.NewPoolWithFunc(len(desc.BSFDescriptors[bs.serverIp]), func(i interface{}) {
		mvFile(i)
		wg.Done()
	})
	defer antp.Release()
	//in case there are same files inside the layer dir

	start = time.Now()
	for _, bfdescriptor := range desc.BSFDescriptors[bs.serverIp] {

		if bfdescriptor.ServerIp != bs.serverIp {
			context.GetLogger(ctx).Debugf("NANNAN: this is not a locally available file, %v", bfdescriptor.ServerIp) // not locally available
			continue
		}

		gid = getGID()
		random_dir := fmt.Sprintf("%f", gid)

		tarfpath := reg.ReplaceAllString(strings.SplitN(bfdescriptor.BlobFilePath, "diff", 2)[1], "") // replace alphanumeric
		destfpath := path.Join(packPath, random_dir, tarfpath)
		//		//context.GetLogger(ctx).Debugf("NANNAN: dest path: %v", destfpath) // not locally available
		wg.Add(1)
		antp.Invoke(&Task{
			Ctx:  ctx,
			Src:  strings.TrimPrefix(bfdescriptor.BlobFilePath, "/var/lib/registry"),
			Desc: destfpath,
			Bs:   bs,
		})
	}
	wg.Wait()
	DurationCP := time.Since(start).Seconds()
	//	fmt.Println("NANNAN: slice IO cp time: %.3f, %v\n", DurationCP, dgst)

	packpath := path.Join("/var/lib/registry", packPath)
	//packpath := packPath
	start = time.Now()
	data, err := archive.Tar(packpath, archive.Gzip)
	if err != nil {
		context.GetLogger(ctx).Warnf("NANNAN: %s, ", err)
		return err
	}

	DurationCMP := time.Since(start).Seconds()
	//	fmt.Println("NANNAN: slice compression time: %.3f, %v\n", DurationCMP, dgst)

	newtardir := path.Join("/var/lib/registry", "/docker/registry/v2/pull_tars/pull_tmp_tarfile")
	if os.MkdirAll(newtardir, 0666) != nil {
		context.GetLogger(ctx).Errorf("NANNAN: ServeBlob <COMPRESS create dir for tarfile> %s, ", err)
		return err
	}

	packFile, err := os.Create(path.Join("/var/lib/registry", "/docker/registry/v2/pull_tars/pull_tmp_tarfile", tmp_dir)) //path.Join(parentDir, "tmp_tar.tar.gz")))
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s, ", err)
		return err
	}

	size, err := io.Copy(packFile, data)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s, ", err)
		return err
	}
	data.Close()
	err = packFile.Sync()
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s, ", err)
		return err
	}

	path_old, err := bs.pathFn(_desc.Digest)
	if err != nil {
		return err
	}

	if bs.redirect {
		redirectURL, err := bs.driver.URLFor(ctx, path_old, map[string]interface{}{"method": r.Method})
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
	http.ServeContent(w, r, _desc.Digest.String(), time.Time{}, packFile)
	DurationNTT := time.Since(start).Seconds()

	//	fmt.Println("NANNAN: slice network transfer time: %.3f, %v\n", DurationNTT, dgst)
	//	DurationRS := DurationNTT + DurationCMP + DurationCP + DurationML
	//	fmt.Println("NANNAN: slice restore time: %.3f, %v\n", DurationRS, dgst)

	//	 put into the disk cache
	bfss, err := ioutil.ReadFile(packFile.Name())
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s, ", err)
	}
	context.GetLogger(ctx).Debugf("NANNAN: slice cache put: %v B", len(bfss))
	err = bs.cache.Dc.Put(dgst.String(), bfss)
	if err != nil {
		context.GetLogger(ctx).Debugf("NANNAN: slice cache cannot write to: digest: %v: %v ", dgst.String(), err)
	}

	packFile.Close()

	//delete tmp_dir and packFile here
	if err = os.RemoveAll(path.Join("/var/lib/registry", "/docker/registry/v2/pull_tars/pull_tmp_tarfile", tmp_dir)); err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: cannot remove all file in: %s: %s",
			path.Join("/var/lib/registry", "/docker/registry/v2/pull_tmp_tarfile", tmp_dir), err)
		return err
	}

	//packpath
	if err = os.RemoveAll(packpath); err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: cannot remove all file in packpath: %s: %s",
			packpath, err)
		return err
	}

	context.GetLogger(ctx).Debugf("NANNAN: slice cache miss: metadata lookup time: %v, slice cp time: %v, slice compression time: %v, slice transfer time: %v, slice size: %v",
		DurationML, DurationCP, DurationCMP, DurationNTT, size)

	return nil
}
