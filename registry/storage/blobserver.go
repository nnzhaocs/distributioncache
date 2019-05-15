package storage

import (
	//	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	//"time"
	//	log "github.com/Sirupsen/logrus"
	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/cache"
	"github.com/opencontainers/go-digest"
	//NANNAN
	"io"
	"os"
	"path"
	"regexp"
	"runtime"

	storagecache "github.com/docker/distribution/registry/storage/cache"
	"github.com/docker/docker/pkg/archive"
	"github.com/serialx/hashring"
	//	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"math/rand"
	"strconv"
	"time"
)

// TODO(stevvooe): This should configurable in the future.
const blobCacheControlMaxAge = 365 * 24 * time.Hour

// blobServer simply serves blobs from a driver instance using a path function
// to identify paths and a descriptor service to fill in metadata.
type blobServer struct {
	driver  driver.StorageDriver
	statter distribution.BlobStatter

	//NANNAN: add a fileDescriptorCacheProvider for restore
	ring                        *hashring.HashRing
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

//NANNAN: TODO: process manfiests

func (bs *blobServer) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {

	//NANNAN: parallelly read file from backend.
	context.GetLogger(ctx).Debug("NANNAN: (*blobServer).ServeBlob")

	_desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return err
	}

	// get filepaths from redis
	start := time.Now()
	desc, err := bs.fileDescriptorCacheProvider.StatBFRecipe(ctx, dgst)
	elapsed := time.Since(start)
	fmt.Println("NANNAN: metadata lookup time: %v, %v", elapsed, dgst)

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

		http.ServeContent(w, r, _desc.Digest.String(), time.Time{}, br)
		return nil
	}
	////NANNAN: for blob-files info
	//type BFDescriptor struct{
	//
	//	BlobFilePath    string // filepath of this blobfile
	//	Digest          digest.Digest
	//	DigestFilePath  string	// digest file path
	//
	//	ServerIp		string
	//}

	gid := getGID()
	//tmp_dir := fmt.Sprintf(gid) //gid

	tmp_dir := strconv.FormatFloat(gid, 'g', 1, 64)
	context.GetLogger(ctx).Debug("NANNAN: serveblob: the gid for this goroutine: =>%", tmp_dir)
	/*; err == nil {
		//	    fmt.Println(s) // 3.14159265
		context.GetLogger(ctx).Debug("NANNAN: PrepareForward: the gid for this goroutine: =>%", tmp_dir)
	}*/

	blobPath, err := PathFor(BlobDataPathSpec{
		Digest: _desc.Digest,
	})
	//	context.GetLogger(ctx).Debugf("NANNAN: blob = %v:%v", blobPath, _desc.Digest)

	layerPath := blobPath

	//	context.GetLogger(ctx).Debug("NANNAN: START RESTORING FROM :=>%s", layerPath)

	parentDir := path.Dir(layerPath)
	packPath := path.Join(parentDir, tmp_dir)

	//	context.GetLogger(ctx).Debug("NANNAN GET: %v", desc)

	reg, err := regexp.Compile("[^a-zA-Z0-9/.-]+")
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s, ", err)
		return err
	}

	//make for loop parallel by using Limiting Concurrency like semaphore
	cores := runtime.GOMAXPROCS(0)
	limChan := make(chan bool, cores)
	//	errChan := make(chan error, len(desc.BFDescriptors))
	defer close(limChan)
	//	defer close(errChan)
	for i := 0; i < cores; i++ {
		limChan <- true
	}
	start = time.Now()
	for _, bfdescriptor := range desc.BFDescriptors {
		<-limChan

		// copy
		go func(bfdescriptor distribution.BFDescriptor, packPath string) {
			if bfdescriptor.ServerIp != bs.serverIp {
				context.GetLogger(ctx).Debug("NANNAN: this is not a locally available file, ", bfdescriptor.ServerIp) // not locally available
				//				limChan <- true

			} else {
				tarfpath := reg.ReplaceAllString(strings.SplitN(bfdescriptor.BlobFilePath, "diff", 2)[1], "") // replace alphanumeric

				//		context.GetLogger(ctx).Debug("NANNAN: START COPY FILE FROM %s TO %s", bfdescriptor.DigestFilePath, bfdescriptor.BlobFilePath)

				contents, err := bs.driver.GetContent(ctx, strings.TrimPrefix(bfdescriptor.DigestFilePath, "/var/lib/registry")) //, dest)
				if err != nil {
					context.GetLogger(ctx).Errorf("NANNAN: STILL SEND TAR %s, ", err) // even if there is an error, meaning the dir is empty.
					//					errChan <- err
					//					limChan <- true
					//				continue
				} else {

					destfpath := path.Join(packPath, tarfpath)

					err = bs.driver.PutContent(ctx, destfpath, contents)
					if err != nil {
						context.GetLogger(ctx).Warnf("NANNAN: STILL SEND TAR %s, ", err)
						//						errChan <- err
						//						limChan <- true
					}
					//					else{
					////						limChan <- true
					//					}
				}
			}
			limChan <- true
		}(bfdescriptor, packPath)
	}
	// leave the errChan
	for i := 0; i < cap(limChan); i++ {
		<-limChan
		context.GetLogger(ctx).Debug("NANNAN: one goroutine is joined")
	}
	elapsed = time.Since(start)
	fmt.Println("NANNAN: slice IO cp time: %v, %v", elapsed, dgst)

	// all goroutines finished here
	context.GetLogger(ctx).Debug("NANNAN: all goroutines finished here") // not locally available

	packpath := path.Join("/var/lib/registry", packPath)
	start = time.Now()
	data, err := archive.Tar(packpath, archive.Gzip)
	if err != nil {
		//TODO: process manifest file
		context.GetLogger(ctx).Warnf("NANNAN: %s, ", err)
		return err
	}

	elapsed = time.Since(start)
	fmt.Println("NANNAN: slice compression time: %v, %v", elapsed, dgst)

	defer data.Close()
	newtardir := path.Join("/var/lib/registry", "/docker/registry/v2/pull_tmp_tarfile")
	if os.MkdirAll(newtardir, 0666) != nil {
		context.GetLogger(ctx).Errorf("NANNAN: ServeBlob <COMPRESS create dir for tarfile> %s, ", err)
		return err
	}

	packFile, err := os.Create(path.Join("/var/lib/registry", "/docker/registry/v2/pull_tmp_tarfile", tmp_dir)) //path.Join(parentDir, "tmp_tar.tar.gz")))
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s, ", err)
		return err
	}

	defer packFile.Close()

	size, err := io.Copy(packFile, data)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s, ", err)
		return err
	}

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
	elapsed = time.Since(start)
	fmt.Println("NANNAN: slice network transfer time: %v, %v", elapsed, dgst)
	//delete tmp_dir and packFile here

	return nil
}

//func (bs *blobServer) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
//	context.GetLogger(ctx).Infof("NANNAN: START serve blob")
//	desc, err := bs.statter.Stat(ctx, dgst)
//	if err != nil {
//		return err
//	}
//	//log.Warnf("FAST: Serving blob %s", desc.Digest.String())
//	path, err := bs.pathFn(desc.Digest)
//	if err != nil {
//		return err
//	}
//
//	if bs.redirect {
//		//log.Warnf("FAST: Redirect enables for %s", desc.Digest.String())
//		redirectURL, err := bs.driver.URLFor(ctx, path, map[string]interface{}{"method": r.Method})
//		switch err.(type) {
//		case nil:
//			// Redirect to storage URL.
//			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
//			return err
//
//		case driver.ErrUnsupportedMethod:
//			// Fallback to serving the content directly.
//		default:
//			// Some unexpected error.
//			return err
//		}
//	}
//
//	if err != nil {
//		return err
//	}
//
//	v, get_err := bs.cache.Get(desc.Digest.String())
//	if get_err != nil {
//		//return errors.Trace(get_err)
//		log.Warnf("ali:err=%s", get_err)
//	}
//	if v != nil { // ali: read hit
//		log.Warnf("FAST: cache hit on %s", desc.Digest.String())
//		br := bytes.NewReader(v)
//
//		http.ServeContent(w, r, desc.Digest.String(), time.Time{}, br)
//
//		//                w.Header().Set("ETag", fmt.Sprintf(`"%s"`, desc.Digest)) // If-None-Match handled by ServeContent
//		//                w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))
//		//
//		//                if w.Header().Get("Docker-Content-Digest") == "" {
//		//                        w.Header().Set("Docker-Content-Digest", desc.Digest.String())
//		//                }
//		//
//		//                if w.Header().Get("Content-Type") == "" {
//		//                        // Set the content type if not already set.
//		//                        w.Header().Set("Content-Type", desc.MediaType)
//		//                }
//		//
//		//                if w.Header().Get("Content-Length") == "" {
//		//                        // Set the content length if not already set.
//		//                        w.Header().Set("Content-Length", fmt.Sprint(desc.Size))
//		//                }
//		//
//		//                log.Warnf("FAST: Close file reader %s", desc.Digest.String())
//		//                http.ServeContent(w, r, desc.Digest.String(), time.Time{}, br)
//		//
//	} else {
//		log.Warnf("FAST: cache miss on %s", desc.Digest.String())
//
//		br, err := newFileReader(ctx, bs.driver, path, desc.Size)
//		if err != nil {
//			return err
//		}
//
//		if br.size < int64(bs.cache.GetEntryLimit()) {
//			buf := new(bytes.Buffer)
//			buf.ReadFrom(br)
//			//log.Warnf("FAST3: length buffer %d", br.size)
//			bs.cache.Set(desc.Digest.String(), buf.Bytes())
//			defer br.Close()
//
//			w.Header().Set("ETag", fmt.Sprintf(`"%s"`, desc.Digest)) // If-None-Match handled by ServeContent
//			w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))
//
//			if w.Header().Get("Docker-Content-Digest") == "" {
//				w.Header().Set("Docker-Content-Digest", desc.Digest.String())
//			}
//
//			if w.Header().Get("Content-Type") == "" {
//				// Set the content type if not already set.
//				w.Header().Set("Content-Type", desc.MediaType)
//			}
//
//			if w.Header().Get("Content-Length") == "" {
//				// Set the content length if not already set.
//				w.Header().Set("Content-Length", fmt.Sprint(desc.Size))
//			}
//
//			v, get_err := bs.cache.Get(desc.Digest.String())
//			if get_err != nil {
//				//return errors.Trace(get_err)
//				log.Debug("ali:err=%s", get_err)
//			}
//			br2 := bytes.NewReader(v)
//
//			//log.Warnf("FAST: Close file reader %s", desc.Digest.String())
//			http.ServeContent(w, r, desc.Digest.String(), time.Time{}, br2)
//		} else {
//			defer br.Close()
//			w.Header().Set("ETag", fmt.Sprintf(`"%s"`, desc.Digest)) // If-None-Match handled by ServeContent
//			w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))
//
//			if w.Header().Get("Docker-Content-Digest") == "" {
//				w.Header().Set("Docker-Content-Digest", desc.Digest.String())
//			}
//
//			if w.Header().Get("Content-Type") == "" {
//				// Set the content type if not already set.
//				w.Header().Set("Content-Type", desc.MediaType)
//			}
//
//			if w.Header().Get("Content-Length") == "" {
//				// Set the content length if not already set.
//				w.Header().Set("Content-Length", fmt.Sprint(desc.Size))
//			}
//			http.ServeContent(w, r, desc.Digest.String(), time.Time{}, br)
//		}
//
//	}
//	return nil
//}
