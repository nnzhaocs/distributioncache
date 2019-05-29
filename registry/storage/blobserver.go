package storage

import (
	//	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
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
	//"runtime"
	"sync"

	storagecache "github.com/docker/distribution/registry/storage/cache"
	"github.com/docker/docker/pkg/archive"
	//"github.com/serialx/hashring"
	//	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"math/rand"
	//"strconv"
	"time"
	//roundrobin "github.com/hlts2/round-robin"
	"github.com/panjf2000/ants"
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
//	Wg   sync.WaitGroup
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
//	wg := task.Wg
	bs := task.Bs
	//	ctx context.Context, src string, desc string, wg sync.WaitGroup

	contents, err := bs.driver.GetContent(ctx, src)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: STILL SEND TAR %s, ", err)
	} else {
		err = bs.driver.PutContent(ctx, desc, contents)
		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: STILL SEND TAR %s, ", err)
		}
	}
	return
}

//func mvFile(ctx context.Context, src string, desc string, wg sync.WaitGroup, bs *blobServer) error {
//
//	contents, err := bs.driver.GetContent(ctx, src)
//	if err != nil {
//		context.GetLogger(ctx).Errorf("NANNAN: STILL SEND TAR %s, ", err)
//	} else {
//		err = bs.driver.PutContent(ctx, desc, contents)
//		if err != nil {
//			context.GetLogger(ctx).Errorf("NANNAN: STILL SEND TAR %s, ", err)
//		}
//	}
//	wg.Done()
//	return err
//}

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
	DurationML := time.Since(start).Seconds()
	fmt.Println("NANNAN: metadata lookup time: %.3f, %v", DurationML, dgst)

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

	gid := getGID()

	tmp_dir := fmt.Sprintf("%f", gid)
	context.GetLogger(ctx).Debug("NANNAN: serveblob: the gid for this goroutine: =>%", tmp_dir)

	packPath := path.Join("/docker/registry/v2/pull_tars/pull_tarfiles", tmp_dir)

	reg, err := regexp.Compile("[^a-zA-Z0-9/.-]+")
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s, ", err)
		return err
	}

	var wg sync.WaitGroup
	antp, _ := ants.NewPoolWithFunc(len(desc.BSFDescriptors[bs.serverIp]), func(i interface{}){
		mvFile(i)
		wg.Done()
	})
	defer antp.Release()
	start = time.Now()
	for _, bfdescriptor := range desc.BSFDescriptors[bs.serverIp] {

		if bfdescriptor.ServerIp != bs.serverIp {
			context.GetLogger(ctx).Debug("NANNAN: this is not a locally available file, ", bfdescriptor.ServerIp) // not locally available
			continue
		}

		tarfpath := reg.ReplaceAllString(strings.SplitN(bfdescriptor.BlobFilePath, "diff", 2)[1], "") // replace alphanumeric
		destfpath := path.Join(packPath, tarfpath)
		wg.Add(1)
		antp.Invoke(&Task{
			Ctx:  ctx,
			Src:  strings.TrimPrefix(bfdescriptor.BlobFilePath, "/var/lib/registry"),
			Desc: destfpath,
//			Wg:   wg,
			Bs:   bs,
		})
	}
	wg.Wait()
	DurationCP := time.Since(start).Seconds()
	fmt.Println("NANNAN: slice IO cp time: %.3f, %v", DurationCP, dgst)

	packpath := path.Join("/var/lib/registry", packPath)
	//packpath := packPath
	start = time.Now()
	data, err := archive.Tar(packpath, archive.Gzip)
	if err != nil {
		context.GetLogger(ctx).Warnf("NANNAN: %s, ", err)
		return err
	}

	DurationCMP := time.Since(start).Seconds()
	fmt.Println("NANNAN: slice compression time: %.3f, %v", DurationCMP, dgst)

	defer data.Close()
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

	defer packFile.Close()

	size, err := io.Copy(packFile, data)
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
	fmt.Println("NANNAN: slice network transfer time: %.3f, %v", DurationNTT, dgst)

	DurationRS := DurationNTT + DurationCMP + DurationCP + DurationML

	fmt.Println("NANNAN: slice restore time: %.3f, %v", DurationRS, dgst)

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

//	bsdedupDescriptor := distribution.BSResDescriptor{
		desc.BSResDescriptors[bs.serverIp].ServerIp = bs.serverIp

		desc.BSResDescriptors[bs.serverIp].DurationRS = DurationRS
		desc.BSResDescriptors[bs.serverIp].DurationNTT = DurationNTT
		desc.BSResDescriptors[bs.serverIp].DurationCMP = DurationCMP
		desc.BSResDescriptors[bs.serverIp].DurationCP =  DurationCP
		desc.BSResDescriptors[bs.serverIp].DurationML =  DurationML

		desc.BSResDescriptors[bs.serverIp].SliceSize = desc.SliceSizeMap[bs.serverIp]
//	}

//	desc.BSResDescriptors[bs.serverIp].ServerIp = bs.serverIp
	
	//update with response time
	err = bs.fileDescriptorCacheProvider.SetBFRecipe(ctx, desc.BlobDigest, desc)
	if err != nil {
		return err
	}

	return nil
}
