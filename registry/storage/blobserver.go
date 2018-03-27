package storage

import (
	"bytes"

	"encoding/json"

	"fmt"
	"net/http"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/cache"
	"github.com/opencontainers/go-digest"
)

// TODO(stevvooe): This should configurable in the future.
const blobCacheControlMaxAge = 365 * 24 * time.Hour

// blobServer simply serves blobs from a driver instance using a path function
// to identify paths and a descriptor service to fill in metadata.
type blobServer struct {
	driver   driver.StorageDriver
	statter  distribution.BlobStatter
	pathFn   func(dgst digest.Digest) (string, error)
	redirect bool // allows disabling URLFor redirects
	cache    *cache.MemCache
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

func (bs *blobServer) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return err
	}
	//log.Warnf("FAST: Serving blob %s", desc.Digest.String())
	path, err := bs.pathFn(desc.Digest)
	if err != nil {
		return err
	}

	if bs.redirect {
		//log.Warnf("FAST: Redirect enables for %s", desc.Digest.String())
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

	if err != nil {
		return err
	}

	v, get_err := bs.cache.Get(desc.Digest.String())
	if get_err != nil {
		//return errors.Trace(get_err)
		log.Warnf("ali:err=%s", get_err)
	}
	if v != nil { // ali: read hit
		log.Warnf("FAST: cache hit on %s", desc.Digest.String())
		br := bytes.NewReader(v)

		http.ServeContent(w, r, desc.Digest.String(), time.Time{}, br)

		//                w.Header().Set("ETag", fmt.Sprintf(`"%s"`, desc.Digest)) // If-None-Match handled by ServeContent
		//                w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))
		//
		//                if w.Header().Get("Docker-Content-Digest") == "" {
		//                        w.Header().Set("Docker-Content-Digest", desc.Digest.String())
		//                }
		//
		//                if w.Header().Get("Content-Type") == "" {
		//                        // Set the content type if not already set.
		//                        w.Header().Set("Content-Type", desc.MediaType)
		//                }
		//
		//                if w.Header().Get("Content-Length") == "" {
		//                        // Set the content length if not already set.
		//                        w.Header().Set("Content-Length", fmt.Sprint(desc.Size))
		//                }
		//
		//                log.Warnf("FAST: Close file reader %s", desc.Digest.String())
		//                http.ServeContent(w, r, desc.Digest.String(), time.Time{}, br)
		//
	} else {
		log.Warnf("FAST: cache miss on %s", desc.Digest.String())

		br, err := newFileReader(ctx, bs.driver, path, desc.Size)
		if err != nil {
			return err
		}

		if br.size < int64(bs.cache.GetEntryLimit()) {
			buf := new(bytes.Buffer)
			buf.ReadFrom(br)
			//log.Warnf("FAST3: length buffer %d", br.size)
			bs.cache.Set(desc.Digest.String(), buf.Bytes())
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

			v, get_err := bs.cache.Get(desc.Digest.String())
			if get_err != nil {
				//return errors.Trace(get_err)
				log.Debug("ali:err=%s", get_err)
			}
			br2 := bytes.NewReader(v)

			//log.Warnf("FAST: Close file reader %s", desc.Digest.String())
			http.ServeContent(w, r, desc.Digest.String(), time.Time{}, br2)
		} else {
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
		}

	}
	return nil
}
