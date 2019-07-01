package cache

import (
	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	digest "github.com/opencontainers/go-digest"
)

// Metrics is used to hold metric counters
// related to the number of times a cache was
// hit or missed.
type Metrics struct {
	Requests uint64
	Hits     uint64
	Misses   uint64
}

// MetricsTracker represents a metric tracker
// which simply counts the number of hits and misses.
type MetricsTracker interface {
	Hit()
	Miss()
	Metrics() Metrics
}

type cachedBlobStatter struct {
	cache     distribution.BlobDescriptorService
	filecache distribution.FileDescriptorService
	backend   distribution.BlobDescriptorService
	tracker   MetricsTracker
}

// NewCachedBlobStatter creates a new statter which prefers a cache and
// falls back to a backend.
func NewCachedBlobStatter(cache distribution.BlobDescriptorService, backend distribution.BlobDescriptorService) distribution.BlobDescriptorService {
	return &cachedBlobStatter{
		cache: cache,
		//		filecache: filecache,
		backend: backend,
	}
}

// NewCachedBlobStatter creates a new statter which prefers a cache and
// falls back to a backend.
func NewCachedBlobStatterWithFileCache(cache distribution.BlobDescriptorService, filecache distribution.FileDescriptorService, backend distribution.BlobDescriptorService) distribution.BlobDescriptorService {
	return &cachedBlobStatter{
		cache:     cache,
		filecache: filecache,
		backend:   backend,
	}
}

// NewCachedBlobStatterWithMetrics creates a new statter which prefers a cache and
// falls back to a backend. Hits and misses will send to the tracker.
func NewCachedBlobStatterWithMetrics(cache distribution.BlobDescriptorService, backend distribution.BlobDescriptorService, tracker MetricsTracker) distribution.BlobStatter {
	return &cachedBlobStatter{
		cache: cache,
		//		filecache: filecache,
		backend: backend,
		tracker: tracker,
	}
}

// NewCachedBlobStatterWithMetrics creates a new statter which prefers a cache and
// falls back to a backend. Hits and misses will send to the tracker.
func NewCachedBlobStatterWithMetricsWithFileCache(cache distribution.BlobDescriptorService, filecache distribution.FileDescriptorService, backend distribution.BlobDescriptorService, tracker MetricsTracker) distribution.BlobStatter {
	return &cachedBlobStatter{
		cache:     cache,
		filecache: filecache,
		backend:   backend,
		tracker:   tracker,
	}
}

//// NewCachedBlobStatterWithMetrics creates a new statter which prefers a cache and
//// falls back to a backend. Hits and misses will send to the tracker.
//func NewCachedBlobStatterWithMetrics(cache distribution.BlobDescriptorService, filecache distribution.FileDescriptorService, backend distribution.BlobDescriptorService, tracker MetricsTracker) distribution.BlobStatter {
//	return &cachedBlobStatter{
//		cache:   cache,
//		filecache: cache,
//		backend: backend,
//		tracker: tracker,
//	}
//}

func (cbds *cachedBlobStatter) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	desc, err := cbds.cache.Stat(ctx, dgst)
	if err != nil {
		if err != distribution.ErrBlobUnknown {
			context.GetLogger(ctx).Errorf("Stat: error retrieving descriptor from cache: %v", err)
		}

		goto fallback
	}

	if cbds.tracker != nil {
		cbds.tracker.Hit()
	}
	return desc, nil
fallback:
	if cbds.tracker != nil {
		cbds.tracker.Miss()
	}
	desc, err = cbds.backend.Stat(ctx, dgst)
	if err != nil {
		return desc, err
	}

	if err := cbds.cache.SetDescriptor(ctx, dgst, desc); err != nil {
		context.GetLogger(ctx).Errorf("Stat SetDescriptor: error adding descriptor %v to cache: %v", desc.Digest, err)
	}

	return desc, err

}

func (cbds *cachedBlobStatter) Clear(ctx context.Context, dgst digest.Digest) error {
	err := cbds.cache.Clear(ctx, dgst)
	if err != nil {
		return err
	}

	err = cbds.backend.Clear(ctx, dgst)
	if err != nil {
		return err
	}
	return nil
}

func (cbds *cachedBlobStatter) SetDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.Descriptor) error {
	if err := cbds.cache.SetDescriptor(ctx, dgst, desc); err != nil {
		context.GetLogger(ctx).Errorf("SetDescriptor: error adding descriptor %v to cache: %v", desc.Digest, err)
	}
	return nil
}

//func logErrorf(ctx context.Context, tracker MetricsTracker, format string, args ...interface{}) {
//	if tracker == nil {
//		return
//	}
//
//	logger := tracker.Logger(ctx)
//	if logger == nil {
//		return
//	}
//	logger.Errorf(format, args...)
//}

//NANNAN
func (cbds *cachedBlobStatter) StatFile(ctx context.Context, dgst digest.Digest) (distribution.FileDescriptor, error) {
	desc, err := cbds.filecache.StatFile(ctx, dgst)
	if err != nil {
		if err != distribution.ErrBlobUnknown {
			context.GetLogger(ctx).Errorf("StatFile: error retrieving descriptor from cache: %v", err)
		}

		goto fallback
	}

	if cbds.tracker != nil {
		cbds.tracker.Hit()
	}
	return desc, nil
fallback:
	if cbds.tracker != nil {
		cbds.tracker.Miss()
	}
	//filecache no backend
	//	desc, err = cbds.backend.Stat(ctx, dgst)
	//	if err != nil {
	//		return desc, err
	//	}
	//
	//	if err := cbds.filecache.SetFileDescriptor(ctx, dgst, desc); err != nil {
	//		context.GetLogger(ctx).Errorf("error adding descriptor %v to cache: %v", desc.Digest, err)
	//	}

	return desc, err
}

func (cbds *cachedBlobStatter) SetFileDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.FileDescriptor) error {
	if err := cbds.filecache.SetFileDescriptor(ctx, dgst, desc); err != nil {
		context.GetLogger(ctx).Errorf("SetFileDescriptor: error adding file descriptor %v to cache: %v", desc.Digest, err)
	}
	return nil
}

func (cbds *cachedBlobStatter) StatBFRecipe(ctx context.Context, dgst digest.Digest) (distribution.BFRecipeDescriptor, error) {
	desc, err := cbds.filecache.StatBFRecipe(ctx, dgst)
	if err != nil {
		if err != distribution.ErrBlobUnknown {
			context.GetLogger(ctx).Errorf("StatBFRecipe: error retrieving descriptor from cache: %v", err)
		}

		goto fallback
	}

	if cbds.tracker != nil {
		cbds.tracker.Hit()
	}
	return desc, nil
fallback:
	if cbds.tracker != nil {
		cbds.tracker.Miss()
	}
	//filecache no backend
	//	desc, err = cbds.backend.Stat(ctx, dgst)
	//	if err != nil {
	//		return desc, err
	//	}
	//
	//	if err := cbds.filecache.SetFileDescriptor(ctx, dgst, desc); err != nil {
	//		context.GetLogger(ctx).Errorf("error adding descriptor %v to cache: %v", desc.Digest, err)
	//	}

	return desc, err
}

func (cbds *cachedBlobStatter) StatBSRecipe(ctx context.Context, dgst digest.Digest) (distribution.BSRecipeDescriptor, error) {
	desc, err := cbds.filecache.StatBSRecipe(ctx, dgst)
	if err != nil {
		if err != distribution.ErrBlobUnknown {
			context.GetLogger(ctx).Errorf("StatBSRecipe: error retrieving descriptor from cache: %v", err)

		}

		goto fallback

	}

	if cbds.tracker != nil {
		cbds.tracker.Hit()

	}
	return desc, nil
fallback:
	if cbds.tracker != nil {
		cbds.tracker.Miss()

	}
	//filecache no backend
	//  desc, err = cbds.backend.Stat(ctx, dgst)
	//  if err != nil {
	//      return desc, err
	//
	// }
	//
	//  if err := cbds.filecache.SetFileDescriptor(ctx, dgst, desc); err != nil {
	//      context.GetLogger(ctx).Errorf("error adding descriptor %v to cache: %v", desc.Digest, err)
	//
	//}

	return desc, err

}

func (cbds *cachedBlobStatter) SetBFRecipe(ctx context.Context, dgst digest.Digest, desc distribution.BFRecipeDescriptor) error {
	if err := cbds.filecache.SetBFRecipe(ctx, dgst, desc); err != nil {
		context.GetLogger(ctx).Errorf("error adding blob file recipe descriptor %v to cache: %v", desc.BlobDigest, err)
	}
	return nil
}
