package gcache

import (
	"fmt"
	//"gcache/gcache"
	"os"
	"time"

	"github.com/allegro/bigcache"
	"github.com/peterbourgon/diskv"
)

// preconstruction cache
type BlobCache struct {
	MemCache  *bigcache.BigCache
	DiskCache *diskv.Diskv

	FileLST  Cache
	LayerLST Cache
	SliceLST Cache
}

var DefaultTTL time.Duration
var FileCacheCap, LayerCacheCap, SliceCacheCap int

func (cache *BlobCache) SetCapTTL(fileCacheCap, layerCacheCap, sliceCacheCap, ttl int) error {

	DefaultTTL = time.Duration(ttl) * time.Millisecond
	fmt.Printf("NANNAN: DefaultTTL: %d\n\n", DefaultTTL)

	FileCacheCap = fileCacheCap
	LayerCacheCap = layerCacheCap
	SliceCacheCap = sliceCacheCap

	fmt.Printf("NANNAN: FileCacheCap: %d B, LayerCacheCap: %d B, SliceCacheCap: %d B\n\n",
		FileCacheCap, LayerCacheCap, SliceCacheCap)
	return nil
}

func Init() (*BlobCache, error) {
	cache := &BlobCache{}
	cache.FileLST = New(FileCacheCap * 1024 * 1024).ARC().EvictedFunc(func(key, value interface{}) {
		if k, ok := key.(string); ok {
			cache.MemCache.Delete(k)
		}
		fmt.Println("NANNAN: evicted key:", key)
	}).
		Expiration(DefaultTTL * 3).
		Build()
	cache.LayerLST = New(LayerCacheCap * 1024 * 1024).ARC().EvictedFunc(func(key, value interface{}) {
		if k, ok := key.(string); ok {
			cache.DiskCache.Erase(k)
		}
		fmt.Println("NANNAN: evicted key:", key)
	}).
		Expiration(DefaultTTL * 2).
		Build()
	cache.SliceLST = New(SliceCacheCap * 1024 * 1024).ARC().EvictedFunc(func(key, value interface{}) {
		if k, ok := key.(string); ok {
			cache.DiskCache.Erase(k)
		}
		fmt.Println("NANNAN: evicted key:", key)
	}).
		Expiration(DefaultTTL * 1).
		Build()

	fmt.Printf("NANNAN: FileCacheCap: %d B, LayerCacheCap: %d B, SliceCacheCap: %d B\n\n",
		FileCacheCap, LayerCacheCap, SliceCacheCap)

	var memcap float32 = float32(FileCacheCap) * 1.2
	config := bigcache.Config{
		Shards:           2,
		LifeWindow:       3600 * time.Minute,
		Verbose:          true,
		HardMaxCacheSize: int(memcap),
		OnRemove:         nil,
	}
	MemCache, err := bigcache.NewBigCache(config)
	if err != nil {
		fmt.Printf("NANNAN: cannot create BlobCache: %s \n", err)
		return nil, err
	}
	cache.MemCache = MemCache

	pth := "/var/lib/registry/docker/registry/v2/pull_tars/diskcache/"
	err = os.MkdirAll(pth, 0777)
	if err != nil {
		fmt.Printf("NANNAN: cannot create DiskCache: %s \n", err)
		return nil, err
	}

	flatTransform := func(s string) []string { return []string{} }
	DiskCache := diskv.New(diskv.Options{
		BasePath:     pth,
		Transform:    flatTransform,
		CacheSizeMax: 1024 * 1024 * 64,
	})

	cache.DiskCache = DiskCache

	fmt.Printf("NANNAN: init cache: mem cache capacity: %d MB \n\n",
		int(memcap))
	return cache, err
}

func LayerHashKey(dgst string) string {
	return "Layer::" + dgst
}

func SliceHashKey(dgst string) string {
	return "Slice::" + dgst
}
func FileHashKey(dgst string) string {
	return "File::" + dgst
}

func (cache *BlobCache) SetLayer(dgst string, bss []byte) bool {
	key := LayerHashKey(dgst)
	size := len(bss)

	if err := cache.LayerLST.Set(key, size); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot set dgst %s: %v\n", dgst, err)
		return false
	}

	if ok := cache.DiskCache.Has(key); ok {
		return true
	}

	if err := cache.DiskCache.Write(key, bss); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot set dgst %s: %v\n", dgst, err)
		return false
	}
	return true
}

func (cache *BlobCache) GetLayer(dgst string) ([]byte, bool) {
	key := LayerHashKey(dgst)

	if _, err := cache.LayerLST.Get(key); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot get dgst %s: %v\n", dgst, err)
		return nil, false
	}

	bss, err := cache.DiskCache.Read(key)
	if err != nil {
		fmt.Printf("NANNAN: BlobCache cannot get dgst %s: %v\n", dgst, err)
		return nil, false
	}
	return bss, true
}

func (cache *BlobCache) SetSlice(dgst string, bss []byte) bool {
	key := SliceHashKey(dgst)
	size := len(bss)

	if err := cache.SliceLST.Set(key, size); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot set dgst %s: %v\n", dgst, err)
		return false
	}

	if ok := cache.DiskCache.Has(key); ok {
		return true
	}

	if err := cache.DiskCache.Write(key, bss); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot set dgst %s: %v\n", dgst, err)
		return false
	}
	return true
}

func (cache *BlobCache) GetSlice(dgst string) ([]byte, bool) {
	key := SliceHashKey(dgst)
	if _, err := cache.SliceLST.Get(key); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot get dgst %s: %v\n", dgst, err)
		return nil, false
	}

	bss, err := cache.DiskCache.Read(key)
	if err != nil {
		fmt.Printf("NANNAN: BlobCache cannot get dgst %s: %v\n", dgst, err)
		return nil, false
	}
	return bss, true
}

func (cache *BlobCache) SetFile(dgst string, bss []byte) bool {
	key := FileHashKey(dgst)
	size := len(bss)

	if err := cache.FileLST.Set(key, size); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot set dgst %s: %v\n", dgst, err)
		return false
	}

	if err := cache.MemCache.Set(key, bss); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot set dgst %s: %v\n", dgst, err)
		return false
	}
	return true
}

func (cache *BlobCache) GetFile(dgst string) ([]byte, bool) {
	key := FileHashKey(dgst)
	if _, err := cache.FileLST.Get(key); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot get dgst %s: %v\n", dgst, err)
		return nil, false
	}

	bss, err := cache.MemCache.Get(key)
	if err != nil {
		fmt.Printf("NANNAN: BlobCache cannot get dgst %s: %v\n", dgst, err)
		return nil, false
	}
	return bss, true
}
