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

	FileLST  *ARC
	LayerLST *ARC
	SliceLST *ARC
}

const (
	TYPE_EXPIARTION_REPO = "expirationrepo"
	TYPE_EXPIARTION_USR  = "expirationusr"
)

var TYPE_EXPIARTION string
var DefaultTTL int

func (cache *BlobCache) SetTTL(usrttl int, repottl int, tp string) error {

	if tp == "expirationrepo" {
		DefaultTTL = repottl
		TYPE_EXPIARTION = TYPE_EXPIARTION_REPO
	} else {
		DefaultTTL = usrttl
		TYPE_EXPIARTION = TYPE_EXPIARTION_USR
	}
	fmt.Printf("NANNAN: DefaultTTL: %d\n\n", cache.DefaultTTL)
	return nil
}

func (cache *BlobCache) NewARClsts(FileCacheCap int, LayerCacheCap int64, SliceCacheCap int64) error {
	cache.FileLST = New(FileCacheCap * 1024 * 1024).ARC().EvictedFunc(func(key, value interface{}) {
		cache.MemCache.Delete(key)
		fmt.Println("NANNAN: evicted key:", key)
	}).
		Expiration(DefaultTTL).
		Build()
	cache.LayerLST = New(LayerCacheCap * 1024 * 1024).ARC().EvictedFunc(func(key, value interface{}) {
		cache.DiskCache.Erase(key)
		fmt.Println("NANNAN: evicted key:", key)
	}).
		Expiration(DefaultTTL).
		Build()
	cache.SliceLST = New(SliceCacheCap * 1024 * 1024).ARC().EvictedFunc(func(key, value interface{}) {
		cache.DiskCache.Erase(key)
		fmt.Println("NANNAN: evicted key:", key)
	}).
		Expiration(DefaultTTL).
		Build()

	fmt.Printf("NANNAN: FileCacheCap: %d B, LayerCacheCap: %d B, SliceCacheCap: %d B\n\n",
		FileCacheCap, LayerCacheCap, SliceCacheCap)
	return
}

func (cache *BlobCache) Init() error {
	config := bigcache.Config{
		Shards:           2,
		LifeWindow:       3600 * time.Minute,
		Verbose:          true,
		HardMaxCacheSize: int(cache.FileCacheCap * 1.2),
		OnRemove:         nil,
	}
	MemCache, err := bigcache.NewBigCache(config)
	if err != nil {
		fmt.Printf("NANNAN: cannot create BlobCache \n")
		return err
	}
	cache.MemCache = MemCache

	pth := "/var/lib/registry/docker/registry/v2/pull_tars/diskcache/"
	err = os.MkdirAll(pth, 0777)
	if err != nil {
		fmt.Printf("NANNAN: cannot create DiskCache \n")
	}

	flatTransform := func(s string) []string { return []string{} }
	DiskCache := diskv.New(diskv.Options{
		BasePath:     pth,
		Transform:    flatTransform,
		CacheSizeMax: 1024 * 1024 * 64,
	})

	cache.DiskCache = DiskCache

	fmt.Printf("NANNAN: init cache: mem cache capacity: %d MB \n\n",
		int(cache.FileCacheCap*1.2))
	return err
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

func (cache *BlobCache) SetLayer(usrname string, reponame string, layerdgst string, bss []byte) bool {
	key := LayerHashKey(layerdgst)
	size := len(bss)
	var val Value

	if TYPE_EXPIARTION == TYPE_EXPIARTION_REPO {
		val = Value{
			Size:      size,
			KeyExpire: reponame,
		}
	} else {
		val = Value{
			Size:      size,
			KeyExpire: usrname,
		}

	}

	if err := cache.LayerLST.SetWithExpire(key, val, DefaultTTL); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot set dgst %s: %v\n", dgst, err)
		return err
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

func (cache *BlobCache) GetLayer(usrname string, reponame string, dgst string) ([]byte, bool) {
	key := LayerHashKey(dgst)
	var val Value

	if TYPE_EXPIARTION == TYPE_EXPIARTION_REPO {
		val = Value{
			Size:      0,
			KeyExpire: reponame,
		}
	} else {
		val = Value{
			Size:      0,
			KeyExpire: usrname,
		}

	}
	if _, err := cache.LayerLST.Get(key, val); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot get dgst %s: %v\n", dgst, err)
	}

	bss, err := cache.DiskCache.Read(key)
	if err != nil {
		fmt.Printf("NANNAN: BlobCache cannot get dgst %s: %v\n", dgst, err)
		return nil, false
	}
	return bss, true
}

func (cache *BlobCache) SetSlice(usrname string, reponame string, dgst string, bss []byte) bool {
	key := SliceHashKey(layerdgst)
	size := len(bss)
	var val Value

	if TYPE_EXPIARTION == TYPE_EXPIARTION_REPO {
		val = Value{
			Size:      size,
			KeyExpire: reponame,
		}
	} else {
		val = Value{
			Size:      size,
			KeyExpire: usrname,
		}

	}

	if err := cache.SliceLST.SetWithExpire(key, val, DefaultTTL); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot set dgst %s: %v\n", dgst, err)
		return err
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

func (cache *BlobCache) GetSlice(usrname string, reponame string, dgst string) ([]byte, bool) {
	key := SliceHashKey(dgst)
	var val Value

	if TYPE_EXPIARTION == TYPE_EXPIARTION_REPO {
		val = Value{
			Size:      0,
			KeyExpire: reponame,
		}
	} else {
		val = Value{
			Size:      0,
			KeyExpire: usrname,
		}

	}
	if _, err := cache.SliceLST.Get(key, val); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot get dgst %s: %v\n", dgst, err)
	}

	bss, err := cache.DiskCache.Read(key)
	if err != nil {
		fmt.Printf("NANNAN: BlobCache cannot get dgst %s: %v\n", dgst, err)
		return nil, false
	}
	return bss, true
}

func (cache *BlobCache) SetFile(usrname string, reponame string, dgst string, bss []byte) bool {
	key := FileHashKey(dgst)
	size := len(bss)
	var val Value

	if TYPE_EXPIARTION == TYPE_EXPIARTION_REPO {
		val = Value{
			Size:      size,
			KeyExpire: reponame,
		}
	} else {
		val = Value{
			Size:      size,
			KeyExpire: usrname,
		}

	}

	if err := cache.FileLST.SetWithExpire(key, val, DefaultTTL); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot set dgst %s: %v\n", dgst, err)
		return err
	}

	if err := cache.MemCache.Set(key, bss); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot set dgst %s: %v\n", dgst, err)
		return false
	}
	return true
}

func (cache *BlobCache) GetFile(usrname string, reponame string, dgst string) ([]byte, bool) {
	key := FileHashKey(dgst)
	var val Value

	if TYPE_EXPIARTION == TYPE_EXPIARTION_REPO {
		val = Value{
			Size:      0,
			KeyExpire: reponame,
		}
	} else {
		val = Value{
			Size:      0,
			KeyExpire: usrname,
		}

	}
	if _, err := cache.FileLST.Get(key, val); err != nil {
		fmt.Printf("NANNAN: BlobCache cannot get dgst %s: %v\n", dgst, err)
	}

	bss, err := cache.MemCache.Get(key)
	if err != nil {
		fmt.Printf("NANNAN: BlobCache cannot get dgst %s: %v\n", dgst, err)
		return nil, false
	}
	return bss, true
}
