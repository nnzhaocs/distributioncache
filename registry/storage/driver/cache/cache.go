package cache

import (
	"fmt"

	"github.com/allegro/bigcache"
	//"github.com/juju/errors"
	//	diskcache "gopkg.in/stash.v1"
	//lru "github.com/hashicorp/golang-lru"
)

type MemCache struct {
	Mc *bigcache.BigCache
	Dc *bigcache.BigCache
	//	Dc *diskcache.Cache
	//	numElements int64
	//	readMiss    float32
	//	readHit     float32
	arc      bool
	capacity int
	disksize int
	diskcnt  int
	//	entryLimit  int
}

func (cache *MemCache) Init() error {
	config := bigcache.Config{
		Shards: 8,
		//LifeWindow:       600 * time.Minute,
		//MaxEntrySize:     500 * 1024 * 1024,
		Verbose:          true,
		HardMaxCacheSize: cache.disksize,
		OnRemove:         nil,
	}
	c, err := bigcache.NewBigCache(config)
	if err != nil {
		return err
	}
	cache.Mc = c
	//	dc, err := diskcache.New(
	//		"/var/lib/registry/docker/registry/v2/pull_tars/diskcache/",
	//		cache.disksize,
	//		int64(cache.diskcnt))

	config2 := bigcache.Config{
		Shards: 2,
		//LifeWindow:       600 * time.Minute,
		//MaxEntrySize:     500 * 1024 * 1024,
		Verbose:          true,
		HardMaxCacheSize: cache.capacity,
		OnRemove:         nil,
	}
	dc, err := bigcache.NewBigCache(config2)
	if err != nil {
		return err
	}

	cache.Dc = dc
	fmt.Printf("NANNAN: ====================> cache capacity: %d MB, %d MB, and %d =================> \n\n",
		cache.capacity,
		cache.disksize,
		cache.diskcnt)

	return err
}

//func (cache *MemCache) GetEntryLimit() int {
//	return cache.entryLimit
//}

func (cache *MemCache) SetType(t string) error {
	switch t {
	case "arc":
		cache.arc = true
	default:
		cache.arc = false
	}
	fmt.Printf("cache type: %s\n\n", t)
	return nil
}

func (cache *MemCache) SetSize(size int) error {
	cache.capacity = size //* 1024 * 1024
	fmt.Printf("Cache Size: %d MB\n\n", cache.capacity)
	return nil
}

func (cache *MemCache) SetDiskCacheSize(size int) error {
	cache.disksize = size // * 1024 * 1024)
	fmt.Printf("Disk cache Size: %d\n\n", cache.disksize)
	return nil
}

func (cache *MemCache) SetDiskCacheCnt(cnt int) error {
	cache.diskcnt = cnt
	fmt.Printf("Disk cnt: %d\n\n", cache.diskcnt)
	return nil
}

//func (cache *MemCache) SetEntrylimit(entryLimit int) error {
//	cache.entryLimit = entryLimit * 1024 * 1024
//	fmt.Printf("CacheSize: %d\n\n", cache.entryLimit)
//	return nil
//}

//func (cache *MemCache) Set(k string, v []byte) {
//	err := cache.mc.Set(k, v)
//	if err != nil {
//		log.Debugf("ali:cache set failed on %s", k)
//	}
//	cache.numElements++
//}
//
//func (cache *MemCache) Get(k string) ([]byte, error) {
//	v, err := cache.mc.Get(k)
//	if err != nil {
//		//return nil, errors.Trace(err)
//		cache.readMiss++
//	} else {
//		//		log.Debugf("ali:cache get v=%s", v)
//		cache.readHit++
//	}
//	return v, nil
//}

//func (cache *MemCache) GetRHR() float32 {
//	if cache.readHit == 0 && cache.readMiss == 0 {
//		return 0
//	}
//	return cache.readHit / (cache.readHit + cache.readMiss)
//}
//
//func (cache *MemCache) GetNumElem() int64 {
//	return cache.numElements
//}

// ali: cache size in MB
//func Init(maxSize int) *MemCache {
//	config := bigcache.Config{
//		Shards:           1024,
//		LifeWindow:       600 * time.Minute,
//		MaxEntrySize:     1024 * 1024,
//		Verbose:          true,
//		HardMaxCacheSize: maxSize,
//		OnRemove:         nil,
//	}
//	cache, err := bigcache.NewBigCache(config)
//	if err != nil {
//		return nil
//	}
//
//	log.Debugf("ali:initialized mem cache with %v MB memory", maxSize)
//	return &MemCache{
//		mc:          cache,
//		numElements: 0,
//		readMiss:    0.0,
//		readHit:     0.0,
//		arc:         true,
//		capacity:    0,
//		entryLimit:  0,
//	}
//}
