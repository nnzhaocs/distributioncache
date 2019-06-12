package cache

import (
	"fmt"
	"time"

	"github.com/allegro/bigcache"
	//"github.com/juju/errors"
	"github.com/ngaut/log"
	diskcache "gopkg.in/stash.v1"
	lru "github.com/hashicorp/golang-lru"
)

type MemCache struct {
	mc          *bigcache.BigCache
	dc			*diskcache.Cache	
//	numElements int64
//	readMiss    float32
//	readHit     float32
	arc         bool
	capacity    int64
	disksize	int64
	diskcnt		int
//	entryLimit  int
}

func (cache *MemCache) Init() error {
	config := bigcache.Config{
		Shards:           1024,
//		LifeWindow:       600 * time.Minute,
//		MaxEntrySize:     cache.entryLimit,
		Verbose:          true,
		HardMaxCacheSize: cache.capacity,
		OnRemove:         nil,
	}
	c, err := bigcache.NewBigCache(config)
	if err != nil {
		return err
	}
	cache.mc = c
	cache.dc = &diskcache.New(
		"/docker/registry/v2/diskcache",
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
	cache.capacity = size*1024*1024
	fmt.Printf("Cache Size: %ld\n\n", cache.capacity)
	return nil
}

func (cache *MemCache) SetDiskCacheSize(size int) error {
	cache.diskcache = size*1024*1024
	fmt.Printf("Cache Size: %ld\n\n", cache.diskcache)
	return nil
}

func (cache *MemCache) SetDiskCacheCnt(cnt int) error {
	cache.diskcnt = cnt
	fmt.Printf("Cache Size: %d\n\n", cache.diskcnt)
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
