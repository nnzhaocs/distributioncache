package cache

import (
	"time"

	"github.com/allegro/bigcache"
	//"github.com/juju/errors"
	"github.com/ngaut/log"
)

type MemCache struct {
	mc          *bigcache.BigCache
	numElements int64
	readMiss    float32
	readHit     float32
}

func (cache *MemCache) Set(k string, v []byte) {
	err := cache.mc.Set(k, v)
	if err != nil {
		log.Debugf("ali:cache set failed on %s", k)
	}
	cache.numElements++
}

func (cache *MemCache) Get(k string) ([]byte, error) {
	v, err := cache.mc.Get(k)
	if err != nil {
		//return nil, errors.Trace(err)
		cache.readMiss++
	} else {
//		log.Debugf("ali:cache get v=%s", v)
		cache.readHit++
	}
	return v, nil
}

func (cache *MemCache) GetRHR() float32 {
	if cache.readHit == 0 && cache.readMiss == 0 {
		return 0
	}
	return cache.readHit / (cache.readHit + cache.readMiss)
}

func (cache *MemCache) GetNumElem() int64 {
	return cache.numElements
}

// ali: cache size in MB
func Init(maxSize int) *MemCache {
	config := bigcache.Config{
		Shards:           1024,
		LifeWindow:       600 * time.Minute,
		MaxEntrySize:     1024 * 1024,
		Verbose:          true,
		HardMaxCacheSize: maxSize,
		OnRemove:         nil,
	}
	cache, err := bigcache.NewBigCache(config)
	if err != nil {
		return nil
	}

	log.Debugf("ali:initialized mem cache with %v MB memory", maxSize)
	return &MemCache{
		mc:          cache,
		numElements: 0,
		readMiss:    0.0,
		readHit:     0.0,
	}
}
