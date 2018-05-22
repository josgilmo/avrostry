package avrostry

import (
	"sync"

	"github.com/linkedin/goavro"
)

type CacheCodec struct {
	sync.RWMutex
	cache map[string]*goavro.Codec
}

func NewCacheCodec() *CacheCodec {
	return &CacheCodec{cache: map[string]*goavro.Codec{}}
}

func (c *CacheCodec) Get(schema string) (*goavro.Codec, error) {
	var (
		err error
	)
	c.RLock()
	codec, exists := c.cache[schema]
	if !exists {
		c.RUnlock()
		c.Lock()
		codec, exists = c.cache[schema]
		if !exists {
			codec, err = goavro.NewCodec(schema)
			if err == nil {
				c.cache[schema] = codec
			}
		}
		c.Unlock()
		c.RLock()
	}
	c.RUnlock()
	return codec, err
}
