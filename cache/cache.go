package cache

import (
	"sync"
	"time"
)

const (
	NeverExpires time.Duration = -1
)

// Possibly [DataRequest] should be part of the [cache] struct instead of the [Get] function
// This would cause less repetition, but would mean every data request would have a separate cache instance
// It depends on the requirements but wouldn't require big changes so I leave it this way for now
type DataRequest func(string) interface{}

type Cache interface {
	Set(string, interface{})
	Get(string, DataRequest) (data interface{}, cached bool)
	DeleteExpired()
	Clear()
}

type cache struct {
	// Depending on the requirements this could be moved to the [Set]/[Get] methods
	expiresAfter time.Duration
	items        map[string]cachedData
	mu           sync.Mutex
}

type cachedData struct {
	data      interface{}
	expiresAt time.Time
}

func New(expiresAfter time.Duration) Cache {
	return &cache{
		expiresAfter: expiresAfter,
		items:        map[string]cachedData{},
	}
}

func (c *cache) Set(id string, data interface{}) {
	c.mu.Lock()

	c.set(id, data)

	c.mu.Unlock()
}

func (c* cache) set(id string, data interface{}) {
	var expiresAt time.Time
	if c.expiresAfter == NeverExpires {
		// I assume that the server will not function non stop for 99 years but if the requirements are different this could be
		// improved
		expiresAt = time.Now().AddDate(99, 0, 0)
	} else {
		expiresAt = time.Now().Add(c.expiresAfter)
	}

	c.items[id] = cachedData{
		data:      data,
		expiresAt: expiresAt,
	}
}

func (c *cache) Get(id string, databaseRequest DataRequest) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, found := c.items[id]

	if expired := item.expired(); !found || expired {
		data := databaseRequest(id)

		c.set(id, data)

		return data, false
	}

	return item.data, true
}

func (c *cache) DeleteExpired() {
	for key, data := range c.items {
		if data.expired() {
			c.delete(key)
		}
	}
}

func (c *cache) Clear() {
	c.mu.Lock()
	c.items = make(map[string]cachedData)
	c.mu.Unlock()
}

func (c *cache) delete(key string) {
	c.mu.Lock()
	delete(c.items, key)
	c.mu.Unlock()

}

func (d cachedData) expired() bool {
	return d.expiresAt.Before(time.Now())
}
