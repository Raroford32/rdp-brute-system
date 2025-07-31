package cache

import (
	"sync"
	"time"
)

// CacheItem represents a single cache entry
type CacheItem struct {
	Value      interface{}
	Expiration int64
}

// Cache represents an in-memory cache with TTL support
type Cache struct {
	mu         sync.RWMutex
	items      map[string]CacheItem
	defaultTTL time.Duration
}

// NewCache creates a new cache instance
func NewCache(defaultTTL time.Duration) *Cache {
	cache := &Cache{
		items:      make(map[string]CacheItem),
		defaultTTL: defaultTTL,
	}
	
	// Start cleanup goroutine
	go cache.cleanupExpired()
	
	return cache
}

// Set adds an item to the cache with the default TTL
func (c *Cache) Set(key string, value interface{}) {
	c.SetWithTTL(key, value, c.defaultTTL)
}

// SetWithTTL adds an item to the cache with a custom TTL
func (c *Cache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	expiration := time.Now().Add(ttl).UnixNano()
	c.items[key] = CacheItem{
		Value:      value,
		Expiration: expiration,
	}
}

// Get retrieves an item from the cache
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	item, found := c.items[key]
	if !found {
		return nil, false
	}
	
	// Check if item has expired
	if time.Now().UnixNano() > item.Expiration {
		return nil, false
	}
	
	return item.Value, true
}

// Delete removes an item from the cache
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	delete(c.items, key)
}

// Clear removes all items from the cache
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.items = make(map[string]CacheItem)
}

// Size returns the number of items in the cache
func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	return len(c.items)
}

// cleanupExpired removes expired items from the cache periodically
func (c *Cache) cleanupExpired() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for range ticker.C {
		c.mu.Lock()
		now := time.Now().UnixNano()
		
		for key, item := range c.items {
			if now > item.Expiration {
				delete(c.items, key)
			}
		}
		
		c.mu.Unlock()
	}
}

// GetOrSet retrieves an item from cache or sets it if not found
func (c *Cache) GetOrSet(key string, getter func() (interface{}, error)) (interface{}, error) {
	// Try to get from cache first
	if value, found := c.Get(key); found {
		return value, nil
	}
	
	// Get the value using the getter function
	value, err := getter()
	if err != nil {
		return nil, err
	}
	
	// Cache the value
	c.Set(key, value)
	
	return value, nil
}

// GetOrSetWithTTL retrieves an item from cache or sets it with custom TTL if not found
func (c *Cache) GetOrSetWithTTL(key string, ttl time.Duration, getter func() (interface{}, error)) (interface{}, error) {
	// Try to get from cache first
	if value, found := c.Get(key); found {
		return value, nil
	}
	
	// Get the value using the getter function
	value, err := getter()
	if err != nil {
		return nil, err
	}
	
	// Cache the value with custom TTL
	c.SetWithTTL(key, value, ttl)
	
	return value, nil
}