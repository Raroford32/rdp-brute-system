package cache

import (
	"fmt"
	"sync"
	"time"
)

// QueryCache provides a simple in-memory cache for database query results
type QueryCache struct {
	cache      map[string]*CacheEntry
	mu         sync.RWMutex
	maxSize    int
	defaultTTL time.Duration
	
	// Statistics
	hits       int64
	misses     int64
	evictions  int64
}

// CacheEntry represents a cached item
type CacheEntry struct {
	Value      interface{}
	ExpiresAt  time.Time
	Size       int
	AccessTime time.Time
	HitCount   int
}

// NewQueryCache creates a new query cache
func NewQueryCache(maxSize int, defaultTTL time.Duration) *QueryCache {
	qc := &QueryCache{
		cache:      make(map[string]*CacheEntry),
		maxSize:    maxSize,
		defaultTTL: defaultTTL,
	}
	
	// Start cleanup routine
	go qc.cleanupRoutine()
	
	return qc
}

// Get retrieves a value from the cache
func (qc *QueryCache) Get(key string) (interface{}, bool) {
	qc.mu.RLock()
	entry, exists := qc.cache[key]
	qc.mu.RUnlock()
	
	if !exists {
		qc.incrementMisses()
		return nil, false
	}
	
	// Check expiration
	if time.Now().After(entry.ExpiresAt) {
		qc.Delete(key)
		qc.incrementMisses()
		return nil, false
	}
	
	// Update access stats
	qc.mu.Lock()
	entry.AccessTime = time.Now()
	entry.HitCount++
	qc.mu.Unlock()
	
	qc.incrementHits()
	return entry.Value, true
}

// Set stores a value in the cache with default TTL
func (qc *QueryCache) Set(key string, value interface{}) {
	qc.SetWithTTL(key, value, qc.defaultTTL)
}

// SetWithTTL stores a value in the cache with custom TTL
func (qc *QueryCache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	entry := &CacheEntry{
		Value:      value,
		ExpiresAt:  time.Now().Add(ttl),
		Size:       1, // Simple size calculation
		AccessTime: time.Now(),
		HitCount:   0,
	}
	
	qc.mu.Lock()
	defer qc.mu.Unlock()
	
	// Check if we need to evict entries
	if len(qc.cache) >= qc.maxSize {
		qc.evictLRU()
	}
	
	qc.cache[key] = entry
}

// Delete removes an entry from the cache
func (qc *QueryCache) Delete(key string) {
	qc.mu.Lock()
	delete(qc.cache, key)
	qc.mu.Unlock()
}

// Clear removes all entries from the cache
func (qc *QueryCache) Clear() {
	qc.mu.Lock()
	qc.cache = make(map[string]*CacheEntry)
	qc.mu.Unlock()
}

// GetStats returns cache statistics
func (qc *QueryCache) GetStats() map[string]interface{} {
	qc.mu.RLock()
	size := len(qc.cache)
	qc.mu.RUnlock()
	
	hitRate := float64(0)
	if qc.hits+qc.misses > 0 {
		hitRate = float64(qc.hits) / float64(qc.hits+qc.misses) * 100
	}
	
	return map[string]interface{}{
		"size":       size,
		"max_size":   qc.maxSize,
		"hits":       qc.hits,
		"misses":     qc.misses,
		"evictions":  qc.evictions,
		"hit_rate":   hitRate,
	}
}

// evictLRU evicts the least recently used entry
func (qc *QueryCache) evictLRU() {
	var oldestKey string
	var oldestTime time.Time
	
	for key, entry := range qc.cache {
		if oldestKey == "" || entry.AccessTime.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.AccessTime
		}
	}
	
	if oldestKey != "" {
		delete(qc.cache, oldestKey)
		qc.evictions++
	}
}

// cleanupRoutine periodically removes expired entries
func (qc *QueryCache) cleanupRoutine() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for range ticker.C {
		qc.cleanupExpired()
	}
}

// cleanupExpired removes all expired entries
func (qc *QueryCache) cleanupExpired() {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	
	now := time.Now()
	for key, entry := range qc.cache {
		if now.After(entry.ExpiresAt) {
			delete(qc.cache, key)
		}
	}
}

// incrementHits safely increments hit counter
func (qc *QueryCache) incrementHits() {
	qc.mu.Lock()
	qc.hits++
	qc.mu.Unlock()
}

// incrementMisses safely increments miss counter
func (qc *QueryCache) incrementMisses() {
	qc.mu.Lock()
	qc.misses++
	qc.mu.Unlock()
}

// Common cache key generators

// KeyForTargets generates a cache key for target queries
func KeyForTargets(limit int, priority bool) string {
	if priority {
		return fmt.Sprintf("targets:priority:%d", limit)
	}
	return fmt.Sprintf("targets:simple:%d", limit)
}

// KeyForCredentials generates a cache key for credential queries
func KeyForCredentials(credType string, limit int) string {
	return fmt.Sprintf("credentials:%s:%d", credType, limit)
}

// KeyForClientPerf generates a cache key for client performance
func KeyForClientPerf(clientID string) string {
	return fmt.Sprintf("client:perf:%s", clientID)
}

// KeyForWorkQueueStats generates a cache key for work queue stats
func KeyForWorkQueueStats() string {
	return "stats:workqueue"
}

// KeyForTaskStats generates a cache key for task statistics
func KeyForTaskStats() string {
	return "stats:tasks"
}