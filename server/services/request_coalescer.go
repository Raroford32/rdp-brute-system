package services

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// RequestCoalescer coalesces multiple similar requests to reduce database load
type RequestCoalescer struct {
	// Coalescing windows
	windows      map[string]*CoalescingWindow
	windowsMu    sync.RWMutex
	windowTTL    time.Duration
	maxBatchSize int
	
	// Cleanup
	cleanupTicker *time.Ticker
	stopChan      chan bool
}

// CoalescingWindow represents a time window for coalescing requests
type CoalescingWindow struct {
	key         string
	requests    []CoalescedRequest
	mu          sync.Mutex
	createdAt   time.Time
	timer       *time.Timer
	processing  bool
	resultChan  chan interface{}
	errorChan   chan error
}

// CoalescedRequest represents a request that can be coalesced
type CoalescedRequest struct {
	ID         string
	ResultChan chan interface{}
	ErrorChan  chan error
}

// CoalescingFunc is a function that processes a batch of coalesced requests
type CoalescingFunc func(requests []CoalescedRequest) (map[string]interface{}, error)

// NewRequestCoalescer creates a new request coalescer
func NewRequestCoalescer(windowTTL time.Duration, maxBatchSize int) *RequestCoalescer {
	rc := &RequestCoalescer{
		windows:       make(map[string]*CoalescingWindow),
		windowTTL:     windowTTL,
		maxBatchSize:  maxBatchSize,
		cleanupTicker: time.NewTicker(1 * time.Minute),
		stopChan:      make(chan bool),
	}
	
	go rc.cleanupRoutine()
	
	return rc
}

// Coalesce adds a request to be coalesced
func (rc *RequestCoalescer) Coalesce(key string, requestID string, processor CoalescingFunc) (interface{}, error) {
	rc.windowsMu.Lock()
	window, exists := rc.windows[key]
	if !exists {
		window = &CoalescingWindow{
			key:        key,
			requests:   make([]CoalescedRequest, 0, rc.maxBatchSize),
			createdAt:  time.Now(),
			resultChan: make(chan interface{}, 1),
			errorChan:  make(chan error, 1),
		}
		rc.windows[key] = window
		
		// Set timer to process window after TTL
		window.timer = time.AfterFunc(rc.windowTTL, func() {
			rc.processWindow(key, processor)
		})
	}
	rc.windowsMu.Unlock()
	
	// Create channels for this request
	resultChan := make(chan interface{}, 1)
	errorChan := make(chan error, 1)
	
	// Add request to window
	window.mu.Lock()
	if window.processing {
		// Window is already being processed, create new window
		window.mu.Unlock()
		return rc.Coalesce(key+"-next", requestID, processor)
	}
	
	window.requests = append(window.requests, CoalescedRequest{
		ID:         requestID,
		ResultChan: resultChan,
		ErrorChan:  errorChan,
	})
	
	// Check if window is full
	if len(window.requests) >= rc.maxBatchSize {
		window.processing = true
		window.mu.Unlock()
		
		// Cancel timer and process immediately
		window.timer.Stop()
		go rc.processWindow(key, processor)
	} else {
		window.mu.Unlock()
	}
	
	// Wait for result
	select {
	case result := <-resultChan:
		return result, nil
	case err := <-errorChan:
		return nil, err
	case <-time.After(rc.windowTTL * 2):
		return nil, ErrCoalescingTimeout
	}
}

// processWindow processes all requests in a window
func (rc *RequestCoalescer) processWindow(key string, processor CoalescingFunc) {
	rc.windowsMu.Lock()
	window, exists := rc.windows[key]
	if !exists {
		rc.windowsMu.Unlock()
		return
	}
	delete(rc.windows, key)
	rc.windowsMu.Unlock()
	
	window.mu.Lock()
	requests := window.requests
	window.mu.Unlock()
	
	if len(requests) == 0 {
		return
	}
	
	// Process batch
	start := time.Now()
	results, err := processor(requests)
	duration := time.Since(start)
	
	if err != nil {
		// Send error to all requests
		for _, req := range requests {
			select {
			case req.ErrorChan <- err:
			default:
			}
		}
		log.Printf("Coalesced batch processing failed for %s: %v", key, err)
		return
	}
	
	// Send results to individual requests
	for _, req := range requests {
		if result, exists := results[req.ID]; exists {
			select {
			case req.ResultChan <- result:
			default:
			}
		} else {
			select {
			case req.ErrorChan <- ErrNoResult:
			default:
			}
		}
	}
	
	log.Printf("Processed coalesced batch for %s: %d requests in %v", key, len(requests), duration)
}

// cleanupRoutine removes old windows
func (rc *RequestCoalescer) cleanupRoutine() {
	for {
		select {
		case <-rc.cleanupTicker.C:
			rc.cleanup()
		case <-rc.stopChan:
			return
		}
	}
}

// cleanup removes expired windows
func (rc *RequestCoalescer) cleanup() {
	rc.windowsMu.Lock()
	defer rc.windowsMu.Unlock()
	
	now := time.Now()
	for key, window := range rc.windows {
		if now.Sub(window.createdAt) > rc.windowTTL*3 {
			window.timer.Stop()
			delete(rc.windows, key)
		}
	}
}

// Stop stops the request coalescer
func (rc *RequestCoalescer) Stop() {
	rc.cleanupTicker.Stop()
	close(rc.stopChan)
	
	// Cancel all pending windows
	rc.windowsMu.Lock()
	for _, window := range rc.windows {
		window.timer.Stop()
	}
	rc.windowsMu.Unlock()
}

// Errors
var (
	ErrCoalescingTimeout = fmt.Errorf("coalescing timeout")
	ErrNoResult          = fmt.Errorf("no result for request")
)

// RequestBatcher provides high-level batching functionality
type RequestBatcher struct {
	coalescer     *RequestCoalescer
	batchHandlers map[string]BatchHandler
	mu            sync.RWMutex
}

// BatchHandler processes a batch of requests
type BatchHandler func(ids []string) (map[string]interface{}, error)

// NewRequestBatcher creates a new request batcher
func NewRequestBatcher() *RequestBatcher {
	return &RequestBatcher{
		coalescer:     NewRequestCoalescer(10*time.Millisecond, 100),
		batchHandlers: make(map[string]BatchHandler),
	}
}

// RegisterHandler registers a batch handler for a request type
func (rb *RequestBatcher) RegisterHandler(requestType string, handler BatchHandler) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.batchHandlers[requestType] = handler
}

// Batch submits a request for batching
func (rb *RequestBatcher) Batch(requestType string, id string) (interface{}, error) {
	rb.mu.RLock()
	handler, exists := rb.batchHandlers[requestType]
	rb.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("no handler for request type: %s", requestType)
	}
	
	// Create processor function
	processor := func(requests []CoalescedRequest) (map[string]interface{}, error) {
		ids := make([]string, len(requests))
		for i, req := range requests {
			ids[i] = req.ID
		}
		return handler(ids)
	}
	
	return rb.coalescer.Coalesce(requestType, id, processor)
}

// Stop stops the request batcher
func (rb *RequestBatcher) Stop() {
	rb.coalescer.Stop()
}