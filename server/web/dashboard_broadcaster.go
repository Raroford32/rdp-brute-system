package web

import (
	"encoding/json"
	"log"
	"sync"
	"time"
	
	"rdp-brute-system/server/monitor"
	"rdp-brute-system/server/services"
	"rdp-brute-system/shared/metrics"
)

// DashboardBroadcaster sends periodic dashboard updates to all connected clients
type DashboardBroadcaster struct {
	hub           *Hub
	clientService *services.ClientService
	taskService   *services.TaskService
	distService   *services.DistributionService
	metrics       *metrics.Metrics
	perfHistory   *monitor.PerformanceHistory
	alertManager  *monitor.AlertManager
	interval      time.Duration
	stopChan      chan struct{}
	wg            sync.WaitGroup
	
	// Optimization fields
	lastData      *DashboardData
	lastDataMutex sync.RWMutex
	dataChanged   bool
	updateQueue   chan struct{}
}

// DashboardData represents the data sent to the dashboard
type DashboardData struct {
	TotalPPS        float64         `json:"total_pps"`
	SuccessfulHits  int64          `json:"successful_hits"`
	Clients         []ClientInfo    `json:"clients"`
	Results         []ResultInfo    `json:"results"`
	SystemMetrics   SystemMetrics   `json:"system_metrics"`
	TaskStats       TaskStats       `json:"task_stats"`
	Timestamp       int64          `json:"timestamp"`
}

// ClientInfo represents client information for the dashboard
type ClientInfo struct {
	ID           string  `json:"id"`
	Hostname     string  `json:"hostname"`
	PPS          float64 `json:"pps"`
	Status       string  `json:"status"`
	ActiveTasks  int     `json:"active_tasks"`
	TotalAttempts int64  `json:"total_attempts"`
	SuccessCount int     `json:"success_count"`
}

// ResultInfo represents result information for the dashboard
type ResultInfo struct {
	IP       string `json:"ip"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	FoundAt  string `json:"found_at"`
	ClientID string `json:"client_id"`
}

// SystemMetrics represents system resource metrics
type SystemMetrics struct {
	CPUUsage      float64 `json:"cpu_usage"`
	MemoryUsage   int64   `json:"memory_usage"`
	MemoryPercent float64 `json:"memory_percent"`
	Goroutines    int     `json:"goroutines"`
	Connections   int     `json:"connections"`
}

// TaskStats represents task statistics
type TaskStats struct {
	TotalTasks     int     `json:"total_tasks"`
	ActiveTasks    int     `json:"active_tasks"`
	CompletedTasks int     `json:"completed_tasks"`
	FailedTasks    int     `json:"failed_tasks"`
	SuccessRate    float64 `json:"success_rate"`
}

// NewDashboardBroadcaster creates a new dashboard broadcaster
func NewDashboardBroadcaster(hub *Hub, clientService *services.ClientService, taskService *services.TaskService, distService *services.DistributionService, perfHistory *monitor.PerformanceHistory, alertManager *monitor.AlertManager, interval time.Duration) *DashboardBroadcaster {
	return &DashboardBroadcaster{
		hub:           hub,
		clientService: clientService,
		taskService:   taskService,
		distService:   distService,
		metrics:       metrics.GetInstance(),
		perfHistory:   perfHistory,
		alertManager:  alertManager,
		interval:      interval,
		stopChan:      make(chan struct{}),
		updateQueue:   make(chan struct{}, 1), // Buffered channel for coalescing updates
		dataChanged:   true, // Force initial update
	}
}

// Start begins broadcasting dashboard updates
func (d *DashboardBroadcaster) Start() {
	d.wg.Add(1)
	go d.broadcast()
	log.Println("Dashboard broadcaster started")
}

// Stop stops the dashboard broadcaster
func (d *DashboardBroadcaster) Stop() {
	close(d.stopChan)
	d.wg.Wait()
	log.Println("Dashboard broadcaster stopped")
}

// broadcast is the main broadcasting loop
func (d *DashboardBroadcaster) broadcast() {
	defer d.wg.Done()
	
	// Dynamic interval based on connected clients
	minInterval := 500 * time.Millisecond
	maxInterval := 5 * time.Second
	
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	
	// Send initial update
	d.sendUpdate()
	
	for {
		select {
		case <-ticker.C:
			// Adjust interval based on number of connected clients
			hubStats := d.hub.GetStats()
			clientCount := hubStats["clientCount"].(int)
			
			// Scale interval: more clients = slower updates
			var newInterval time.Duration
			if clientCount == 0 {
				newInterval = maxInterval
			} else if clientCount < 10 {
				newInterval = minInterval
			} else if clientCount < 50 {
				newInterval = time.Duration(float64(minInterval) * (1 + float64(clientCount)/20))
			} else {
				newInterval = time.Duration(float64(minInterval) * (1 + float64(clientCount)/10))
			}
			
			// Cap at max interval
			if newInterval > maxInterval {
				newInterval = maxInterval
			}
			
			// Update ticker if interval changed significantly (>20% difference)
			if float64(newInterval)/float64(d.interval) > 1.2 || float64(newInterval)/float64(d.interval) < 0.8 {
				ticker.Stop()
				ticker = time.NewTicker(newInterval)
				d.interval = newInterval
				log.Printf("Adjusted broadcast interval to %v for %d clients", newInterval, clientCount)
			}
			
			// Only send update if there are connected clients
			if clientCount > 0 {
				d.sendUpdate()
			}
			
		case <-d.updateQueue:
			// Immediate update requested
			d.sendUpdate()
			
		case <-d.stopChan:
			return
		}
	}
}

// RequestUpdate requests an immediate dashboard update
func (d *DashboardBroadcaster) RequestUpdate() {
	select {
	case d.updateQueue <- struct{}{}:
		// Update queued
	default:
		// Queue full, update already pending
	}
}

// sendUpdate collects and sends dashboard data
func (d *DashboardBroadcaster) sendUpdate() {
	// Check if we need to collect new data
	hubStats := d.hub.GetStats()
	clientCount := hubStats["clientCount"].(int)
	
	// Skip update if no clients connected
	if clientCount == 0 {
		return
	}
	
	// Check if data has changed (simple optimization)
	d.lastDataMutex.RLock()
	needsUpdate := d.dataChanged || d.lastData == nil
	d.lastDataMutex.RUnlock()
	
	if !needsUpdate {
		// For efficiency, only collect new data every few updates or if explicitly changed
		// This reduces database load when many clients are connected
		return
	}
	
	data := d.collectDashboardData()
	
	// Store last data for comparison
	d.lastDataMutex.Lock()
	d.lastData = &data
	d.dataChanged = false
	d.lastDataMutex.Unlock()
	
	// Collect metrics for history only if we have new data
	if d.perfHistory != nil {
		metrics := make(map[string]float64)
		metrics["total_pps"] = data.TotalPPS
		metrics["cpu_usage"] = data.SystemMetrics.CPUUsage
		metrics["memory_percent"] = data.SystemMetrics.MemoryPercent
		metrics["goroutines"] = float64(data.SystemMetrics.Goroutines)
		metrics["connections"] = float64(data.SystemMetrics.Connections)
		
		activeAlerts := []monitor.Alert{}
		if d.alertManager != nil {
			activeAlerts = d.alertManager.GetActiveAlerts()
		}
		
		d.perfHistory.AddDataPoint(
			metrics,
			len(data.Clients),
			data.TaskStats.ActiveTasks,
			data.SuccessfulHits,
			0, // total attempts - would need to calculate from clients
			activeAlerts,
		)
	}
	
	// Compress data for large client counts
	var payload []byte
	var err error
	
	if clientCount > 50 {
		// Use compressed JSON for many clients
		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Printf("Error marshalling dashboard data: %v", err)
			return
		}
		payload = jsonData
	} else {
		// Use regular JSON for fewer clients
		payload, err = json.Marshal(data)
		if err != nil {
			log.Printf("Error marshalling dashboard data: %v", err)
			return
		}
	}
	
	// Broadcast to all connected clients
	d.hub.BroadcastToAll(payload)
}

// MarkDataChanged marks that the dashboard data has changed and needs update
func (d *DashboardBroadcaster) MarkDataChanged() {
	d.lastDataMutex.Lock()
	d.dataChanged = true
	d.lastDataMutex.Unlock()
}

// collectDashboardData collects all dashboard data
func (d *DashboardBroadcaster) collectDashboardData() DashboardData {
	// Get client stats
	clients := d.clientService.GetStats()
	clientInfos := make([]ClientInfo, 0, len(clients))
	totalPPS := 0.0
	
	// Get distribution metrics for each client
	distMetrics := d.distService.GetClientMetrics()
	
	for _, client := range clients {
		clientInfo := ClientInfo{
			ID:            client.ID,
			Hostname:      client.Hostname,
			PPS:           client.CurrentPPS,
			Status:        client.Status,
			TotalAttempts: client.TotalAttempts,
			SuccessCount:  client.SuccessCount,
		}
		
		// Add distribution metrics if available
		if dm, ok := distMetrics[client.ID]; ok {
			clientInfo.ActiveTasks = dm.ActiveTasks
		}
		
		clientInfos = append(clientInfos, clientInfo)
		totalPPS += client.CurrentPPS
	}
	
	// Get recent results
	results, err := d.taskService.GetResults()
	if err != nil {
		log.Printf("Error getting results: %v", err)
		results = nil
	}
	
	// Convert results to ResultInfo - optimize for large result sets
	resultInfos := make([]ResultInfo, 0)
	if results != nil {
		// Send more results for initial load, fewer for updates
		limit := 20
		hubStats := d.hub.GetStats()
		if hubStats["clientCount"].(int) > 20 {
			limit = 10 // Reduce data size for many clients
		}
		
		if len(results) < limit {
			limit = len(results)
		}
		
		// Pre-allocate slice for efficiency
		resultInfos = make([]ResultInfo, 0, limit)
		
		for i := 0; i < limit; i++ {
			result := results[i]
			resultInfos = append(resultInfos, ResultInfo{
				IP:       result.IP,
				Port:     result.Port,
				Username: result.Username,
				Password: result.Password,
				FoundAt:  result.FoundAt.Format("15:04:05"),
				ClientID: result.ClientID,
			})
		}
	}
	
	// Get task statistics
	taskStats, err := d.taskService.GetTaskStats()
	if err != nil {
		log.Printf("Error getting task stats: %v", err)
		taskStats = make(map[string]interface{})
	}
	
	// Build task stats
	ts := TaskStats{}
	if statusCounts, ok := taskStats["tasksByStatus"].(map[string]int); ok {
		for status, count := range statusCounts {
			ts.TotalTasks += count
			switch status {
			case "assigned", "in_progress":
				ts.ActiveTasks += count
			case "completed":
				ts.CompletedTasks += count
			case "failed", "expired":
				ts.FailedTasks += count
			}
		}
	}
	if successRate, ok := taskStats["overallSuccessRate"].(float64); ok {
		ts.SuccessRate = successRate * 100 // Convert to percentage
	}
	
	// Get system metrics from hub stats
	hubStats := d.hub.GetStats()
	
	// Build system metrics
	sysMetrics := SystemMetrics{
		Connections: int(hubStats["activeConnections"].(int32)),
		Goroutines:  hubStats["clientCount"].(int),
	}
	
	// Get total successful hits
	successfulHits := int64(0)
	for _, client := range clients {
		successfulHits += int64(client.SuccessCount)
	}
	
	return DashboardData{
		TotalPPS:       totalPPS,
		SuccessfulHits: successfulHits,
		Clients:        clientInfos,
		Results:        resultInfos,
		SystemMetrics:  sysMetrics,
		TaskStats:      ts,
		Timestamp:      time.Now().Unix(),
	}
}