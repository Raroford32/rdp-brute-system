package services

import (
	"container/heap"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"rdp-brute-system/server/database"
	"rdp-brute-system/server/models"
	"rdp-brute-system/shared/protocol"
)

// WorkPoolService manages the global work pool with work stealing and priority queue
type WorkPoolService struct {
	db                *database.DB
	distributionSvc   *DistributionService
	
	// Work pool tracking
	globalWorkPool    *GlobalWorkPool
	workPoolMu        sync.RWMutex
	
	// Work stealing
	workStealingEnabled bool
	stealingThreshold   float64 // % of work imbalance to trigger stealing
	
	// Priority queue
	priorityQueue     *PriorityWorkQueue
	priorityMu        sync.Mutex
	
	// Progress tracking
	progressTracker   *ProgressTracker
	
	// Metrics
	metrics           *WorkPoolMetrics
	
	stopChan          chan bool
}

// GlobalWorkPool tracks all pending work
type GlobalWorkPool struct {
	pendingTargets    map[int]*WorkItem     // targetID -> WorkItem
	assignedTargets   map[int]string        // targetID -> clientID
	clientWorkload    map[string]*ClientWorkload
	totalCombinations int64
	mu                sync.RWMutex
}

// WorkItem represents a unit of work
type WorkItem struct {
	TargetID     int
	IP           string
	Port         int
	Priority     int
	AddedAt      time.Time
	Combinations int // Number of username/password combinations
	Progress     float64
	Attempts     int
}

// ClientWorkload tracks work assigned to a client
type ClientWorkload struct {
	ClientID         string
	AssignedTargets  []int
	TotalCombinations int64
	CompletedWork    int64
	LastUpdate       time.Time
	AvgPPS           float64
	EstimatedTimeLeft time.Duration
}

// PriorityWorkQueue implements a priority queue for work items
type PriorityWorkQueue struct {
	items    []*WorkItem
	itemMap  map[int]*WorkItem // targetID -> WorkItem for fast lookup
}

// ProgressTracker tracks real-time progress
type ProgressTracker struct {
	targetProgress   map[int]*TargetProgress
	clientProgress   map[string]*ClientProgress
	mu               sync.RWMutex
	lastUpdate       time.Time
}

// TargetProgress tracks progress for a specific target
type TargetProgress struct {
	TargetID         int
	TotalCombinations int
	TestedCombinations int
	SuccessfulAttempts int
	LastUpdate       time.Time
	EstimatedCompletion time.Time
}

// ClientProgress tracks client-specific progress
type ClientProgress struct {
	ClientID          string
	ActiveTargets     int
	CompletionRate    float64
	IdleTime          time.Duration
	LastWorkRequest   time.Time
	WorkStealingCount int
}

// WorkPoolMetrics tracks pool performance
type WorkPoolMetrics struct {
	TotalWork            int64
	CompletedWork        int64
	ActiveClients        int32
	IdleClients          int32
	WorkStealingEvents   int64
	AvgWorkDistribution  float64
	LoadBalanceScore     float64
}

// NewWorkPoolService creates a new work pool service
func NewWorkPoolService(db *database.DB, distributionSvc *DistributionService) *WorkPoolService {
	wps := &WorkPoolService{
		db:                  db,
		distributionSvc:     distributionSvc,
		workStealingEnabled: true,
		stealingThreshold:   0.3, // 30% imbalance triggers stealing
		stopChan:           make(chan bool),
		globalWorkPool: &GlobalWorkPool{
			pendingTargets:  make(map[int]*WorkItem),
			assignedTargets: make(map[int]string),
			clientWorkload:  make(map[string]*ClientWorkload),
		},
		priorityQueue: &PriorityWorkQueue{
			items:   make([]*WorkItem, 0),
			itemMap: make(map[int]*WorkItem),
		},
		progressTracker: &ProgressTracker{
			targetProgress: make(map[int]*TargetProgress),
			clientProgress: make(map[string]*ClientProgress),
			lastUpdate:     time.Now(),
		},
		metrics: &WorkPoolMetrics{},
	}
	
	// Initialize priority queue as a heap
	heap.Init(wps.priorityQueue)
	
	return wps
}

// Start begins the work pool service
func (wps *WorkPoolService) Start() {
	log.Println("Work pool service started with work stealing and priority queue")
	
	// Start work pool monitoring
	go wps.monitorWorkPool()
	
	// Start work stealing routine
	go wps.workStealingRoutine()
	
	// Start progress tracking
	go wps.progressTrackingRoutine()
	
	// Start metrics collection
	go wps.metricsRoutine()
}

// Stop gracefully shuts down the work pool service
func (wps *WorkPoolService) Stop() {
	log.Println("Stopping work pool service")
	close(wps.stopChan)
}

// AddWork adds new work items to the pool
func (wps *WorkPoolService) AddWork(targets []models.Target, priority int) error {
	wps.globalWorkPool.mu.Lock()
	defer wps.globalWorkPool.mu.Unlock()
	
	// Get credential count for combination calculation
	userCount, err := wps.db.Query("SELECT COUNT(*) FROM credentials WHERE type = 'username'")
	if err != nil {
		return err
	}
	defer userCount.Close()
	
	var uCount int
	if userCount.Next() {
		userCount.Scan(&uCount)
	}
	
	passCount, err := wps.db.Query("SELECT COUNT(*) FROM credentials WHERE type = 'password'")
	if err != nil {
		return err
	}
	defer passCount.Close()
	
	var pCount int
	if passCount.Next() {
		passCount.Scan(&pCount)
	}
	
	combinations := uCount * pCount
	
	// Add targets to work pool
	for _, target := range targets {
		workItem := &WorkItem{
			TargetID:     target.ID,
			IP:           target.IP,
			Port:         target.Port,
			Priority:     priority,
			AddedAt:      time.Now(),
			Combinations: combinations,
			Progress:     0,
			Attempts:     target.Attempts,
		}
		
		wps.globalWorkPool.pendingTargets[target.ID] = workItem
		atomic.AddInt64(&wps.globalWorkPool.totalCombinations, int64(combinations))
		
		// Add to priority queue
		wps.priorityMu.Lock()
		heap.Push(wps.priorityQueue, workItem)
		wps.priorityMu.Unlock()
	}
	
	log.Printf("Added %d targets to work pool (total combinations: %d)", 
		len(targets), wps.globalWorkPool.totalCombinations)
	
	return nil
}

// GetWork retrieves work for a client with priority and load balancing
func (wps *WorkPoolService) GetWork(clientID string, requestedSize int) ([]*WorkItem, error) {
	wps.globalWorkPool.mu.Lock()
	defer wps.globalWorkPool.mu.Unlock()
	
	// Initialize client workload if needed
	if _, exists := wps.globalWorkPool.clientWorkload[clientID]; !exists {
		wps.globalWorkPool.clientWorkload[clientID] = &ClientWorkload{
			ClientID:        clientID,
			AssignedTargets: make([]int, 0),
			LastUpdate:      time.Now(),
		}
	}
	
	clientWorkload := wps.globalWorkPool.clientWorkload[clientID]
	
	// Check if client is overloaded
	if wps.isClientOverloaded(clientID) {
		log.Printf("Client %s is overloaded, not assigning new work", clientID)
		return nil, nil
	}
	
	// Get work from priority queue
	workItems := make([]*WorkItem, 0, requestedSize)
	
	wps.priorityMu.Lock()
	defer wps.priorityMu.Unlock()
	
	for i := 0; i < requestedSize && wps.priorityQueue.Len() > 0; i++ {
		// Pop highest priority item
		item := heap.Pop(wps.priorityQueue).(*WorkItem)
		
		// Verify it's still pending
		if pendingItem, exists := wps.globalWorkPool.pendingTargets[item.TargetID]; exists {
			workItems = append(workItems, pendingItem)
			
			// Move from pending to assigned
			delete(wps.globalWorkPool.pendingTargets, item.TargetID)
			wps.globalWorkPool.assignedTargets[item.TargetID] = clientID
			
			// Update client workload
			clientWorkload.AssignedTargets = append(clientWorkload.AssignedTargets, item.TargetID)
			clientWorkload.TotalCombinations += int64(item.Combinations)
		}
	}
	
	if len(workItems) > 0 {
		clientWorkload.LastUpdate = time.Now()
		log.Printf("Assigned %d work items to client %s", len(workItems), clientID)
		
		// Update progress tracker
		wps.updateClientProgress(clientID, len(workItems))
	}
	
	return workItems, nil
}

// StealWork implements work stealing for idle clients
func (wps *WorkPoolService) StealWork(idleClientID string) ([]*WorkItem, error) {
	wps.globalWorkPool.mu.Lock()
	defer wps.globalWorkPool.mu.Unlock()
	
	if !wps.workStealingEnabled {
		return nil, nil
	}
	
	// Find the most loaded client
	var mostLoadedClient string
	var maxWork int64
	
	for clientID, workload := range wps.globalWorkPool.clientWorkload {
		if clientID != idleClientID && workload.TotalCombinations > maxWork {
			mostLoadedClient = clientID
			maxWork = workload.TotalCombinations
		}
	}
	
	if mostLoadedClient == "" || maxWork == 0 {
		return nil, nil
	}
	
	// Calculate work imbalance
	idleWorkload := wps.globalWorkPool.clientWorkload[idleClientID]
	imbalance := float64(maxWork - idleWorkload.TotalCombinations) / float64(maxWork)
	
	if imbalance < wps.stealingThreshold {
		return nil, nil // Not enough imbalance to justify stealing
	}
	
	// Steal up to 50% of the difference
	targetSteal := int((maxWork - idleWorkload.TotalCombinations) / 2)
	
	// Find targets to steal
	stolenItems := make([]*WorkItem, 0)
	loadedWorkload := wps.globalWorkPool.clientWorkload[mostLoadedClient]
	
	for i := len(loadedWorkload.AssignedTargets) - 1; i >= 0 && targetSteal > 0; i-- {
		targetID := loadedWorkload.AssignedTargets[i]
		
		// Move target from loaded client to idle client
		wps.globalWorkPool.assignedTargets[targetID] = idleClientID
		
		// Update workloads
		loadedWorkload.AssignedTargets = append(loadedWorkload.AssignedTargets[:i], 
			loadedWorkload.AssignedTargets[i+1:]...)
		idleWorkload.AssignedTargets = append(idleWorkload.AssignedTargets, targetID)
		
		// Create work item (would need to fetch from DB in real implementation)
		workItem := &WorkItem{
			TargetID: targetID,
			Priority: 2, // Medium priority for stolen work
		}
		stolenItems = append(stolenItems, workItem)
		
		targetSteal--
		atomic.AddInt64(&wps.metrics.WorkStealingEvents, 1)
	}
	
	if len(stolenItems) > 0 {
		log.Printf("Client %s stole %d work items from client %s", 
			idleClientID, len(stolenItems), mostLoadedClient)
		
		// Update progress tracker
		wps.progressTracker.mu.Lock()
		if cp, exists := wps.progressTracker.clientProgress[idleClientID]; exists {
			cp.WorkStealingCount++
		}
		wps.progressTracker.mu.Unlock()
	}
	
	return stolenItems, nil
}

// UpdateProgress updates progress for a specific target
func (wps *WorkPoolService) UpdateProgress(clientID string, targetID int, 
	testedCombinations int, successfulAttempts int) {
	
	wps.progressTracker.mu.Lock()
	defer wps.progressTracker.mu.Unlock()
	
	// Update target progress
	tp, exists := wps.progressTracker.targetProgress[targetID]
	if !exists {
		tp = &TargetProgress{
			TargetID: targetID,
		}
		wps.progressTracker.targetProgress[targetID] = tp
	}
	
	tp.TestedCombinations += testedCombinations
	tp.SuccessfulAttempts += successfulAttempts
	tp.LastUpdate = time.Now()
	
	// Calculate estimated completion
	if tp.TotalCombinations > 0 && tp.TestedCombinations > 0 {
		progress := float64(tp.TestedCombinations) / float64(tp.TotalCombinations)
		if progress > 0 {
			elapsed := time.Since(tp.LastUpdate)
			remaining := elapsed / time.Duration(progress) * time.Duration(1-progress)
			tp.EstimatedCompletion = time.Now().Add(remaining)
		}
	}
	
	// Update client workload
	wps.globalWorkPool.mu.Lock()
	if workload, exists := wps.globalWorkPool.clientWorkload[clientID]; exists {
		workload.CompletedWork += int64(testedCombinations)
		workload.LastUpdate = time.Now()
		
		// Update estimated time left
		if workload.AvgPPS > 0 {
			remainingWork := workload.TotalCombinations - workload.CompletedWork
			workload.EstimatedTimeLeft = time.Duration(float64(remainingWork)/workload.AvgPPS) * time.Second
		}
	}
	wps.globalWorkPool.mu.Unlock()
}

// GetPoolStatus returns the current status of the work pool
func (wps *WorkPoolService) GetPoolStatus() map[string]interface{} {
	wps.globalWorkPool.mu.RLock()
	defer wps.globalWorkPool.mu.RUnlock()
	
	status := map[string]interface{}{
		"pending_targets":      len(wps.globalWorkPool.pendingTargets),
		"assigned_targets":     len(wps.globalWorkPool.assignedTargets),
		"total_combinations":   wps.globalWorkPool.totalCombinations,
		"active_clients":       len(wps.globalWorkPool.clientWorkload),
		"work_stealing_events": atomic.LoadInt64(&wps.metrics.WorkStealingEvents),
	}
	
	// Calculate load distribution
	var minWork, maxWork int64 = int64(^uint64(0) >> 1), 0
	var totalWork int64
	
	for _, workload := range wps.globalWorkPool.clientWorkload {
		work := workload.TotalCombinations
		if work < minWork {
			minWork = work
		}
		if work > maxWork {
			maxWork = work
		}
		totalWork += work
	}
	
	if len(wps.globalWorkPool.clientWorkload) > 0 {
		avgWork := totalWork / int64(len(wps.globalWorkPool.clientWorkload))
		status["avg_work_per_client"] = avgWork
		status["work_imbalance"] = float64(maxWork-minWork) / float64(maxWork)
	}
	
	return status
}

// monitorWorkPool monitors the global work pool health
func (wps *WorkPoolService) monitorWorkPool() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			wps.checkPoolHealth()
		case <-wps.stopChan:
			return
		}
	}
}

// checkPoolHealth checks and logs pool health metrics
func (wps *WorkPoolService) checkPoolHealth() {
	wps.globalWorkPool.mu.RLock()
	pendingCount := len(wps.globalWorkPool.pendingTargets)
	assignedCount := len(wps.globalWorkPool.assignedTargets)
	clientCount := len(wps.globalWorkPool.clientWorkload)
	wps.globalWorkPool.mu.RUnlock()
	
	// Check for low work
	if pendingCount < 100 && pendingCount > 0 {
		log.Printf("Warning: Work pool running low (%d pending, %d assigned targets)", pendingCount, assignedCount)
	}
	
	// Check for idle clients
	idleClients := 0
	wps.progressTracker.mu.RLock()
	for _, cp := range wps.progressTracker.clientProgress {
		if cp.ActiveTargets == 0 && time.Since(cp.LastWorkRequest) > 30*time.Second {
			idleClients++
		}
	}
	wps.progressTracker.mu.RUnlock()
	
	if idleClients > 0 {
		log.Printf("Warning: %d idle clients detected", idleClients)
	}
	
	// Update metrics
	atomic.StoreInt32(&wps.metrics.ActiveClients, int32(clientCount))
	atomic.StoreInt32(&wps.metrics.IdleClients, int32(idleClients))
}

// workStealingRoutine monitors for work stealing opportunities
func (wps *WorkPoolService) workStealingRoutine() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			wps.checkWorkStealing()
		case <-wps.stopChan:
			return
		}
	}
}

// checkWorkStealing checks if work stealing is needed
func (wps *WorkPoolService) checkWorkStealing() {
	wps.globalWorkPool.mu.RLock()
	defer wps.globalWorkPool.mu.RUnlock()
	
	if len(wps.globalWorkPool.clientWorkload) < 2 {
		return // Need at least 2 clients for work stealing
	}
	
	// Find idle and overloaded clients
	var idleClients, overloadedClients []string
	var totalWork int64
	
	for clientID, workload := range wps.globalWorkPool.clientWorkload {
		totalWork += workload.TotalCombinations
		
		if workload.TotalCombinations == 0 {
			idleClients = append(idleClients, clientID)
		} else if wps.isClientOverloaded(clientID) {
			overloadedClients = append(overloadedClients, clientID)
		}
	}
	
	// Trigger work stealing for idle clients
	for _, idleClient := range idleClients {
		if len(overloadedClients) > 0 {
			// Work stealing is handled by GetWork/StealWork methods
			log.Printf("Work stealing opportunity: idle client %s, %d overloaded clients", 
				idleClient, len(overloadedClients))
		}
	}
}

// progressTrackingRoutine updates progress tracking
func (wps *WorkPoolService) progressTrackingRoutine() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			wps.updateProgressMetrics()
		case <-wps.stopChan:
			return
		}
	}
}

// updateProgressMetrics updates progress-related metrics
func (wps *WorkPoolService) updateProgressMetrics() {
	wps.progressTracker.mu.Lock()
	defer wps.progressTracker.mu.Unlock()
	
	now := time.Now()
	wps.progressTracker.lastUpdate = now
	
	// Update client idle times
	for clientID, cp := range wps.progressTracker.clientProgress {
		if cp.ActiveTargets == 0 {
			cp.IdleTime = now.Sub(cp.LastWorkRequest)
		} else {
			cp.IdleTime = 0
		}
		// Update the map with the modified progress
		wps.progressTracker.clientProgress[clientID] = cp
	}
}

// metricsRoutine collects and updates metrics
func (wps *WorkPoolService) metricsRoutine() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			wps.collectMetrics()
		case <-wps.stopChan:
			return
		}
	}
}

// collectMetrics collects current metrics
func (wps *WorkPoolService) collectMetrics() {
	wps.globalWorkPool.mu.RLock()
	
	var totalWork, completedWork int64
	var workDistribution []float64
	
	for _, workload := range wps.globalWorkPool.clientWorkload {
		totalWork += workload.TotalCombinations
		completedWork += workload.CompletedWork
		
		if workload.TotalCombinations > 0 {
			workDistribution = append(workDistribution, 
				float64(workload.TotalCombinations))
		}
	}
	
	wps.globalWorkPool.mu.RUnlock()
	
	// Update metrics
	atomic.StoreInt64(&wps.metrics.TotalWork, totalWork)
	atomic.StoreInt64(&wps.metrics.CompletedWork, completedWork)
	
	// Calculate load balance score (0-1, 1 being perfect balance)
	if len(workDistribution) > 1 {
		var mean, variance float64
		for _, w := range workDistribution {
			mean += w
		}
		mean /= float64(len(workDistribution))
		
		for _, w := range workDistribution {
			variance += (w - mean) * (w - mean)
		}
		variance /= float64(len(workDistribution))
		
		// Normalize to 0-1 score
		if mean > 0 {
			wps.metrics.LoadBalanceScore = 1.0 - (variance / (mean * mean))
		}
	}
}

// Helper methods

func (wps *WorkPoolService) isClientOverloaded(clientID string) bool {
	workload, exists := wps.globalWorkPool.clientWorkload[clientID]
	if !exists {
		return false
	}
	
	// Check if client has too much work relative to their performance
	if workload.AvgPPS > 0 && workload.EstimatedTimeLeft > 30*time.Minute {
		return true
	}
	
	// Check if client hasn't updated in a while (might be struggling)
	if time.Since(workload.LastUpdate) > 5*time.Minute && workload.TotalCombinations > 0 {
		return true
	}
	
	return false
}

func (wps *WorkPoolService) updateClientProgress(clientID string, newTargets int) {
	wps.progressTracker.mu.Lock()
	defer wps.progressTracker.mu.Unlock()
	
	cp, exists := wps.progressTracker.clientProgress[clientID]
	if !exists {
		cp = &ClientProgress{
			ClientID: clientID,
		}
		wps.progressTracker.clientProgress[clientID] = cp
	}
	
	cp.ActiveTargets += newTargets
	cp.LastWorkRequest = time.Now()
	cp.IdleTime = 0
}

// Priority queue implementation

func (pq *PriorityWorkQueue) Len() int { return len(pq.items) }

func (pq *PriorityWorkQueue) Less(i, j int) bool {
	// Higher priority first, then older items
	if pq.items[i].Priority != pq.items[j].Priority {
		return pq.items[i].Priority > pq.items[j].Priority
	}
	return pq.items[i].AddedAt.Before(pq.items[j].AddedAt)
}

func (pq *PriorityWorkQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

func (pq *PriorityWorkQueue) Push(x interface{}) {
	item := x.(*WorkItem)
	pq.items = append(pq.items, item)
	pq.itemMap[item.TargetID] = item
}

func (pq *PriorityWorkQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	pq.items = old[0 : n-1]
	delete(pq.itemMap, item.TargetID)
	return item
}

// WorkItemToTask converts a WorkItem to a protocol.Task
func (wps *WorkPoolService) WorkItemToTask(items []*WorkItem, clientID string) (*protocol.Task, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("no work items provided")
	}
	
	// Convert work items to IPs
	ips := make([]string, len(items))
	for i, item := range items {
		ips[i] = item.IP
	}
	
	// Get credentials (reuse from distribution service logic)
	usernames, passwords, err := wps.distributionSvc.getOptimizedCredentials(50)
	if err != nil {
		return nil, err
	}
	
	task := &protocol.Task{
		ID:        fmt.Sprintf("wp-%s-%d", clientID, time.Now().Unix()),
		IPs:       ips,
		Usernames: usernames,
		Passwords: passwords,
		Priority:  items[0].Priority, // Use first item's priority
	}
	
	return task, nil
}
