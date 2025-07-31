package services

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"rdp-brute-system/server/database"
	"rdp-brute-system/server/models"
	"rdp-brute-system/shared/protocol"
)

// OperationState represents the state of an operation
type OperationState string

const (
	OperationStateIdle      OperationState = "idle"
	OperationStateRunning   OperationState = "running"
	OperationStatePaused    OperationState = "paused"
	OperationStateCompleted OperationState = "completed"
	OperationStateStopped   OperationState = "stopped"
)

// Operation represents a brute-force operation
type Operation struct {
	ID          string          `json:"id" db:"id"`
	Name        string          `json:"name" db:"name"`
	Description string          `json:"description" db:"description"`
	State       OperationState  `json:"state" db:"state"`
	Targets     []string        `json:"targets"`
	Credentials OperationCreds  `json:"credentials"`
	CreatedAt   time.Time       `json:"created_at" db:"created_at"`
	StartedAt   *time.Time      `json:"started_at" db:"started_at"`
	PausedAt    *time.Time      `json:"paused_at" db:"paused_at"`
	CompletedAt *time.Time      `json:"completed_at" db:"completed_at"`
	StoppedAt   *time.Time      `json:"stopped_at" db:"stopped_at"`
	Statistics  OperationStats  `json:"statistics"`
	Settings    OperationSettings `json:"settings"`
}

// OperationCreds contains credential configuration for an operation
type OperationCreds struct {
	Usernames []string `json:"usernames"`
	Passwords []string `json:"passwords"`
}

// OperationStats contains runtime statistics for an operation
type OperationStats struct {
	TotalTargets      int       `json:"total_targets"`
	CompletedTargets  int       `json:"completed_targets"`
	TotalAttempts     int64     `json:"total_attempts"`
	SuccessfulFinds   int       `json:"successful_finds"`
	AveragePPS        float64   `json:"average_pps"`
	ElapsedTime       string    `json:"elapsed_time"`
	EstimatedTimeLeft string    `json:"estimated_time_left"`
	LastUpdate        time.Time `json:"last_update"`
}

// OperationSettings contains configuration for an operation
type OperationSettings struct {
	MaxThreadsPerClient int  `json:"max_threads_per_client"`
	Priority            int  `json:"priority"`
	AllowGracefulStop   bool `json:"allow_graceful_stop"`
}

// OperationService manages brute-force operations
type OperationService struct {
	db                  *database.DB
	distributionService *DistributionService
	taskService         *TaskService
	clientService       *ClientService
	
	// Operation management
	operations      map[string]*Operation
	operationsMu    sync.RWMutex
	activeOperation *Operation
	
	// Context management for graceful shutdown
	ctx            context.Context
	cancel         context.CancelFunc
	
	// Operation state change notifications
	stateChangeChan chan OperationStateChange
	
	// Graceful shutdown tracking
	gracefulStops   map[string]*GracefulStop
	gracefulStopsMu sync.RWMutex
}

// GracefulStop tracks graceful shutdown progress
type GracefulStop struct {
	OperationID      string
	StartedAt        time.Time
	ClientsNotified  int
	ClientsCompleted int
	TasksReclaimed   int
}

// OperationStateChange represents a state change event
type OperationStateChange struct {
	OperationID string
	OldState    OperationState
	NewState    OperationState
	Timestamp   time.Time
}

// NewOperationService creates a new operation service
func NewOperationService(db *database.DB, distributionService *DistributionService, 
	taskService *TaskService, clientService *ClientService) *OperationService {
	
	ctx, cancel := context.WithCancel(context.Background())
	
	return &OperationService{
		db:                  db,
		distributionService: distributionService,
		taskService:         taskService,
		clientService:       clientService,
		operations:          make(map[string]*Operation),
		ctx:                 ctx,
		cancel:              cancel,
		stateChangeChan:     make(chan OperationStateChange, 100),
		gracefulStops:       make(map[string]*GracefulStop),
	}
}

// CreateOperation creates a new operation
func (s *OperationService) CreateOperation(name, description string, targets []string, 
	usernames, passwords []string, settings OperationSettings) (*Operation, error) {
	
	s.operationsMu.Lock()
	defer s.operationsMu.Unlock()
	
	// Validate inputs
	if name == "" {
		return nil, fmt.Errorf("operation name is required")
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("at least one target is required")
	}
	if len(usernames) == 0 || len(passwords) == 0 {
		return nil, fmt.Errorf("credentials are required")
	}
	
	operation := &Operation{
		ID:          uuid.New().String(),
		Name:        name,
		Description: description,
		State:       OperationStateIdle,
		Targets:     targets,
		Credentials: OperationCreds{
			Usernames: usernames,
			Passwords: passwords,
		},
		CreatedAt: time.Now(),
		Statistics: OperationStats{
			TotalTargets: len(targets),
			LastUpdate:   time.Now(),
		},
		Settings: settings,
	}
	
	// Save to database
	if err := s.db.CreateOperation(operation); err != nil {
		return nil, fmt.Errorf("failed to save operation: %v", err)
	}
	
	// Store in memory
	s.operations[operation.ID] = operation
	
	log.Printf("Created operation %s (%s) with %d targets", operation.ID, operation.Name, len(targets))
	
	return operation, nil
}

// StartOperation starts an operation
func (s *OperationService) StartOperation(operationID string) error {
	s.operationsMu.Lock()
	defer s.operationsMu.Unlock()
	
	operation, exists := s.operations[operationID]
	if !exists {
		// Try to load from database
		opData, err := s.db.GetOperation(operationID)
		if err != nil {
			return fmt.Errorf("operation not found: %s", operationID)
		}
		
		// Convert from database format to Operation struct
		opMap, ok := opData.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid operation data format")
		}
		
		operation = &Operation{
			ID:          opMap["id"].(string),
			Name:        opMap["name"].(string),
			Description: opMap["description"].(string),
			State:       OperationState(opMap["state"].(string)),
			// Add other fields as needed
		}
		s.operations[operationID] = operation
	}
	
	// Check if we can start
	if s.activeOperation != nil && s.activeOperation.State == OperationStateRunning {
		return fmt.Errorf("another operation is already running: %s", s.activeOperation.Name)
	}
	
	// Validate state transition
	if operation.State != OperationStateIdle && operation.State != OperationStatePaused {
		return fmt.Errorf("operation cannot be started from state: %s", operation.State)
	}
	
	oldState := operation.State
	operation.State = OperationStateRunning
	now := time.Now()
	operation.StartedAt = &now
	s.activeOperation = operation
	
	// Update database
	if err := s.db.UpdateOperationState(operationID, string(operation.State)); err != nil {
		operation.State = oldState // Rollback
		return fmt.Errorf("failed to update operation state: %v", err)
	}
	
	// Load targets into the system
	if err := s.loadOperationTargets(operation); err != nil {
		operation.State = oldState // Rollback
		s.db.UpdateOperationState(operationID, string(oldState))
		return fmt.Errorf("failed to load targets: %v", err)
	}
	
	// Notify state change
	s.notifyStateChange(operationID, oldState, operation.State)
	
	// Start operation monitoring
	go s.monitorOperation(operation)
	
	log.Printf("Started operation %s (%s)", operation.ID, operation.Name)
	
	return nil
}

// StopOperation stops an operation with optional graceful shutdown
func (s *OperationService) StopOperation(operationID string, graceful bool) error {
	s.operationsMu.Lock()
	defer s.operationsMu.Unlock()
	
	operation, exists := s.operations[operationID]
	if !exists {
		return fmt.Errorf("operation not found: %s", operationID)
	}
	
	// Validate state transition
	if operation.State != OperationStateRunning && operation.State != OperationStatePaused {
		return fmt.Errorf("operation cannot be stopped from state: %s", operation.State)
	}
	
	oldState := operation.State
	
	if graceful && operation.Settings.AllowGracefulStop {
		// Initiate graceful shutdown
		log.Printf("Initiating graceful shutdown for operation %s", operationID)
		
		gracefulStop := &GracefulStop{
			OperationID: operationID,
			StartedAt:   time.Now(),
		}
		
		s.gracefulStopsMu.Lock()
		s.gracefulStops[operationID] = gracefulStop
		s.gracefulStopsMu.Unlock()
		
		// Start graceful shutdown process
		go s.performGracefulShutdown(operation, gracefulStop)
		
		return nil
	}
	
	// Immediate stop
	operation.State = OperationStateStopped
	now := time.Now()
	operation.StoppedAt = &now
	
	if s.activeOperation != nil && s.activeOperation.ID == operationID {
		s.activeOperation = nil
	}
	
	// Update database
	if err := s.db.UpdateOperationState(operationID, string(operation.State)); err != nil {
		operation.State = oldState // Rollback
		return fmt.Errorf("failed to update operation state: %v", err)
	}
	
	// Clear all tasks and targets
	if err := s.clearOperationTasks(operation); err != nil {
		log.Printf("Error clearing operation tasks: %v", err)
	}
	
	// Notify all clients to stop working on this operation
	s.notifyClientsOperationStop(operationID, graceful)
	
	// Notify state change
	s.notifyStateChange(operationID, oldState, operation.State)
	
	log.Printf("Stopped operation %s (%s)", operation.ID, operation.Name)
	
	return nil
}

// PauseOperation pauses an operation
func (s *OperationService) PauseOperation(operationID string) error {
	s.operationsMu.Lock()
	defer s.operationsMu.Unlock()
	
	operation, exists := s.operations[operationID]
	if !exists {
		return fmt.Errorf("operation not found: %s", operationID)
	}
	
	// Validate state transition
	if operation.State != OperationStateRunning {
		return fmt.Errorf("operation cannot be paused from state: %s", operation.State)
	}
	
	oldState := operation.State
	operation.State = OperationStatePaused
	now := time.Now()
	operation.PausedAt = &now
	
	// Update database
	if err := s.db.UpdateOperationState(operationID, string(operation.State)); err != nil {
		operation.State = oldState // Rollback
		return fmt.Errorf("failed to update operation state: %v", err)
	}
	
	// Notify distribution service to pause task assignment
	s.distributionService.PauseTaskAssignment(operationID)
	
	// Notify state change
	s.notifyStateChange(operationID, oldState, operation.State)
	
	log.Printf("Paused operation %s (%s)", operation.ID, operation.Name)
	
	return nil
}

// ResumeOperation resumes a paused operation
func (s *OperationService) ResumeOperation(operationID string) error {
	s.operationsMu.Lock()
	defer s.operationsMu.Unlock()
	
	operation, exists := s.operations[operationID]
	if !exists {
		return fmt.Errorf("operation not found: %s", operationID)
	}
	
	// Validate state transition
	if operation.State != OperationStatePaused {
		return fmt.Errorf("operation cannot be resumed from state: %s", operation.State)
	}
	
	// Check if we can resume
	if s.activeOperation != nil && s.activeOperation.State == OperationStateRunning {
		return fmt.Errorf("another operation is already running: %s", s.activeOperation.Name)
	}
	
	oldState := operation.State
	operation.State = OperationStateRunning
	s.activeOperation = operation
	
	// Update database
	if err := s.db.UpdateOperationState(operationID, string(operation.State)); err != nil {
		operation.State = oldState // Rollback
		return fmt.Errorf("failed to update operation state: %v", err)
	}
	
	// Resume task assignment
	s.distributionService.ResumeTaskAssignment(operationID)
	
	// Notify state change
	s.notifyStateChange(operationID, oldState, operation.State)
	
	// Resume monitoring
	go s.monitorOperation(operation)
	
	log.Printf("Resumed operation %s (%s)", operation.ID, operation.Name)
	
	return nil
}

// GetOperation retrieves an operation by ID
func (s *OperationService) GetOperation(operationID string) (*Operation, error) {
	s.operationsMu.RLock()
	defer s.operationsMu.RUnlock()
	
	operation, exists := s.operations[operationID]
	if !exists {
		// Try to load from database
		opData, err := s.db.GetOperation(operationID)
		if err != nil {
			return nil, fmt.Errorf("operation not found: %s", operationID)
		}
		
		// Convert from database format to Operation struct
		opMap, ok := opData.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid operation data format")
		}
		
		operation = &Operation{
			ID:          opMap["id"].(string),
			Name:        opMap["name"].(string),
			Description: opMap["description"].(string),
			State:       OperationState(opMap["state"].(string)),
			// Add other fields as needed
		}
		return operation, nil
	}
	
	return operation, nil
}

// ListOperations returns all operations
func (s *OperationService) ListOperations() ([]*Operation, error) {
	s.operationsMu.RLock()
	defer s.operationsMu.RUnlock()
	
	// Load from database to ensure we have all operations
	operationsData, err := s.db.ListOperations()
	if err != nil {
		return nil, err
	}
	
	// Update memory cache
	for _, opData := range operationsData {
		opMap, ok := opData.(map[string]interface{})
		if !ok {
			continue // Skip invalid data
		}
		
		opID := opMap["id"].(string)
		if _, exists := s.operations[opID]; !exists {
			operation := &Operation{
				ID:          opID,
				Name:        opMap["name"].(string),
				Description: opMap["description"].(string),
				State:       OperationState(opMap["state"].(string)),
				// Add other fields as needed
			}
			s.operations[opID] = operation
		}
	}
	
	// Return from memory (includes runtime stats)
	result := make([]*Operation, 0, len(s.operations))
	for _, op := range s.operations {
		result = append(result, op)
	}
	
	return result, nil
}

// DeleteOperation deletes a completed or stopped operation
func (s *OperationService) DeleteOperation(operationID string) error {
	s.operationsMu.Lock()
	defer s.operationsMu.Unlock()
	
	operation, exists := s.operations[operationID]
	if !exists {
		// Try to load from database
		opData, err := s.db.GetOperation(operationID)
		if err != nil {
			return fmt.Errorf("operation not found: %s", operationID)
		}
		
		// Convert from database format to Operation struct
		opMap, ok := opData.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid operation data format")
		}
		
		operation = &Operation{
			ID:          opMap["id"].(string),
			Name:        opMap["name"].(string),
			Description: opMap["description"].(string),
			State:       OperationState(opMap["state"].(string)),
			// Add other fields as needed
		}
	}
	
	// Only allow deletion of completed or stopped operations
	if operation.State != OperationStateCompleted && operation.State != OperationStateStopped {
		return fmt.Errorf("operation must be completed or stopped before deletion")
	}
	
	// Delete from database
	if err := s.db.DeleteOperation(operationID); err != nil {
		return fmt.Errorf("failed to delete operation: %v", err)
	}
	
	// Remove from memory
	delete(s.operations, operationID)
	
	log.Printf("Deleted operation %s (%s)", operation.ID, operation.Name)
	
	return nil
}

// GetActiveOperation returns the currently active operation
func (s *OperationService) GetActiveOperation() *Operation {
	s.operationsMu.RLock()
	defer s.operationsMu.RUnlock()
	
	return s.activeOperation
}

// loadOperationTargets loads targets for an operation into the system
func (s *OperationService) loadOperationTargets(operation *Operation) error {
	// Clear any existing targets
	if err := s.db.ClearPendingTargets(); err != nil {
		return err
	}
	
	// Load operation targets
	targets := make([]models.Target, len(operation.Targets))
	for i, ip := range operation.Targets {
		targets[i] = models.Target{
			IP:        ip,
			Port:      3389, // Default RDP port
			Status:    "pending",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}
	
	// Batch insert targets
	if err := s.db.BulkInsertTargets(targets); err != nil {
		return err
	}
	
	// Load credentials
	if err := s.db.SetOperationCredentials(operation.Credentials.Usernames, 
		operation.Credentials.Passwords); err != nil {
		return err
	}
	
	log.Printf("Loaded %d targets and %d credentials for operation %s",
		len(targets), len(operation.Credentials.Usernames)*len(operation.Credentials.Passwords),
		operation.ID)
	
	return nil
}

// clearOperationTasks clears all tasks related to an operation
func (s *OperationService) clearOperationTasks(operation *Operation) error {
	// Get all active tasks
	tasks, err := s.db.GetActiveTasks()
	if err != nil {
		return err
	}
	
	// Reclaim all tasks
	for _, task := range tasks {
		if err := s.db.ReclaimTask(task.ID); err != nil {
			log.Printf("Error reclaiming task %s: %v", task.ID, err)
		}
	}
	
	// Reset all targets to pending
	if err := s.db.ResetAllTargetsToPending(); err != nil {
		return err
	}
	
	log.Printf("Cleared %d tasks for operation %s", len(tasks), operation.ID)
	
	return nil
}

// performGracefulShutdown performs a graceful shutdown of an operation
func (s *OperationService) performGracefulShutdown(operation *Operation, gracefulStop *GracefulStop) {
	log.Printf("Starting graceful shutdown for operation %s", operation.ID)
	
	// Set a timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()
	
	// Notify all clients about graceful shutdown
	clients := s.clientService.GetOnlineClients()
	if clients != nil {
		gracefulStop.ClientsNotified = len(clients)
		
		// Send graceful stop command to each client
		for _, client := range clients {
			if err := s.clientService.SendOperationCommand(client.ID, 
				protocol.MsgOperationStop, map[string]interface{}{
					"operation_id": operation.ID,
					"graceful":     true,
				}); err != nil {
				log.Printf("Error notifying client %s: %v", client.ID, err)
			}
		}
	}
	
	// Monitor client completion
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			// Timeout reached, force stop
			log.Printf("Graceful shutdown timeout for operation %s, forcing stop", operation.ID)
			s.forceStopOperation(operation.ID)
			return
			
		case <-ticker.C:
			// Check if all clients have completed
			activeTaskCount, err := s.db.GetActiveTaskCount()
			if err == nil && activeTaskCount == 0 {
				// All tasks completed
				log.Printf("All clients completed work for operation %s", operation.ID)
				s.completeGracefulShutdown(operation.ID)
				return
			}
			
			// Update progress
			s.gracefulStopsMu.Lock()
			if gs, exists := s.gracefulStops[operation.ID]; exists {
				completed, _ := s.db.GetCompletedTaskCount()
				gs.ClientsCompleted = completed
			}
			s.gracefulStopsMu.Unlock()
		}
	}
}

// completeGracefulShutdown completes the graceful shutdown process
func (s *OperationService) completeGracefulShutdown(operationID string) {
	s.operationsMu.Lock()
	defer s.operationsMu.Unlock()
	
	operation, exists := s.operations[operationID]
	if !exists {
		return
	}
	
	oldState := operation.State
	operation.State = OperationStateStopped
	now := time.Now()
	operation.StoppedAt = &now
	
	if s.activeOperation != nil && s.activeOperation.ID == operationID {
		s.activeOperation = nil
	}
	
	// Update database
	s.db.UpdateOperationState(operationID, string(operation.State))
	
	// Clear graceful stop tracking
	s.gracefulStopsMu.Lock()
	delete(s.gracefulStops, operationID)
	s.gracefulStopsMu.Unlock()
	
	// Notify state change
	s.notifyStateChange(operationID, oldState, operation.State)
	
	log.Printf("Completed graceful shutdown for operation %s", operationID)
}

// forceStopOperation forces an operation to stop immediately
func (s *OperationService) forceStopOperation(operationID string) {
	s.StopOperation(operationID, false)
	
	// Clear graceful stop tracking
	s.gracefulStopsMu.Lock()
	delete(s.gracefulStops, operationID)
	s.gracefulStopsMu.Unlock()
}

// monitorOperation monitors an operation's progress
func (s *OperationService) monitorOperation(operation *Operation) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-s.ctx.Done():
			return
			
		case <-ticker.C:
			// Check if operation is still running
			s.operationsMu.RLock()
			if operation.State != OperationStateRunning {
				s.operationsMu.RUnlock()
				return
			}
			s.operationsMu.RUnlock()
			
			// Update statistics
			stats, err := s.db.GetOperationStats(operation.ID)
			if err != nil {
				log.Printf("Error getting operation stats: %v", err)
				continue
			}
			
			s.operationsMu.Lock()
			operation.Statistics = OperationStats{
				TotalTargets:      stats.TotalTargets,
				CompletedTargets:  stats.CompletedTargets,
				TotalAttempts:     stats.TotalAttempts,
				SuccessfulFinds:   stats.SuccessfulFinds,
				AveragePPS:        stats.AveragePPS,
				ElapsedTime:       stats.ElapsedTime,
				EstimatedTimeLeft: stats.EstimatedTimeLeft,
				LastUpdate:        stats.LastUpdate,
			}
			
			// Calculate elapsed time
			if operation.StartedAt != nil {
				elapsed := time.Since(*operation.StartedAt)
				operation.Statistics.ElapsedTime = formatDuration(elapsed)
				
				// Estimate time left
				if stats.CompletedTargets > 0 && stats.AveragePPS > 0 {
					remainingTargets := stats.TotalTargets - stats.CompletedTargets
					remainingCombinations := float64(remainingTargets) * 
						float64(len(operation.Credentials.Usernames)) * 
						float64(len(operation.Credentials.Passwords))
					
					// Get total system PPS
					systemPPS := s.distributionService.workQueueMonitor.GetWorkRate()
					if systemPPS > 0 {
						remainingSeconds := remainingCombinations / systemPPS
						operation.Statistics.EstimatedTimeLeft = formatDuration(time.Duration(remainingSeconds) * time.Second)
					}
				}
			}
			
			// Check if operation is complete
			if stats.CompletedTargets >= stats.TotalTargets {
				operation.State = OperationStateCompleted
				now := time.Now()
				operation.CompletedAt = &now
				
				if s.activeOperation != nil && s.activeOperation.ID == operation.ID {
					s.activeOperation = nil
				}
				
				s.db.UpdateOperationState(operation.ID, string(operation.State))
				s.notifyStateChange(operation.ID, OperationStateRunning, operation.State)
				
				log.Printf("Operation %s completed: %d targets, %d successes",
					operation.ID, stats.TotalTargets, stats.SuccessfulFinds)
				
				s.operationsMu.Unlock()
				return
			}
			
			s.operationsMu.Unlock()
		}
	}
}

// notifyStateChange sends a state change notification
func (s *OperationService) notifyStateChange(operationID string, oldState, newState OperationState) {
	select {
	case s.stateChangeChan <- OperationStateChange{
		OperationID: operationID,
		OldState:    oldState,
		NewState:    newState,
		Timestamp:   time.Now(),
	}:
	default:
		// Channel full, log and continue
		log.Printf("State change channel full for operation %s", operationID)
	}
}

// notifyClientsOperationStop notifies all clients to stop working on an operation
func (s *OperationService) notifyClientsOperationStop(operationID string, graceful bool) {
	clients := s.clientService.GetOnlineClients()
	if clients == nil {
		log.Printf("No online clients found")
		return
	}
	
	for _, client := range clients {
		if err := s.clientService.SendOperationCommand(client.ID,
			protocol.MsgOperationStop, map[string]interface{}{
				"operation_id": operationID,
				"graceful":     graceful,
			}); err != nil {
			log.Printf("Error notifying client %s about operation stop: %v", client.ID, err)
		}
	}
}

// GetStateChangeChannel returns the state change notification channel
func (s *OperationService) GetStateChangeChannel() <-chan OperationStateChange {
	return s.stateChangeChan
}

// formatDuration formats a duration into a human-readable string
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
}

// Shutdown gracefully shuts down the operation service
func (s *OperationService) Shutdown() {
	log.Println("Shutting down operation service")
	
	// Stop any running operations gracefully
	s.operationsMu.RLock()
	if s.activeOperation != nil && s.activeOperation.State == OperationStateRunning {
		operationID := s.activeOperation.ID
		s.operationsMu.RUnlock()
		
		s.StopOperation(operationID, true)
		
		// Wait for graceful shutdown to complete (max 30 seconds)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ctx.Done():
				// Force stop if timeout
				s.forceStopOperation(operationID)
				return
			case <-ticker.C:
				s.operationsMu.RLock()
				if s.activeOperation == nil || s.activeOperation.State != OperationStateRunning {
					s.operationsMu.RUnlock()
					return
				}
				s.operationsMu.RUnlock()
			}
		}
	} else {
		s.operationsMu.RUnlock()
	}
	
	s.cancel()
	close(s.stateChangeChan)
}
