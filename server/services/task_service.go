package services

import (
	"bufio"
	"fmt"
	"log"
	"mime/multipart"
	"time"

	"rdp-brute-system/server/cache"
	"rdp-brute-system/server/database"
	"rdp-brute-system/server/models"
	"rdp-brute-system/shared/protocol"
	"sync"
)

type TaskService struct {
	db    *database.DB
	mu    sync.Mutex
	cache *cache.Cache
}

func NewTaskService(db *database.DB) *TaskService {
	return &TaskService{
		db:    db,
		cache: cache.NewCache(10 * time.Minute), // 10 minute cache TTL
	}
}

func (s *TaskService) LoadIPs(fileHeader *multipart.FileHeader) error {
	count, err := s.LoadIPsWithProgress(fileHeader)
	if err != nil {
		return err
	}
	log.Printf("Loaded %d IP addresses", count)
	return nil
}

// LoadIPsWithProgress loads IPs from file with progress tracking for large files
func (s *TaskService) LoadIPsWithProgress(fileHeader *multipart.FileHeader) (int, error) {
	file, err := fileHeader.Open()
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// Use larger buffer for better performance with large files
	const batchSize = 1000
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 64KB initial, 1MB max line
	
	var targets []models.Target
	var totalCount int
	
	for scanner.Scan() {
		ip := scanner.Text()
		if ip != "" {
			target := models.Target{
				IP:        ip,
				Port:      3389, // Default RDP port
				Status:    "pending",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			}
			targets = append(targets, target)
			
			// Batch insert for better performance
			if len(targets) >= batchSize {
				if err := s.db.AddTargets(targets); err != nil {
					return totalCount, err
				}
				totalCount += len(targets)
				log.Printf("Progress: Loaded %d IPs...", totalCount)
				targets = targets[:0] // Reset slice while keeping capacity
			}
		}
	}
	
	// Insert remaining targets
	if len(targets) > 0 {
		if err := s.db.AddTargets(targets); err != nil {
			return totalCount, err
		}
		totalCount += len(targets)
	}
	
	if err := scanner.Err(); err != nil {
		return totalCount, err
	}
	
	return totalCount, nil
}

func (s *TaskService) LoadCredentials(usersFile, passwordsFile *multipart.FileHeader) error {
	userCount, passCount, err := s.LoadCredentialsWithProgress(usersFile, passwordsFile)
	if err != nil {
		return err
	}
	log.Printf("Loaded %d usernames and %d passwords", userCount, passCount)
	return nil
}

// LoadCredentialsWithProgress loads credentials with progress tracking for large files
func (s *TaskService) LoadCredentialsWithProgress(usersFile, passwordsFile *multipart.FileHeader) (int, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	const batchSize = 1000
	
	// Process users file
	users, err := readLinesInBatches(usersFile, batchSize)
	if err != nil {
		return 0, 0, err
	}

	// Process passwords file
	passwords, err := readLinesInBatches(passwordsFile, batchSize)
	if err != nil {
		return len(users), 0, err
	}

	// Batch process credentials
	var credentials []models.Credential
	userCount := 0
	
	// Process usernames in batches
	for _, user := range users {
		cred := models.Credential{
			Username: user,
			Type:     "username",
		}
		credentials = append(credentials, cred)
		
		if len(credentials) >= batchSize {
			if err := s.db.AddCredentials(credentials); err != nil {
				return userCount, 0, err
			}
			userCount += len(credentials)
			log.Printf("Progress: Loaded %d usernames...", userCount)
			credentials = credentials[:0]
		}
	}
	
	// Process remaining usernames
	if len(credentials) > 0 {
		if err := s.db.AddCredentials(credentials); err != nil {
			return userCount, 0, err
		}
		userCount += len(credentials)
		credentials = credentials[:0]
	}

	// Process passwords in batches
	passCount := 0
	for _, password := range passwords {
		cred := models.Credential{
			Password: password,
			Type:     "password",
		}
		credentials = append(credentials, cred)
		
		if len(credentials) >= batchSize {
			if err := s.db.AddCredentials(credentials); err != nil {
				return userCount, passCount, err
			}
			passCount += len(credentials)
			log.Printf("Progress: Loaded %d passwords...", passCount)
			credentials = credentials[:0]
		}
	}
	
	// Process remaining passwords
	if len(credentials) > 0 {
		if err := s.db.AddCredentials(credentials); err != nil {
			return userCount, passCount, err
		}
		passCount += len(credentials)
	}

	return userCount, passCount, nil
}

func (s *TaskService) GetResults() ([]models.Result, error) {
	// Try to get from cache first
	cacheKey := "task:results"
	if cached, found := s.cache.Get(cacheKey); found {
		return cached.([]models.Result), nil
	}
	
	// Get from database
	results, err := s.db.GetResults()
	if err != nil {
		return nil, err
	}
	
	// Cache the results with shorter TTL since they change frequently
	s.cache.SetWithTTL(cacheKey, results, 30*time.Second)
	
	return results, nil
}

// GetTasksForClient is deprecated - use DistributionService.GetTask instead
// Kept for backwards compatibility but will be removed in future versions
func (s *TaskService) GetTasksForClient(clientID string, maxTasks int, preferredSize int) []protocol.Task {
	log.Printf("Warning: GetTasksForClient is deprecated, use DistributionService.GetTask instead")
	return []protocol.Task{}
}

// StoreResults stores task results
func (s *TaskService) StoreResults(clientID string, taskID string, results []protocol.Result) {
	for _, result := range results {
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
			log.Printf("Error saving result: %v", err)
		} else {
			log.Printf("Saved successful result: %s:%d %s:%s", result.IP, result.Port, result.Username, result.Password)
			// Invalidate results cache when new result is saved
			s.cache.Delete("task:results")
		}
	}
}

// InvalidateCache clears all cached data
func (s *TaskService) InvalidateCache() {
	s.cache.Clear()
}

// InvalidateTaskCache invalidates specific task-related caches
func (s *TaskService) InvalidateTaskCache(taskID string) {
	s.cache.Delete("task:all")
	s.cache.Delete("task:stats")
	s.cache.Delete(fmt.Sprintf("task:%s", taskID))
}

// UpdateTaskStatus updates the status of a task with enhanced metrics
func (s *TaskService) UpdateTaskStatus(taskID string, statusData protocol.StatusUpdateData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	log.Printf("Task %s status updated: %s (progress: %.2f%%, attempts: %d, successes: %d)",
		taskID, statusData.Status, statusData.Progress, statusData.Attempts, statusData.SuccessCount)
	
	// Update task progress in database
	if err := s.db.UpdateTaskProgress(taskID, statusData.Progress, statusData.Attempts, statusData.SuccessCount, statusData.PPS); err != nil {
		log.Printf("Error updating task progress: %v", err)
	}
	
	// Invalidate task-related caches
	s.InvalidateTaskCache(taskID)
	
	// Update task status if changed
	switch statusData.Status {
	case "in_progress":
		if err := s.updateTaskToInProgress(taskID); err != nil {
			log.Printf("Error updating task to in_progress: %v", err)
		}
	case "completed":
		if err := s.completeTask(taskID); err != nil {
			log.Printf("Error completing task: %v", err)
		}
	case "failed":
		if err := s.failTask(taskID); err != nil {
			log.Printf("Error failing task: %v", err)
		}
	}
}

// updateTaskToInProgress marks a task as in progress
func (s *TaskService) updateTaskToInProgress(taskID string) error {
	query := `
		UPDATE tasks
		SET status = 'in_progress', started_at = $1, last_update = $1
		WHERE id = $2 AND status = 'assigned'
	`
	_, err := s.db.Exec(query, time.Now(), taskID)
	return err
}

// completeTask marks a task as completed
func (s *TaskService) completeTask(taskID string) error {
	now := time.Now()
	query := `
		UPDATE tasks
		SET status = 'completed', completed_at = $1, last_update = $1
		WHERE id = $2
	`
	_, err := s.db.Exec(query, now, taskID)
	
	if err == nil {
		// Update target status to completed
		targetQuery := `
			UPDATE targets
			SET status = 'completed', updated_at = $1
			WHERE id IN (
				SELECT target_id FROM task_targets WHERE task_id = $2
			)
		`
		_, err = s.db.Exec(targetQuery, now, taskID)
	}
	
	return err
}

// failTask marks a task as failed
func (s *TaskService) failTask(taskID string) error {
	now := time.Now()
	query := `
		UPDATE tasks
		SET status = 'failed', completed_at = $1, last_update = $1
		WHERE id = $2
	`
	_, err := s.db.Exec(query, now, taskID)
	
	if err == nil {
		// Reset target status to pending for retry
		targetQuery := `
			UPDATE targets
			SET status = 'pending', updated_at = $1
			WHERE id IN (
				SELECT target_id FROM task_targets WHERE task_id = $2
			)
		`
		_, err = s.db.Exec(targetQuery, now, taskID)
	}
	
	return err
}

// CreateTask creates a new task - deprecated, use DistributionService instead
func (s *TaskService) CreateTask(task *models.Task) error {
	log.Printf("Warning: CreateTask is deprecated, use DistributionService.GetTask instead")
	return nil
}

// GetTasks returns all tasks from the database with enhanced data
func (s *TaskService) GetTasks() ([]models.Task, error) {
	// Try to get from cache first
	cacheKey := "task:all"
	cached, err := s.cache.GetOrSetWithTTL(cacheKey, 1*time.Minute, func() (interface{}, error) {
		query := `
			SELECT id, client_id, status, priority, target_count, progress,
				   attempts, success_count, assigned_at, started_at, completed_at
			FROM tasks
			ORDER BY assigned_at DESC
			LIMIT 1000
		`
		
		rows, err := s.db.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		
		var tasks []models.Task
		for rows.Next() {
			var task models.Task
			var startedAt, completedAt *time.Time
			
			err := rows.Scan(
				&task.ID,
				&task.ClientID,
				&task.Status,
				&task.Priority,
				&task.TargetCount,
				&task.Progress,
				&task.Attempts,
				&task.SuccessCount,
				&task.AssignedAt,
				&startedAt,
				&completedAt,
			)
			if err != nil {
				return nil, err
			}
			
			if startedAt != nil {
				task.StartedAt = *startedAt
			}
			if completedAt != nil {
				task.CompletedAt = *completedAt
			}
			
			tasks = append(tasks, task)
		}
		
		return tasks, nil
	})
	
	if err != nil {
		return nil, err
	}
	
	return cached.([]models.Task), nil
}

// GetTaskStats returns task statistics
func (s *TaskService) GetTaskStats() (map[string]interface{}, error) {
	// Cache task stats for 2 minutes
	cacheKey := "task:stats"
	cached, err := s.cache.GetOrSetWithTTL(cacheKey, 2*time.Minute, func() (interface{}, error) {
		stats := make(map[string]interface{})
		
		// Get task counts by status
		statusQuery := `
			SELECT status, COUNT(*) as count
			FROM tasks
			GROUP BY status
		`
		rows, err := s.db.Query(statusQuery)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		
		statusCounts := make(map[string]int)
		for rows.Next() {
			var status string
			var count int
			if err := rows.Scan(&status, &count); err != nil {
				return nil, err
			}
			statusCounts[status] = count
		}
		stats["tasksByStatus"] = statusCounts
		
		// Get average completion time
		avgTimeQuery := `
			SELECT AVG(EXTRACT(EPOCH FROM (completed_at - assigned_at))/60) as avg_minutes
			FROM tasks
			WHERE status = 'completed' AND completed_at IS NOT NULL
		`
		var avgMinutes *float64
		if err := s.db.QueryRow(avgTimeQuery).Scan(&avgMinutes); err == nil && avgMinutes != nil {
			stats["avgCompletionMinutes"] = *avgMinutes
		}
		
		// Get success rate
		successQuery := `
			SELECT
				COALESCE(SUM(success_count), 0)::float / GREATEST(SUM(attempts), 1)::float as success_rate
			FROM tasks
			WHERE status = 'completed'
		`
		var successRate float64
		if err := s.db.QueryRow(successQuery).Scan(&successRate); err == nil {
			stats["overallSuccessRate"] = successRate
		}
		
		return stats, nil
	})
	
	if err != nil {
		return nil, err
	}
	
	return cached.(map[string]interface{}), nil
}

func readLines(fileHeader *multipart.FileHeader) ([]string, error) {
	file, err := fileHeader.Open()
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// readLinesInBatches reads lines from file efficiently for large files
func readLinesInBatches(fileHeader *multipart.FileHeader, batchSize int) ([]string, error) {
	file, err := fileHeader.Open()
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	// Use larger buffer for better performance
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 64KB initial, 1MB max line
	
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			lines = append(lines, line)
		}
	}
	
	return lines, scanner.Err()
}
