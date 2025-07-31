package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"rdp-brute-system/server/models"
	"rdp-brute-system/server/cache"
	"rdp-brute-system/shared/logger"
)

type DB struct {
	*sqlx.DB
	
	// Prepared statements cache
	stmtCache     map[string]*sqlx.Stmt
	stmtCacheMu   sync.RWMutex
	
	// Bulk operation buffers
	resultBuffer  []models.Result
	resultBufferMu sync.Mutex
	bulkTimer     *time.Timer
	
	// Query result cache
	queryCache    *cache.QueryCache
}

// Prepared statement keys
const (
	StmtGetTargets          = "getTargets"
	StmtGetTargetsPriority  = "getTargetsPriority"
	StmtUpdateTargetStatus  = "updateTargetStatus"
	StmtGetUsernames        = "getUsernames"
	StmtGetPasswords        = "getPasswords"
	StmtUpdateClientHB      = "updateClientHeartbeat"
	StmtGetClientPerf       = "getClientPerformance"
	StmtSaveResult          = "saveResult"
	StmtGetClientWorkload   = "getClientWorkload"
	StmtUpdateTaskProgress  = "updateTaskProgress"
	
	// Bulk operation settings
	BulkBufferSize    = 1000
	BulkFlushInterval = 500 * time.Millisecond
)

// NewDB initializes the database connection with optimized connection pooling
func NewDB(dataSourceName string) (*DB, error) {
	db, err := sqlx.Connect("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}

	// Optimized connection pool settings for high throughput
	db.SetMaxOpenConns(100)                    // Increased from 50
	db.SetMaxIdleConns(25)                     // Increased from 10
	db.SetConnMaxLifetime(10 * time.Minute)   // Increased from 5 minutes
	db.SetConnMaxIdleTime(5 * time.Minute)    // Increased from 1 minute

	logger.ServerLogger.Info("Database connection successful with optimized connection pooling")
	
	wrappedDB := &DB{
		DB:           db,
		stmtCache:    make(map[string]*sqlx.Stmt),
		resultBuffer: make([]models.Result, 0, BulkBufferSize),
		queryCache:   cache.NewQueryCache(1000, 30*time.Second), // 1000 entries, 30s TTL
	}
	
	// Initialize prepared statements
	if err := wrappedDB.initPreparedStatements(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize prepared statements: %v", err)
	}
	
	// Start bulk operation processor
	go wrappedDB.bulkProcessor()
	
	return wrappedDB, nil
}

// initPreparedStatements prepares frequently used queries
func (db *DB) initPreparedStatements() error {
	statements := map[string]string{
		StmtGetTargets: `
			SELECT id, ip, port FROM targets 
			WHERE status = 'pending' 
			LIMIT $1`,
		
		StmtGetTargetsPriority: `
			SELECT id, ip, port
			FROM targets
			WHERE status = 'pending'
			ORDER BY
				CASE WHEN attempts = 0 THEN 0 ELSE 1 END,
				attempts ASC,
				last_attempt ASC NULLS FIRST
			LIMIT $1`,
		
		StmtUpdateTargetStatus: `
			UPDATE targets 
			SET status = $1, updated_at = $2 
			WHERE id = $3`,
		
		StmtGetUsernames: `
			SELECT username FROM credentials 
			WHERE type = 'username' 
			LIMIT $1`,
		
		StmtGetPasswords: `
			SELECT password FROM credentials 
			WHERE type = 'password' 
			LIMIT $1`,
		
		StmtUpdateClientHB: `
			UPDATE clients
			SET last_heartbeat = $1, current_pps = $2, 
				total_attempts = total_attempts + $3, 
				success_count = success_count + $4, 
				last_updated = $1
			WHERE id = $5`,
		
		StmtGetClientPerf: `
			SELECT 
				COALESCE(AVG(avg_pps), 0),
				COALESCE(SUM(success_count)::float / GREATEST(SUM(attempts), 1)::float, 0)
			FROM tasks
			WHERE client_id = $1 AND status = 'completed' AND completed_at > $2`,
		
		StmtSaveResult: `
			INSERT INTO results 
			(target_id, client_id, ip, port, username, password, found_at, verified)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		
		StmtGetClientWorkload: `
			SELECT COALESCE(SUM(target_count), 0)
			FROM tasks
			WHERE client_id = $1 AND status IN ('assigned', 'in_progress')`,
		
		StmtUpdateTaskProgress: `
			UPDATE tasks
			SET progress = $1,
				attempts = attempts + $2,
				success_count = success_count + $3,
				avg_pps = $4,
				last_update = $5
			WHERE id = $6`,
	}
	
	for key, query := range statements {
		stmt, err := db.Preparex(query)
		if err != nil {
			return fmt.Errorf("failed to prepare statement %s: %v", key, err)
		}
		db.stmtCache[key] = stmt
	}
	
	logger.ServerLogger.Info("Initialized prepared statements", map[string]interface{}{
		"count": len(statements),
	})
	return nil
}

// getStmt retrieves a prepared statement from the cache
func (db *DB) getStmt(key string) *sqlx.Stmt {
	db.stmtCacheMu.RLock()
	defer db.stmtCacheMu.RUnlock()
	return db.stmtCache[key]
}

// Close closes the database connection pool and prepared statements
func (db *DB) Close() error {
	// Flush any pending bulk operations
	db.flushResultBuffer()
	
	// Close prepared statements
	db.stmtCacheMu.Lock()
	for _, stmt := range db.stmtCache {
		stmt.Close()
	}
	db.stmtCacheMu.Unlock()
	
	return db.DB.Close()
}

// PingContext checks if the database is reachable
func (db *DB) PingContext(ctx context.Context) error {
	return db.DB.PingContext(ctx)
}

// Stats returns database connection pool statistics
func (db *DB) Stats() sql.DBStats {
	return db.DB.Stats()
}

// CreateTables creates the database tables if they don't exist with optimized indexes
func (db *DB) CreateTables() {
	// Clients table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS clients (
			id TEXT PRIMARY KEY,
			hostname TEXT,
			os TEXT,
			cpu_cores INTEGER,
			max_threads INTEGER,
			version TEXT,
			last_heartbeat TIMESTAMP,
			status TEXT,
			total_attempts BIGINT,
			success_count INTEGER,
			current_pps REAL,
			registered_at TIMESTAMP,
			last_updated TIMESTAMP
		);
		
		CREATE INDEX IF NOT EXISTS idx_clients_status ON clients(status);
		CREATE INDEX IF NOT EXISTS idx_clients_heartbeat ON clients(last_heartbeat);
	`)
	if err != nil {
		logger.ServerLogger.Fatal("Error creating clients table", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Targets table with optimized indexes
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS targets (
			id SERIAL PRIMARY KEY,
			ip TEXT,
			port INTEGER,
			status TEXT,
			attempts INTEGER DEFAULT 0,
			last_attempt TIMESTAMP,
			created_at TIMESTAMP,
			updated_at TIMESTAMP
		);
		
		CREATE INDEX IF NOT EXISTS idx_targets_status ON targets(status);
		CREATE INDEX IF NOT EXISTS idx_targets_priority ON targets(status, attempts, last_attempt);
		CREATE INDEX IF NOT EXISTS idx_targets_ip ON targets(ip);
	`)
	if err != nil {
		logger.ServerLogger.Fatal("Error creating targets table", map[string]interface{}{"error": err.Error()})
	}

	// Credentials table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS credentials (
			id SERIAL PRIMARY KEY,
			username TEXT,
			password TEXT,
			type TEXT
		);
		
		CREATE INDEX IF NOT EXISTS idx_credentials_type ON credentials(type);
	`)
	if err != nil {
		logger.ServerLogger.Fatal("Error creating credentials table", map[string]interface{}{"error": err.Error()})
	}

	// Results table with indexes
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS results (
			id SERIAL PRIMARY KEY,
			target_id INTEGER REFERENCES targets(id),
			client_id TEXT REFERENCES clients(id),
			ip TEXT,
			port INTEGER,
			username TEXT,
			password TEXT,
			found_at TIMESTAMP,
			verified BOOLEAN
		);
		
		CREATE INDEX IF NOT EXISTS idx_results_found_at ON results(found_at DESC);
		CREATE INDEX IF NOT EXISTS idx_results_client ON results(client_id);
		CREATE INDEX IF NOT EXISTS idx_results_ip ON results(ip);
	`)
	if err != nil {
		logger.ServerLogger.Fatal("Error creating results table", map[string]interface{}{"error": err.Error()})
	}

	// Tasks table with enhanced tracking and indexes
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS tasks (
			id TEXT PRIMARY KEY,
			client_id TEXT REFERENCES clients(id),
			status TEXT,
			priority INTEGER,
			target_count INTEGER,
			progress REAL,
			attempts INTEGER DEFAULT 0,
			success_count INTEGER DEFAULT 0,
			assigned_at TIMESTAMP,
			started_at TIMESTAMP,
			completed_at TIMESTAMP,
			expires_at TIMESTAMP,
			last_update TIMESTAMP,
			batch_size INTEGER,
			avg_pps REAL
		);
		
		CREATE INDEX IF NOT EXISTS idx_tasks_client ON tasks(client_id);
		CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
		CREATE INDEX IF NOT EXISTS idx_tasks_expires ON tasks(expires_at);
		CREATE INDEX IF NOT EXISTS idx_tasks_client_status ON tasks(client_id, status);
	`)
	if err != nil {
		logger.ServerLogger.Fatal("Error creating tasks table", map[string]interface{}{"error": err.Error()})
	}

	// Task-Targets table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS task_targets (
			task_id TEXT REFERENCES tasks(id),
			target_id INTEGER REFERENCES targets(id),
			PRIMARY KEY (task_id, target_id)
		);
		
		CREATE INDEX IF NOT EXISTS idx_task_targets_task ON task_targets(task_id);
		CREATE INDEX IF NOT EXISTS idx_task_targets_target ON task_targets(target_id);
	`)
	if err != nil {
		logger.ServerLogger.Fatal("Error creating task_targets table", map[string]interface{}{"error": err.Error()})
	}

	// Config table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS config (
			key TEXT PRIMARY KEY,
			value TEXT,
			updated_at TIMESTAMP
		)
	`)
	if err != nil {
		logger.ServerLogger.Fatal("Error creating config table", map[string]interface{}{"error": err.Error()})
	}

	// Operations table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS operations (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT,
			state TEXT NOT NULL,
			targets TEXT NOT NULL, -- JSON array
			credentials TEXT NOT NULL, -- JSON object
			created_at TIMESTAMP NOT NULL,
			started_at TIMESTAMP,
			paused_at TIMESTAMP,
			completed_at TIMESTAMP,
			stopped_at TIMESTAMP,
			statistics TEXT, -- JSON object
			settings TEXT -- JSON object
		);
		
		CREATE INDEX IF NOT EXISTS idx_operations_state ON operations(state);
		CREATE INDEX IF NOT EXISTS idx_operations_created ON operations(created_at DESC);
	`)
	if err != nil {
		logger.ServerLogger.Fatal("Error creating operations table", map[string]interface{}{"error": err.Error()})
	}

	// Add operation_id to tasks table if not exists
	_, err = db.Exec(`
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_name='tasks' AND column_name='operation_id') THEN
				ALTER TABLE tasks ADD COLUMN operation_id TEXT REFERENCES operations(id);
				CREATE INDEX idx_tasks_operation ON tasks(operation_id);
			END IF;
		END$$;
	`)
	if err != nil {
		logger.ServerLogger.Warn("Could not add operation_id to tasks table", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Add operation_id to results table if not exists
	_, err = db.Exec(`
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_name='results' AND column_name='operation_id') THEN
				ALTER TABLE results ADD COLUMN operation_id TEXT REFERENCES operations(id);
				CREATE INDEX idx_results_operation ON results(operation_id);
			END IF;
		END$$;
	`)
	if err != nil {
		logger.ServerLogger.Warn("Could not add operation_id to results table", map[string]interface{}{
			"error": err.Error(),
		})
	}

	logger.ServerLogger.Info("Database tables created successfully with optimized indexes", nil)
}

// RegisterClient registers a new client or updates an existing one
func (db *DB) RegisterClient(client *models.Client) error {
	query := `
		INSERT INTO clients (id, hostname, os, cpu_cores, max_threads, version, last_heartbeat, status, total_attempts, success_count, current_pps, registered_at, last_updated)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (id) DO UPDATE SET
			hostname = EXCLUDED.hostname,
			os = EXCLUDED.os,
			cpu_cores = EXCLUDED.cpu_cores,
			max_threads = EXCLUDED.max_threads,
			version = EXCLUDED.version,
			last_heartbeat = EXCLUDED.last_heartbeat,
			status = EXCLUDED.status,
			last_updated = EXCLUDED.last_updated
	`
	_, err := db.Exec(query, client.ID, client.Hostname, client.OS, client.CPUCores, client.MaxThreads, client.Version, client.LastHeartbeat, client.Status, client.TotalAttempts, client.SuccessCount, client.CurrentPPS, client.RegisteredAt, client.LastUpdated)
	return err
}

// UpdateClientHeartbeat updates the client's heartbeat and stats using prepared statement
func (db *DB) UpdateClientHeartbeat(id string, pps float64, attempts int64, successes int) error {
	stmt := db.getStmt(StmtUpdateClientHB)
	_, err := stmt.Exec(time.Now(), pps, attempts, successes, id)
	return err
}

// AddTargets adds a list of targets to the database using bulk insert
func (db *DB) AddTargets(targets []models.Target) error {
	if len(targets) == 0 {
		return nil
	}
	
	// Use COPY for bulk insert - much faster than individual inserts
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	
	stmt, err := tx.Prepare(`
		INSERT INTO targets (ip, port, status, created_at, updated_at) 
		VALUES ($1, $2, $3, $4, $5)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	
	now := time.Now()
	
	// Process in batches
	batchSize := 1000
	for i := 0; i < len(targets); i += batchSize {
		end := i + batchSize
		if end > len(targets) {
			end = len(targets)
		}
		
		for _, target := range targets[i:end] {
			_, err := stmt.Exec(target.IP, target.Port, "pending", now, now)
			if err != nil {
				return err
			}
		}
	}
	
	return tx.Commit()
}

// GetResults retrieves all results from the database with pagination
func (db *DB) GetResults() ([]models.Result, error) {
	return db.GetResultsPaginated(0, 1000)
}

// GetResultsPaginated retrieves results with pagination
func (db *DB) GetResultsPaginated(offset, limit int) ([]models.Result, error) {
	query := `
		SELECT id, target_id, client_id, ip, port, username, password, found_at, verified
		FROM results
		ORDER BY found_at DESC
		LIMIT $1 OFFSET $2
	`
	
	rows, err := db.Query(query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.Result
	for rows.Next() {
		var result models.Result
		err := rows.Scan(
			&result.ID,
			&result.TargetID,
			&result.ClientID,
			&result.IP,
			&result.Port,
			&result.Username,
			&result.Password,
			&result.FoundAt,
			&result.Verified,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return results, rows.Err()
}

// AddCredentials adds a list of credentials to the database using bulk insert
func (db *DB) AddCredentials(credentials []models.Credential) error {
	if len(credentials) == 0 {
		return nil
	}
	
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	
	// Use a single prepared statement for all inserts
	stmt, err := tx.Prepare("INSERT INTO credentials (username, password, type) VALUES ($1, $2, $3)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	
	// Process in batches
	batchSize := 1000
	for i := 0; i < len(credentials); i += batchSize {
		end := i + batchSize
		if end > len(credentials) {
			end = len(credentials)
		}
		
		for _, cred := range credentials[i:end] {
			_, err := stmt.Exec(cred.Username, cred.Password, cred.Type)
			if err != nil {
				return err
			}
		}
	}
	
	return tx.Commit()
}

// SaveResult saves a successful result - adds to buffer for bulk processing
func (db *DB) SaveResult(result *models.Result) error {
	db.resultBufferMu.Lock()
	defer db.resultBufferMu.Unlock()
	
	db.resultBuffer = append(db.resultBuffer, *result)
	
	// Flush if buffer is full
	if len(db.resultBuffer) >= BulkBufferSize {
		return db.flushResultBufferLocked()
	}
	
	// Start flush timer if not already running
	if db.bulkTimer == nil {
		db.bulkTimer = time.AfterFunc(BulkFlushInterval, func() {
			db.flushResultBuffer()
		})
	}
	
	return nil
}

// flushResultBuffer flushes the result buffer
func (db *DB) flushResultBuffer() error {
	db.resultBufferMu.Lock()
	defer db.resultBufferMu.Unlock()
	return db.flushResultBufferLocked()
}

// flushResultBufferLocked flushes the buffer (must be called with lock held)
func (db *DB) flushResultBufferLocked() error {
	if len(db.resultBuffer) == 0 {
		return nil
	}
	
	// Cancel timer
	if db.bulkTimer != nil {
		db.bulkTimer.Stop()
		db.bulkTimer = nil
	}
	
	// Bulk insert results
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	
	stmt := db.getStmt(StmtSaveResult)
	for _, result := range db.resultBuffer {
		_, err := tx.Stmt(stmt.Stmt).Exec(
			result.TargetID, result.ClientID, result.IP, result.Port,
			result.Username, result.Password, result.FoundAt, result.Verified,
		)
		if err != nil {
			logger.ServerLogger.Error("Error saving result", map[string]interface{}{
				"error": err.Error(),
			})
			continue
		}
	}
	
	if err := tx.Commit(); err != nil {
		return err
	}
	
	logger.ServerLogger.Info("Flushed results to database", map[string]interface{}{
		"count": len(db.resultBuffer),
	})
	db.resultBuffer = db.resultBuffer[:0]
	
	return nil
}

// bulkProcessor periodically flushes buffers
func (db *DB) bulkProcessor() {
	ticker := time.NewTicker(BulkFlushInterval)
	defer ticker.Stop()
	
	for range ticker.C {
		db.flushResultBuffer()
	}
}

// GetTargets retrieves a batch of pending targets using prepared statement with caching
func (db *DB) GetTargets(limit int) ([]models.Target, error) {
	// Check cache first
	cacheKey := cache.KeyForTargets(limit, false)
	if cached, found := db.queryCache.Get(cacheKey); found {
		return cached.([]models.Target), nil
	}
	
	stmt := db.getStmt(StmtGetTargets)
	rows, err := stmt.Query(limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var targets []models.Target
	for rows.Next() {
		var target models.Target
		if err := rows.Scan(&target.ID, &target.IP, &target.Port); err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	
	// Cache result for short period (5 seconds for targets)
	db.queryCache.SetWithTTL(cacheKey, targets, 5*time.Second)
	
	return targets, nil
}

// GetTargetsWithPriority retrieves targets prioritizing untested ones using prepared statement with caching
func (db *DB) GetTargetsWithPriority(limit int) ([]models.Target, error) {
	// Check cache first
	cacheKey := cache.KeyForTargets(limit, true)
	if cached, found := db.queryCache.Get(cacheKey); found {
		return cached.([]models.Target), nil
	}
	
	stmt := db.getStmt(StmtGetTargetsPriority)
	rows, err := stmt.Query(limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var targets []models.Target
	for rows.Next() {
		var target models.Target
		if err := rows.Scan(&target.ID, &target.IP, &target.Port); err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	
	// Cache result for short period (5 seconds for targets)
	db.queryCache.SetWithTTL(cacheKey, targets, 5*time.Second)
	
	return targets, nil
}

// GetUsernames retrieves a batch of usernames using prepared statement with caching
func (db *DB) GetUsernames(limit int) ([]string, error) {
	// Check cache first
	cacheKey := cache.KeyForCredentials("username", limit)
	if cached, found := db.queryCache.Get(cacheKey); found {
		return cached.([]string), nil
	}
	
	stmt := db.getStmt(StmtGetUsernames)
	rows, err := stmt.Query(limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var usernames []string
	for rows.Next() {
		var username string
		if err := rows.Scan(&username); err != nil {
			return nil, err
		}
		usernames = append(usernames, username)
	}
	
	// Cache for longer period (5 minutes for credentials)
	db.queryCache.SetWithTTL(cacheKey, usernames, 5*time.Minute)
	
	return usernames, nil
}

// GetPasswords retrieves a batch of passwords using prepared statement with caching
func (db *DB) GetPasswords(limit int) ([]string, error) {
	// Check cache first
	cacheKey := cache.KeyForCredentials("password", limit)
	if cached, found := db.queryCache.Get(cacheKey); found {
		return cached.([]string), nil
	}
	
	stmt := db.getStmt(StmtGetPasswords)
	rows, err := stmt.Query(limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var passwords []string
	for rows.Next() {
		var password string
		if err := rows.Scan(&password); err != nil {
			return nil, err
		}
		passwords = append(passwords, password)
	}
	
	// Cache for longer period (5 minutes for credentials)
	db.queryCache.SetWithTTL(cacheKey, passwords, 5*time.Minute)
	
	return passwords, nil
}

// UpdateTargetStatus updates the status of a target using prepared statement
func (db *DB) UpdateTargetStatus(targetID int, status string) error {
	stmt := db.getStmt(StmtUpdateTargetStatus)
	_, err := stmt.Exec(status, time.Now(), targetID)
	return err
}

// UpdateTargetsBulkStatus updates multiple targets' status in a single transaction
func (db *DB) UpdateTargetsBulkStatus(targetIDs []int, status string) error {
	if len(targetIDs) == 0 {
		return nil
	}
	
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	
	stmt := db.getStmt(StmtUpdateTargetStatus)
	now := time.Now()
	
	for _, targetID := range targetIDs {
		_, err := tx.Stmt(stmt.Stmt).Exec(status, now, targetID)
		if err != nil {
			return err
		}
	}
	
	if err := tx.Commit(); err != nil {
		return err
	}
	
	// Invalidate target cache on update
	db.queryCache.Clear()
	
	return nil
}

// CreateTask creates a new task and assigns targets to it with expiration
func (db *DB) CreateTask(task *models.Task, targets []models.Target, expirationMinutes int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Calculate expiration time
	expiresAt := time.Now().Add(time.Duration(expirationMinutes) * time.Minute)

	// Insert the task with enhanced fields
	taskQuery := `
		INSERT INTO tasks (id, client_id, status, priority, target_count, progress, assigned_at, expires_at, last_update, batch_size)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err = tx.Exec(taskQuery, task.ID, task.ClientID, "assigned", task.Priority, len(targets), 0.0, time.Now(), expiresAt, time.Now(), len(targets))
	if err != nil {
		return err
	}

	// Prepare statements for bulk operations
	taskTargetStmt, err := tx.Prepare("INSERT INTO task_targets (task_id, target_id) VALUES ($1, $2)")
	if err != nil {
		return err
	}
	defer taskTargetStmt.Close()

	updateTargetStmt, err := tx.Prepare("UPDATE targets SET status = 'in_progress', updated_at = $1 WHERE id = $2")
	if err != nil {
		return err
	}
	defer updateTargetStmt.Close()

	now := time.Now()
	for _, target := range targets {
		_, err = taskTargetStmt.Exec(task.ID, target.ID)
		if err != nil {
			return err
		}
		_, err = updateTargetStmt.Exec(now, target.ID)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetExpiredTasks retrieves tasks that have expired
func (db *DB) GetExpiredTasks() ([]models.Task, error) {
	query := `
		SELECT id, client_id, status, priority, target_count, progress, attempts, success_count,
			   assigned_at, started_at, completed_at
		FROM tasks
		WHERE status IN ('assigned', 'in_progress') AND expires_at < $1
	`
	
	rows, err := db.Query(query, time.Now())
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
}

// ReclaimTask resets a task's targets to pending status
func (db *DB) ReclaimTask(taskID string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get all targets associated with this task
	targetQuery := `
		SELECT target_id FROM task_targets WHERE task_id = $1
	`
	rows, err := tx.Query(targetQuery, taskID)
	if err != nil {
		return err
	}
	
	var targetIDs []int
	for rows.Next() {
		var targetID int
		if err := rows.Scan(&targetID); err != nil {
			rows.Close()
			return err
		}
		targetIDs = append(targetIDs, targetID)
	}
	rows.Close()

	// Update targets back to pending in bulk
	if len(targetIDs) > 0 {
		updateStmt, err := tx.Prepare("UPDATE targets SET status = 'pending', updated_at = $1 WHERE id = $2")
		if err != nil {
			return err
		}
		defer updateStmt.Close()
		
		now := time.Now()
		for _, targetID := range targetIDs {
			_, err = updateStmt.Exec(now, targetID)
			if err != nil {
				return err
			}
		}
	}

	// Delete task-target associations
	_, err = tx.Exec("DELETE FROM task_targets WHERE task_id = $1", taskID)
	if err != nil {
		return err
	}

	// Update task status to expired
	_, err = tx.Exec("UPDATE tasks SET status = 'expired', last_update = $1 WHERE id = $2", time.Now(), taskID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// GetClientPerformance retrieves performance metrics for a client using prepared statement with caching
func (db *DB) GetClientPerformance(clientID string) (avgPPS float64, successRate float64, err error) {
	// Check cache first
	cacheKey := cache.KeyForClientPerf(clientID)
	if cached, found := db.queryCache.Get(cacheKey); found {
		perf := cached.([]float64)
		return perf[0], perf[1], nil
	}
	
	stmt := db.getStmt(StmtGetClientPerf)
	thirtyDaysAgo := time.Now().AddDate(0, 0, -30)
	err = stmt.QueryRow(clientID, thirtyDaysAgo).Scan(&avgPPS, &successRate)
	if err != nil && err != sql.ErrNoRows {
		return 0, 0, err
	}
	
	// Cache result for 1 minute
	db.queryCache.SetWithTTL(cacheKey, []float64{avgPPS, successRate}, 1*time.Minute)
	
	return avgPPS, successRate, nil
}

// UpdateTaskProgress updates task progress and metrics using prepared statement
func (db *DB) UpdateTaskProgress(taskID string, progress float64, attempts int, successes int, pps float64) error {
	stmt := db.getStmt(StmtUpdateTaskProgress)
	_, err := stmt.Exec(progress, attempts, successes, pps, time.Now(), taskID)
	return err
}

// GetActiveTasksForClient returns active tasks for a specific client
func (db *DB) GetActiveTasksForClient(clientID string) ([]models.Task, error) {
	query := `
		SELECT id, client_id, status, priority, target_count, progress
		FROM tasks
		WHERE client_id = $1 AND status IN ('assigned', 'in_progress')
	`
	
	rows, err := db.Query(query, clientID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []models.Task
	for rows.Next() {
		var task models.Task
		err := rows.Scan(
			&task.ID,
			&task.ClientID,
			&task.Status,
			&task.Priority,
			&task.TargetCount,
			&task.Progress,
		)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

// GetClientWorkload returns the current workload (number of targets) for a client using prepared statement
func (db *DB) GetClientWorkload(clientID string) (int, error) {
	stmt := db.getStmt(StmtGetClientWorkload)
	var workload int
	err := stmt.QueryRow(clientID).Scan(&workload)
	return workload, err
}

// GetDatabaseStats returns current database performance statistics
func (db *DB) GetDatabaseStats() map[string]interface{} {
	stats := db.Stats()
	
	dbStats := map[string]interface{}{
		"open_connections":    stats.OpenConnections,
		"in_use":              stats.InUse,
		"idle":                stats.Idle,
		"wait_count":          stats.WaitCount,
		"wait_duration":       stats.WaitDuration.String(),
		"max_idle_closed":     stats.MaxIdleClosed,
		"max_lifetime_closed": stats.MaxLifetimeClosed,
	}
	
	// Add buffer stats
	db.resultBufferMu.Lock()
	dbStats["result_buffer_size"] = len(db.resultBuffer)
	db.resultBufferMu.Unlock()
	
	// Add prepared statement count
	db.stmtCacheMu.RLock()
	dbStats["prepared_statements"] = len(db.stmtCache)
	db.stmtCacheMu.RUnlock()
	
	// Add query cache stats
	cacheStats := db.queryCache.GetStats()
	dbStats["query_cache"] = cacheStats
	
	return dbStats
}

// OptimizeDatabase runs database optimization queries
func (db *DB) OptimizeDatabase() error {
	// Run VACUUM to reclaim space
	if _, err := db.Exec("VACUUM ANALYZE"); err != nil {
		return fmt.Errorf("vacuum failed: %v", err)
	}
	
	// Update statistics for query planner
	tables := []string{"clients", "targets", "credentials", "results", "tasks", "task_targets"}
	for _, table := range tables {
		if _, err := db.Exec(fmt.Sprintf("ANALYZE %s", table)); err != nil {
			logger.ServerLogger.Error("Failed to analyze table", map[string]interface{}{
				"table": table,
				"error": err.Error(),
			})
		}
	}
	
	logger.ServerLogger.Info("Database optimization completed")
	return nil
}

// WorkQueueStats represents work queue statistics
type WorkQueueStats struct {
	TotalTargets     int64
	CompletedTargets int64
	PendingTargets   int64
	InProgressTargets int64
}

// GetWorkQueueStats returns current work queue statistics with caching
func (db *DB) GetWorkQueueStats() (*WorkQueueStats, error) {
	// Check cache first
	cacheKey := cache.KeyForWorkQueueStats()
	if cached, found := db.queryCache.Get(cacheKey); found {
		return cached.(*WorkQueueStats), nil
	}
	
	stats := &WorkQueueStats{}
	
	// Get total targets
	err := db.QueryRow("SELECT COUNT(*) FROM targets").Scan(&stats.TotalTargets)
	if err != nil {
		return nil, err
	}
	
	// Get targets by status
	statusQuery := `
		SELECT status, COUNT(*)
		FROM targets
		GROUP BY status
	`
	rows, err := db.Query(statusQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		
		switch status {
		case "completed":
			stats.CompletedTargets = count
		case "pending":
			stats.PendingTargets = count
		case "in_progress":
			stats.InProgressTargets = count
		}
	}
	
	if err := rows.Err(); err != nil {
		return nil, err
	}
	
	// Cache for 10 seconds
	db.queryCache.SetWithTTL(cacheKey, stats, 10*time.Second)
	
	return stats, nil
}

// GetActiveTasks returns all active (assigned or in_progress) tasks
func (db *DB) GetActiveTasks() ([]models.Task, error) {
	query := `
		SELECT id, client_id, status, priority, target_count, progress,
		       attempts, success_count, assigned_at, started_at, completed_at,
		       last_update
		FROM tasks
		WHERE status IN ('assigned', 'in_progress')
		ORDER BY assigned_at DESC
	`
	
	rows, err := db.Query(query)
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
			&task.LastUpdate,
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
	
	return tasks, rows.Err()
}

// GetTaskTargets retrieves all targets associated with a task
func (db *DB) GetTaskTargets(taskID string) ([]models.Target, error) {
	query := `
		SELECT t.id, t.ip, t.port, t.status, t.attempts, t.last_attempt,
		       t.created_at, t.updated_at
		FROM targets t
		JOIN task_targets tt ON t.id = tt.target_id
		WHERE tt.task_id = $1
		ORDER BY t.id
	`
	
	rows, err := db.Query(query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var targets []models.Target
	for rows.Next() {
		var target models.Target
		var lastAttempt *time.Time
		
		err := rows.Scan(
			&target.ID,
			&target.IP,
			&target.Port,
			&target.Status,
			&target.Attempts,
			&lastAttempt,
			&target.CreatedAt,
			&target.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		
		if lastAttempt != nil {
			target.LastAttempt = *lastAttempt
		}
		
		targets = append(targets, target)
	}
	
	return targets, rows.Err()
}

// CompleteTask marks a task as completed
func (db *DB) CompleteTask(taskID string) error {
	now := time.Now()
	
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	
	// Update task status
	_, err = tx.Exec(`
		UPDATE tasks
		SET status = 'completed',
		    completed_at = $1,
		    last_update = $1,
		    progress = 100
		WHERE id = $2
	`, now, taskID)
	if err != nil {
		return err
	}
	
	// Update associated targets to completed
	_, err = tx.Exec(`
		UPDATE targets
		SET status = 'completed', updated_at = $1
		WHERE id IN (
			SELECT target_id FROM task_targets WHERE task_id = $2
		)
	`, now, taskID)
	if err != nil {
		return err
	}
	
	return tx.Commit()
}

// UpdateTaskHealth updates task health metrics
func (db *DB) UpdateTaskHealth(taskID string, progress float64, pps float64,
	successCount int, errorCount int) error {
	
	query := `
		UPDATE tasks
		SET progress = $1,
		    avg_pps = $2,
		    success_count = success_count + $3,
		    attempts = attempts + $4,
		    last_update = $5
		WHERE id = $6
	`
	
	totalAttempts := successCount + errorCount
	_, err := db.Exec(query, progress, pps, successCount, totalAttempts, time.Now(), taskID)
	return err
}

// CreateOperation creates a new operation in the database
func (db *DB) CreateOperation(operation interface{}) error {
	// Convert operation struct to JSON for storage
	operationJSON, err := json.Marshal(operation)
	if err != nil {
		return fmt.Errorf("failed to marshal operation: %v", err)
	}
	
	// Parse the JSON to extract fields
	var op struct {
		ID          string          `json:"id"`
		Name        string          `json:"name"`
		Description string          `json:"description"`
		State       string          `json:"state"`
		Targets     []string        `json:"targets"`
		Credentials json.RawMessage `json:"credentials"`
		CreatedAt   time.Time       `json:"created_at"`
		Statistics  json.RawMessage `json:"statistics"`
		Settings    json.RawMessage `json:"settings"`
	}
	
	if err := json.Unmarshal(operationJSON, &op); err != nil {
		return fmt.Errorf("failed to unmarshal operation: %v", err)
	}
	
	// Convert arrays/objects to JSON strings for storage
	targetsJSON, _ := json.Marshal(op.Targets)
	
	query := `
		INSERT INTO operations (id, name, description, state, targets, credentials,
			created_at, statistics, settings)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	
	_, err = db.Exec(query,
		op.ID, op.Name, op.Description, op.State,
		string(targetsJSON), string(op.Credentials),
		op.CreatedAt, string(op.Statistics), string(op.Settings))
	
	return err
}

// GetOperation retrieves an operation by ID
func (db *DB) GetOperation(operationID string) (interface{}, error) {
	query := `
		SELECT id, name, description, state, targets, credentials,
			created_at, started_at, paused_at, completed_at, stopped_at,
			statistics, settings
		FROM operations
		WHERE id = $1
	`
	
	var op struct {
		ID          string
		Name        string
		Description string
		State       string
		Targets     string
		Credentials string
		CreatedAt   time.Time
		StartedAt   sql.NullTime
		PausedAt    sql.NullTime
		CompletedAt sql.NullTime
		StoppedAt   sql.NullTime
		Statistics  sql.NullString
		Settings    sql.NullString
	}
	
	err := db.QueryRow(query, operationID).Scan(
		&op.ID, &op.Name, &op.Description, &op.State,
		&op.Targets, &op.Credentials, &op.CreatedAt,
		&op.StartedAt, &op.PausedAt, &op.CompletedAt, &op.StoppedAt,
		&op.Statistics, &op.Settings)
	
	if err != nil {
		return nil, err
	}
	
	// Build operation object
	// This is a simplified version - in practice, you'd unmarshal into the proper Operation struct
	result := map[string]interface{}{
		"id":          op.ID,
		"name":        op.Name,
		"description": op.Description,
		"state":       op.State,
		"created_at":  op.CreatedAt,
	}
	
	// Parse JSON fields
	var targets []string
	json.Unmarshal([]byte(op.Targets), &targets)
	result["targets"] = targets
	
	var credentials map[string]interface{}
	json.Unmarshal([]byte(op.Credentials), &credentials)
	result["credentials"] = credentials
	
	// Add optional timestamps
	if op.StartedAt.Valid {
		result["started_at"] = op.StartedAt.Time
	}
	if op.PausedAt.Valid {
		result["paused_at"] = op.PausedAt.Time
	}
	if op.CompletedAt.Valid {
		result["completed_at"] = op.CompletedAt.Time
	}
	if op.StoppedAt.Valid {
		result["stopped_at"] = op.StoppedAt.Time
	}
	
	// Parse optional JSON fields
	if op.Statistics.Valid {
		var stats map[string]interface{}
		json.Unmarshal([]byte(op.Statistics.String), &stats)
		result["statistics"] = stats
	}
	if op.Settings.Valid {
		var settings map[string]interface{}
		json.Unmarshal([]byte(op.Settings.String), &settings)
		result["settings"] = settings
	}
	
	return result, nil
}

// UpdateOperationState updates the state of an operation
func (db *DB) UpdateOperationState(operationID string, state string) error {
	now := time.Now()
	var query string
	
	switch state {
	case "running":
		query = `UPDATE operations SET state = $1, started_at = $2 WHERE id = $3`
		_, err := db.Exec(query, state, now, operationID)
		return err
	case "paused":
		query = `UPDATE operations SET state = $1, paused_at = $2 WHERE id = $3`
		_, err := db.Exec(query, state, now, operationID)
		return err
	case "completed":
		query = `UPDATE operations SET state = $1, completed_at = $2 WHERE id = $3`
		_, err := db.Exec(query, state, now, operationID)
		return err
	case "stopped":
		query = `UPDATE operations SET state = $1, stopped_at = $2 WHERE id = $3`
		_, err := db.Exec(query, state, now, operationID)
		return err
	default:
		query = `UPDATE operations SET state = $1 WHERE id = $2`
		_, err := db.Exec(query, state, operationID)
		return err
	}
}

// ListOperations returns all operations
func (db *DB) ListOperations() ([]interface{}, error) {
	query := `
		SELECT id, name, description, state, created_at, started_at,
			completed_at, stopped_at
		FROM operations
		ORDER BY created_at DESC
	`
	
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var operations []interface{}
	for rows.Next() {
		var id, name, description, state string
		var createdAt time.Time
		var startedAt, completedAt, stoppedAt sql.NullTime
		
		err := rows.Scan(&id, &name, &description, &state, &createdAt,
			&startedAt, &completedAt, &stoppedAt)
		if err != nil {
			return nil, err
		}
		
		op := map[string]interface{}{
			"id":          id,
			"name":        name,
			"description": description,
			"state":       state,
			"created_at":  createdAt,
		}
		
		if startedAt.Valid {
			op["started_at"] = startedAt.Time
		}
		if completedAt.Valid {
			op["completed_at"] = completedAt.Time
		}
		if stoppedAt.Valid {
			op["stopped_at"] = stoppedAt.Time
		}
		
		operations = append(operations, op)
	}
	
	return operations, rows.Err()
}

// DeleteOperation deletes an operation
func (db *DB) DeleteOperation(operationID string) error {
	// Delete in order of dependencies
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	
	// Delete related results
	_, err = tx.Exec("DELETE FROM results WHERE operation_id = $1", operationID)
	if err != nil {
		return err
	}
	
	// Delete related tasks
	_, err = tx.Exec("DELETE FROM task_targets WHERE task_id IN (SELECT id FROM tasks WHERE operation_id = $1)", operationID)
	if err != nil {
		return err
	}
	
	_, err = tx.Exec("DELETE FROM tasks WHERE operation_id = $1", operationID)
	if err != nil {
		return err
	}
	
	// Delete the operation
	_, err = tx.Exec("DELETE FROM operations WHERE id = $1", operationID)
	if err != nil {
		return err
	}
	
	return tx.Commit()
}

// GetOperationStats retrieves statistics for an operation
func (db *DB) GetOperationStats(operationID string) (*OperationStats, error) {
	stats := &OperationStats{}
	
	// Get total targets from operation
	var targetsJSON string
	err := db.QueryRow("SELECT targets FROM operations WHERE id = $1", operationID).Scan(&targetsJSON)
	if err != nil {
		return nil, err
	}
	
	var targets []string
	if err := json.Unmarshal([]byte(targetsJSON), &targets); err != nil {
		return nil, err
	}
	stats.TotalTargets = len(targets)
	
	// Get completed targets
	err = db.QueryRow(`
		SELECT COUNT(DISTINCT t.id)
		FROM targets t
		JOIN task_targets tt ON t.id = tt.target_id
		JOIN tasks tk ON tt.task_id = tk.id
		WHERE tk.operation_id = $1 AND t.status = 'completed'
	`, operationID).Scan(&stats.CompletedTargets)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	
	// Get attempts and successes
	err = db.QueryRow(`
		SELECT COALESCE(SUM(attempts), 0), COALESCE(SUM(success_count), 0)
		FROM tasks
		WHERE operation_id = $1
	`, operationID).Scan(&stats.TotalAttempts, &stats.SuccessfulFinds)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	
	// Get average PPS
	err = db.QueryRow(`
		SELECT COALESCE(AVG(avg_pps), 0)
		FROM tasks
		WHERE operation_id = $1 AND avg_pps > 0
	`, operationID).Scan(&stats.AveragePPS)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	
	stats.LastUpdate = time.Now()
	
	return stats, nil
}

// OperationStats represents operation statistics
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

// ClearPendingTargets clears all pending targets
func (db *DB) ClearPendingTargets() error {
	_, err := db.Exec("DELETE FROM targets WHERE status = 'pending'")
	return err
}

// BulkInsertTargets inserts multiple targets efficiently
func (db *DB) BulkInsertTargets(targets []models.Target) error {
	if len(targets) == 0 {
		return nil
	}
	
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	
	stmt, err := tx.Prepare(`
		INSERT INTO targets (ip, port, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	
	for _, target := range targets {
		_, err := stmt.Exec(target.IP, target.Port, target.Status,
			target.CreatedAt, target.UpdatedAt)
		if err != nil {
			return err
		}
	}
	
	return tx.Commit()
}

// SetOperationCredentials stores credentials for the current operation
func (db *DB) SetOperationCredentials(usernames, passwords []string) error {
	// Clear existing credentials
	if _, err := db.Exec("DELETE FROM credentials"); err != nil {
		return err
	}
	
	// Insert new credentials
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	
	stmt, err := tx.Prepare("INSERT INTO credentials (username, password, type) VALUES ($1, $2, $3)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	
	// Insert usernames
	for _, username := range usernames {
		if _, err := stmt.Exec(username, "", "username"); err != nil {
			return err
		}
	}
	
	// Insert passwords
	for _, password := range passwords {
		if _, err := stmt.Exec("", password, "password"); err != nil {
			return err
		}
	}
	
	return tx.Commit()
}

// ResetAllTargetsToPending resets all targets to pending status
func (db *DB) ResetAllTargetsToPending() error {
	_, err := db.Exec(`
		UPDATE targets
		SET status = 'pending', updated_at = $1
		WHERE status IN ('in_progress', 'assigned')
	`, time.Now())
	return err
}

// GetActiveTaskCount returns the number of active tasks
func (db *DB) GetActiveTaskCount() (int, error) {
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*)
		FROM tasks
		WHERE status IN ('assigned', 'in_progress')
	`).Scan(&count)
	return count, err
}

// GetCompletedTaskCount returns the number of completed tasks
func (db *DB) GetCompletedTaskCount() (int, error) {
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*)
		FROM tasks
		WHERE status = 'completed'
	`).Scan(&count)
	return count, err
}
