package api

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"rdp-brute-system/server/database"
	"rdp-brute-system/server/services"
)

// HealthService manages health checks
type HealthService struct {
	db              *database.DB
	clientService   *services.ClientService
	distributionSvc *services.DistributionService
	workPoolSvc     *services.WorkPoolService
	
	checks          map[string]HealthCheck
	checksMu        sync.RWMutex
	lastCheckResult map[string]*HealthCheckResult
	resultMu        sync.RWMutex
}

// HealthCheck represents a health check function
type HealthCheck func(ctx context.Context) error

// HealthCheckResult stores the result of a health check
type HealthCheckResult struct {
	Status    string    `json:"status"`
	Error     string    `json:"error,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
	Duration  string    `json:"duration"`
}

// HealthStatus represents the overall health status
type HealthStatus struct {
	Status    string                           `json:"status"`
	Timestamp time.Time                        `json:"timestamp"`
	Checks    map[string]*HealthCheckResult    `json:"checks"`
	Metrics   map[string]interface{}           `json:"metrics"`
}

// NewHealthService creates a new health service
func NewHealthService(db *database.DB, clientSvc *services.ClientService, 
	distSvc *services.DistributionService, poolSvc *services.WorkPoolService) *HealthService {
	
	hs := &HealthService{
		db:              db,
		clientService:   clientSvc,
		distributionSvc: distSvc,
		workPoolSvc:     poolSvc,
		checks:          make(map[string]HealthCheck),
		lastCheckResult: make(map[string]*HealthCheckResult),
	}
	
	// Register default health checks
	hs.RegisterCheck("database", hs.checkDatabase)
	hs.RegisterCheck("clients", hs.checkClients)
	hs.RegisterCheck("work_pool", hs.checkWorkPool)
	hs.RegisterCheck("distribution", hs.checkDistribution)
	
	// Start periodic health checks
	go hs.periodicHealthCheck()
	
	return hs
}

// RegisterCheck registers a new health check
func (hs *HealthService) RegisterCheck(name string, check HealthCheck) {
	hs.checksMu.Lock()
	defer hs.checksMu.Unlock()
	hs.checks[name] = check
}

// GetHealth returns the current health status
func (hs *HealthService) GetHealth(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	
	status := &HealthStatus{
		Timestamp: time.Now(),
		Checks:    make(map[string]*HealthCheckResult),
		Metrics:   make(map[string]interface{}),
	}
	
	// Run all health checks in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex
	overallHealthy := true
	
	hs.checksMu.RLock()
	for name, check := range hs.checks {
		wg.Add(1)
		go func(name string, check HealthCheck) {
			defer wg.Done()
			
			start := time.Now()
			err := check(ctx)
			duration := time.Since(start)
			
			result := &HealthCheckResult{
				CheckedAt: time.Now(),
				Duration:  duration.String(),
			}
			
			if err != nil {
				result.Status = "unhealthy"
				result.Error = err.Error()
				mu.Lock()
				overallHealthy = false
				mu.Unlock()
			} else {
				result.Status = "healthy"
			}
			
			mu.Lock()
			status.Checks[name] = result
			mu.Unlock()
			
			// Store result for periodic checks
			hs.resultMu.Lock()
			hs.lastCheckResult[name] = result
			hs.resultMu.Unlock()
		}(name, check)
	}
	hs.checksMu.RUnlock()
	
	wg.Wait()
	
	// Set overall status
	if overallHealthy {
		status.Status = "healthy"
	} else {
		status.Status = "unhealthy"
	}
	
	// Add metrics
	status.Metrics = hs.collectMetrics()
	
	// Return appropriate status code
	statusCode := http.StatusOK
	if !overallHealthy {
		statusCode = http.StatusServiceUnavailable
	}
	
	c.JSON(statusCode, status)
}

// GetReadiness checks if the service is ready to accept traffic
func (hs *HealthService) GetReadiness(c *gin.Context) {
	// Quick readiness check
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	
	// Check database connectivity
	if err := hs.db.PingContext(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "not_ready",
			"error":  "database not reachable",
		})
		return
	}
	
	// Check if we have active clients
	activeClients := hs.clientService.GetActiveClientCount()
	if activeClients == 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "not_ready",
			"error":  "no active clients",
		})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{
		"status": "ready",
		"active_clients": activeClients,
	})
}

// GetLiveness checks if the service is alive
func (hs *HealthService) GetLiveness(c *gin.Context) {
	// Simple liveness check - just return OK
	c.JSON(http.StatusOK, gin.H{
		"status": "alive",
		"timestamp": time.Now(),
	})
}

// GetMetrics returns system metrics
func (hs *HealthService) GetMetrics(c *gin.Context) {
	metrics := hs.collectMetrics()
	c.JSON(http.StatusOK, metrics)
}

// Health check implementations
func (hs *HealthService) checkDatabase(ctx context.Context) error {
	return hs.db.PingContext(ctx)
}

func (hs *HealthService) checkClients(ctx context.Context) error {
	activeClients := hs.clientService.GetActiveClientCount()
	if activeClients == 0 {
		return ErrNoActiveClients
	}
	return nil
}

func (hs *HealthService) checkWorkPool(ctx context.Context) error {
	status := hs.workPoolSvc.GetPoolStatus()
	
	// Check if work pool is healthy
	if pending, ok := status["pending_targets"].(int); ok && pending > 10000 {
		return ErrWorkPoolOverloaded
	}
	
	return nil
}

func (hs *HealthService) checkDistribution(ctx context.Context) error {
	// Check distribution service health
	metrics := hs.distributionSvc.GetClientMetrics()
	
	if len(metrics) == 0 {
		return ErrNoClientsRegistered
	}
	
	// Check if any client is severely underperforming
	for _, m := range metrics {
		if m.AvgPPS < 10 && time.Since(m.LastUpdate) < 5*time.Minute {
			return ErrClientUnderperforming
		}
	}
	
	return nil
}

// collectMetrics collects system metrics
func (hs *HealthService) collectMetrics() map[string]interface{} {
	metrics := make(map[string]interface{})
	
	// Database metrics
	dbStats := hs.db.Stats()
	metrics["database"] = map[string]interface{}{
		"open_connections": dbStats.OpenConnections,
		"in_use":          dbStats.InUse,
		"idle":            dbStats.Idle,
		"wait_count":      dbStats.WaitCount,
		"wait_duration":   dbStats.WaitDuration.String(),
	}
	
	// Client metrics
	activeClients := hs.clientService.GetActiveClientCount()
	clientMetrics := hs.distributionSvc.GetClientMetrics()
	
	var totalPPS float64
	for _, m := range clientMetrics {
		totalPPS += m.AvgPPS
	}
	
	metrics["clients"] = map[string]interface{}{
		"active":    activeClients,
		"total":     len(clientMetrics),
		"total_pps": totalPPS,
	}
	
	// Work pool metrics
	poolStatus := hs.workPoolSvc.GetPoolStatus()
	metrics["work_pool"] = poolStatus
	
	// Get database statistics
	dbMetrics := hs.db.GetDatabaseStats()
	for k, v := range dbMetrics {
		metrics["db_"+k] = v
	}
	
	return metrics
}

// periodicHealthCheck runs health checks periodically
func (hs *HealthService) periodicHealthCheck() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		
		hs.checksMu.RLock()
		for name, check := range hs.checks {
			go func(name string, check HealthCheck) {
				start := time.Now()
				err := check(ctx)
				duration := time.Since(start)
				
				result := &HealthCheckResult{
					CheckedAt: time.Now(),
					Duration:  duration.String(),
				}
				
				if err != nil {
					result.Status = "unhealthy"
					result.Error = err.Error()
				} else {
					result.Status = "healthy"
				}
				
				hs.resultMu.Lock()
				hs.lastCheckResult[name] = result
				hs.resultMu.Unlock()
			}(name, check)
		}
		hs.checksMu.RUnlock()
		
		cancel()
	}
}

// RegisterHealthRoutes registers health check routes
func RegisterHealthRoutes(router *gin.Engine, healthService *HealthService) {
	health := router.Group("/health")
	{
		health.GET("/", healthService.GetHealth)
		health.GET("/ready", healthService.GetReadiness)
		health.GET("/live", healthService.GetLiveness)
		health.GET("/metrics", healthService.GetMetrics)
	}
}

// Health check errors
var (
	ErrNoActiveClients       = fmt.Errorf("no active clients")
	ErrWorkPoolOverloaded    = fmt.Errorf("work pool overloaded")
	ErrNoClientsRegistered   = fmt.Errorf("no clients registered")
	ErrClientUnderperforming = fmt.Errorf("client severely underperforming")
)