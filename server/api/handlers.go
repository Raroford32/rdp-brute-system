package api

import (
	"net/http"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"rdp-brute-system/server/models"
	"rdp-brute-system/server/services"
	"rdp-brute-system/shared/protocol"
)

type Handler struct {
	taskService *services.TaskService
	clientService *services.ClientService
	distService *services.DistributionService
	operationService *services.OperationService
}

func NewHandler(ts *services.TaskService, cs *services.ClientService, ds *services.DistributionService, os *services.OperationService) *Handler {
	return &Handler{
		taskService: ts,
		clientService: cs,
		distService: ds,
		operationService: os,
	}
}

func (h *Handler) CreateTask(c *gin.Context) {
	var task models.Task
	if err := c.ShouldBindJSON(&task); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.taskService.CreateTask(&task); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create task"})
		return
	}

	c.JSON(http.StatusCreated, task)
}

func (h *Handler) GetTasks(c *gin.Context) {
	tasks, err := h.taskService.GetTasks()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get tasks"})
		return
	}

	c.JSON(http.StatusOK, tasks)
}

func (h *Handler) GetClients(c *gin.Context) {
	clients := h.clientService.GetClients()
	c.JSON(http.StatusOK, clients)
}

func (h *Handler) StartTask(c *gin.Context) {
	h.distService.Start()
	c.JSON(http.StatusOK, gin.H{"message": "Task distribution started"})
}

func (h *Handler) StopTask(c *gin.Context) {
	h.distService.Stop()
	c.JSON(http.StatusOK, gin.H{"message": "Task distribution stopped"})
}

// UploadFiles handles file uploads for targets, usernames, and passwords
func (h *Handler) UploadFiles(c *gin.Context) {
	// Parse multipart form with a 256MB limit for large files
	err := c.Request.ParseMultipartForm(256 << 20)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse form data: " + err.Error()})
		return
	}

	uploadStats := make(map[string]interface{})
	
	// Handle IP targets file
	ipsFile, ipsHeader, err := c.Request.FormFile("ips")
	if err == nil {
		ipsFile.Close()
		startTime := time.Now()
		count, err := h.taskService.LoadIPsWithProgress(ipsHeader)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load IP targets: " + err.Error()})
			return
		}
		uploadStats["ips"] = map[string]interface{}{
			"count": count,
			"duration": time.Since(startTime).Seconds(),
			"size_mb": float64(ipsHeader.Size) / (1024 * 1024),
		}
	}

	// Handle usernames and passwords files
	usersFile, usersHeader, usersErr := c.Request.FormFile("users")
	passwordsFile, passwordsHeader, passwordsErr := c.Request.FormFile("passwords")

	if usersErr == nil && passwordsErr == nil {
		usersFile.Close()
		passwordsFile.Close()
		startTime := time.Now()
		userCount, passCount, err := h.taskService.LoadCredentialsWithProgress(usersHeader, passwordsHeader)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load credentials: " + err.Error()})
			return
		}
		uploadStats["credentials"] = map[string]interface{}{
			"user_count": userCount,
			"password_count": passCount,
			"duration": time.Since(startTime).Seconds(),
			"users_size_mb": float64(usersHeader.Size) / (1024 * 1024),
			"passwords_size_mb": float64(passwordsHeader.Size) / (1024 * 1024),
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Files uploaded successfully",
		"stats": uploadStats,
	})
}

// GetResults returns all successful brute-force results
func (h *Handler) GetResults(c *gin.Context) {
	results, err := h.taskService.GetResults()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get results"})
		return
	}

	c.JSON(http.StatusOK, results)
}

// GetDashboardStats returns dashboard statistics for WebSocket updates
func (h *Handler) GetDashboardStats(c *gin.Context) {
	clients := h.clientService.GetClients()
	results, _ := h.taskService.GetResults()

	// Calculate total PPS from all online clients
	var totalPPS float64
	var onlineClients int
	for _, client := range clients {
		if client.Status == "online" {
			totalPPS += client.CurrentPPS
			onlineClients++
		}
	}

	// Convert clients to the format expected by the frontend
	var clientsData []map[string]interface{}
	for _, client := range clients {
		clientData := map[string]interface{}{
			"id":     client.ID,
			"pps":    client.CurrentPPS,
			"status": client.Status,
		}
		clientsData = append(clientsData, clientData)
	}

	// Convert results to the format expected by the frontend
	var resultsData []map[string]interface{}
	for _, result := range results {
		resultData := map[string]interface{}{
			"ip":       result.IP,
			"username": result.Username,
			"password": result.Password,
		}
		resultsData = append(resultsData, resultData)
	}

	dashboardData := map[string]interface{}{
		"total_pps":       totalPPS,
		"successful_hits": len(results),
		"clients":         clientsData,
		"results":         resultsData,
		"online_clients":  onlineClients,
		"total_clients":   len(clients),
	}

	c.JSON(http.StatusOK, dashboardData)
}

// RegisterClient handles client registration
func (h *Handler) RegisterClient(c *gin.Context) {
	var regData protocol.RegisterData
	if err := c.ShouldBindJSON(&regData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Register client with the service
	clientID := h.clientService.RegisterClient(regData)
	
	// Return registration response
	response := protocol.RegisterResponse{
		ClientID: clientID,
	}
	
	c.JSON(http.StatusOK, response)
}

// ClientHeartbeat handles client heartbeat messages
func (h *Handler) ClientHeartbeat(c *gin.Context) {
	clientID := c.GetHeader("X-Client-ID")
	if clientID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Client ID required"})
		return
	}
	
	var hbData protocol.HeartbeatData
	if err := c.ShouldBindJSON(&hbData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	// Update client metrics
	h.clientService.UpdateClientMetrics(clientID, hbData)
	
	// Return heartbeat response
	response := protocol.HeartbeatResponse{
		Status: "ok",
	}
	
	c.JSON(http.StatusOK, response)
}

// ReportResult handles result reporting from clients
func (h *Handler) ReportResult(c *gin.Context) {
	clientID := c.GetHeader("X-Client-ID")
	if clientID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Client ID required"})
		return
	}
	
	var reportData protocol.ResultReportData
	if err := c.ShouldBindJSON(&reportData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	// Convert protocol results to models.Result format and store
	var results []protocol.Result
	for _, r := range reportData.Results {
		results = append(results, protocol.Result{
			IP:       r.IP,
			Port:     r.Port,
			Username: r.Username,
			Password: r.Password,
			Found:    r.Found,
		})
	}
	
	// Store results through task service
	h.taskService.StoreResults(clientID, reportData.TaskID, results)
	
	// Return response
	response := protocol.ReportResultResponse{
		Status: "accepted",
	}
	
	c.JSON(http.StatusOK, response)
}

// GetPerformanceHistory returns historical performance data
func (h *Handler) GetPerformanceHistory(c *gin.Context) {
	// Get query parameters
	period := c.DefaultQuery("period", "1h")
	metric := c.DefaultQuery("metric", "all")
	
	// This would typically fetch from the history service
	// For now, return a placeholder
	data := map[string]interface{}{
		"period": period,
		"metric": metric,
		"data":   []interface{}{},
	}
	
	c.JSON(http.StatusOK, data)
}

// GetAlerts returns active alerts
func (h *Handler) GetAlerts(c *gin.Context) {
	// This would typically fetch from the alert manager
	// For now, return a placeholder
	alerts := []interface{}{}
	
	c.JSON(http.StatusOK, alerts)
}

// GetCPUProfile returns CPU profiling data
func (h *Handler) GetCPUProfile(c *gin.Context) {
	duration := c.DefaultQuery("seconds", "30")
	
	// Enable CPU profiling
	if err := pprof.StartCPUProfile(c.Writer); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not start CPU profile"})
		return
	}
	
	// Parse duration
	seconds, _ := strconv.Atoi(duration)
	time.Sleep(time.Duration(seconds) * time.Second)
	
	pprof.StopCPUProfile()
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", "attachment; filename=cpu.prof")
}

// GetHeapProfile returns heap profiling data
func (h *Handler) GetHeapProfile(c *gin.Context) {
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", "attachment; filename=heap.prof")
	pprof.WriteHeapProfile(c.Writer)
}

// GetGoroutineProfile returns goroutine profiling data
func (h *Handler) GetGoroutineProfile(c *gin.Context) {
	c.Header("Content-Type", "text/plain")
	pprof.Lookup("goroutine").WriteTo(c.Writer, 2)
}

// Operation Management Handlers

// CreateOperation creates a new brute-force operation
func (h *Handler) CreateOperation(c *gin.Context) {
	var req struct {
		Name        string   `json:"name" binding:"required"`
		Description string   `json:"description"`
		Targets     []string `json:"targets" binding:"required,min=1"`
		Usernames   []string `json:"usernames" binding:"required,min=1"`
		Passwords   []string `json:"passwords" binding:"required,min=1"`
		Settings    struct {
			MaxThreadsPerClient int  `json:"max_threads_per_client"`
			Priority            int  `json:"priority"`
			AllowGracefulStop   bool `json:"allow_graceful_stop"`
		} `json:"settings"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Set defaults for settings
	if req.Settings.MaxThreadsPerClient == 0 {
		req.Settings.MaxThreadsPerClient = 10
	}
	if req.Settings.Priority == 0 {
		req.Settings.Priority = 2
	}

	// Create operation
	operation, err := h.operationService.CreateOperation(
		req.Name, req.Description, req.Targets,
		req.Usernames, req.Passwords,
		services.OperationSettings{
			MaxThreadsPerClient: req.Settings.MaxThreadsPerClient,
			Priority:            req.Settings.Priority,
			AllowGracefulStop:   req.Settings.AllowGracefulStop,
		},
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, operation)
}

// StartOperation starts a specific operation
func (h *Handler) StartOperation(c *gin.Context) {
	operationID := c.Param("id")
	
	if err := h.operationService.StartOperation(operationID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Operation started successfully"})
}

// StopOperation stops a specific operation
func (h *Handler) StopOperation(c *gin.Context) {
	operationID := c.Param("id")
	
	var req struct {
		Graceful bool `json:"graceful"`
	}
	
	// Parse optional JSON body
	c.ShouldBindJSON(&req)
	
	if err := h.operationService.StopOperation(operationID, req.Graceful); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	message := "Operation stopped successfully"
	if req.Graceful {
		message = "Graceful shutdown initiated"
	}
	
	c.JSON(http.StatusOK, gin.H{"message": message})
}

// PauseOperation pauses a running operation
func (h *Handler) PauseOperation(c *gin.Context) {
	operationID := c.Param("id")
	
	if err := h.operationService.PauseOperation(operationID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Operation paused successfully"})
}

// ResumeOperation resumes a paused operation
func (h *Handler) ResumeOperation(c *gin.Context) {
	operationID := c.Param("id")
	
	if err := h.operationService.ResumeOperation(operationID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Operation resumed successfully"})
}

// GetOperations lists all operations
func (h *Handler) GetOperations(c *gin.Context) {
	operations, err := h.operationService.ListOperations()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, operations)
}

// GetOperation gets a specific operation details
func (h *Handler) GetOperation(c *gin.Context) {
	operationID := c.Param("id")
	
	operation, err := h.operationService.GetOperation(operationID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, operation)
}

// DeleteOperation deletes a completed or stopped operation
func (h *Handler) DeleteOperation(c *gin.Context) {
	operationID := c.Param("id")
	
	if err := h.operationService.DeleteOperation(operationID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Operation deleted successfully"})
}

// GetActiveOperation returns the currently active operation
func (h *Handler) GetActiveOperation(c *gin.Context) {
	operation := h.operationService.GetActiveOperation()
	if operation == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "No active operation"})
		return
	}

	c.JSON(http.StatusOK, operation)
}
