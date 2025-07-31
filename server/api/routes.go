package api

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"rdp-brute-system/server/services"
)

func RegisterRoutes(router *gin.Engine, taskService *services.TaskService, clientService *services.ClientService, distService *services.DistributionService, operationService *services.OperationService) {
	handler := NewHandler(taskService, clientService, distService, operationService)

	// Prometheus metrics endpoint
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	api := router.Group("/api")
	{
		// Task management
		api.POST("/tasks", handler.CreateTask)
		api.GET("/tasks", handler.GetTasks)
		api.POST("/tasks/start", handler.StartTask)
		api.POST("/tasks/stop", handler.StopTask)
		
		// File upload for targets and credentials
		api.POST("/upload", handler.UploadFiles)
		
		// Client management
		api.GET("/clients", handler.GetClients)
		
		// Results retrieval
		api.GET("/results", handler.GetResults)
		
		// Dashboard statistics
		api.GET("/stats", handler.GetDashboardStats)
		
		// Performance and history endpoints
		api.GET("/performance/history", handler.GetPerformanceHistory)
		api.GET("/alerts", handler.GetAlerts)
		
		// Profiling endpoints
		api.GET("/debug/pprof/profile", handler.GetCPUProfile)
		api.GET("/debug/pprof/heap", handler.GetHeapProfile)
		api.GET("/debug/pprof/goroutine", handler.GetGoroutineProfile)
		
		// Client API v1 endpoints
		v1 := api.Group("/v1/client")
		{
			v1.POST("/register", handler.RegisterClient)
			v1.POST("/heartbeat", handler.ClientHeartbeat)
			v1.POST("/report", handler.ReportResult)
		}
		
		// Operation management endpoints
		operations := api.Group("/operations")
		{
			operations.POST("/create", handler.CreateOperation)
			operations.GET("", handler.GetOperations)
			operations.GET("/active", handler.GetActiveOperation)
			operations.GET("/:id", handler.GetOperation)
			operations.POST("/:id/start", handler.StartOperation)
			operations.POST("/:id/stop", handler.StopOperation)
			operations.POST("/:id/pause", handler.PauseOperation)
			operations.POST("/:id/resume", handler.ResumeOperation)
			operations.DELETE("/:id", handler.DeleteOperation)
		}
	}
}
