package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"rdp-brute-system/server/api"
	"rdp-brute-system/server/database"
	"rdp-brute-system/server/middleware"
	"rdp-brute-system/server/monitor"
	"rdp-brute-system/server/services"
	"rdp-brute-system/server/web"
	"rdp-brute-system/shared/logger"
	"rdp-brute-system/shared/metrics"
)

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		// Continue without .env file, use environment variables
	}
	
	// Initialize logging system
	logDir := getEnv("LOG_DIR", "./logs")
	silentMode := getEnv("SILENT_MODE", "true") == "true"
	if err := logger.InitializeLoggers(logDir, silentMode); err != nil {
		os.Exit(1)
	}
	
	// Set Gin to release mode for production
	if silentMode {
		gin.SetMode(gin.ReleaseMode)
	}
	
	// Get database configuration from environment variables
	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")
	dbUser := getEnv("DB_USER", "user")
	dbPassword := getEnv("DB_PASSWORD", "password")
	dbName := getEnv("DB_NAME", "rdp_brute")
	sslMode := getEnv("DB_SSLMODE", "disable")
	
	// Construct connection string
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		dbHost, dbPort, dbUser, dbPassword, dbName, sslMode)
	
	logger.ServerLogger.Info("Connecting to database", map[string]interface{}{
		"host": dbHost,
		"port": dbPort,
		"name": dbName,
	})
	
	// Initialize Database
	db, err := database.NewDB(dsn)
	if err != nil {
		logger.ServerLogger.Fatal("Failed to connect to database", map[string]interface{}{
			"error": err.Error(),
		})
	}
	db.CreateTables()

	// Initialize metrics
	metricsInstance := metrics.GetInstance()
	
	// Initialize Services
	clientService := services.NewClientService(db)
	taskService := services.NewTaskService(db)
	distService := services.NewDistributionService(taskService, clientService)
	operationService := services.NewOperationService(db, distService, taskService, clientService)

	// Start client cleanup routine
	clientService.StartCleanupRoutine()
	
	// Start distribution service (includes task recovery)
	distService.Start()
	defer distService.Stop()
	
	// Start performance history tracking
	historyDir := getEnv("HISTORY_DATA_DIR", "./data/history")
	retentionDays := 30 // Keep 30 days of history
	perfHistory := monitor.NewPerformanceHistory(historyDir, retentionDays)
	perfHistory.Start()
	defer perfHistory.Stop()
	
	// Start alert manager
	alertManager := monitor.NewAlertManager(metricsInstance, 5*time.Second)
	alertManager.Start()
	defer alertManager.Stop()
	
	// Start system monitoring with alert manager
	sysMonitor := monitor.NewSystemMonitor(metricsInstance, alertManager, 10*time.Second)
	sysMonitor.Start()
	defer sysMonitor.Stop()
	
	// Add alert handler to log alerts and record in history
	alertManager.AddHandler(func(alert *monitor.Alert, isResolved bool) {
		if isResolved {
			logger.ServerLogger.Info("Alert resolved", map[string]interface{}{
				"alert": alert.Name,
			})
		} else {
			logger.ServerLogger.Warn("Alert triggered", map[string]interface{}{
				"alert":       alert.Name,
				"description": alert.Description,
			})
		}
	})

	// Initialize WebSocket Hub with distribution service
	hub := web.NewHub(clientService, taskService, distService)
	go hub.Run()
	
	// Set the hub in ClientService for sending operation commands
	clientService.SetHub(hub)
	
	// Start dashboard broadcaster with performance history and alert manager
	dashboardBroadcaster := web.NewDashboardBroadcaster(hub, clientService, taskService, distService, perfHistory, alertManager, 2*time.Second)
	dashboardBroadcaster.Start()
	defer dashboardBroadcaster.Stop()

	// Setup Gin Router with middlewares
	router := gin.New()
	router.Use(gin.Recovery())
	
	// Only add Gin logger in development mode
	if !silentMode {
		router.Use(gin.Logger())
	}
	
	// Apply rate limiting to all routes
	router.Use(middleware.DefaultAPIRateLimiter())

	// Serve Static Files and HTML
	router.Static("/static", "./server/web/static")
	router.LoadHTMLGlob("server/web/*.html")

	router.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", nil)
	})

	// API Routes
	api.RegisterRoutes(router, taskService, clientService, distService, operationService)

	// WebSocket Route with specific rate limiting
	router.GET("/ws", middleware.DefaultWSRateLimiter(), func(c *gin.Context) {
		web.ServeWs(hub, c.Writer, c.Request)
	})

	// Get server host and port from environment
	serverHost := getEnv("SERVER_HOST", "0.0.0.0")
	serverPort := getEnv("SERVER_PORT", "8080")
	serverAddr := fmt.Sprintf("%s:%s", serverHost, serverPort)
	
	// Create HTTP server
	srv := &http.Server{
		Addr:         serverAddr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	
	// Start server in a goroutine
	go func() {
		logger.ServerLogger.Info("Server starting", map[string]interface{}{
			"address": serverAddr,
		})
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.ServerLogger.Fatal("Failed to run server", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}()
	
	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	
	logger.ServerLogger.Info("Shutdown signal received, stopping server")
	
	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	// Shutdown the HTTP server
	if err := srv.Shutdown(ctx); err != nil {
		logger.ServerLogger.Error("Server forced to shutdown", map[string]interface{}{
			"error": err.Error(),
		})
	}
	
	// Shutdown the WebSocket hub
	if err := hub.Shutdown(ctx); err != nil {
		logger.ServerLogger.Error("WebSocket hub shutdown error", map[string]interface{}{
			"error": err.Error(),
		})
	}
	
	// Close database connection
	if err := db.Close(); err != nil {
		logger.ServerLogger.Error("Database close error", map[string]interface{}{
			"error": err.Error(),
		})
	}
	
	logger.ServerLogger.Info("Server exited gracefully")
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
