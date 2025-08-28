package server

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"rdp-brute-system/server/api"
	"rdp-brute-system/server/database"
	"rdp-brute-system/server/services"
	"rdp-brute-system/server/web"
	"rdp-brute-system/shared/logger"
)

//go:embed web/static/*
var staticFiles embed.FS

//go:embed web/*.html
var templateFiles embed.FS

func Run() {
	RunWithContext(context.Background())
}

func RunWithContext(ctx context.Context) {
	// Initialize logging system first
	if err := logger.InitializeLoggers("./logs", false); err != nil {
		fmt.Printf("Failed to initialize loggers: %v\n", err)
		os.Exit(1)
	}
	
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		fmt.Printf("No .env file found (this is normal)\n")
	}
	
	logger.ServerLogger.Info("Starting RDP Brute-Force Server", nil)
	
	
	if gin.Mode() == gin.DebugMode {
		gin.SetMode(gin.ReleaseMode)
	}
	
	dbPath := getEnv("DB_PATH", "./rdp_brute.db")
	
	// Construct SQLite connection string
	dsn := fmt.Sprintf("file:%s?cache=shared&mode=rwc&_journal_mode=WAL&_foreign_keys=on", dbPath)
	
	logger.ServerLogger.Info("Connecting to SQLite database", map[string]interface{}{
		"path": dbPath,
	})
	
	// Initialize Database with SQLite
	db, err := database.NewSQLiteDB(dsn)
	if err != nil {
		logger.ServerLogger.Fatal("Failed to connect to database", map[string]interface{}{
			"error": err.Error(),
		})
	}
	defer db.Close()
	
	// Create database tables
	db.CreateTables()
	
	logger.ServerLogger.Info("Database connected successfully", nil)
	
	taskService := services.NewTaskService(db)
	clientService := services.NewClientService(db)
	distributionService := services.NewDistributionService(taskService, clientService)
	
	hub := web.NewHub(clientService, taskService, distributionService)
	go hub.Run()
	
	clientService.SetHub(hub)
	
	apiHandlers := api.NewHandler(taskService, clientService, distributionService, nil)
	
	router := gin.New()
	
	router.Use(gin.Logger())
	router.Use(gin.Recovery())

	staticFS, _ := fs.Sub(staticFiles, "web/static")
	router.StaticFS("/static", http.FS(staticFS))
	
	router.SetHTMLTemplate(template.Must(template.ParseFS(templateFiles, "web/*.html")))

	router.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", nil)
	})
	
	router.POST("/api/upload", apiHandlers.UploadFiles)
	router.GET("/api/clients", apiHandlers.GetClients)
	router.GET("/api/tasks", apiHandlers.GetTasks)
	router.GET("/api/results", apiHandlers.GetResults)
	router.GET("/api/stats", apiHandlers.GetDashboardStats)
	
	router.GET("/ws", func(c *gin.Context) {
		web.ServeWs(hub, c.Writer, c.Request)
	})
	
	port := getEnv("PORT", "8080")
	host := getEnv("HOST", "0.0.0.0")
	
	// Create HTTP server
	srv := &http.Server{
		Addr:    fmt.Sprintf("%s:%s", host, port),
		Handler: router,
	}
	
	logger.ServerLogger.Info("Server starting", map[string]interface{}{
		"host": host,
		"port": port,
	})
	
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.ServerLogger.Fatal("Failed to start server", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}()
	
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		logger.ServerLogger.Info("Server context cancelled")
	case <-quit:
		logger.ServerLogger.Info("Shutdown signal received, stopping server")
	}
	
	// Create shutdown context with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	// Shutdown the HTTP server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.ServerLogger.Error("Server forced to shutdown", map[string]interface{}{
			"error": err.Error(),
		})
	}
	
	// Shutdown the WebSocket hub
	if err := hub.Shutdown(shutdownCtx); err != nil {
		logger.ServerLogger.Error("WebSocket hub shutdown error", map[string]interface{}{
			"error": err.Error(),
		})
	}
	
	logger.ServerLogger.Info("Server shutdown complete")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
