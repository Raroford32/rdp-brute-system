package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"rdp-brute-system/client/rdp"
	"rdp-brute-system/client/worker"
	"rdp-brute-system/shared/logger"
	"rdp-brute-system/shared/protocol"
)

func Run() {
	RunWithContext(context.Background())
}

func RunWithContext(ctx context.Context) {
	if err := logger.InitializeLoggers("./logs", false); err != nil {
		fmt.Printf("Failed to initialize loggers: %v\n", err)
		os.Exit(1)
	}

	taskQueue := make(chan protocol.Task, 1000)
	resultQueue := make(chan rdp.Result, 1000)

	w := worker.New(1, 100, taskQueue, resultQueue)

	w.Start()

	logger.ClientLogger.Info("RDP Brute-Force Client started successfully", nil)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		logger.ClientLogger.Info("Client context cancelled", nil)
	case <-quit:
		logger.ClientLogger.Info("Client interrupt received", nil)
	}

	logger.ClientLogger.Info("Client shutting down...", nil)

	w.Stop()

	logger.ClientLogger.Info("Client shutdown complete", nil)
}
