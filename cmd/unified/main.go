package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"rdp-brute-system/client"
	"rdp-brute-system/server"
)


var (
	mode = flag.String("mode", "unified", "Run mode: server, client, or unified")
	help = flag.Bool("help", false, "Show help message")
)

func main() {
	flag.Parse()

	if *help {
		showHelp()
		return
	}

	switch *mode {
	case "server":
		runServer()
	case "client":
		runClient()
	case "unified":
		runUnified()
	default:
		fmt.Printf("Invalid mode: %s\n", *mode)
		showHelp()
		os.Exit(1)
	}
}

func showHelp() {
	fmt.Println("RDP Brute-Force System - Unified Deployment")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  rdp-brute-unified [flags]")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  -mode string")
	fmt.Println("        Run mode: server, client, or unified (default \"unified\")")
	fmt.Println("  -help")
	fmt.Println("        Show this help message")
	fmt.Println()
	fmt.Println("Modes:")
	fmt.Println("  server   - Run only the server component")
	fmt.Println("  client   - Run only the client component")
	fmt.Println("  unified  - Run both server and client components")
	fmt.Println()
	fmt.Println("Environment Variables:")
	fmt.Println("  See .env.example for configuration options")
}

func runServer() {
	fmt.Println("Starting RDP Brute-Force Server...")
	server.Run()
}

func runClient() {
	fmt.Println("Starting RDP Brute-Force Client...")
	client.Run()
}

func runUnified() {
	fmt.Println("Starting RDP Brute-Force System in Unified Mode...")
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("Starting server component...")
		server.RunWithContext(ctx)
	}()

	time.Sleep(2 * time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("Starting client component...")
		client.RunWithContext(ctx)
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	fmt.Println("\nShutting down unified system...")
	cancel()
	
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("Unified system shutdown complete")
	case <-time.After(30 * time.Second):
		fmt.Println("Shutdown timeout reached, forcing exit")
	}
}
