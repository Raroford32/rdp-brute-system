package utils

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"runtime"
	"strings"
	"time"
)

// GenerateID generates a unique ID
func GenerateID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// GetSystemInfo returns system information
func GetSystemInfo() (hostname string, os string, cpuCores int) {
	hostname, _ = GetHostname()
	os = runtime.GOOS
	cpuCores = runtime.NumCPU()
	return
}

// GetHostname returns the system hostname
func GetHostname() (string, error) {
	return net.LookupAddr("127.0.0.1")
}

// ParseIPRange parses IP ranges and CIDR notations
func ParseIPRange(input string) ([]string, error) {
	var ips []string
	
	// Check if it's a CIDR notation
	if strings.Contains(input, "/") {
		_, ipnet, err := net.ParseCIDR(input)
		if err != nil {
			return nil, err
		}
		
		for ip := ipnet.IP.Mask(ipnet.Mask); ipnet.Contains(ip); inc(ip) {
			ips = append(ips, ip.String())
		}
		return ips, nil
	}
	
	// Check if it's a range (e.g., 192.168.1.1-192.168.1.10)
	if strings.Contains(input, "-") {
		parts := strings.Split(input, "-")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid IP range format")
		}
		
		start := net.ParseIP(strings.TrimSpace(parts[0]))
		end := net.ParseIP(strings.TrimSpace(parts[1]))
		
		if start == nil || end == nil {
			return nil, fmt.Errorf("invalid IP address in range")
		}
		
		for ip := start; !ip.Equal(end); inc(ip) {
			ips = append(ips, ip.String())
		}
		ips = append(ips, end.String())
		return ips, nil
	}
	
	// Single IP
	ip := net.ParseIP(input)
	if ip == nil {
		return nil, fmt.Errorf("invalid IP address")
	}
	
	return []string{input}, nil
}

// inc increments an IP address
func inc(ip net.IP) {
	for j := len(ip) - 1; j >= 0; j-- {
		ip[j]++
		if ip[j] > 0 {
			break
		}
	}
}

// Retry executes a function with retry logic
func Retry(attempts int, delay time.Duration, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if i < attempts-1 {
			time.Sleep(delay)
		}
	}
	return err
}

// ChunkSlice splits a slice into chunks
func ChunkSlice[T any](slice []T, chunkSize int) [][]T {
	var chunks [][]T
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}
	return chunks
}

// GetMemoryUsage returns current memory usage in MB
func GetMemoryUsage() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / 1024 / 1024
}

// GetCPUUsage returns approximate CPU usage percentage
func GetCPUUsage() float64 {
	// This is a simplified version
	// In production, you'd want to use more sophisticated monitoring
	return float64(runtime.NumGoroutine()) / float64(runtime.NumCPU()) * 10
}
