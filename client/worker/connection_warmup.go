package worker

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"rdp-brute-system/client/rdp"
	"rdp-brute-system/shared/logger"
)

// ConnectionWarmer pre-establishes connections to improve latency
type ConnectionWarmer struct {
	worker         *Worker
	warmupTargets  map[string]*WarmupTarget
	warmupMu       sync.RWMutex
	warmupInterval time.Duration
	stopChan       chan bool
	
	// Metrics
	warmupHits     int64
	warmupMisses   int64
	totalWarmups   int64
}

// WarmupTarget represents a target for connection warming
type WarmupTarget struct {
	IP             string
	Port           int
	LastWarmup     time.Time
	WarmupSuccess  bool
	ProtocolHint   uint32
	ResponseTime   time.Duration
	Priority       int
}

// NewConnectionWarmer creates a new connection warmer
func NewConnectionWarmer(worker *Worker) *ConnectionWarmer {
	return &ConnectionWarmer{
		worker:         worker,
		warmupTargets:  make(map[string]*WarmupTarget),
		warmupInterval: 30 * time.Second,
		stopChan:       make(chan bool),
	}
}

// Start begins the connection warming routine
func (cw *ConnectionWarmer) Start() {
	go cw.warmupRoutine()
	logger.WorkerLogger.Info("Connection warmer started")
}

// Stop stops the connection warming
func (cw *ConnectionWarmer) Stop() {
	close(cw.stopChan)
}

// AddTarget adds a target for connection warming
func (cw *ConnectionWarmer) AddTarget(ip string, port int, priority int) {
	cw.warmupMu.Lock()
	defer cw.warmupMu.Unlock()
	
	key := fmt.Sprintf("%s:%d", ip, port)
	if _, exists := cw.warmupTargets[key]; !exists {
		cw.warmupTargets[key] = &WarmupTarget{
			IP:       ip,
			Port:     port,
			Priority: priority,
		}
	}
}

// GetWarmConnection retrieves a warmed connection if available
func (cw *ConnectionWarmer) GetWarmConnection(ip string, port int) (net.Conn, uint32, bool) {
	cw.warmupMu.RLock()
	key := fmt.Sprintf("%s:%d", ip, port)
	target, exists := cw.warmupTargets[key]
	cw.warmupMu.RUnlock()
	
	if !exists || !target.WarmupSuccess {
		atomic.AddInt64(&cw.warmupMisses, 1)
		return nil, 0, false
	}
	
	// Check if warmup is fresh
	if time.Since(target.LastWarmup) > cw.warmupInterval {
		atomic.AddInt64(&cw.warmupMisses, 1)
		return nil, 0, false
	}
	
	atomic.AddInt64(&cw.warmupHits, 1)
	
	// Establish pre-warmed connection
	conn, err := net.DialTimeout("tcp", key, 500*time.Millisecond)
	if err != nil {
		// Mark as failed
		cw.warmupMu.Lock()
		target.WarmupSuccess = false
		cw.warmupMu.Unlock()
		return nil, 0, false
	}
	
	return conn, target.ProtocolHint, true
}

// warmupRoutine periodically warms up connections
func (cw *ConnectionWarmer) warmupRoutine() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			cw.performWarmup()
		case <-cw.stopChan:
			return
		}
	}
}

// performWarmup warms up connections to high-priority targets
func (cw *ConnectionWarmer) performWarmup() {
	cw.warmupMu.RLock()
	targets := make([]*WarmupTarget, 0, len(cw.warmupTargets))
	for _, target := range cw.warmupTargets {
		if time.Since(target.LastWarmup) > cw.warmupInterval/2 {
			targets = append(targets, target)
		}
	}
	cw.warmupMu.RUnlock()
	
	// Sort by priority and limit batch size
	if len(targets) > 50 {
		targets = targets[:50]
	}
	
	// Warm up connections in parallel
	var wg sync.WaitGroup
	for _, target := range targets {
		wg.Add(1)
		go func(t *WarmupTarget) {
			defer wg.Done()
			cw.warmupConnection(t)
		}(target)
	}
	wg.Wait()
}

// warmupConnection performs the actual connection warmup
func (cw *ConnectionWarmer) warmupConnection(target *WarmupTarget) {
	start := time.Now()
	
	// Quick connectivity check
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", target.IP, target.Port), 1*time.Second)
	if err != nil {
		cw.warmupMu.Lock()
		target.WarmupSuccess = false
		target.LastWarmup = time.Now()
		cw.warmupMu.Unlock()
		return
	}
	defer conn.Close()
	
	// Test RDP negotiation to get protocol hint
	err = rdp.SendX224ConnectionRequest(conn, rdp.PROTOCOL_HYBRID|rdp.PROTOCOL_SSL|rdp.PROTOCOL_RDP)
	if err != nil {
		cw.warmupMu.Lock()
		target.WarmupSuccess = false
		target.LastWarmup = time.Now()
		cw.warmupMu.Unlock()
		return
	}
	
	// Read response to determine supported protocol
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	protocol, err := rdp.ReadX224ConnectionConfirm(conn)
	
	responseTime := time.Since(start)
	
	cw.warmupMu.Lock()
	target.LastWarmup = time.Now()
	target.ResponseTime = responseTime
	if err == nil {
		target.WarmupSuccess = true
		target.ProtocolHint = protocol
	} else {
		target.WarmupSuccess = false
	}
	cw.warmupMu.Unlock()
	
	atomic.AddInt64(&cw.totalWarmups, 1)
}

// GetMetrics returns warmup metrics
func (cw *ConnectionWarmer) GetMetrics() (hits, misses, total int64, hitRate float64) {
	hits = atomic.LoadInt64(&cw.warmupHits)
	misses = atomic.LoadInt64(&cw.warmupMisses)
	total = atomic.LoadInt64(&cw.totalWarmups)
	
	if hits+misses > 0 {
		hitRate = float64(hits) / float64(hits+misses) * 100
	}
	
	return
}

// PrioritizeTargets updates target priorities based on success patterns
func (cw *ConnectionWarmer) PrioritizeTargets(successfulIPs []string) {
	cw.warmupMu.Lock()
	defer cw.warmupMu.Unlock()
	
	// Increase priority for successful IPs from same subnet
	for _, successIP := range successfulIPs {
		// Extract subnet (simple /24 check)
		subnet := getSubnet24(successIP)
		
		for _, target := range cw.warmupTargets {
			if getSubnet24(target.IP) == subnet {
				target.Priority += 10
			}
		}
	}
}

// getSubnet24 extracts /24 subnet from IP
func getSubnet24(ip string) string {
	// Simple implementation - in production use proper IP parsing
	for i := len(ip) - 1; i >= 0; i-- {
		if ip[i] == '.' {
			return ip[:i]
		}
	}
	return ip
}
