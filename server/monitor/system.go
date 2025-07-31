package monitor

import (
	"context"
	"os"
	"runtime"
	"sync"
	"time"
	
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	"rdp-brute-system/shared/logger"
	"rdp-brute-system/shared/metrics"
)

// SystemMonitor collects and reports system metrics
type SystemMonitor struct {
	metrics      *metrics.Metrics
	alertManager *AlertManager
	hostname     string
	interval     time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	
	// Last network counters for calculating rates
	lastNetSent  uint64
	lastNetRecv  uint64
	lastNetTime  time.Time
	mu           sync.Mutex
}

// NewSystemMonitor creates a new system monitor
func NewSystemMonitor(metricsInstance *metrics.Metrics, alertManager *AlertManager, interval time.Duration) *SystemMonitor {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	return &SystemMonitor{
		metrics:      metricsInstance,
		alertManager: alertManager,
		hostname:     hostname,
		interval:     interval,
		ctx:          ctx,
		cancel:       cancel,
		lastNetTime:  time.Now(),
	}
}

// Start begins monitoring system metrics
func (m *SystemMonitor) Start() {
	m.wg.Add(1)
	go m.monitor()
	logger.ServerLogger.Info("System monitoring started")
}

// Stop stops the system monitor
func (m *SystemMonitor) Stop() {
	m.cancel()
	m.wg.Wait()
	logger.ServerLogger.Info("System monitoring stopped")
}

// monitor is the main monitoring loop
func (m *SystemMonitor) monitor() {
	defer m.wg.Done()
	
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	
	// Collect metrics immediately
	m.collectMetrics()
	
	for {
		select {
		case <-ticker.C:
			m.collectMetrics()
		case <-m.ctx.Done():
			return
		}
	}
}

// collectMetrics collects all system metrics
func (m *SystemMonitor) collectMetrics() {
	// CPU usage
	cpuPercent, err := cpu.Percent(time.Second, false)
	if err == nil && len(cpuPercent) > 0 {
		m.metrics.CPUUsage.WithLabelValues(m.hostname).Set(cpuPercent[0])
		
		// Update alert manager
		if m.alertManager != nil {
			m.alertManager.SetMetricValue("cpu_usage", cpuPercent[0])
		}
	}
	
	// Memory usage
	memInfo, err := mem.VirtualMemory()
	if err == nil {
		m.metrics.MemoryUsage.WithLabelValues(m.hostname, "used").Set(float64(memInfo.Used))
		m.metrics.MemoryUsage.WithLabelValues(m.hostname, "total").Set(float64(memInfo.Total))
		m.metrics.MemoryUsage.WithLabelValues(m.hostname, "available").Set(float64(memInfo.Available))
		m.metrics.MemoryUsage.WithLabelValues(m.hostname, "percent").Set(memInfo.UsedPercent)
		
		// Update alert manager
		if m.alertManager != nil {
			m.alertManager.SetMetricValue("memory_percent", memInfo.UsedPercent)
		}
	}
	
	// Disk usage
	diskInfo, err := disk.Usage("/")
	if err == nil {
		m.metrics.DiskUsage.WithLabelValues(m.hostname, "/").Set(float64(diskInfo.Used))
		m.metrics.DiskUsage.WithLabelValues(m.hostname, "/").Set(float64(diskInfo.Used))
		
		// Update alert manager
		if m.alertManager != nil {
			m.alertManager.SetMetricValue("disk_usage_percent", diskInfo.UsedPercent)
		}
	}
	
	// Network I/O
	m.collectNetworkMetrics()
	
	// Goroutines
	goroutineCount := runtime.NumGoroutine()
	m.metrics.Goroutines.WithLabelValues(m.hostname).Set(float64(goroutineCount))
	
	// Update alert manager
	if m.alertManager != nil {
		m.alertManager.SetMetricValue("goroutines", float64(goroutineCount))
	}
}

// collectNetworkMetrics collects network I/O metrics
func (m *SystemMonitor) collectNetworkMetrics() {
	ioCounters, err := net.IOCounters(false)
	if err != nil || len(ioCounters) == 0 {
		return
	}
	
	m.mu.Lock()
	defer m.mu.Unlock()
	
	total := ioCounters[0]
	now := time.Now()
	
	// Calculate rates if we have previous data
	if m.lastNetSent > 0 && m.lastNetRecv > 0 {
		duration := now.Sub(m.lastNetTime).Seconds()
		if duration > 0 {
			sentRate := float64(total.BytesSent-m.lastNetSent) / duration
			recvRate := float64(total.BytesRecv-m.lastNetRecv) / duration
			
			// Update counters (these are cumulative)
			m.metrics.NetworkSent.WithLabelValues(m.hostname, "all").Add(float64(total.BytesSent - m.lastNetSent))
			m.metrics.NetworkReceived.WithLabelValues(m.hostname, "all").Add(float64(total.BytesRecv - m.lastNetRecv))
			
			// You could also track rates if needed
			m.metrics.Throughput.WithLabelValues("network_sent").Set(sentRate)
			m.metrics.Throughput.WithLabelValues("network_received").Set(recvRate)
		}
	}
	
	// Update last values
	m.lastNetSent = total.BytesSent
	m.lastNetRecv = total.BytesRecv
	m.lastNetTime = now
}

// CollectDatabaseStats collects database connection pool stats
func (m *SystemMonitor) CollectDatabaseStats(stats map[string]interface{}) {
	if openConns, ok := stats["open_connections"].(int); ok {
		m.metrics.DatabaseConnections.WithLabelValues("postgres", "open").Set(float64(openConns))
	}
	if idleConns, ok := stats["idle_connections"].(int); ok {
		m.metrics.DatabaseConnections.WithLabelValues("postgres", "idle").Set(float64(idleConns))
	}
	if inUseConns, ok := stats["in_use_connections"].(int); ok {
		m.metrics.DatabaseConnections.WithLabelValues("postgres", "in_use").Set(float64(inUseConns))
	}
}
