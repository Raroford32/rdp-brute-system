package profiling

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"rdp-brute-system/shared/logging"
)

var (
	logger = logging.Default()

	// Performance metrics
	cpuUsage = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "system_cpu_usage_percent",
		Help: "Current CPU usage percentage",
	})

	memoryUsage = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "system_memory_usage_mb",
		Help: "Current memory usage in MB",
	})

	goroutineCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "system_goroutines_count",
		Help: "Current number of goroutines",
	})

	// Request metrics
	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request duration in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "endpoint", "status"})

	requestCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total number of HTTP requests",
	}, []string{"method", "endpoint", "status"})

	// Database metrics
	dbQueryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "database_query_duration_seconds",
		Help:    "Database query duration in seconds",
		Buckets: []float64{0.001, 0.01, 0.1, 1, 10},
	}, []string{"query_type", "table"})

	dbQueryCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "database_queries_total",
		Help: "Total number of database queries",
	}, []string{"query_type", "table", "status"})

	// Cache metrics
	cacheOperationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cache_operation_duration_seconds",
		Help:    "Cache operation duration in seconds",
		Buckets: []float64{0.0001, 0.001, 0.01, 0.1},
	}, []string{"operation", "cache_name"})

	cacheHitRate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cache_hit_rate",
		Help: "Cache hit rate percentage",
	}, []string{"cache_name"})

	// RDP client metrics
	rdpConnectionDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rdp_connection_duration_seconds",
		Help:    "RDP connection duration in seconds",
		Buckets: []float64{0.1, 1, 10, 30, 60},
	}, []string{"target", "status"})

	rdpPasswordsPerSecond = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "rdp_passwords_per_second",
		Help: "Current passwords per second rate",
	})

	rdpTotalAttempts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rdp_total_attempts",
		Help: "Total number of RDP password attempts",
	})

	rdpSuccessCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rdp_success_count",
		Help: "Total number of successful RDP connections",
	})

	// WebSocket metrics
	websocketConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "websocket_connections_active",
		Help: "Current number of active WebSocket connections",
	})

	websocketMessagesSent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "websocket_messages_sent_total",
		Help: "Total number of WebSocket messages sent",
	}, []string{"message_type"})

	websocketMessagesReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "websocket_messages_received_total",
		Help: "Total number of WebSocket messages received",
	}, []string{"message_type"})

	// Worker pool metrics
	workerPoolSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "worker_pool_size",
		Help: "Current size of worker pools",
	}, []string{"pool_name"})

	workerQueueSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "worker_queue_size",
		Help: "Current size of worker queues",
	}, []string{"pool_name"})

	workerTaskDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "worker_task_duration_seconds",
		Help:    "Worker task duration in seconds",
		Buckets: []float64{0.01, 0.1, 1, 10, 60},
	}, []string{"pool_name", "task_type"})
)

func init() {
	// Register metrics with Prometheus
	prometheus.MustRegister(cpuUsage)
	prometheus.MustRegister(memoryUsage)
	prometheus.MustRegister(goroutineCount)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(requestCount)
	prometheus.MustRegister(dbQueryDuration)
	prometheus.MustRegister(dbQueryCount)
	prometheus.MustRegister(cacheOperationDuration)
	prometheus.MustRegister(cacheHitRate)
	prometheus.MustRegister(rdpConnectionDuration)
	prometheus.MustRegister(rdpPasswordsPerSecond)
	prometheus.MustRegister(rdpTotalAttempts)
	prometheus.MustRegister(rdpSuccessCount)
	prometheus.MustRegister(websocketConnections)
	prometheus.MustRegister(websocketMessagesSent)
	prometheus.MustRegister(websocketMessagesReceived)
	prometheus.MustRegister(workerPoolSize)
	prometheus.MustRegister(workerQueueSize)
	prometheus.MustRegister(workerTaskDuration)
}

type Profiler struct {
	ctx            context.Context
	cancel         context.CancelFunc
	cpuProfileFile *os.File
	memProfileFile *os.File
	wg             sync.WaitGroup
}

func NewProfiler() *Profiler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Profiler{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (p *Profiler) StartCPUProfile(filename string) error {
	var err error
	p.cpuProfileFile, err = os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create CPU profile: %w", err)
	}

	if err := pprof.StartCPUProfile(p.cpuProfileFile); err != nil {
		p.cpuProfileFile.Close()
		return fmt.Errorf("could not start CPU profile: %w", err)
	}

	logger.Info("CPU profiling started", "filename", filename)
	return nil
}

func (p *Profiler) StopCPUProfile() {
	if p.cpuProfileFile != nil {
		pprof.StopCPUProfile()
		p.cpuProfileFile.Close()
		p.cpuProfileFile = nil
		logger.Info("CPU profiling stopped")
	}
}

func (p *Profiler) WriteMemoryProfile(filename string) error {
	var err error
	p.memProfileFile, err = os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create memory profile: %w", err)
	}

	runtime.GC() // get up-to-date statistics
	if err := pprof.WriteHeapProfile(p.memProfileFile); err != nil {
		p.memProfileFile.Close()
		return fmt.Errorf("could not write memory profile: %w", err)
	}

	p.memProfileFile.Close()
	logger.Info("Memory profile written", "filename", filename)
	return nil
}

func (p *Profiler) StartSystemMonitoring(interval time.Duration) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-p.ctx.Done():
				return
			case <-ticker.C:
				p.collectSystemMetrics()
			}
		}
	}()

	logger.Info("System monitoring started", "interval", interval)
}

func (p *Profiler) Stop() {
	p.cancel()
	p.StopCPUProfile()
	p.wg.Wait()
	logger.Info("Profiler stopped")
}

func (p *Profiler) collectSystemMetrics() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Update Prometheus metrics
	memoryUsage.Set(float64(m.Alloc) / 1024 / 1024) // Convert to MB
	goroutineCount.Set(float64(runtime.NumGoroutine()))

	// Log system resources
	logger.LogSystemResources(
		getCPUUsage(),
		float64(m.Alloc)/1024/1024,
		runtime.NumGoroutine(),
	)
}

func getCPUUsage() float64 {
	// This is a simplified CPU usage calculation
	// In a real implementation, you might want to use a more sophisticated method
	// or a library like github.com/shirou/gopsutil
	return 0.0 // Placeholder
}

// Middleware for HTTP request profiling
func HTTPRequestProfiler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		
		// Create a response writer to capture status code
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		
		// Call next handler
		next.ServeHTTP(rw, r)
		
		// Record metrics
		duration := time.Since(start).Seconds()
		method := r.Method
		endpoint := r.URL.Path
		status := fmt.Sprintf("%d", rw.statusCode)
		
		requestDuration.WithLabelValues(method, endpoint, status).Observe(duration)
		requestCount.WithLabelValues(method, endpoint, status).Inc()
		
		// Log request
		logger.LogRequest(method, endpoint, status, time.Since(start), map[string]interface{}{
			"user_agent": r.UserAgent(),
			"remote_addr": r.RemoteAddr,
		})
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(statusCode int) {
	rw.statusCode = statusCode
	rw.ResponseWriter.WriteHeader(statusCode)
}

// Database query profiling
func RecordDBQuery(queryType, table string, duration time.Duration, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}

	dbQueryDuration.WithLabelValues(queryType, table).Observe(duration.Seconds())
	dbQueryCount.WithLabelValues(queryType, table, status).Inc()

	logger.LogQuery(queryType+" "+table, duration, nil, err)
}

// Cache operation profiling
func RecordCacheOperation(operation, cacheName string, hit bool, duration time.Duration) {
	cacheOperationDuration.WithLabelValues(operation, cacheName).Observe(duration.Seconds())
	logger.LogCache(operation, cacheName, hit, duration)
}

// RDP connection profiling
func RecordRDPConnection(target, status string, duration time.Duration) {
	rdpConnectionDuration.WithLabelValues(target, status).Observe(duration.Seconds())
}

func RecordRDPAttempt(success bool) {
	rdpTotalAttempts.Inc()
	if success {
		rdpSuccessCount.Inc()
	}
}

func UpdatePasswordsPerSecond(rate float64) {
	rdpPasswordsPerSecond.Set(rate)
}

// WebSocket profiling
func RecordWebSocketConnection(delta int) {
	websocketConnections.Add(float64(delta))
}

func RecordWebSocketMessage(messageType, direction string) {
	if direction == "sent" {
		websocketMessagesSent.WithLabelValues(messageType).Inc()
	} else {
		websocketMessagesReceived.WithLabelValues(messageType).Inc()
	}
}

// Worker pool profiling
func RecordWorkerPoolMetrics(poolName string, poolSize, queueSize int) {
	workerPoolSize.WithLabelValues(poolName).Set(float64(poolSize))
	workerQueueSize.WithLabelValues(poolName).Set(float64(queueSize))
}

func RecordWorkerTask(poolName, taskType string, duration time.Duration) {
	workerTaskDuration.WithLabelValues(poolName, taskType).Observe(duration.Seconds())
}

// Setup profiling endpoints
func SetupProfilingEndpoints(mux *http.ServeMux) {
	// Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	// pprof endpoints
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Custom profiling endpoints
	mux.HandleFunc("/debug/cpu/start", func(w http.ResponseWriter, r *http.Request) {
		profiler := NewProfiler()
		if err := profiler.StartCPUProfile("cpu.prof"); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("CPU profiling started"))
	})

	mux.HandleFunc("/debug/cpu/stop", func(w http.ResponseWriter, r *http.Request) {
		profiler := NewProfiler()
		profiler.StopCPUProfile()
		w.Write([]byte("CPU profiling stopped"))
	})

	mux.HandleFunc("/debug/memory", func(w http.ResponseWriter, r *http.Request) {
		profiler := NewProfiler()
		if err := profiler.WriteMemoryProfile("mem.prof"); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("Memory profile written"))
	})
}

// Performance alerting
type AlertRule struct {
	Name        string
	Metric      string
	Threshold   float64
	Operator    string // ">", "<", ">=", "<=", "=="
	Duration    time.Duration
	Message     string
	Severity    string // "info", "warning", "error", "critical"
}

type AlertManager struct {
	rules  []AlertRule
	active map[string]time.Time
	mu     sync.RWMutex
}

func NewAlertManager() *AlertManager {
	return &AlertManager{
		active: make(map[string]time.Time),
	}
}

func (am *AlertManager) AddRule(rule AlertRule) {
	am.rules = append(am.rules, rule)
}

func (am *AlertManager) CheckRules() {
	for _, rule := range am.rules {
		am.checkRule(rule)
	}
}

func (am *AlertManager) checkRule(rule AlertRule) {
	var value float64
	var ok bool

	// Get metric value from Prometheus
	switch rule.Metric {
	case "system_cpu_usage_percent":
		value = getCPUUsage()
		ok = true
	case "system_memory_usage_mb":
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		value = float64(m.Alloc) / 1024 / 1024
		ok = true
	case "system_goroutines_count":
		value = float64(runtime.NumGoroutine())
		ok = true
	}

	if !ok {
		return
	}

	triggered := false
	switch rule.Operator {
	case ">":
		triggered = value > rule.Threshold
	case "<":
		triggered = value < rule.Threshold
	case ">=":
		triggered = value >= rule.Threshold
	case "<=":
		triggered = value <= rule.Threshold
	case "==":
		triggered = value == rule.Threshold
	}

	if triggered {
		am.mu.Lock()
		lastTriggered, exists := am.active[rule.Name]
		if !exists || time.Since(lastTriggered) >= rule.Duration {
			am.active[rule.Name] = time.Now()
			am.mu.Unlock()
			
			// Send alert
			am.sendAlert(rule, value)
		} else {
			am.mu.Unlock()
		}
	} else {
		am.mu.Lock()
		delete(am.active, rule.Name)
		am.mu.Unlock()
	}
}

func (am *AlertManager) sendAlert(rule AlertRule, value float64) {
	fields := map[string]interface{}{
		"rule_name":  rule.Name,
		"metric":     rule.Metric,
		"value":      value,
		"threshold":  rule.Threshold,
		"operator":   rule.Operator,
		"severity":   rule.Severity,
	}

	switch rule.Severity {
	case "info":
		logger.WithFields(fields).Info(rule.Message)
	case "warning":
		logger.WithFields(fields).Warn(rule.Message)
	case "error":
		logger.WithFields(fields).Error(rule.Message)
	case "critical":
		logger.WithFields(fields).Error(rule.Message)
	}
}