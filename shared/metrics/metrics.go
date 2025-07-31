package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	instance *Metrics
	once     sync.Once
)

type Metrics struct {
	// Client metrics
	RDPConnectionsTotal    *prometheus.CounterVec
	RDPConnectionErrors    *prometheus.CounterVec
	RDPConnectionDuration  *prometheus.HistogramVec
	PasswordsPerSecond     *prometheus.GaugeVec
	WorkerPoolSize         *prometheus.GaugeVec
	ActiveWorkers          *prometheus.GaugeVec
	BatchProcessingTime    *prometheus.HistogramVec
	BatchSize              *prometheus.HistogramVec
	RetryAttempts          *prometheus.CounterVec

	// Server metrics
	HTTPRequestsTotal      *prometheus.CounterVec
	HTTPRequestDuration    *prometheus.HistogramVec
	WebSocketConnections   *prometheus.GaugeVec
	WebSocketMessages      *prometheus.CounterVec
	DatabaseConnections    *prometheus.GaugeVec
	DatabaseQueryDuration  *prometheus.HistogramVec
	CacheHits              *prometheus.CounterVec
	CacheMisses            *prometheus.CounterVec
	RateLimitHits          *prometheus.CounterVec

	// System metrics
	CPUUsage               *prometheus.GaugeVec
	MemoryUsage            *prometheus.GaugeVec
	DiskUsage              *prometheus.GaugeVec
	NetworkSent            *prometheus.CounterVec
	NetworkReceived        *prometheus.CounterVec
	Goroutines             *prometheus.GaugeVec

	// Task metrics
	TasksTotal             *prometheus.CounterVec
	TasksCompleted         *prometheus.CounterVec
	TasksFailed            *prometheus.CounterVec
	TaskDuration           *prometheus.HistogramVec
	TaskQueueSize          *prometheus.GaugeVec

	// Alert metrics
	AlertsTotal            *prometheus.CounterVec
	AlertsActive           *prometheus.GaugeVec

	// Performance metrics
	ResponseTimeP95        *prometheus.GaugeVec
	ResponseTimeP99        *prometheus.GaugeVec
	Throughput             *prometheus.GaugeVec
	ErrorRate              *prometheus.GaugeVec

	// Custom metrics collectors
	customMetrics map[string]prometheus.Metric
	mu            sync.RWMutex
}

func New() *Metrics {
	once.Do(func() {
		instance = &Metrics{
			customMetrics: make(map[string]prometheus.Metric),
		}
		instance.initializeMetrics()
	})
	return instance
}

func (m *Metrics) initializeMetrics() {
	// Client metrics
	m.RDPConnectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rdp_connections_total",
			Help: "Total number of RDP connection attempts",
		},
		[]string{"status", "worker_id"},
	)

	m.RDPConnectionErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rdp_connection_errors_total",
			Help: "Total number of RDP connection errors",
		},
		[]string{"error_type", "worker_id"},
	)

	m.RDPConnectionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "rdp_connection_duration_seconds",
			Help:    "Duration of RDP connections",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"status", "worker_id"},
	)

	m.PasswordsPerSecond = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "passwords_per_second",
			Help: "Number of passwords processed per second",
		},
		[]string{"worker_id", "target"},
	)

	m.WorkerPoolSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "worker_pool_size",
			Help: "Size of the worker pool",
		},
		[]string{"pool_type"},
	)

	m.ActiveWorkers = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "active_workers",
			Help: "Number of active workers",
		},
		[]string{"pool_type"},
	)

	m.BatchProcessingTime = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "batch_processing_duration_seconds",
			Help:    "Time taken to process batches",
			Buckets: []float64{0.001, 0.01, 0.1, 1, 5, 10},
		},
		[]string{"batch_type"},
	)

	m.BatchSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "batch_size",
			Help:    "Size of processed batches",
			Buckets: []float64{10, 50, 100, 500, 1000, 5000},
		},
		[]string{"batch_type"},
	)

	m.RetryAttempts = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "retry_attempts_total",
			Help: "Total number of retry attempts",
		},
		[]string{"operation", "worker_id"},
	)

	// Server metrics
	m.HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "endpoint", "status"},
	)

	m.HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)

	m.WebSocketConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "websocket_connections",
			Help: "Number of active WebSocket connections",
		},
		[]string{"connection_type"},
	)

	m.WebSocketMessages = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "websocket_messages_total",
			Help: "Total number of WebSocket messages",
		},
		[]string{"message_type", "direction"},
	)

	m.DatabaseConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "database_connections",
			Help: "Number of active database connections",
		},
		[]string{"database", "state"},
	)

	m.DatabaseQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "database_query_duration_seconds",
			Help:    "Duration of database queries",
			Buckets: []float64{0.001, 0.01, 0.1, 1, 5, 10},
		},
		[]string{"query_type", "table"},
	)

	m.CacheHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_hits_total",
			Help: "Total number of cache hits",
		},
		[]string{"cache_name"},
	)

	m.CacheMisses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_misses_total",
			Help: "Total number of cache misses",
		},
		[]string{"cache_name"},
	)

	m.RateLimitHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rate_limit_hits_total",
			Help: "Total number of rate limit hits",
		},
		[]string{"endpoint", "client_ip"},
	)

	// System metrics
	m.CPUUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "system_cpu_usage_percent",
			Help: "System CPU usage percentage",
		},
		[]string{"host"},
	)

	m.MemoryUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "system_memory_usage_bytes",
			Help: "System memory usage in bytes",
		},
		[]string{"host", "type"},
	)

	m.DiskUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "system_disk_usage_bytes",
			Help: "System disk usage in bytes",
		},
		[]string{"host", "mount_point"},
	)

	m.NetworkSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "system_network_sent_bytes",
			Help: "Total network bytes sent",
		},
		[]string{"host", "interface"},
	)

	m.NetworkReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "system_network_received_bytes",
			Help: "Total network bytes received",
		},
		[]string{"host", "interface"},
	)

	m.Goroutines = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "system_goroutines",
			Help: "Number of goroutines",
		},
		[]string{"host"},
	)

	// Task metrics
	m.TasksTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tasks_total",
			Help: "Total number of tasks",
		},
		[]string{"task_type"},
	)

	m.TasksCompleted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tasks_completed_total",
			Help: "Total number of completed tasks",
		},
		[]string{"task_type", "status"},
	)

	m.TasksFailed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tasks_failed_total",
			Help: "Total number of failed tasks",
		},
		[]string{"task_type", "error_type"},
	)

	m.TaskDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "task_duration_seconds",
			Help:    "Duration of task execution",
			Buckets: []float64{1, 5, 10, 30, 60, 300, 600},
		},
		[]string{"task_type"},
	)

	m.TaskQueueSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "task_queue_size",
			Help: "Size of the task queue",
		},
		[]string{"queue_name"},
	)

	// Alert metrics
	m.AlertsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "alerts_total",
			Help: "Total number of alerts",
		},
		[]string{"alert_type", "severity"},
	)

	m.AlertsActive = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "alerts_active",
			Help: "Number of active alerts",
		},
		[]string{"alert_type", "severity"},
	)

	// Performance metrics
	m.ResponseTimeP95 = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "response_time_p95_seconds",
			Help: "95th percentile response time",
		},
		[]string{"endpoint"},
	)

	m.ResponseTimeP99 = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "response_time_p99_seconds",
			Help: "99th percentile response time",
		},
		[]string{"endpoint"},
	)

	m.Throughput = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "throughput_per_second",
			Help: "Throughput per second",
		},
		[]string{"operation"},
	)

	m.ErrorRate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "error_rate_percent",
			Help: "Error rate percentage",
		},
		[]string{"operation"},
	)
}

// Helper methods for common metric operations
func (m *Metrics) RecordRDPConnection(status, workerID string, duration time.Duration) {
	m.RDPConnectionsTotal.WithLabelValues(status, workerID).Inc()
	m.RDPConnectionDuration.WithLabelValues(status, workerID).Observe(duration.Seconds())
}

func (m *Metrics) RecordRDPConnectionError(errorType, workerID string) {
	m.RDPConnectionErrors.WithLabelValues(errorType, workerID).Inc()
}

func (m *Metrics) UpdatePasswordsPerSecond(workerID, target string, value float64) {
	m.PasswordsPerSecond.WithLabelValues(workerID, target).Set(value)
}

func (m *Metrics) UpdateWorkerPoolSize(poolType string, size int) {
	m.WorkerPoolSize.WithLabelValues(poolType).Set(float64(size))
}

func (m *Metrics) UpdateActiveWorkers(poolType string, count int) {
	m.ActiveWorkers.WithLabelValues(poolType).Set(float64(count))
}

func (m *Metrics) RecordBatchProcessing(batchType string, duration time.Duration, size int) {
	m.BatchProcessingTime.WithLabelValues(batchType).Observe(duration.Seconds())
	m.BatchSize.WithLabelValues(batchType).Observe(float64(size))
}

func (m *Metrics) RecordRetryAttempt(operation, workerID string) {
	m.RetryAttempts.WithLabelValues(operation, workerID).Inc()
}

func (m *Metrics) RecordHTTPRequest(method, endpoint, status string, duration time.Duration) {
	m.HTTPRequestsTotal.WithLabelValues(method, endpoint, status).Inc()
	m.HTTPRequestDuration.WithLabelValues(method, endpoint).Observe(duration.Seconds())
}

func (m *Metrics) UpdateWebSocketConnections(connectionType string, count int) {
	m.WebSocketConnections.WithLabelValues(connectionType).Set(float64(count))
}

func (m *Metrics) RecordWebSocketMessage(messageType, direction string) {
	m.WebSocketMessages.WithLabelValues(messageType, direction).Inc()
}

func (m *Metrics) UpdateDatabaseConnections(database, state string, count int) {
	m.DatabaseConnections.WithLabelValues(database, state).Set(float64(count))
}

func (m *Metrics) RecordDatabaseQuery(queryType, table string, duration time.Duration) {
	m.DatabaseQueryDuration.WithLabelValues(queryType, table).Observe(duration.Seconds())
}

func (m *Metrics) RecordCacheHit(cacheName string) {
	m.CacheHits.WithLabelValues(cacheName).Inc()
}

func (m *Metrics) RecordCacheMiss(cacheName string) {
	m.CacheMisses.WithLabelValues(cacheName).Inc()
}

func (m *Metrics) RecordRateLimitHit(endpoint, clientIP string) {
	m.RateLimitHits.WithLabelValues(endpoint, clientIP).Inc()
}

func (m *Metrics) UpdateSystemMetrics(host string, cpuUsage float64, memoryUsage, goroutines int) {
	m.CPUUsage.WithLabelValues(host).Set(cpuUsage)
	m.MemoryUsage.WithLabelValues(host, "used").Set(float64(memoryUsage))
	m.Goroutines.WithLabelValues(host).Set(float64(goroutines))
}

func (m *Metrics) RecordTask(taskType, status string, duration time.Duration) {
	m.TasksTotal.WithLabelValues(taskType).Inc()
	if status == "completed" {
		m.TasksCompleted.WithLabelValues(taskType, status).Inc()
	} else {
		m.TasksFailed.WithLabelValues(taskType, status).Inc()
	}
	m.TaskDuration.WithLabelValues(taskType).Observe(duration.Seconds())
}

func (m *Metrics) UpdateTaskQueueSize(queueName string, size int) {
	m.TaskQueueSize.WithLabelValues(queueName).Set(float64(size))
}

func (m *Metrics) RecordAlert(alertType, severity string) {
	m.AlertsTotal.WithLabelValues(alertType, severity).Inc()
	m.AlertsActive.WithLabelValues(alertType, severity).Inc()
}

func (m *Metrics) ResolveAlert(alertType, severity string) {
	m.AlertsActive.WithLabelValues(alertType, severity).Dec()
}

func (m *Metrics) UpdatePerformanceMetrics(endpoint string, p95, p99 float64) {
	m.ResponseTimeP95.WithLabelValues(endpoint).Set(p95)
	m.ResponseTimeP99.WithLabelValues(endpoint).Set(p99)
}

func (m *Metrics) UpdateThroughput(operation string, value float64) {
	m.Throughput.WithLabelValues(operation).Set(value)
}

func (m *Metrics) UpdateErrorRate(operation string, rate float64) {
	m.ErrorRate.WithLabelValues(operation).Set(rate)
}

// Custom metrics management
func (m *Metrics) AddCustomMetric(name string, metric prometheus.Metric) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.customMetrics[name] = metric
}

func (m *Metrics) GetCustomMetric(name string) (prometheus.Metric, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	metric, exists := m.customMetrics[name]
	return metric, exists
}

func (m *Metrics) RemoveCustomMetric(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.customMetrics, name)
}

// GetInstance returns the singleton instance of Metrics
func GetInstance() *Metrics {
	return New()
}