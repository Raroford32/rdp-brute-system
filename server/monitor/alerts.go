package monitor

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
	
	"rdp-brute-system/shared/metrics"
)

// AlertType represents the type of alert
type AlertType string

const (
	AlertTypeCritical AlertType = "critical"
	AlertTypeWarning  AlertType = "warning"
	AlertTypeInfo     AlertType = "info"
)

// Alert represents a performance alert
type Alert struct {
	ID          string
	Type        AlertType
	Name        string
	Description string
	Metric      string
	Threshold   float64
	Value       float64
	TriggeredAt time.Time
	ResolvedAt  *time.Time
	Count       int
}

// AlertCondition defines when an alert should be triggered
type AlertCondition struct {
	Name        string
	Description string
	MetricName  string
	Threshold   float64
	Operator    string // >, <, >=, <=, ==
	Type        AlertType
	Duration    time.Duration // How long the condition must persist
	Cooldown    time.Duration // How long before the alert can trigger again
}

// AlertManager manages performance alerts
type AlertManager struct {
	conditions      []AlertCondition
	activeAlerts    map[string]*Alert
	alertHistory    []Alert
	metrics         *metrics.Metrics
	mu              sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	checkInterval   time.Duration
	alertHandlers   []AlertHandler
	
	// Metric values cache
	metricsCache    map[string]float64
	metricsCacheMu  sync.RWMutex
}

// AlertHandler is called when an alert is triggered or resolved
type AlertHandler func(alert *Alert, isResolved bool)

// NewAlertManager creates a new alert manager
func NewAlertManager(metricsInstance *metrics.Metrics, checkInterval time.Duration) *AlertManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	am := &AlertManager{
		activeAlerts:   make(map[string]*Alert),
		alertHistory:   make([]Alert, 0),
		metrics:        metricsInstance,
		ctx:            ctx,
		cancel:         cancel,
		checkInterval:  checkInterval,
		alertHandlers:  make([]AlertHandler, 0),
		metricsCache:   make(map[string]float64),
	}
	
	// Define default alert conditions
	am.defineDefaultConditions()
	
	return am
}

// defineDefaultConditions sets up the default alert conditions
func (am *AlertManager) defineDefaultConditions() {
	am.conditions = []AlertCondition{
		{
			Name:        "High CPU Usage",
			Description: "CPU usage is above 80%",
			MetricName:  "cpu_usage",
			Threshold:   80.0,
			Operator:    ">",
			Type:        AlertTypeWarning,
			Duration:    1 * time.Minute,
			Cooldown:    5 * time.Minute,
		},
		{
			Name:        "Critical CPU Usage",
			Description: "CPU usage is above 95%",
			MetricName:  "cpu_usage",
			Threshold:   95.0,
			Operator:    ">",
			Type:        AlertTypeCritical,
			Duration:    30 * time.Second,
			Cooldown:    5 * time.Minute,
		},
		{
			Name:        "High Memory Usage",
			Description: "Memory usage is above 85%",
			MetricName:  "memory_percent",
			Threshold:   85.0,
			Operator:    ">",
			Type:        AlertTypeWarning,
			Duration:    1 * time.Minute,
			Cooldown:    5 * time.Minute,
		},
		{
			Name:        "Critical Memory Usage",
			Description: "Memory usage is above 95%",
			MetricName:  "memory_percent",
			Threshold:   95.0,
			Operator:    ">",
			Type:        AlertTypeCritical,
			Duration:    30 * time.Second,
			Cooldown:    5 * time.Minute,
		},
		{
			Name:        "Low PPS Performance",
			Description: "Passwords per second dropped below 100",
			MetricName:  "total_pps",
			Threshold:   100.0,
			Operator:    "<",
			Type:        AlertTypeWarning,
			Duration:    2 * time.Minute,
			Cooldown:    10 * time.Minute,
		},
		{
			Name:        "High Error Rate",
			Description: "Error rate is above 10%",
			MetricName:  "error_rate",
			Threshold:   10.0,
			Operator:    ">",
			Type:        AlertTypeWarning,
			Duration:    1 * time.Minute,
			Cooldown:    5 * time.Minute,
		},
		{
			Name:        "Critical Error Rate",
			Description: "Error rate is above 25%",
			MetricName:  "error_rate",
			Threshold:   25.0,
			Operator:    ">",
			Type:        AlertTypeCritical,
			Duration:    30 * time.Second,
			Cooldown:    5 * time.Minute,
		},
		{
			Name:        "High Connection Errors",
			Description: "Connection error rate is above 5%",
			MetricName:  "connection_error_rate",
			Threshold:   5.0,
			Operator:    ">",
			Type:        AlertTypeWarning,
			Duration:    1 * time.Minute,
			Cooldown:    5 * time.Minute,
		},
		{
			Name:        "Database Connection Pool Exhausted",
			Description: "Available database connections below 10%",
			MetricName:  "db_pool_available_percent",
			Threshold:   10.0,
			Operator:    "<",
			Type:        AlertTypeCritical,
			Duration:    10 * time.Second,
			Cooldown:    2 * time.Minute,
		},
		{
			Name:        "High Rate Limit Hits",
			Description: "Rate limit hits per minute above 100",
			MetricName:  "rate_limit_hits_per_minute",
			Threshold:   100.0,
			Operator:    ">",
			Type:        AlertTypeWarning,
			Duration:    1 * time.Minute,
			Cooldown:    5 * time.Minute,
		},
	}
}

// AddCondition adds a custom alert condition
func (am *AlertManager) AddCondition(condition AlertCondition) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.conditions = append(am.conditions, condition)
}

// AddHandler adds an alert handler
func (am *AlertManager) AddHandler(handler AlertHandler) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.alertHandlers = append(am.alertHandlers, handler)
}

// Start begins monitoring for alerts
func (am *AlertManager) Start() {
	am.wg.Add(1)
	go am.monitor()
	log.Println("Alert manager started")
}

// Stop stops the alert manager
func (am *AlertManager) Stop() {
	am.cancel()
	am.wg.Wait()
	log.Println("Alert manager stopped")
}

// monitor is the main monitoring loop
func (am *AlertManager) monitor() {
	defer am.wg.Done()
	
	ticker := time.NewTicker(am.checkInterval)
	defer ticker.Stop()
	
	conditionStates := make(map[string]time.Time)
	lastAlertTime := make(map[string]time.Time)
	
	for {
		select {
		case <-ticker.C:
			am.checkConditions(conditionStates, lastAlertTime)
		case <-am.ctx.Done():
			return
		}
	}
}

// checkConditions checks all alert conditions
func (am *AlertManager) checkConditions(conditionStates, lastAlertTime map[string]time.Time) {
	// Update metrics cache
	am.updateMetricsCache()
	
	for _, condition := range am.conditions {
		value, exists := am.getMetricValue(condition.MetricName)
		if !exists {
			continue
		}
		
		triggered := am.evaluateCondition(value, condition.Threshold, condition.Operator)
		alertKey := fmt.Sprintf("%s_%s", condition.Name, condition.MetricName)
		
		if triggered {
			// Check if condition has persisted long enough
			if startTime, ok := conditionStates[alertKey]; ok {
				if time.Since(startTime) >= condition.Duration {
					// Check cooldown
					if lastAlert, ok := lastAlertTime[alertKey]; ok {
						if time.Since(lastAlert) < condition.Cooldown {
							continue
						}
					}
					
					// Trigger alert
					am.triggerAlert(condition, value)
					lastAlertTime[alertKey] = time.Now()
					delete(conditionStates, alertKey)
				}
			} else {
				// Start tracking condition
				conditionStates[alertKey] = time.Now()
			}
		} else {
			// Condition not met, reset tracking
			delete(conditionStates, alertKey)
			
			// Check if we should resolve an active alert
			am.resolveAlert(alertKey)
		}
	}
}

// evaluateCondition evaluates if a condition is met
func (am *AlertManager) evaluateCondition(value, threshold float64, operator string) bool {
	switch operator {
	case ">":
		return value > threshold
	case "<":
		return value < threshold
	case ">=":
		return value >= threshold
	case "<=":
		return value <= threshold
	case "==":
		return value == threshold
	default:
		return false
	}
}

// triggerAlert creates and activates an alert
func (am *AlertManager) triggerAlert(condition AlertCondition, value float64) {
	am.mu.Lock()
	defer am.mu.Unlock()
	
	alertKey := fmt.Sprintf("%s_%s", condition.Name, condition.MetricName)
	
	// Check if alert already exists
	if existingAlert, ok := am.activeAlerts[alertKey]; ok {
		existingAlert.Count++
		existingAlert.Value = value
		return
	}
	
	// Create new alert
	alert := &Alert{
		ID:          alertKey,
		Type:        condition.Type,
		Name:        condition.Name,
		Description: fmt.Sprintf("%s (value: %.2f, threshold: %.2f)", condition.Description, value, condition.Threshold),
		Metric:      condition.MetricName,
		Threshold:   condition.Threshold,
		Value:       value,
		TriggeredAt: time.Now(),
		Count:       1,
	}
	
	am.activeAlerts[alertKey] = alert
	
	// Record in metrics
	am.metrics.RecordAlert(string(condition.Type), "triggered")
	
	// Call handlers
	for _, handler := range am.alertHandlers {
		go handler(alert, false)
	}
	
	log.Printf("Alert triggered: %s - %s", alert.Name, alert.Description)
}

// resolveAlert resolves an active alert
func (am *AlertManager) resolveAlert(alertKey string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	
	alert, ok := am.activeAlerts[alertKey]
	if !ok {
		return
	}
	
	// Mark as resolved
	now := time.Now()
	alert.ResolvedAt = &now
	
	// Move to history
	am.alertHistory = append(am.alertHistory, *alert)
	delete(am.activeAlerts, alertKey)
	
	// Record in metrics
	am.metrics.ResolveAlert(string(alert.Type), "resolved")
	
	// Call handlers
	for _, handler := range am.alertHandlers {
		go handler(alert, true)
	}
	
	log.Printf("Alert resolved: %s", alert.Name)
}

// GetActiveAlerts returns all active alerts
func (am *AlertManager) GetActiveAlerts() []Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	alerts := make([]Alert, 0, len(am.activeAlerts))
	for _, alert := range am.activeAlerts {
		alerts = append(alerts, *alert)
	}
	return alerts
}

// GetAlertHistory returns alert history
func (am *AlertManager) GetAlertHistory(limit int) []Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	start := len(am.alertHistory) - limit
	if start < 0 {
		start = 0
	}
	
	return am.alertHistory[start:]
}

// updateMetricsCache updates the cached metric values
func (am *AlertManager) updateMetricsCache() {
	am.metricsCacheMu.Lock()
	defer am.metricsCacheMu.Unlock()
	
	// This would normally pull from actual metrics
	// For now, we'll simulate some values
	// In production, this would interface with Prometheus or the metrics system
	
	// You would implement actual metric collection here
	// For example:
	// am.metricsCache["cpu_usage"] = getCurrentCPUUsage()
	// am.metricsCache["memory_percent"] = getCurrentMemoryPercent()
	// etc.
}

// getMetricValue gets a metric value from cache
func (am *AlertManager) getMetricValue(metricName string) (float64, bool) {
	am.metricsCacheMu.RLock()
	defer am.metricsCacheMu.RUnlock()
	
	value, exists := am.metricsCache[metricName]
	return value, exists
}

// SetMetricValue sets a metric value (for testing or external updates)
func (am *AlertManager) SetMetricValue(metricName string, value float64) {
	am.metricsCacheMu.Lock()
	defer am.metricsCacheMu.Unlock()
	
	am.metricsCache[metricName] = value
}