package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	
	"rdp-brute-system/shared/logger"
)

// HistoricalDataPoint represents a single point in time for metrics
type HistoricalDataPoint struct {
	Timestamp       time.Time              `json:"timestamp"`
	Metrics         map[string]float64     `json:"metrics"`
	ActiveClients   int                    `json:"active_clients"`
	ActiveTasks     int                    `json:"active_tasks"`
	SuccessfulHits  int64                  `json:"successful_hits"`
	TotalAttempts   int64                  `json:"total_attempts"`
	Alerts          []Alert                `json:"alerts,omitempty"`
}

// PerformanceHistory manages historical performance data
type PerformanceHistory struct {
	dataDir         string
	retentionDays   int
	dataPoints      []HistoricalDataPoint
	mu              sync.RWMutex
	saveInterval    time.Duration
	aggregateInterval time.Duration
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	
	// Hourly aggregates for faster queries
	hourlyAggregates map[string]*HourlyAggregate
}

// HourlyAggregate holds aggregated data for an hour
type HourlyAggregate struct {
	Hour            time.Time          `json:"hour"`
	AvgMetrics      map[string]float64 `json:"avg_metrics"`
	MaxMetrics      map[string]float64 `json:"max_metrics"`
	MinMetrics      map[string]float64 `json:"min_metrics"`
	TotalAttempts   int64             `json:"total_attempts"`
	SuccessfulHits  int64             `json:"successful_hits"`
	AlertCount      int               `json:"alert_count"`
	DataPointCount  int               `json:"data_point_count"`
}

// NewPerformanceHistory creates a new performance history manager
func NewPerformanceHistory(dataDir string, retentionDays int) *PerformanceHistory {
	ctx, cancel := context.WithCancel(context.Background())
	
	ph := &PerformanceHistory{
		dataDir:          dataDir,
		retentionDays:    retentionDays,
		dataPoints:       make([]HistoricalDataPoint, 0),
		saveInterval:     5 * time.Minute,
		aggregateInterval: 1 * time.Hour,
		ctx:              ctx,
		cancel:           cancel,
		hourlyAggregates: make(map[string]*HourlyAggregate),
	}
	
	// Validate and create data directory if it doesn't exist
	// Ensure dataDir is within safe boundaries
	cleanDir := filepath.Clean(dataDir)
	if !filepath.IsAbs(cleanDir) {
		cleanDir, _ = filepath.Abs(cleanDir)
	}
	
	if err := os.MkdirAll(cleanDir, 0750); err != nil {
		logger.ServerLogger.Error("Failed to create history data directory", map[string]interface{}{
			"error": err.Error(),
		})
	}
	ph.dataDir = cleanDir
	
	// Load existing data
	ph.loadHistoricalData()
	
	return ph
}

// Start begins the historical data collection
func (ph *PerformanceHistory) Start() {
	ph.wg.Add(2)
	go ph.saveRoutine()
	go ph.cleanupRoutine()
	logger.ServerLogger.Info("Performance history tracking started")
}

// Stop stops the performance history manager
func (ph *PerformanceHistory) Stop() {
	ph.cancel()
	ph.wg.Wait()
	
	// Save any remaining data
	ph.saveCurrentData()
	logger.ServerLogger.Info("Performance history tracking stopped")
}

// AddDataPoint adds a new data point to the history
func (ph *PerformanceHistory) AddDataPoint(metrics map[string]float64, activeClients, activeTasks int, successfulHits, totalAttempts int64, alerts []Alert) {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	
	dataPoint := HistoricalDataPoint{
		Timestamp:      time.Now(),
		Metrics:        metrics,
		ActiveClients:  activeClients,
		ActiveTasks:    activeTasks,
		SuccessfulHits: successfulHits,
		TotalAttempts:  totalAttempts,
		Alerts:         alerts,
	}
	
	ph.dataPoints = append(ph.dataPoints, dataPoint)
	
	// Update hourly aggregate
	ph.updateHourlyAggregate(dataPoint)
	
	// Keep only recent data points in memory (last 24 hours)
	cutoff := time.Now().Add(-24 * time.Hour)
	var recentPoints []HistoricalDataPoint
	for _, dp := range ph.dataPoints {
		if dp.Timestamp.After(cutoff) {
			recentPoints = append(recentPoints, dp)
		}
	}
	ph.dataPoints = recentPoints
}

// updateHourlyAggregate updates the hourly aggregate with a new data point
func (ph *PerformanceHistory) updateHourlyAggregate(dp HistoricalDataPoint) {
	hourKey := dp.Timestamp.Format("2006-01-02-15")
	hour := dp.Timestamp.Truncate(time.Hour)
	
	agg, exists := ph.hourlyAggregates[hourKey]
	if !exists {
		agg = &HourlyAggregate{
			Hour:       hour,
			AvgMetrics: make(map[string]float64),
			MaxMetrics: make(map[string]float64),
			MinMetrics: make(map[string]float64),
		}
		ph.hourlyAggregates[hourKey] = agg
	}
	
	// Update aggregates
	agg.DataPointCount++
	agg.TotalAttempts += dp.TotalAttempts
	agg.SuccessfulHits += dp.SuccessfulHits
	agg.AlertCount += len(dp.Alerts)
	
	// Update metric aggregates
	for name, value := range dp.Metrics {
		// Average (we'll divide by count later)
		agg.AvgMetrics[name] += value
		
		// Max
		if current, ok := agg.MaxMetrics[name]; !ok || value > current {
			agg.MaxMetrics[name] = value
		}
		
		// Min
		if current, ok := agg.MinMetrics[name]; !ok || value < current {
			agg.MinMetrics[name] = value
		}
	}
}

// GetRecentHistory returns recent historical data points
func (ph *PerformanceHistory) GetRecentHistory(duration time.Duration) []HistoricalDataPoint {
	ph.mu.RLock()
	defer ph.mu.RUnlock()
	
	cutoff := time.Now().Add(-duration)
	var recent []HistoricalDataPoint
	
	for _, dp := range ph.dataPoints {
		if dp.Timestamp.After(cutoff) {
			recent = append(recent, dp)
		}
	}
	
	return recent
}

// GetHourlyAggregates returns hourly aggregates for a time range
func (ph *PerformanceHistory) GetHourlyAggregates(start, end time.Time) []HourlyAggregate {
	ph.mu.RLock()
	defer ph.mu.RUnlock()
	
	var aggregates []HourlyAggregate
	
	for _, agg := range ph.hourlyAggregates {
		if agg.Hour.After(start) && agg.Hour.Before(end) {
			// Calculate averages
			aggCopy := *agg
			for name, total := range aggCopy.AvgMetrics {
				aggCopy.AvgMetrics[name] = total / float64(agg.DataPointCount)
			}
			aggregates = append(aggregates, aggCopy)
		}
	}
	
	return aggregates
}

// GetMetricHistory returns history for a specific metric
func (ph *PerformanceHistory) GetMetricHistory(metricName string, duration time.Duration) []MetricDataPoint {
	ph.mu.RLock()
	defer ph.mu.RUnlock()
	
	cutoff := time.Now().Add(-duration)
	var metricHistory []MetricDataPoint
	
	for _, dp := range ph.dataPoints {
		if dp.Timestamp.After(cutoff) {
			if value, exists := dp.Metrics[metricName]; exists {
				metricHistory = append(metricHistory, MetricDataPoint{
					Timestamp: dp.Timestamp,
					Value:     value,
				})
			}
		}
	}
	
	return metricHistory
}

// isPathSafe checks if a path is within the allowed directory
func isPathSafe(path, allowedDir string) bool {
		// Clean and resolve paths
		cleanPath := filepath.Clean(path)
		cleanAllowed := filepath.Clean(allowedDir)
		
		// Make absolute
		if !filepath.IsAbs(cleanPath) {
			cleanPath, _ = filepath.Abs(cleanPath)
		}
		if !filepath.IsAbs(cleanAllowed) {
			cleanAllowed, _ = filepath.Abs(cleanAllowed)
		}
		
		// Check if path is within allowed directory
		rel, err := filepath.Rel(cleanAllowed, cleanPath)
		if err != nil {
			return false
		}
		
		// Path should not start with ".." (parent directory)
		return !strings.HasPrefix(rel, "..")
}

// MetricDataPoint represents a single metric value at a point in time
type MetricDataPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

// saveRoutine periodically saves data to disk
func (ph *PerformanceHistory) saveRoutine() {
	defer ph.wg.Done()
	
	ticker := time.NewTicker(ph.saveInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			ph.saveCurrentData()
		case <-ph.ctx.Done():
			return
		}
	}
}

// saveCurrentData saves current data points to disk
func (ph *PerformanceHistory) saveCurrentData() {
	ph.mu.RLock()
	defer ph.mu.RUnlock()
	
	if len(ph.dataPoints) == 0 {
		return
	}
	
	// Save data points by day
	dataByDay := make(map[string][]HistoricalDataPoint)
	for _, dp := range ph.dataPoints {
		day := dp.Timestamp.Format("2006-01-02")
		dataByDay[day] = append(dataByDay[day], dp)
	}
	
	for day, points := range dataByDay {
		filename := filepath.Join(ph.dataDir, fmt.Sprintf("history_%s.json", day))
		
		// Load existing data for the day
		existingData := ph.loadDayData(filename)
		
		// Merge with new data
		merged := ph.mergeDataPoints(existingData, points)
		
		// Save merged data
		data, err := json.Marshal(merged)
		if err != nil {
			logger.ServerLogger.Error("Failed to marshal historical data", map[string]interface{}{
			"error": err.Error(),
		})
			continue
		}
		
		// Validate filename is within dataDir
		if !isPathSafe(filename, ph.dataDir) {
			logger.ServerLogger.Error("Attempted to write to unsafe path", map[string]interface{}{
			"filename": filename,
		})
			continue
		}
		
		if err := os.WriteFile(filename, data, 0640); err != nil {
			logger.ServerLogger.Error("Failed to save historical data", map[string]interface{}{
			"error": err.Error(),
		})
		}
	}
	
	// Save hourly aggregates
	ph.saveHourlyAggregates()
}

// saveHourlyAggregates saves hourly aggregates to disk
func (ph *PerformanceHistory) saveHourlyAggregates() {
	aggregatesFile := filepath.Join(ph.dataDir, "hourly_aggregates.json")
	
	data, err := json.Marshal(ph.hourlyAggregates)
	if err != nil {
		logger.ServerLogger.Error("Failed to marshal hourly aggregates", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}
	
	// Validate file path
	if !isPathSafe(aggregatesFile, ph.dataDir) {
		logger.ServerLogger.Error("Attempted to write to unsafe path", map[string]interface{}{
			"filename": aggregatesFile,
		})
		return
	}
	
	if err := os.WriteFile(aggregatesFile, data, 0640); err != nil {
		logger.ServerLogger.Error("Failed to save hourly aggregates", map[string]interface{}{
			"error": err.Error(),
		})
	}
}

// loadDayData loads data for a specific day
func (ph *PerformanceHistory) loadDayData(filename string) []HistoricalDataPoint {
	// Validate filename is within dataDir
	if !isPathSafe(filename, ph.dataDir) {
		logger.ServerLogger.Error("Attempted to read from unsafe path", map[string]interface{}{
			"filename": filename,
		})
		return []HistoricalDataPoint{}
	}
	
	data, err := os.ReadFile(filename)
	if err != nil {
		return []HistoricalDataPoint{}
	}
	
	var points []HistoricalDataPoint
	if err := json.Unmarshal(data, &points); err != nil {
		logger.ServerLogger.Error("Failed to unmarshal historical data", map[string]interface{}{
			"error": err.Error(),
		})
		return []HistoricalDataPoint{}
	}
	
	return points
}

// mergeDataPoints merges two sets of data points, removing duplicates
func (ph *PerformanceHistory) mergeDataPoints(existing, new []HistoricalDataPoint) []HistoricalDataPoint {
	// Use a map to track unique timestamps
	uniquePoints := make(map[int64]HistoricalDataPoint)
	
	// Add existing points
	for _, dp := range existing {
		uniquePoints[dp.Timestamp.Unix()] = dp
	}
	
	// Add new points (will overwrite if duplicate)
	for _, dp := range new {
		uniquePoints[dp.Timestamp.Unix()] = dp
	}
	
	// Convert back to slice
	merged := make([]HistoricalDataPoint, 0, len(uniquePoints))
	for _, dp := range uniquePoints {
		merged = append(merged, dp)
	}
	
	return merged
}

// loadHistoricalData loads historical data from disk
func (ph *PerformanceHistory) loadHistoricalData() {
	// Load hourly aggregates
	aggregatesFile := filepath.Join(ph.dataDir, "hourly_aggregates.json")
	if data, err := os.ReadFile(aggregatesFile); err == nil {
		if err := json.Unmarshal(data, &ph.hourlyAggregates); err != nil {
			logger.ServerLogger.Error("Failed to load hourly aggregates", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}
	
	// Load recent data points (last 24 hours)
	cutoff := time.Now().Add(-24 * time.Hour)
	pattern := filepath.Join(ph.dataDir, "history_*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		logger.ServerLogger.Error("Failed to list history files", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}
	
	for _, file := range files {
		points := ph.loadDayData(file)
		for _, dp := range points {
			if dp.Timestamp.After(cutoff) {
				ph.dataPoints = append(ph.dataPoints, dp)
			}
		}
	}
}

// cleanupRoutine removes old data files
func (ph *PerformanceHistory) cleanupRoutine() {
	defer ph.wg.Done()
	
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	
	// Run cleanup immediately
	ph.cleanupOldData()
	
	for {
		select {
		case <-ticker.C:
			ph.cleanupOldData()
		case <-ph.ctx.Done():
			return
		}
	}
}

// cleanupOldData removes data older than retention period
func (ph *PerformanceHistory) cleanupOldData() {
	cutoff := time.Now().AddDate(0, 0, -ph.retentionDays)
	
	pattern := filepath.Join(ph.dataDir, "history_*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		logger.ServerLogger.Error("Failed to list history files for cleanup", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}
	
	for _, file := range files {
		// Extract date from filename
		base := filepath.Base(file)
		if len(base) >= 21 { // history_YYYY-MM-DD.json
			dateStr := base[8:18]
			if date, err := time.Parse("2006-01-02", dateStr); err == nil {
				if date.Before(cutoff) {
					if err := os.Remove(file); err != nil {
						logger.ServerLogger.Error("Failed to remove old history file", map[string]interface{}{
							"file": file,
							"error": err.Error(),
						})
					} else {
						logger.ServerLogger.Error("Removed old history file", map[string]interface{}{
							"file": file,
						})
					}
				}
			}
		}
	}
	
	// Clean up old hourly aggregates
	ph.mu.Lock()
	defer ph.mu.Unlock()
	
	for key, agg := range ph.hourlyAggregates {
		if agg.Hour.Before(cutoff) {
			delete(ph.hourlyAggregates, key)
		}
	}
}
