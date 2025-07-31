package logging

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Logger struct {
	*logrus.Logger
	config *Config
}

type Config struct {
	Level      string `yaml:"level"`
	Format     string `yaml:"format"`
	Output     string `yaml:"output"`
	MaxSize    int    `yaml:"max_size"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAge     int    `yaml:"max_age"`
	Compress   bool   `yaml:"compress"`
}

type ContextKey string

const (
	RequestIDKey ContextKey = "request_id"
	UserIDKey    ContextKey = "user_id"
	TraceIDKey   ContextKey = "trace_id"
)

var (
	defaultLogger *Logger
	once          sync.Once
)

func New(config *Config) (*Logger, error) {
	logger := logrus.New()
	
	// Set log level
	level, err := logrus.ParseLevel(config.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level: %w", err)
	}
	logger.SetLevel(level)

	// Set formatter
	switch config.Format {
	case "json":
		logger.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: time.RFC3339,
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime:  "timestamp",
				logrus.FieldKeyLevel: "level",
				logrus.FieldKeyMsg:   "message",
			},
		})
	case "text":
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: time.RFC3339,
			DisableColors:   false,
		})
	default:
		return nil, fmt.Errorf("unsupported log format: %s", config.Format)
	}

	// Set output
	var output io.Writer
	switch config.Output {
	case "stdout":
		output = os.Stdout
	case "stderr":
		output = os.Stderr
	default:
		// Assume it's a file path
		output = &lumberjack.Logger{
			Filename:   config.Output,
			MaxSize:    config.MaxSize,
			MaxBackups: config.MaxBackups,
			MaxAge:     config.MaxAge,
			Compress:   config.Compress,
		}
	}
	logger.SetOutput(output)

	return &Logger{
		Logger: logger,
		config: config,
	}, nil
}

func Default() *Logger {
	once.Do(func() {
		defaultLogger, _ = New(&Config{
			Level:      "info",
			Format:     "json",
			Output:     "stdout",
			MaxSize:    100,
			MaxBackups: 3,
			MaxAge:     28,
			Compress:   true,
		})
	})
	return defaultLogger
}

// Context logging methods
func (l *Logger) WithContext(ctx context.Context) *logrus.Entry {
	entry := l.Logger.WithContext(ctx)
	
	if requestID := ctx.Value(RequestIDKey); requestID != nil {
		entry = entry.WithField("request_id", requestID)
	}
	
	if userID := ctx.Value(UserIDKey); userID != nil {
		entry = entry.WithField("user_id", userID)
	}
	
	if traceID := ctx.Value(TraceIDKey); traceID != nil {
		entry = entry.WithField("trace_id", traceID)
	}
	
	return entry
}

// Performance logging methods
func (l *Logger) WithPerformance(operation string) *PerformanceLogger {
	return &PerformanceLogger{
		logger:    l,
		operation: operation,
		startTime: time.Now(),
		fields:    make(map[string]interface{}),
	}
}

type PerformanceLogger struct {
	logger    *Logger
	operation string
	startTime time.Time
	fields    map[string]interface{}
}

func (pl *PerformanceLogger) WithField(key string, value interface{}) *PerformanceLogger {
	pl.fields[key] = value
	return pl
}

func (pl *PerformanceLogger) WithFields(fields map[string]interface{}) *PerformanceLogger {
	for k, v := range fields {
		pl.fields[k] = v
	}
	return pl
}

func (pl *PerformanceLogger) Success() {
	duration := time.Since(pl.startTime)
	fields := pl.fields
	fields["duration_ms"] = duration.Milliseconds()
	fields["status"] = "success"
	
	pl.logger.WithFields(fields).Infof("Operation completed: %s", pl.operation)
}

func (pl *PerformanceLogger) Error(err error) {
	duration := time.Since(pl.startTime)
	fields := pl.fields
	fields["duration_ms"] = duration.Milliseconds()
	fields["status"] = "error"
	fields["error"] = err.Error()
	
	pl.logger.WithFields(fields).Errorf("Operation failed: %s", pl.operation)
}

// Request logging methods
func (l *Logger) LogRequest(method, path, status string, duration time.Duration, fields map[string]interface{}) {
	entry := l.WithFields(fields)
	entry = entry.WithFields(logrus.Fields{
		"type":     "request",
		"method":   method,
		"path":     path,
		"status":   status,
		"duration": duration.Milliseconds(),
	})
	
	if status[0] == '5' {
		entry.Error("HTTP request")
	} else if status[0] == '4' {
		entry.Warn("HTTP request")
	} else {
		entry.Info("HTTP request")
	}
}

// Database logging methods
func (l *Logger) LogQuery(query string, duration time.Duration, args []interface{}, err error) {
	fields := logrus.Fields{
		"type":     "database",
		"query":    query,
		"duration": duration.Milliseconds(),
		"args":     args,
	}
	
	if err != nil {
		fields["error"] = err.Error()
		l.WithFields(fields).Error("Database query failed")
	} else {
		l.WithFields(fields).Debug("Database query executed")
	}
}

// Cache logging methods
func (l *Logger) LogCache(operation, key string, hit bool, duration time.Duration) {
	fields := logrus.Fields{
		"type":     "cache",
		"operation": operation,
		"key":      key,
		"hit":      hit,
		"duration": duration.Milliseconds(),
	}
	
	l.WithFields(fields).Debug("Cache operation")
}

// Security logging methods
func (l *Logger) LogSecurity(event, userID, clientIP string, details map[string]interface{}) {
	fields := logrus.Fields{
		"type":      "security",
		"event":     event,
		"user_id":   userID,
		"client_ip": clientIP,
	}
	
	for k, v := range details {
		fields[k] = v
	}
	
	l.WithFields(fields).Warn("Security event")
}

// System resource logging methods
func (l *Logger) LogSystemResources(cpu, memory float64, goroutines int) {
	fields := logrus.Fields{
		"type":       "system",
		"cpu_percent": cpu,
		"memory_mb":  memory,
		"goroutines": goroutines,
	}
	
	l.WithFields(fields).Info("System resources")
}

// Business logic logging methods
func (l *Logger) LogBusiness(event, entityType, entityID string, details map[string]interface{}) {
	fields := logrus.Fields{
		"type":        "business",
		"event":       event,
		"entity_type": entityType,
		"entity_id":   entityID,
	}
	
	for k, v := range details {
		fields[k] = v
	}
	
	l.WithFields(fields).Info("Business event")
}

// Helper methods for common logging patterns
func (l *Logger) WithError(err error) *logrus.Entry {
	return l.Logger.WithError(err)
}

func (l *Logger) WithField(key string, value interface{}) *logrus.Entry {
	return l.Logger.WithField(key, value)
}

func (l *Logger) WithFields(fields logrus.Fields) *logrus.Entry {
	return l.Logger.WithFields(fields)
}

// Convenience methods that include caller information
func (l *Logger) DebugWithCaller(args ...interface{}) {
	l.WithCaller().Debug(args...)
}

func (l *Logger) InfoWithCaller(args ...interface{}) {
	l.WithCaller().Info(args...)
}

func (l *Logger) WarnWithCaller(args ...interface{}) {
	l.WithCaller().Warn(args...)
}

func (l *Logger) ErrorWithCaller(args ...interface{}) {
	l.WithCaller().Error(args...)
}

func (l *Logger) FatalWithCaller(args ...interface{}) {
	l.WithCaller().Fatal(args...)
}

func (l *Logger) PanicWithCaller(args ...interface{}) {
	l.WithCaller().Panic(args...)
}

func (l *Logger) WithCaller() *logrus.Entry {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		file = "unknown"
		line = 0
	}
	return l.WithField("caller", fmt.Sprintf("%s:%d", file, line))
}

// Global convenience functions
func Debug(args ...interface{}) {
	Default().Debug(args...)
}

func Info(args ...interface{}) {
	Default().Info(args...)
}

func Warn(args ...interface{}) {
	Default().Warn(args...)
}

func Error(args ...interface{}) {
	Default().Error(args...)
}

func Fatal(args ...interface{}) {
	Default().Fatal(args...)
}

func Panic(args ...interface{}) {
	Default().Panic(args...)
}

func Debugf(format string, args ...interface{}) {
	Default().Debugf(format, args...)
}

func Infof(format string, args ...interface{}) {
	Default().Infof(format, args...)
}

func Warnf(format string, args ...interface{}) {
	Default().Warnf(format, args...)
}

func Errorf(format string, args ...interface{}) {
	Default().Errorf(format, args...)
}

func Fatalf(format string, args ...interface{}) {
	Default().Fatalf(format, args...)
}

func Panicf(format string, args ...interface{}) {
	Default().Panicf(format, args...)
}

func WithError(err error) *logrus.Entry {
	return Default().WithError(err)
}

func WithField(key string, value interface{}) *logrus.Entry {
	return Default().WithField(key, value)
}

func WithFields(fields logrus.Fields) *logrus.Entry {
	return Default().WithFields(fields)
}

func WithContext(ctx context.Context) *logrus.Entry {
	return Default().WithContext(ctx)
}

func WithPerformance(operation string) *PerformanceLogger {
	return Default().WithPerformance(operation)
}