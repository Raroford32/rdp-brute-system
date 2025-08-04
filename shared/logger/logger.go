package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

// LogLevel represents different log levels
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	FATAL
)

// Logger provides structured logging with file output
type Logger struct {
	*logrus.Logger
	component string
	silent    bool
}

var (
	// Global loggers for different components
	ServerLogger *Logger
	ClientLogger *Logger
	WorkerLogger *Logger
	RDPLogger    *Logger
)

// InitializeLoggers sets up all component loggers
func InitializeLoggers(logDir string, silent bool) error {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %v", err)
	}

	ServerLogger = NewLogger("server", logDir, silent)
	ClientLogger = NewLogger("client", logDir, silent)
	WorkerLogger = NewLogger("worker", logDir, silent)
	RDPLogger = NewLogger("rdp", logDir, silent)

	return nil
}

// NewLogger creates a new logger for a specific component
func NewLogger(component, logDir string, silent bool) *Logger {
	logger := logrus.New()
	
	// Configure log format
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "message",
		},
	})

	// Set up file rotation
	logFile := &lumberjack.Logger{
		Filename:   filepath.Join(logDir, fmt.Sprintf("%s.log", component)),
		MaxSize:    100, // MB
		MaxBackups: 5,
		MaxAge:     30, // days
		Compress:   true,
	}

	if silent {
		// Silent mode: only log to file
		logger.SetOutput(logFile)
		logger.SetLevel(logrus.InfoLevel)
	} else {
		// Development mode: log to both file and console
		multiWriter := io.MultiWriter(os.Stdout, logFile)
		logger.SetOutput(multiWriter)
		logger.SetLevel(logrus.DebugLevel)
	}

	return &Logger{
		Logger:    logger,
		component: component,
		silent:    silent,
	}
}

// WithFields adds structured fields to log entry
func (l *Logger) WithFields(fields map[string]interface{}) *logrus.Entry {
	return l.Logger.WithFields(fields).WithField("component", l.component)
}

// Debug logs debug messages (only in non-silent mode)
func (l *Logger) Debug(msg string, fields ...map[string]interface{}) {
	entry := l.Logger.WithField("component", l.component)
	if len(fields) > 0 {
		entry = entry.WithFields(fields[0])
	}
	entry.Debug(msg)
}

// Info logs info messages
func (l *Logger) Info(msg string, fields ...map[string]interface{}) {
	entry := l.Logger.WithField("component", l.component)
	if len(fields) > 0 {
		entry = entry.WithFields(fields[0])
	}
	entry.Info(msg)
}

// Warn logs warning messages
func (l *Logger) Warn(msg string, fields ...map[string]interface{}) {
	entry := l.Logger.WithField("component", l.component)
	if len(fields) > 0 {
		entry = entry.WithFields(fields[0])
	}
	entry.Warn(msg)
}

// Error logs error messages
func (l *Logger) Error(msg string, fields ...map[string]interface{}) {
	entry := l.Logger.WithField("component", l.component)
	if len(fields) > 0 {
		entry = entry.WithFields(fields[0])
	}
	entry.Error(msg)
}

// Fatal logs fatal messages and exits
func (l *Logger) Fatal(msg string, fields ...map[string]interface{}) {
	entry := l.Logger.WithField("component", l.component)
	if len(fields) > 0 {
		entry = entry.WithFields(fields[0])
	}
	entry.Fatal(msg)
}

// Progress logs progress information (silent in production)
func (l *Logger) Progress(msg string, fields ...map[string]interface{}) {
	if !l.silent {
		l.Info(msg, fields...)
	}
}

// Success logs successful operations
func (l *Logger) Success(msg string, fields ...map[string]interface{}) {
	entry := l.Logger.WithField("component", l.component).WithField("status", "success")
	if len(fields) > 0 {
		entry = entry.WithFields(fields[0])
	}
	entry.Info(msg)
}

// Failure logs failed operations
func (l *Logger) Failure(msg string, fields ...map[string]interface{}) {
	entry := l.Logger.WithField("component", l.component).WithField("status", "failure")
	if len(fields) > 0 {
		entry = entry.WithFields(fields[0])
	}
	entry.Error(msg)
}

// Performance logs performance metrics
func (l *Logger) Performance(msg string, fields ...map[string]interface{}) {
	entry := l.Logger.WithField("component", l.component).WithField("type", "performance")
	if len(fields) > 0 {
		entry = entry.WithFields(fields[0])
	}
	entry.Info(msg)
}

// Security logs security-related events
func (l *Logger) Security(msg string, fields ...map[string]interface{}) {
	entry := l.Logger.WithField("component", l.component).WithField("type", "security")
	if len(fields) > 0 {
		entry = entry.WithFields(fields[0])
	}
	entry.Warn(msg)
}

// SetSilent enables or disables silent mode
func (l *Logger) SetSilent(silent bool) {
	l.silent = silent
	if silent {
		l.Logger.SetLevel(logrus.InfoLevel)
	} else {
		l.Logger.SetLevel(logrus.DebugLevel)
	}
}

// IsSilent returns whether the logger is in silent mode
func (l *Logger) IsSilent() bool {
	return l.silent
}
