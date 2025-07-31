package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server      ServerConfig      `mapstructure:"server"`
	Client      ClientConfig      `mapstructure:"client"`
	Database    DatabaseConfig    `mapstructure:"database"`
	Redis       RedisConfig       `mapstructure:"redis"`
	Monitoring  MonitoringConfig  `mapstructure:"monitoring"`
	Logging     LoggingConfig     `mapstructure:"logging"`
	Performance PerformanceConfig `mapstructure:"performance"`
}

type ServerConfig struct {
	Host                 string        `mapstructure:"host"`
	Port                 int           `mapstructure:"port"`
	ReadTimeout          time.Duration `mapstructure:"read_timeout"`
	WriteTimeout         time.Duration `mapstructure:"write_timeout"`
	IdleTimeout          time.Duration `mapstructure:"idle_timeout"`
	MaxConnections       int           `mapstructure:"max_connections"`
	RateLimit            int           `mapstructure:"rate_limit"`
	RateLimitWindow      time.Duration `mapstructure:"rate_limit_window"`
	GracefulShutdownTime time.Duration `mapstructure:"graceful_shutdown_time"`
}

type ClientConfig struct {
	WorkerPoolSize      int           `mapstructure:"worker_pool_size"`
	ConnectionPoolSize  int           `mapstructure:"connection_pool_size"`
	ConnectionTimeout   time.Duration `mapstructure:"connection_timeout"`
	BackoffInitialDelay time.Duration `mapstructure:"backoff_initial_delay"`
	BackoffMaxDelay     time.Duration `mapstructure:"backoff_max_delay"`
	BackoffMultiplier   float64       `mapstructure:"backoff_multiplier"`
	BatchSize           int           `mapstructure:"batch_size"`
	BatchTimeout        time.Duration `mapstructure:"batch_timeout"`
	MaxRetries          int           `mapstructure:"max_retries"`
}

type DatabaseConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	User            string        `mapstructure:"user"`
	Password        string        `mapstructure:"password"`
	DBName          string        `mapstructure:"dbname"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time"`
}

type RedisConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
	PoolSize int    `mapstructure:"pool_size"`
}

type MonitoringConfig struct {
	Enabled          bool          `mapstructure:"enabled"`
	PrometheusPort   int           `mapstructure:"prometheus_port"`
	MetricsInterval  time.Duration `mapstructure:"metrics_interval"`
	ResourceInterval time.Duration `mapstructure:"resource_interval"`
	AlertThresholds  AlertThresholds `mapstructure:"alert_thresholds"`
}

type AlertThresholds struct {
	CPUUsage     float64 `mapstructure:"cpu_usage"`
	MemoryUsage  float64 `mapstructure:"memory_usage"`
	DiskUsage    float64 `mapstructure:"disk_usage"`
	NetworkUsage float64 `mapstructure:"network_usage"`
	ErrorRate    float64 `mapstructure:"error_rate"`
}

type LoggingConfig struct {
	Level      string `mapstructure:"level"`
	Format     string `mapstructure:"format"`
	Output     string `mapstructure:"output"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAge     int    `mapstructure:"max_age"`
	Compress   bool   `mapstructure:"compress"`
}

type PerformanceConfig struct {
	EnableProfiling     bool          `mapstructure:"enable_profiling"`
	ProfilingPort       int           `mapstructure:"profiling_port"`
	CacheTTL            time.Duration `mapstructure:"cache_ttl"`
	EnableHotReload     bool          `mapstructure:"enable_hot_reload"`
	HotReloadInterval   time.Duration `mapstructure:"hot_reload_interval"`
	EnableRequestLog    bool          `mapstructure:"enable_request_log"`
	RequestLogSampling  float64       `mapstructure:"request_log_sampling"`
}

var (
	instance *Config
	once     sync.Once
)

func Load(configPath string) (*Config, error) {
	var err error
	once.Do(func() {
		instance, err = loadConfig(configPath)
	})
	return instance, err
}

func loadConfig(configPath string) (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	
	if configPath != "" {
		viper.SetConfigFile(configPath)
	} else {
		viper.AddConfigPath(".")
		viper.AddConfigPath("./config")
		viper.AddConfigPath("/etc/rdp-brute-system")
	}

	// Set defaults
	setDefaults()

	// Environment variables
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Read environment
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	return &config, nil
}

func setDefaults() {
	// Server defaults
	viper.SetDefault("server.host", "0.0.0.0")
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("server.read_timeout", "30s")
	viper.SetDefault("server.write_timeout", "30s")
	viper.SetDefault("server.idle_timeout", "60s")
	viper.SetDefault("server.max_connections", 1000)
	viper.SetDefault("server.rate_limit", 100)
	viper.SetDefault("server.rate_limit_window", "1s")
	viper.SetDefault("server.graceful_shutdown_time", "30s")

	// Client defaults
	viper.SetDefault("client.worker_pool_size", 10)
	viper.SetDefault("client.connection_pool_size", 5)
	viper.SetDefault("client.connection_timeout", "10s")
	viper.SetDefault("client.backoff_initial_delay", "100ms")
	viper.SetDefault("client.backoff_max_delay", "5s")
	viper.SetDefault("client.backoff_multiplier", 2.0)
	viper.SetDefault("client.batch_size", 100)
	viper.SetDefault("client.batch_timeout", "1s")
	viper.SetDefault("client.max_retries", 3)

	// Database defaults
	viper.SetDefault("database.host", "localhost")
	viper.SetDefault("database.port", 5432)
	viper.SetDefault("database.user", "postgres")
	viper.SetDefault("database.password", "postgres")
	viper.SetDefault("database.dbname", "rdp_brute")
	viper.SetDefault("database.max_open_conns", 25)
	viper.SetDefault("database.max_idle_conns", 5)
	viper.SetDefault("database.conn_max_lifetime", "5m")
	viper.SetDefault("database.conn_max_idle_time", "5m")

	// Redis defaults
	viper.SetDefault("redis.host", "localhost")
	viper.SetDefault("redis.port", 6379)
	viper.SetDefault("redis.password", "")
	viper.SetDefault("redis.db", 0)
	viper.SetDefault("redis.pool_size", 10)

	// Monitoring defaults
	viper.SetDefault("monitoring.enabled", true)
	viper.SetDefault("monitoring.prometheus_port", 9090)
	viper.SetDefault("monitoring.metrics_interval", "15s")
	viper.SetDefault("monitoring.resource_interval", "30s")
	viper.SetDefault("monitoring.alert_thresholds.cpu_usage", 80.0)
	viper.SetDefault("monitoring.alert_thresholds.memory_usage", 80.0)
	viper.SetDefault("monitoring.alert_thresholds.disk_usage", 80.0)
	viper.SetDefault("monitoring.alert_thresholds.network_usage", 80.0)
	viper.SetDefault("monitoring.alert_thresholds.error_rate", 5.0)

	// Logging defaults
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "json")
	viper.SetDefault("logging.output", "stdout")
	viper.SetDefault("logging.max_size", 100)
	viper.SetDefault("logging.max_backups", 3)
	viper.SetDefault("logging.max_age", 28)
	viper.SetDefault("logging.compress", true)

	// Performance defaults
	viper.SetDefault("performance.enable_profiling", false)
	viper.SetDefault("performance.profiling_port", 6060)
	viper.SetDefault("performance.cache_ttl", "5m")
	viper.SetDefault("performance.enable_hot_reload", true)
	viper.SetDefault("performance.hot_reload_interval", "30s")
	viper.SetDefault("performance.enable_request_log", false)
	viper.SetDefault("performance.request_log_sampling", 0.1)
}

func GetEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func GetEnvInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func GetEnvFloat(key string, defaultValue float64) float64 {
	if value, exists := os.LookupEnv(key); exists {
		if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
			return floatValue
		}
	}
	return defaultValue
}

func GetEnvBool(key string, defaultValue bool) bool {
	if value, exists := os.LookupEnv(key); exists {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func GetEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value, exists := os.LookupEnv(key); exists {
		if durationValue, err := time.ParseDuration(value); err == nil {
			return durationValue
		}
	}
	return defaultValue
}