package models

import (
	"database/sql"
	"time"
)

// Client represents a connected brute-force client
type Client struct {
	ID               string    `json:"id" db:"id"`
	Hostname         string    `json:"hostname" db:"hostname"`
	OS               string    `json:"os" db:"os"`
	CPUCores         int       `json:"cpu_cores" db:"cpu_cores"`
	MaxThreads       int       `json:"max_threads" db:"max_threads"`
	Version          string    `json:"version" db:"version"`
	LastHeartbeat    time.Time `json:"last_heartbeat" db:"last_heartbeat"`
	Status           string    `json:"status" db:"status"` // online, offline
	TotalAttempts    int64     `json:"total_attempts" db:"total_attempts"`
	SuccessCount     int       `json:"success_count" db:"success_count"`
	CurrentPPS       float64   `json:"current_pps" db:"current_pps"`
	RegisteredAt     time.Time `json:"registered_at" db:"registered_at"`
	LastUpdated      time.Time `json:"last_updated" db:"last_updated"`
}

// Target represents an IP target
type Target struct {
	ID          int       `json:"id" db:"id"`
	IP          string    `json:"ip" db:"ip"`
	Port        int       `json:"port" db:"port"`
	Status      string    `json:"status" db:"status"` // pending, in_progress, completed, failed
	Attempts    int       `json:"attempts" db:"attempts"`
	LastAttempt time.Time `json:"last_attempt" db:"last_attempt"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
}

// Credential represents a username/password combination
type Credential struct {
	ID       int    `json:"id" db:"id"`
	Username string `json:"username" db:"username"`
	Password string `json:"password" db:"password"`
	Type     string `json:"type" db:"type"` // username or password
}

// Result represents a successful credential find
type Result struct {
	ID         int       `json:"id" db:"id"`
	TargetID   int       `json:"target_id" db:"target_id"`
	ClientID   string    `json:"client_id" db:"client_id"`
	IP         string    `json:"ip" db:"ip"`
	Port       int       `json:"port" db:"port"`
	Username   string    `json:"username" db:"username"`
	Password   string    `json:"password" db:"password"`
	FoundAt    time.Time `json:"found_at" db:"found_at"`
	Verified   bool      `json:"verified" db:"verified"`
}

// Task represents a brute-force task
type Task struct {
	ID           string         `json:"id" db:"id"`
	ClientID     string         `json:"client_id" db:"client_id"`
	Status       string         `json:"status" db:"status"` // assigned, in_progress, completed, failed
	Priority     int            `json:"priority" db:"priority"`
	TargetCount  int            `json:"target_count" db:"target_count"`
	Progress     float64        `json:"progress" db:"progress"`
	Attempts     int            `json:"attempts" db:"attempts"`
	SuccessCount int            `json:"success_count" db:"success_count"`
	AssignedAt   time.Time      `json:"assigned_at" db:"assigned_at"`
	StartedAt    time.Time      `json:"started_at" db:"started_at"`
	CompletedAt  time.Time      `json:"completed_at" db:"completed_at"`
	OperationID  string         `json:"operation_id" db:"operation_id"`
	LastUpdate   sql.NullTime   `json:"last_update" db:"last_update"`
}

// TaskTarget represents the relationship between tasks and targets
type TaskTarget struct {
	TaskID   string `json:"task_id" db:"task_id"`
	TargetID int    `json:"target_id" db:"target_id"`
}

// Stats represents system-wide statistics
type Stats struct {
	TotalClients      int     `json:"total_clients"`
	OnlineClients     int     `json:"online_clients"`
	TotalTargets      int     `json:"total_targets"`
	CompletedTargets  int     `json:"completed_targets"`
	TotalAttempts     int64   `json:"total_attempts"`
	SuccessfulFinds   int     `json:"successful_finds"`
	SystemPPS         float64 `json:"system_pps"`
	AverageClientPPS  float64 `json:"average_client_pps"`
}

// Config represents system configuration
type Config struct {
	Key       string    `json:"key" db:"key"`
	Value     string    `json:"value" db:"value"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}
