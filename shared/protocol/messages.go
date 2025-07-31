package protocol

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// MessageType defines the type of message being sent
type MessageType string

const (
	// Client to Server messages
	MsgRegister      MessageType = "register"
	MsgHeartbeat     MessageType = "heartbeat"
	MsgTaskRequest   MessageType = "task_request"
	MsgResultReport  MessageType = "result_report"
	MsgStatusUpdate  MessageType = "status_update"

	// Server to Client messages
	MsgTaskAssign      MessageType = "task_assign"
	MsgConfigUpdate    MessageType = "config_update"
	MsgShutdown        MessageType = "shutdown"
	MsgAck             MessageType = "ack"
	MsgOperationStop   MessageType = "operation_stop"
	MsgOperationPause  MessageType = "operation_pause"
	MsgOperationResume MessageType = "operation_resume"
)

// JSONTime wraps time.Time to provide custom JSON marshaling
type JSONTime struct {
	time.Time
}

// MarshalJSON implements json.Marshaler interface
func (t JSONTime) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, t.Format(time.RFC3339))), nil
}

// UnmarshalJSON implements json.Unmarshaler interface
func (t *JSONTime) UnmarshalJSON(data []byte) error {
	// Remove quotes from JSON string
	str := strings.Trim(string(data), `"`)
	if str == "null" || str == "" {
		t.Time = time.Time{}
		return nil
	}
	
	parsedTime, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return fmt.Errorf("failed to parse time: %v", err)
	}
	t.Time = parsedTime
	return nil
}

// Message is the base structure for all protocol messages
type Message struct {
	Type      MessageType     `json:"type"`
	ClientID  string          `json:"client_id"`
	Timestamp JSONTime        `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

// RegisterData contains client registration information
type RegisterData struct {
	Hostname     string `json:"hostname"`
	OS           string `json:"os"`
	CPUCores     int    `json:"cpu_cores"`
	MaxThreads   int    `json:"max_threads"`
	Version      string `json:"version"`
}

// HeartbeatData contains client status information
type HeartbeatData struct {
	ActiveThreads   int     `json:"active_threads"`
	PPS             float64 `json:"pps"` // Passwords per second
	TotalAttempts   int64   `json:"total_attempts"`
	SuccessCount    int     `json:"success_count"`
	CPUUsage        float64 `json:"cpu_usage"`
	MemoryUsage     float64 `json:"memory_usage"`
	NetworkBandwidth float64 `json:"network_bandwidth"`
}

// TaskRequestData contains task request parameters
type TaskRequestData struct {
	MaxTasks      int `json:"max_tasks"`
	PreferredSize int `json:"preferred_size"`
}

// Task represents a brute force task
type Task struct {
	ID        string   `json:"id"`
	IPs       []string `json:"ips"`
	Usernames []string `json:"usernames"`
	Passwords []string `json:"passwords"`
	Priority  int      `json:"priority"`
}

// TaskAssignData contains assigned tasks
type TaskAssignData struct {
	Tasks []Task `json:"tasks"`
}

// Result represents a successful credential
type Result struct {
	IP       string   `json:"ip"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	Port     int      `json:"port"`
	Found    JSONTime `json:"found"`
}

// ResultReportData contains results from brute force attempts
type ResultReportData struct {
	TaskID   string   `json:"task_id"`
	Results  []Result `json:"results"`
	Attempts int      `json:"attempts"`
	Duration int64    `json:"duration"` // milliseconds
}

// ConfigUpdateData contains client configuration updates
type ConfigUpdateData struct {
	MaxThreads       int `json:"max_threads,omitempty"`
	ConnectionTimeout int `json:"connection_timeout,omitempty"`
	RetryAttempts    int `json:"retry_attempts,omitempty"`
	BatchSize        int `json:"batch_size,omitempty"`
}

// StatusUpdateData contains detailed status information with performance metrics
type StatusUpdateData struct {
	TaskID       string  `json:"task_id"`
	Progress     float64 `json:"progress"` // 0-100
	Status       string  `json:"status"`   // assigned, in_progress, completed, failed
	Error        string  `json:"error,omitempty"`
	Attempts     int     `json:"attempts"`
	SuccessCount int     `json:"success_count"`
	PPS          float64 `json:"pps"` // Current PPS for this task
}

// RegisterResponse contains the client's assigned ID
type RegisterResponse struct {
	ClientID string `json:"client_id"`
	Message  string `json:"message"`
}

// HeartbeatResponse confirms receipt of heartbeat
type HeartbeatResponse struct {
	Status string `json:"status"`
}

// ReportResultResponse confirms receipt of a result
type ReportResultResponse struct {
	Status string `json:"status"`
}

// OperationCommandData contains operation control commands
type OperationCommandData struct {
	OperationID string `json:"operation_id"`
	Command     string `json:"command"` // stop, pause, resume
	Graceful    bool   `json:"graceful,omitempty"`
}
