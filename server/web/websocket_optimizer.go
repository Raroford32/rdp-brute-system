package web

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"sync"
	"time"
)

// WebSocketOptimizer provides optimization for WebSocket communications
type WebSocketOptimizer struct {
	// Message batching
	batchBuffer   [][]byte
	batchMu       sync.Mutex
	batchTimer    *time.Timer
	batchInterval time.Duration
	maxBatchSize  int
	
	// Compression
	compressionEnabled bool
	compressionLevel   int
	
	// Message deduplication
	lastMessages   map[string][]byte
	lastMessagesMu sync.RWMutex
	
	// Metrics
	messagesSent      int64
	messagesDropped   int64
	bytesCompressed   int64
	bytesUncompressed int64
}

// NewWebSocketOptimizer creates a new WebSocket optimizer
func NewWebSocketOptimizer() *WebSocketOptimizer {
	return &WebSocketOptimizer{
		batchBuffer:        make([][]byte, 0, 100),
		batchInterval:      50 * time.Millisecond,
		maxBatchSize:       100,
		compressionEnabled: true,
		compressionLevel:   gzip.BestSpeed,
		lastMessages:       make(map[string][]byte),
	}
}

// OptimizeMessage optimizes a message before sending
func (wo *WebSocketOptimizer) OptimizeMessage(clientID string, message []byte) ([]byte, bool) {
	// Check for duplicate messages
	if wo.isDuplicate(clientID, message) {
		wo.messagesDropped++
		return nil, false
	}
	
	// Apply compression if beneficial
	if wo.compressionEnabled && len(message) > 1024 {
		compressed := wo.compress(message)
		if len(compressed) < len(message) {
			wo.bytesCompressed += int64(len(compressed))
			wo.bytesUncompressed += int64(len(message))
			return compressed, true
		}
	}
	
	wo.messagesSent++
	return message, true
}

// BatchMessage adds a message to the batch
func (wo *WebSocketOptimizer) BatchMessage(message []byte) {
	wo.batchMu.Lock()
	defer wo.batchMu.Unlock()
	
	wo.batchBuffer = append(wo.batchBuffer, message)
	
	// Start timer if this is the first message
	if len(wo.batchBuffer) == 1 {
		wo.batchTimer = time.AfterFunc(wo.batchInterval, wo.flushBatch)
	}
	
	// Flush if batch is full
	if len(wo.batchBuffer) >= wo.maxBatchSize {
		wo.flushBatchLocked()
	}
}

// flushBatch sends all batched messages
func (wo *WebSocketOptimizer) flushBatch() {
	wo.batchMu.Lock()
	defer wo.batchMu.Unlock()
	wo.flushBatchLocked()
}

// flushBatchLocked flushes batch (must be called with lock held)
func (wo *WebSocketOptimizer) flushBatchLocked() {
	if len(wo.batchBuffer) == 0 {
		return
	}
	
	// Cancel timer
	if wo.batchTimer != nil {
		wo.batchTimer.Stop()
		wo.batchTimer = nil
	}
	
	// Create batch message
	_ = map[string]interface{}{
		"type":     "batch",
		"messages": wo.batchBuffer,
		"count":    len(wo.batchBuffer),
	}
	
	// Send batch
	// This would be sent to the actual WebSocket connection
	
	// Clear buffer
	wo.batchBuffer = wo.batchBuffer[:0]
}

// compress compresses a message using gzip
func (wo *WebSocketOptimizer) compress(data []byte) []byte {
	var buf bytes.Buffer
	writer, _ := gzip.NewWriterLevel(&buf, wo.compressionLevel)
	writer.Write(data)
	writer.Close()
	return buf.Bytes()
}

// isDuplicate checks if message is duplicate
func (wo *WebSocketOptimizer) isDuplicate(clientID string, message []byte) bool {
	wo.lastMessagesMu.Lock()
	defer wo.lastMessagesMu.Unlock()
	
	// Parse message to check type
	var msg map[string]interface{}
	if err := json.Unmarshal(message, &msg); err != nil {
		return false
	}
	
	// Only deduplicate certain message types
	msgType, ok := msg["type"].(string)
	if !ok || msgType != "dashboard_update" {
		return false
	}
	
	// Check if identical to last message
	lastMsg, exists := wo.lastMessages[clientID]
	if exists && bytes.Equal(lastMsg, message) {
		return true
	}
	
	// Store this message
	wo.lastMessages[clientID] = message
	
	// Clean up old entries periodically
	if len(wo.lastMessages) > 1000 {
		wo.lastMessages = make(map[string][]byte)
	}
	
	return false
}

// GetMetrics returns optimizer metrics
func (wo *WebSocketOptimizer) GetMetrics() map[string]interface{} {
	compressionRatio := float64(0)
	if wo.bytesUncompressed > 0 {
		compressionRatio = float64(wo.bytesCompressed) / float64(wo.bytesUncompressed) * 100
	}
	
	return map[string]interface{}{
		"messages_sent":       wo.messagesSent,
		"messages_dropped":    wo.messagesDropped,
		"bytes_compressed":    wo.bytesCompressed,
		"bytes_uncompressed":  wo.bytesUncompressed,
		"compression_ratio":   compressionRatio,
		"batch_buffer_size":   len(wo.batchBuffer),
	}
}

// MessagePrioritizer prioritizes messages based on importance
type MessagePrioritizer struct {
	highPriority   chan []byte
	normalPriority chan []byte
	lowPriority    chan []byte
}

// NewMessagePrioritizer creates a new message prioritizer
func NewMessagePrioritizer() *MessagePrioritizer {
	return &MessagePrioritizer{
		highPriority:   make(chan []byte, 100),
		normalPriority: make(chan []byte, 1000),
		lowPriority:    make(chan []byte, 1000),
	}
}

// AddMessage adds a message with priority
func (mp *MessagePrioritizer) AddMessage(message []byte, priority int) {
	switch priority {
	case 0: // High priority
		select {
		case mp.highPriority <- message:
		default:
			// Drop if queue full
		}
	case 1: // Normal priority
		select {
		case mp.normalPriority <- message:
		default:
			// Drop if queue full
		}
	default: // Low priority
		select {
		case mp.lowPriority <- message:
		default:
			// Drop if queue full
		}
	}
}

// GetNext returns the next message based on priority
func (mp *MessagePrioritizer) GetNext() ([]byte, bool) {
	// Check high priority first
	select {
	case msg := <-mp.highPriority:
		return msg, true
	default:
	}
	
	// Then normal priority
	select {
	case msg := <-mp.normalPriority:
		return msg, true
	default:
	}
	
	// Finally low priority
	select {
	case msg := <-mp.lowPriority:
		return msg, true
	default:
		return nil, false
	}
}
