package web

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"rdp-brute-system/shared/protocol"
)

// Client represents a connected brute force client
type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	send     chan []byte
	id       string
	
	// Performance tracking
	lastActivity  time.Time
	messageCount  int64
	errorCount    int32
	
	// Rate limiting
	rateLimiter   *time.Ticker
	rateLimit     int // messages per second
	
	// Context for cancellation
	ctx           context.Context
	cancel        context.CancelFunc
	
	// Mutex for thread-safe operations
	mu            sync.RWMutex
}

func NewClient(hub *Hub, conn *websocket.Conn) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	
	// Configure connection settings
	conn.SetReadLimit(maxMessageSize)
	conn.SetReadDeadline(time.Now().Add(pongTimeout))
	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	
	return &Client{
		hub:          hub,
		conn:         conn,
		send:         make(chan []byte, sendBufferSize),
		lastActivity: time.Now(),
		rateLimit:    100, // 100 messages per second default
		rateLimiter:  time.NewTicker(time.Second / 100),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// readPump pumps messages from the websocket connection to the hub.
func (c *Client) readPump() {
	defer func() {
		c.cancel()
		c.rateLimiter.Stop()
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongTimeout))
		c.updateActivity()
		return nil
	})

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			_, message, err := c.conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("WebSocket error from %s: %v", c.conn.RemoteAddr(), err)
				}
				return
			}

			// Rate limiting check
			select {
			case <-c.rateLimiter.C:
				// Process the message
				c.hub.IncrementMessagesReceived()
				c.updateActivity()
				c.incrementMessageCount()
				c.processMessage(message)
			default:
				// Rate limit exceeded, drop message
				log.Printf("Rate limit exceeded for client %s", c.id)
				c.incrementErrorCount()
			}
		}
	}
}

// writePump pumps messages from the hub to the websocket connection.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingInterval)
	defer func() {
		ticker.Stop()
		c.conn.Close()
		c.cancel()
	}()

	for {
		select {
		case <-c.ctx.Done():
			return
			
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// Write message with batching support
			if err := c.writeMessageBatch(message); err != nil {
				log.Printf("Write error for client %s: %v", c.id, err)
				return
			}
			
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("Ping error for client %s: %v", c.id, err)
				return
			}
		}
	}
}

// writeMessageBatch writes a message and batches any queued messages
func (c *Client) writeMessageBatch(firstMessage []byte) error {
	w, err := c.conn.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	
	// Write the first message
	if _, err := w.Write(firstMessage); err != nil {
		return err
	}
	
	// Batch additional queued messages (up to a limit)
	batchLimit := 10
	batchCount := 0
	
	for batchCount < batchLimit {
		select {
		case message := <-c.send:
			if _, err := w.Write(message); err != nil {
				return err
			}
			batchCount++
		default:
			// No more messages to batch
			return w.Close()
		}
	}
	
	return w.Close()
}

// processMessage handles incoming messages from the client
func (c *Client) processMessage(message []byte) {
	var msg protocol.Message
	if err := json.Unmarshal(message, &msg); err != nil {
		log.Printf("Error unmarshalling message from %s: %v", c.id, err)
		c.incrementErrorCount()
		return
	}

	// Set client ID if not already set
	if msg.ClientID != "" && c.id == "" {
		c.setClientID(msg.ClientID)
	}

	// Handle different message types
	switch msg.Type {
	case protocol.MsgRegister:
		// Handle client registration
		var regData protocol.RegisterData
		if err := json.Unmarshal(msg.Data, &regData); err != nil {
			log.Printf("Error unmarshalling register data: %v", err)
			return
		}
		c.id = c.hub.clientService.RegisterClient(regData)
		c.hub.clientService.UpdateClientStatus(c.id, "connected")

	case protocol.MsgHeartbeat:
		// Handle client heartbeat
		var hbData protocol.HeartbeatData
		if err := json.Unmarshal(msg.Data, &hbData); err != nil {
			log.Printf("Error unmarshalling heartbeat data: %v", err)
			return
		}
		c.hub.clientService.UpdateClientStatus(c.id, "online")
		c.hub.clientService.UpdateClientMetrics(c.id, hbData)
		
		// Update performance metrics in distribution service
		c.hub.distService.UpdateClientPerformance(c.id, hbData.PPS, hbData.TotalAttempts, hbData.SuccessCount)

	case protocol.MsgTaskRequest:
		// Handle task request using the new distribution service
		var reqData protocol.TaskRequestData
		if err := json.Unmarshal(msg.Data, &reqData); err != nil {
			log.Printf("Error unmarshalling task request data: %v", err)
			return
		}
		
		// Get a single task using the intelligent distribution
		task, err := c.hub.distService.GetTask(c.id)
		if err != nil {
			log.Printf("Error getting task for client %s: %v", c.id, err)
			return
		}
		
		// If no task available, send empty response
		if task == nil {
			assignData := protocol.TaskAssignData{Tasks: []protocol.Task{}}
			dataBytes, err := json.Marshal(assignData)
			if err != nil {
				log.Printf("Error marshalling empty task assign data: %v", err)
				return
			}
			response := protocol.Message{
				Type:     protocol.MsgTaskAssign,
				ClientID: c.id,
				Data:     json.RawMessage(dataBytes),
			}
			c.sendResponse(response)
			return
		}
		
		// Send the task to the client
		assignData := protocol.TaskAssignData{Tasks: []protocol.Task{*task}}
		dataBytes, err := json.Marshal(assignData)
		if err != nil {
			log.Printf("Error marshalling task assign data: %v", err)
			return
		}
		response := protocol.Message{
			Type:     protocol.MsgTaskAssign,
			ClientID: c.id,
			Data:     json.RawMessage(dataBytes),
		}
		c.sendResponse(response)

	case protocol.MsgResultReport:
		// Handle result report
		var resultData protocol.ResultReportData
		if err := json.Unmarshal(msg.Data, &resultData); err != nil {
			log.Printf("Error unmarshalling result data: %v", err)
			return
		}
		c.hub.taskService.StoreResults(c.id, resultData.TaskID, resultData.Results)
		
		// Save results using distribution service
		for _, result := range resultData.Results {
			if err := c.hub.distService.SaveResult(c.id, &result); err != nil {
				log.Printf("Error saving result: %v", err)
			}
		}
		
		// Mark task as completed if results were found
		if len(resultData.Results) > 0 {
			c.hub.distService.CompleteTask(c.id, resultData.TaskID, time.Second)
		}

	case protocol.MsgStatusUpdate:
		// Handle status update
		var statusData protocol.StatusUpdateData
		if err := json.Unmarshal(msg.Data, &statusData); err != nil {
			log.Printf("Error unmarshalling status data: %v", err)
			return
		}
		c.hub.taskService.UpdateTaskStatus(statusData.TaskID, statusData)
	}
}

// sendResponse sends a message to the client
func (c *Client) sendResponse(msg protocol.Message) {
	response, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error marshalling response: %v", err)
		c.incrementErrorCount()
		return
	}
	
	select {
	case c.send <- response:
		// Message queued successfully
	case <-c.ctx.Done():
		// Client is shutting down
		return
	default:
		// Send channel is full
		log.Printf("Send buffer full for client %s, dropping message", c.id)
		c.incrementErrorCount()
	}
}

// Helper methods for thread-safe operations

func (c *Client) setClientID(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.id = id
}

func (c *Client) getClientID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.id
}

func (c *Client) updateActivity() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastActivity = time.Now()
}

func (c *Client) getLastActivity() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastActivity
}

func (c *Client) incrementMessageCount() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messageCount++
}

func (c *Client) incrementErrorCount() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errorCount++
}

func (c *Client) GetStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	return map[string]interface{}{
		"id":            c.id,
		"messageCount":  c.messageCount,
		"errorCount":    c.errorCount,
		"lastActivity":  c.lastActivity,
		"rateLimit":     c.rateLimit,
		"idleTime":      time.Since(c.lastActivity).Seconds(),
	}
}

// SetRateLimit updates the client's rate limit
func (c *Client) SetRateLimit(messagesPerSecond int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if messagesPerSecond <= 0 {
		messagesPerSecond = 100 // Default
	}
	
	c.rateLimit = messagesPerSecond
	c.rateLimiter.Stop()
	c.rateLimiter = time.NewTicker(time.Second / time.Duration(messagesPerSecond))
}

// Close gracefully closes the client connection
func (c *Client) Close() {
	c.cancel()
	c.conn.Close()
}
