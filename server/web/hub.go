package web

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"rdp-brute-system/server/services"
)

const (
	// Connection limits
	maxConnections = 1000
	maxMessageSize = 512 * 1024 // 512KB
	
	// Timeouts
	writeTimeout   = 10 * time.Second
	pongTimeout    = 60 * time.Second
	pingInterval   = (pongTimeout * 9) / 10
	
	// Buffer sizes
	sendBufferSize = 256
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow connections from any origin
	},
}

// ServeWs handles websocket requests from the peer.
func ServeWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	client := NewClient(hub, conn)
	client.hub.register <- client

	// Allow collection of memory referenced by the caller by doing all work in
	// new goroutines.
	go client.writePump()
	go client.readPump()
}

// Hub maintains the set of active clients and broadcasts messages to the
// clients.
type Hub struct {
	// Registered clients with thread-safe access
	clients    map[*Client]bool
	clientsMu  sync.RWMutex
	
	// Connection tracking
	connCount  int32
	connLimit  int32

	// Inbound messages from the clients.
	broadcast chan *BroadcastMessage

	// Register requests from the clients.
	register chan *Client

	// Unregister requests from clients.
	unregister chan *Client
	
	// Shutdown channel
	shutdown chan struct{}
	wg       sync.WaitGroup

	// Services
	clientService *services.ClientService
	taskService   *services.TaskService
	distService   *services.DistributionService
	
	// Metrics
	totalConnections   int64
	activeConnections  int32
	messagesSent       int64
	messagesReceived   int64
}

// BroadcastMessage represents a message to be broadcast
type BroadcastMessage struct {
	Data      []byte
	Filter    func(*Client) bool // Optional filter function
	Exclude   *Client           // Optional client to exclude
}

func NewHub(clientService *services.ClientService, taskService *services.TaskService, distService *services.DistributionService) *Hub {
	return &Hub{
		broadcast:     make(chan *BroadcastMessage, 256),
		register:      make(chan *Client, 16),
		unregister:    make(chan *Client, 16),
		clients:       make(map[*Client]bool),
		shutdown:      make(chan struct{}),
		connLimit:     maxConnections,
		clientService: clientService,
		taskService:   taskService,
		distService:   distService,
	}
}

func (h *Hub) Run() {
	// Start metrics reporter
	h.wg.Add(1)
	go h.metricsReporter()
	
	for {
		select {
		case client := <-h.register:
			h.registerClient(client)
			
		case client := <-h.unregister:
			h.unregisterClient(client)
			
		case message := <-h.broadcast:
			h.broadcastMessage(message)
			
		case <-h.shutdown:
			h.shutdownHub()
			return
		}
	}
}

// registerClient handles client registration
func (h *Hub) registerClient(client *Client) {
	h.clientsMu.Lock()
	defer h.clientsMu.Unlock()
	
	// Check connection limit
	if atomic.LoadInt32(&h.connCount) >= h.connLimit {
		log.Printf("Connection limit reached, rejecting client: %s", client.conn.RemoteAddr())
		client.conn.Close()
		return
	}
	
	h.clients[client] = true
	atomic.AddInt32(&h.connCount, 1)
	atomic.AddInt32(&h.activeConnections, 1)
	atomic.AddInt64(&h.totalConnections, 1)
	
	log.Printf("Client connected: %s (active: %d)", client.conn.RemoteAddr(), atomic.LoadInt32(&h.activeConnections))
}

// unregisterClient handles client unregistration
func (h *Hub) unregisterClient(client *Client) {
	h.clientsMu.Lock()
	defer h.clientsMu.Unlock()
	
	if _, ok := h.clients[client]; ok {
		delete(h.clients, client)
		close(client.send)
		atomic.AddInt32(&h.connCount, -1)
		atomic.AddInt32(&h.activeConnections, -1)
		
		// Unregister from client service if registered
		if client.id != "" {
			h.clientService.UnregisterClient(client.id)
		}
		
		log.Printf("Client disconnected: %s (active: %d)", client.conn.RemoteAddr(), atomic.LoadInt32(&h.activeConnections))
	}
}

// broadcastMessage sends a message to filtered clients
func (h *Hub) broadcastMessage(msg *BroadcastMessage) {
	h.clientsMu.RLock()
	defer h.clientsMu.RUnlock()
	
	for client := range h.clients {
		// Skip excluded client
		if msg.Exclude != nil && client == msg.Exclude {
			continue
		}
		
		// Apply filter if provided
		if msg.Filter != nil && !msg.Filter(client) {
			continue
		}
		
		select {
		case client.send <- msg.Data:
			atomic.AddInt64(&h.messagesSent, 1)
		default:
			// Client's send channel is full, close it
			h.wg.Add(1)
			go func(c *Client) {
				defer h.wg.Done()
				h.unregister <- c
			}(client)
		}
	}
}

// BroadcastToAll sends a message to all connected clients
func (h *Hub) BroadcastToAll(data []byte) {
	h.broadcast <- &BroadcastMessage{Data: data}
}

// BroadcastToClient sends a message to a specific client
func (h *Hub) BroadcastToClient(clientID string, data []byte) {
	h.broadcast <- &BroadcastMessage{
		Data: data,
		Filter: func(c *Client) bool {
			return c.id == clientID
		},
	}
}

// shutdownHub gracefully shuts down the hub
func (h *Hub) shutdownHub() {
	log.Println("Shutting down WebSocket hub...")
	
	// Close all client connections
	h.clientsMu.Lock()
	for client := range h.clients {
		close(client.send)
		client.conn.Close()
	}
	h.clients = make(map[*Client]bool)
	h.clientsMu.Unlock()
	
	// Wait for all goroutines to finish
	h.wg.Wait()
	
	log.Println("WebSocket hub shutdown complete")
}

// Shutdown initiates graceful shutdown
func (h *Hub) Shutdown(ctx context.Context) error {
	select {
	case h.shutdown <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetStats returns current hub statistics
func (h *Hub) GetStats() map[string]interface{} {
	h.clientsMu.RLock()
	clientCount := len(h.clients)
	h.clientsMu.RUnlock()
	
	return map[string]interface{}{
		"totalConnections":   atomic.LoadInt64(&h.totalConnections),
		"activeConnections":  atomic.LoadInt32(&h.activeConnections),
		"clientCount":        clientCount,
		"messagesSent":       atomic.LoadInt64(&h.messagesSent),
		"messagesReceived":   atomic.LoadInt64(&h.messagesReceived),
		"connectionLimit":    h.connLimit,
	}
}

// metricsReporter periodically logs hub metrics
func (h *Hub) metricsReporter() {
	defer h.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			stats := h.GetStats()
			log.Printf("WebSocket Hub Stats: %+v", stats)
		case <-h.shutdown:
			return
		}
	}
}

// IncrementMessagesReceived increments the received message counter
func (h *Hub) IncrementMessagesReceived() {
	atomic.AddInt64(&h.messagesReceived, 1)
}
