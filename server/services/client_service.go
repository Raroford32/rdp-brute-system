package services

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"rdp-brute-system/server/cache"
	"rdp-brute-system/server/database"
	"rdp-brute-system/server/models"
	"rdp-brute-system/shared/protocol"
	"sync"
)

type ClientService struct {
	db      *database.DB
	clients map[string]*models.Client
	mu      sync.RWMutex
	cache   *cache.Cache
	hub     WebSocketHub // Interface to decouple from web package
}

// WebSocketHub interface for sending messages to clients
type WebSocketHub interface {
	BroadcastToClient(clientID string, data []byte)
	BroadcastToAll(data []byte)
}

func NewClientService(db *database.DB) *ClientService {
	return &ClientService{
		db:      db,
		clients: make(map[string]*models.Client),
		cache:   cache.NewCache(5 * time.Minute), // 5 minute cache TTL
	}
}

// SetHub sets the WebSocket hub for sending messages
func (s *ClientService) SetHub(hub WebSocketHub) {
	s.hub = hub
}

func (s *ClientService) RegisterClient(regData protocol.RegisterData) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	clientID := uuid.New().String()
	client := &models.Client{
		ID:            clientID,
		Hostname:      regData.Hostname,
		OS:            regData.OS,
		CPUCores:      regData.CPUCores,
		MaxThreads:    regData.MaxThreads,
		Version:       regData.Version,
		Status:        "online",
		RegisteredAt:  time.Now(),
		LastHeartbeat: time.Now(),
		LastUpdated:   time.Now(),
		TotalAttempts: 0,
		SuccessCount:  0,
		CurrentPPS:    0,
	}
	
	s.clients[clientID] = client
	
	// Register client in database
	if err := s.db.RegisterClient(client); err != nil {
		log.Printf("Error registering client in database: %v", err)
	}
	
	log.Printf("Registered new client: %s (hostname: %s, cores: %d, threads: %d)",
		clientID, regData.Hostname, regData.CPUCores, regData.MaxThreads)
	return clientID
}

func (s *ClientService) UnregisterClient(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if client, exists := s.clients[id]; exists {
		client.Status = "offline"
		client.LastUpdated = time.Now()
		
		// Update status in database
		query := `UPDATE clients SET status = 'offline', last_updated = $1 WHERE id = $2`
		if _, err := s.db.Exec(query, time.Now(), id); err != nil {
			log.Printf("Error updating client status in database: %v", err)
		}
	}
	
	delete(s.clients, id)
	log.Printf("Unregistered client: %s", id)
}

func (s *ClientService) UpdateClientStatus(id string, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if client, ok := s.clients[id]; ok {
		client.Status = status
		client.LastHeartbeat = time.Now()
		client.LastUpdated = time.Now()
	}
}

func (s *ClientService) UpdateClientMetrics(id string, hbData protocol.HeartbeatData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if client, ok := s.clients[id]; ok {
		client.CurrentPPS = hbData.PPS
		client.TotalAttempts += hbData.TotalAttempts
		client.SuccessCount += hbData.SuccessCount
		client.LastHeartbeat = time.Now()
		client.LastUpdated = time.Now()
		client.Status = "online"
		
		// Update heartbeat in database
		if err := s.db.UpdateClientHeartbeat(id, hbData.PPS, hbData.TotalAttempts, hbData.SuccessCount); err != nil {
			log.Printf("Error updating client heartbeat in database: %v", err)
		}
	}
}

func (s *ClientService) UpdateStats(id string, pps float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if client, ok := s.clients[id]; ok {
		client.CurrentPPS = pps
		client.LastUpdated = time.Now()
	}
}

func (s *ClientService) GetStats() []*models.Client {
	// Try to get from cache first
	cacheKey := "client:stats"
	if cached, found := s.cache.Get(cacheKey); found {
		return cached.([]*models.Client)
	}
	
	s.mu.RLock()
	stats := make([]*models.Client, 0, len(s.clients))
	for _, client := range s.clients {
		stats = append(stats, client)
	}
	s.mu.RUnlock()
	
	// Cache the result
	s.cache.SetWithTTL(cacheKey, stats, 10*time.Second) // Short TTL for stats
	
	return stats
}

// GetClients returns all connected clients
func (s *ClientService) GetClients() []*models.Client {
	return s.GetStats()
}

// GetClient returns a specific client by ID
func (s *ClientService) GetClient(clientID string) (*models.Client, bool) {
	// Try to get from cache first
	cacheKey := fmt.Sprintf("client:%s", clientID)
	if cached, found := s.cache.Get(cacheKey); found {
		client := cached.(*models.Client)
		return client, true
	}
	
	s.mu.RLock()
	client, exists := s.clients[clientID]
	s.mu.RUnlock()
	
	if exists {
		// Cache the client data
		s.cache.SetWithTTL(cacheKey, client, 1*time.Minute)
	}
	
	return client, exists
}

// IsClientOnline checks if a client is online
func (s *ClientService) IsClientOnline(clientID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	client, exists := s.clients[clientID]
	if !exists {
		return false
	}
	
	// Consider client offline if no heartbeat for 2 minutes
	return time.Since(client.LastHeartbeat) < 2*time.Minute
}

// GetOnlineClients returns only online clients
func (s *ClientService) GetOnlineClients() []*models.Client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	var onlineClients []*models.Client
	for _, client := range s.clients {
		if time.Since(client.LastHeartbeat) < 2*time.Minute {
			onlineClients = append(onlineClients, client)
		}
	}
	
	return onlineClients
}

// GetActiveClientCount returns the number of active/online clients
func (s *ClientService) GetActiveClientCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	count := 0
	for _, client := range s.clients {
		if time.Since(client.LastHeartbeat) < 2*time.Minute {
			count++
		}
	}
	
	return count
}

// CleanupOfflineClients removes clients that haven't sent heartbeat in a while
func (s *ClientService) CleanupOfflineClients() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	for id, client := range s.clients {
		if time.Since(client.LastHeartbeat) > 5*time.Minute {
			client.Status = "offline"
			
			// Update in database
			query := `UPDATE clients SET status = 'offline', last_updated = $1 WHERE id = $2`
			if _, err := s.db.Exec(query, time.Now(), id); err != nil {
				log.Printf("Error updating offline client in database: %v", err)
			}
			
			delete(s.clients, id)
			// Remove from cache
			s.cache.Delete(fmt.Sprintf("client:%s", id))
			log.Printf("Cleaned up offline client: %s", id)
		}
	}
	
	// Clear stats cache when cleaning up
	s.cache.Delete("client:stats")
}

// StartCleanupRoutine starts a routine to clean up offline clients
func (s *ClientService) StartCleanupRoutine() {
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		for range ticker.C {
			s.CleanupOfflineClients()
		}
	}()
}

// SendOperationCommand sends an operation command to a specific client
func (s *ClientService) SendOperationCommand(clientID string, messageType protocol.MessageType, data interface{}) error {
	// Check if client is online
	if !s.IsClientOnline(clientID) {
		return fmt.Errorf("client %s is not online", clientID)
	}
	
	// Check if hub is set
	if s.hub == nil {
		return fmt.Errorf("websocket hub not initialized")
	}
	
	// Marshal the command data
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal command data: %v", err)
	}
	
	// Create protocol message
	msg := protocol.Message{
		Type:      messageType,
		ClientID:  clientID,
		Timestamp: protocol.JSONTime{Time: time.Now()},
		Data:      dataBytes,
	}
	
	// Marshal the message
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}
	
	// Send through hub
	s.hub.BroadcastToClient(clientID, msgBytes)
	
	log.Printf("Sent operation command %s to client %s", messageType, clientID)
	return nil
}

// BroadcastOperationCommand sends an operation command to all online clients
func (s *ClientService) BroadcastOperationCommand(messageType protocol.MessageType, data interface{}) error {
	// Check if hub is set
	if s.hub == nil {
		return fmt.Errorf("websocket hub not initialized")
	}
	
	// Marshal the command data
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal command data: %v", err)
	}
	
	// Create protocol message
	msg := protocol.Message{
		Type:      messageType,
		Timestamp: protocol.JSONTime{Time: time.Now()},
		Data:      dataBytes,
	}
	
	// Marshal the message
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}
	
	// Broadcast to all clients
	s.hub.BroadcastToAll(msgBytes)
	
	log.Printf("Broadcast operation command %s to all clients", messageType)
	return nil
}
