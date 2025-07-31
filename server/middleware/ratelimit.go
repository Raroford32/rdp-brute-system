package middleware

import (
	"net/http"
	"sync"
	"time"
	
	"golang.org/x/time/rate"
)

// RateLimiter holds the rate limiters for each visitor
type RateLimiter struct {
	visitors map[string]*visitor
	mu       sync.RWMutex
	rate     rate.Limit
	burst    int
}

// visitor holds the rate limiter and last seen time for each visitor
type visitor struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(requestsPerSecond float64, burst int) *RateLimiter {
	rl := &RateLimiter{
		visitors: make(map[string]*visitor),
		rate:     rate.Limit(requestsPerSecond),
		burst:    burst,
	}
	
	// Start cleanup routine
	go rl.cleanupVisitors()
	
	return rl
}

// getVisitor retrieves and returns the rate limiter for the current visitor
func (rl *RateLimiter) getVisitor(ip string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	
	v, exists := rl.visitors[ip]
	if !exists {
		limiter := rate.NewLimiter(rl.rate, rl.burst)
		rl.visitors[ip] = &visitor{
			limiter:  limiter,
			lastSeen: time.Now(),
		}
		return limiter
	}
	
	v.lastSeen = time.Now()
	return v.limiter
}

// cleanupVisitors removes old entries from the visitors map
func (rl *RateLimiter) cleanupVisitors() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for range ticker.C {
		rl.mu.Lock()
		for ip, v := range rl.visitors {
			if time.Since(v.lastSeen) > 3*time.Minute {
				delete(rl.visitors, ip)
			}
		}
		rl.mu.Unlock()
	}
}

// Limit is the middleware function
func (rl *RateLimiter) Limit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := getIP(r)
		limiter := rl.getVisitor(ip)
		
		if !limiter.Allow() {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		
		next.ServeHTTP(w, r)
	}
}

// getIP gets the real IP address
func getIP(r *http.Request) string {
	// Check X-Forwarded-For header
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		return forwarded
	}
	
	// Check X-Real-IP header
	realIP := r.Header.Get("X-Real-IP")
	if realIP != "" {
		return realIP
	}
	
	// Fall back to RemoteAddr
	return r.RemoteAddr
}

// GlobalRateLimiter for API endpoints
var (
	APIRateLimiter *RateLimiter
	WSRateLimiter  *RateLimiter
)

func init() {
	// Initialize rate limiters with default values
	// 100 requests per second with burst of 200 for API
	APIRateLimiter = NewRateLimiter(100, 200)
	
	// 10 WebSocket connections per second with burst of 20
	WSRateLimiter = NewRateLimiter(10, 20)
}