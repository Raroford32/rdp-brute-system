package middleware

import (
	"net/http"
	"strings"
	
	"github.com/gin-gonic/gin"
)

// GinRateLimiter wraps the rate limiter for Gin framework
func GinRateLimiter(rl *RateLimiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := getClientIP(c)
		limiter := rl.getVisitor(ip)
		
		if !limiter.Allow() {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": "Too many requests",
				"message": "Rate limit exceeded. Please try again later.",
			})
			c.Abort()
			return
		}
		
		c.Next()
	}
}

// getClientIP gets the real client IP address
func getClientIP(c *gin.Context) string {
	// Check X-Forwarded-For header
	forwarded := c.GetHeader("X-Forwarded-For")
	if forwarded != "" {
		// Take the first IP if there are multiple
		ips := strings.Split(forwarded, ",")
		if len(ips) > 0 {
			return strings.TrimSpace(ips[0])
		}
	}
	
	// Check X-Real-IP header
	realIP := c.GetHeader("X-Real-IP")
	if realIP != "" {
		return realIP
	}
	
	// Fall back to ClientIP
	return c.ClientIP()
}

// ConfigurableRateLimiter creates a rate limiter with custom settings
func ConfigurableRateLimiter(requestsPerSecond float64, burst int) gin.HandlerFunc {
	rl := NewRateLimiter(requestsPerSecond, burst)
	return GinRateLimiter(rl)
}

// DefaultAPIRateLimiter provides a default rate limiter for API endpoints
func DefaultAPIRateLimiter() gin.HandlerFunc {
	return GinRateLimiter(APIRateLimiter)
}

// DefaultWSRateLimiter provides a default rate limiter for WebSocket connections
func DefaultWSRateLimiter() gin.HandlerFunc {
	return GinRateLimiter(WSRateLimiter)
}