package worker

import (
	"math"
	"sort"
	"sync"
	"time"
)

// TargetPrioritizer manages target prioritization based on success patterns
type TargetPrioritizer struct {
	targets      map[string]*TargetInfo
	subnets      map[string]*SubnetInfo
	mu           sync.RWMutex
	decayFactor  float64
}

// TargetInfo tracks information about a specific target
type TargetInfo struct {
	IP              string
	LastAttempt     time.Time
	SuccessCount    int
	FailureCount    int
	AvgResponseTime time.Duration
	Priority        float64
	Subnet          string
}

// SubnetInfo tracks subnet-level statistics
type SubnetInfo struct {
	Subnet          string
	SuccessCount    int
	FailureCount    int
	LastSuccess     time.Time
	Priority        float64
}

// NewTargetPrioritizer creates a new target prioritizer
func NewTargetPrioritizer() *TargetPrioritizer {
	return &TargetPrioritizer{
		targets:     make(map[string]*TargetInfo),
		subnets:     make(map[string]*SubnetInfo),
		decayFactor: 0.95, // Priority decays by 5% per hour
	}
}

// GetPriority returns the priority for a target IP
func (tp *TargetPrioritizer) GetPriority(ip string) float64 {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	
	// Check if we have target-specific info
	if info, exists := tp.targets[ip]; exists {
		tp.applyTimeDecay(info)
		return info.Priority
	}
	
	// Check subnet priority
	subnet := getSubnet24(ip)
	if subnetInfo, exists := tp.subnets[subnet]; exists {
		tp.applySubnetDecay(subnetInfo)
		return subnetInfo.Priority * 0.5 // Subnet priority is weighted less
	}
	
	// Default priority
	return 1.0
}

// UpdateResult updates prioritization based on check result
func (tp *TargetPrioritizer) UpdateResult(ip string, success bool, responseTime time.Duration) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	
	// Get or create target info
	info, exists := tp.targets[ip]
	if !exists {
		info = &TargetInfo{
			IP:     ip,
			Subnet: getSubnet24(ip),
		}
		tp.targets[ip] = info
	}
	
	// Update target info
	info.LastAttempt = time.Now()
	if success {
		info.SuccessCount++
		info.Priority += 10.0 // Boost priority for success
	} else {
		info.FailureCount++
		info.Priority -= 1.0 // Slight decrease for failure
		
		// Heavy penalty for repeated failures
		if info.FailureCount > 10 && info.SuccessCount == 0 {
			info.Priority = -1.0 // Mark for skipping
		}
	}
	
	// Update response time average
	if info.AvgResponseTime == 0 {
		info.AvgResponseTime = responseTime
	} else {
		info.AvgResponseTime = (info.AvgResponseTime + responseTime) / 2
	}
	
	// Adjust priority based on response time
	if responseTime < 500*time.Millisecond {
		info.Priority += 2.0 // Fast responding targets
	} else if responseTime > 3*time.Second {
		info.Priority -= 2.0 // Slow targets
	}
	
	// Update subnet info
	subnetInfo, exists := tp.subnets[info.Subnet]
	if !exists {
		subnetInfo = &SubnetInfo{
			Subnet: info.Subnet,
		}
		tp.subnets[info.Subnet] = subnetInfo
	}
	
	if success {
		subnetInfo.SuccessCount++
		subnetInfo.LastSuccess = time.Now()
		subnetInfo.Priority += 5.0
	} else {
		subnetInfo.FailureCount++
		subnetInfo.Priority -= 0.5
	}
	
	// Apply bounds
	info.Priority = math.Max(-10.0, math.Min(100.0, info.Priority))
	subnetInfo.Priority = math.Max(0.0, math.Min(50.0, subnetInfo.Priority))
}

// PrioritizeSubnet increases priority for all IPs in the same subnet
func (tp *TargetPrioritizer) PrioritizeSubnet(successfulIP string) {
	subnet := getSubnet24(successfulIP)
	
	tp.mu.Lock()
	defer tp.mu.Unlock()
	
	// Boost subnet priority
	if subnetInfo, exists := tp.subnets[subnet]; exists {
		subnetInfo.Priority += 10.0
		subnetInfo.Priority = math.Min(50.0, subnetInfo.Priority)
	}
	
	// Boost all targets in the same subnet
	for ip, info := range tp.targets {
		if info.Subnet == subnet && ip != successfulIP {
			info.Priority += 5.0
			info.Priority = math.Min(100.0, info.Priority)
		}
	}
}

// SortByPriority sorts IPs by their priority (highest first)
func (tp *TargetPrioritizer) SortByPriority(ips []string) []string {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	
	// Create a copy to avoid modifying the original
	sorted := make([]string, len(ips))
	copy(sorted, ips)
	
	// Sort by priority
	sort.Slice(sorted, func(i, j int) bool {
		priI := tp.getPriorityNoLock(sorted[i])
		priJ := tp.getPriorityNoLock(sorted[j])
		
		// If priorities are equal, use response time as tiebreaker
		if math.Abs(priI-priJ) < 0.001 {
			infoI, existsI := tp.targets[sorted[i]]
			infoJ, existsJ := tp.targets[sorted[j]]
			
			if existsI && existsJ {
				return infoI.AvgResponseTime < infoJ.AvgResponseTime
			}
		}
		
		return priI > priJ
	})
	
	return sorted
}

// GetTopTargets returns the highest priority targets
func (tp *TargetPrioritizer) GetTopTargets(count int) []string {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	
	// Create slice of all targets with positive priority
	var targets []*TargetInfo
	for _, info := range tp.targets {
		if info.Priority > 0 {
			tp.applyTimeDecay(info)
			targets = append(targets, info)
		}
	}
	
	// Sort by priority
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].Priority > targets[j].Priority
	})
	
	// Return top IPs
	result := make([]string, 0, count)
	for i := 0; i < count && i < len(targets); i++ {
		result = append(result, targets[i].IP)
	}
	
	return result
}

// GetMetrics returns prioritization metrics
func (tp *TargetPrioritizer) GetMetrics() map[string]interface{} {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	
	highPriority := 0
	lowPriority := 0
	skipTargets := 0
	
	for _, info := range tp.targets {
		if info.Priority > 10 {
			highPriority++
		} else if info.Priority < 0 {
			skipTargets++
		} else if info.Priority < 5 {
			lowPriority++
		}
	}
	
	successfulSubnets := 0
	for _, subnet := range tp.subnets {
		if subnet.SuccessCount > 0 {
			successfulSubnets++
		}
	}
	
	return map[string]interface{}{
		"total_targets":      len(tp.targets),
		"high_priority":      highPriority,
		"low_priority":       lowPriority,
		"skip_targets":       skipTargets,
		"total_subnets":      len(tp.subnets),
		"successful_subnets": successfulSubnets,
	}
}

// applyTimeDecay reduces priority over time
func (tp *TargetPrioritizer) applyTimeDecay(info *TargetInfo) {
	elapsed := time.Since(info.LastAttempt)
	hours := elapsed.Hours()
	
	if hours > 1 {
		// Apply exponential decay
		decayMultiplier := math.Pow(tp.decayFactor, hours)
		info.Priority *= decayMultiplier
	}
}

// applySubnetDecay reduces subnet priority over time
func (tp *TargetPrioritizer) applySubnetDecay(info *SubnetInfo) {
	elapsed := time.Since(info.LastSuccess)
	hours := elapsed.Hours()
	
	if hours > 1 {
		decayMultiplier := math.Pow(tp.decayFactor, hours)
		info.Priority *= decayMultiplier
	}
}

// getPriorityNoLock gets priority without locking (must be called with lock held)
func (tp *TargetPrioritizer) getPriorityNoLock(ip string) float64 {
	if info, exists := tp.targets[ip]; exists {
		tp.applyTimeDecay(info)
		return info.Priority
	}
	
	subnet := getSubnet24(ip)
	if subnetInfo, exists := tp.subnets[subnet]; exists {
		tp.applySubnetDecay(subnetInfo)
		return subnetInfo.Priority * 0.5
	}
	
	return 1.0
}

// CleanupOldEntries removes old entries to prevent memory growth
func (tp *TargetPrioritizer) CleanupOldEntries() {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	
	cutoff := time.Now().Add(-24 * time.Hour)
	
	// Clean up old targets
	for ip, info := range tp.targets {
		if info.LastAttempt.Before(cutoff) && info.Priority < 1.0 {
			delete(tp.targets, ip)
		}
	}
	
	// Clean up old subnets
	for subnet, info := range tp.subnets {
		if info.LastSuccess.Before(cutoff) && info.Priority < 1.0 {
			delete(tp.subnets, subnet)
		}
	}
}

// StartCleanupRoutine starts periodic cleanup
func (tp *TargetPrioritizer) StartCleanupRoutine() {
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		
		for range ticker.C {
			tp.CleanupOldEntries()
		}
	}()
}