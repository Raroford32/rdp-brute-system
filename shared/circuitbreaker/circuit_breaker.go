package circuitbreaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// State represents the state of CircuitBreaker
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

// String returns the string representation of the state
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	name            string
	maxRequests     uint32
	interval        time.Duration
	timeout         time.Duration
	readyToTrip     func(counts Counts) bool
	onStateChange   func(name string, from State, to State)
	
	mutex           sync.Mutex
	state           State
	generation      uint64
	counts          Counts
	expiry          time.Time
	
	// For half-open state
	successReqs     int32
	halfOpenReqs    int32
}

// Counts holds the numbers of requests and their successes/failures
type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
}

// Options configures a CircuitBreaker
type Options struct {
	Name          string
	MaxRequests   uint32
	Interval      time.Duration
	Timeout       time.Duration
	ReadyToTrip   func(counts Counts) bool
	OnStateChange func(name string, from State, to State)
}

// NewCircuitBreaker creates a new CircuitBreaker
func NewCircuitBreaker(opts Options) *CircuitBreaker {
	cb := &CircuitBreaker{
		name:          opts.Name,
		maxRequests:   opts.MaxRequests,
		interval:      opts.Interval,
		timeout:       opts.Timeout,
		readyToTrip:   opts.ReadyToTrip,
		onStateChange: opts.OnStateChange,
	}
	
	if cb.name == "" {
		cb.name = "circuit-breaker"
	}
	if cb.maxRequests == 0 {
		cb.maxRequests = 10
	}
	if cb.interval == 0 {
		cb.interval = 60 * time.Second
	}
	if cb.timeout == 0 {
		cb.timeout = 60 * time.Second
	}
	if cb.readyToTrip == nil {
		cb.readyToTrip = func(counts Counts) bool {
			return counts.ConsecutiveFailures > 5
		}
	}
	
	return cb
}

// Execute runs the given request if the CircuitBreaker accepts it
func (cb *CircuitBreaker) Execute(req func() error) error {
	generation, err := cb.beforeRequest()
	if err != nil {
		return err
	}
	
	defer func() {
		if r := recover(); r != nil {
			cb.afterRequest(generation, false)
			panic(r)
		}
	}()
	
	err = req()
	cb.afterRequest(generation, err == nil)
	return err
}

// Call is an alias for Execute for compatibility
func (cb *CircuitBreaker) Call(req func() error) error {
	return cb.Execute(req)
}

// State returns the current state of the CircuitBreaker
func (cb *CircuitBreaker) State() State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	
	now := time.Now()
	state, _ := cb.currentState(now)
	return state
}

// Counts returns the current counts
func (cb *CircuitBreaker) Counts() Counts {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	
	return cb.counts
}

// Name returns the name of the CircuitBreaker
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	
	now := time.Now()
	state, generation := cb.currentState(now)
	
	switch state {
	case StateOpen:
		return generation, ErrOpenState
	case StateHalfOpen:
		if atomic.LoadInt32(&cb.halfOpenReqs) >= int32(cb.maxRequests) {
			return generation, ErrTooManyRequests
		}
		atomic.AddInt32(&cb.halfOpenReqs, 1)
	}
	
	cb.counts.Requests++
	return generation, nil
}

func (cb *CircuitBreaker) afterRequest(before uint64, success bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	
	now := time.Now()
	state, generation := cb.currentState(now)
	if generation != before {
		return
	}
	
	if success {
		cb.onSuccess(state, now)
	} else {
		cb.onFailure(state, now)
	}
}

func (cb *CircuitBreaker) onSuccess(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.TotalSuccesses++
		cb.counts.ConsecutiveSuccesses++
		cb.counts.ConsecutiveFailures = 0
		
	case StateHalfOpen:
		cb.counts.TotalSuccesses++
		cb.counts.ConsecutiveSuccesses++
		atomic.AddInt32(&cb.successReqs, 1)
		
		if atomic.LoadInt32(&cb.successReqs) >= int32(cb.maxRequests) {
			cb.setState(StateClosed, now)
		}
	}
}

func (cb *CircuitBreaker) onFailure(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.TotalFailures++
		cb.counts.ConsecutiveSuccesses = 0
		cb.counts.ConsecutiveFailures++
		
		if cb.readyToTrip(cb.counts) {
			cb.setState(StateOpen, now)
		}
		
	case StateHalfOpen:
		cb.setState(StateOpen, now)
	}
}

func (cb *CircuitBreaker) currentState(now time.Time) (State, uint64) {
	switch cb.state {
	case StateClosed:
		if cb.expiry.Before(now) {
			cb.toNewGeneration(now)
		}
		
	case StateOpen:
		if cb.expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	}
	
	return cb.state, cb.generation
}

func (cb *CircuitBreaker) setState(state State, now time.Time) {
	if cb.state == state {
		return
	}
	
	prev := cb.state
	cb.state = state
	
	cb.toNewGeneration(now)
	
	if cb.onStateChange != nil {
		go cb.onStateChange(cb.name, prev, state)
	}
}

func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	cb.generation++
	cb.counts = Counts{}
	atomic.StoreInt32(&cb.successReqs, 0)
	atomic.StoreInt32(&cb.halfOpenReqs, 0)
	
	switch cb.state {
	case StateClosed:
		cb.expiry = now.Add(cb.interval)
	case StateOpen:
		cb.expiry = now.Add(cb.timeout)
	case StateHalfOpen:
		cb.expiry = time.Time{}
	}
}

// Errors
var (
	ErrOpenState       = errors.New("circuit breaker is open")
	ErrTooManyRequests = errors.New("too many requests in half-open state")
)

// MultiCircuitBreaker manages multiple circuit breakers
type MultiCircuitBreaker struct {
	breakers map[string]*CircuitBreaker
	mutex    sync.RWMutex
	defaults Options
}

// NewMultiCircuitBreaker creates a new MultiCircuitBreaker
func NewMultiCircuitBreaker(defaults Options) *MultiCircuitBreaker {
	return &MultiCircuitBreaker{
		breakers: make(map[string]*CircuitBreaker),
		defaults: defaults,
	}
}

// Get returns a circuit breaker for the given name
func (mcb *MultiCircuitBreaker) Get(name string) *CircuitBreaker {
	mcb.mutex.RLock()
	cb, exists := mcb.breakers[name]
	mcb.mutex.RUnlock()
	
	if exists {
		return cb
	}
	
	// Create new circuit breaker
	mcb.mutex.Lock()
	defer mcb.mutex.Unlock()
	
	// Double-check
	cb, exists = mcb.breakers[name]
	if exists {
		return cb
	}
	
	opts := mcb.defaults
	opts.Name = name
	cb = NewCircuitBreaker(opts)
	mcb.breakers[name] = cb
	
	return cb
}

// Execute runs the request through the named circuit breaker
func (mcb *MultiCircuitBreaker) Execute(name string, req func() error) error {
	return mcb.Get(name).Execute(req)
}

// GetAll returns all circuit breakers
func (mcb *MultiCircuitBreaker) GetAll() map[string]*CircuitBreaker {
	mcb.mutex.RLock()
	defer mcb.mutex.RUnlock()
	
	result := make(map[string]*CircuitBreaker, len(mcb.breakers))
	for k, v := range mcb.breakers {
		result[k] = v
	}
	return result
}

// Reset resets the circuit breaker for the given name
func (mcb *MultiCircuitBreaker) Reset(name string) {
	mcb.mutex.Lock()
	defer mcb.mutex.Unlock()
	
	if cb, exists := mcb.breakers[name]; exists {
		cb.mutex.Lock()
		cb.state = StateClosed
		cb.generation++
		cb.counts = Counts{}
		cb.expiry = time.Now().Add(cb.interval)
		cb.mutex.Unlock()
	}
}