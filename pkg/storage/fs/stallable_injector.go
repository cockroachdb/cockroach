// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/vfs/errorfs"
	"golang.org/x/time/rate"
)

// StallMode defines the type of stall behavior.
type StallMode int32

const (
	// StallModeNone means no stalling - operations proceed normally.
	StallModeNone StallMode = iota
	// StallModeBlock blocks operations indefinitely until Release() is called.
	StallModeBlock
	// StallModeDelay adds a fixed delay to operations.
	StallModeDelay
	// StallModeThrottle limits throughput to a specified bytes/second rate.
	StallModeThrottle
)

// String returns a string representation of the StallMode.
func (m StallMode) String() string {
	switch m {
	case StallModeNone:
		return "none"
	case StallModeBlock:
		return "block"
	case StallModeDelay:
		return "delay"
	case StallModeThrottle:
		return "throttle"
	default:
		return fmt.Sprintf("unknown(%d)", m)
	}
}

// StallableInjector implements errorfs.Injector with controllable stall behavior.
//
// The injector intercepts VFS operations and can block, delay, or throttle them
// based on configuration. It is designed for unit testing disk stall scenarios.
//
// There are two modes of operation:
//  1. Standalone mode: Use NewStallableInjector and control via SetBlock/SetDelay/etc
//  2. Controller mode: Use NewStallableInjectorWithController to delegate to external controller
//
// Thread-safety: All methods are safe for concurrent use.
type StallableInjector struct {
	mu struct {
		sync.RWMutex
		mode         StallMode
		delay        time.Duration
		bytesPerSec  int64
		limiter      *rate.Limiter
		blockCh      chan struct{} // closed to release blocked operations
		blockedCount int           // number of currently blocked goroutines
		blockedCond  *sync.Cond    // signals when blockedCount changes
	}

	// opFilter optionally filters which operations to affect.
	// If nil, all operations are affected.
	opFilter func(errorfs.Op) bool

	// controller, if non-nil, delegates stall behavior to an external controller.
	// When set, the injector's own mode/delay/etc settings are ignored.
	controller StallController

	// enabled controls whether stalling is active (only used in standalone mode).
	enabled atomic.Bool

	// Stats for observability.
	stalledOps atomic.Int64
}

// NewStallableInjector creates a new StallableInjector.
//
// The opFilter function, if non-nil, determines which operations are subject
// to stalling. If nil, all operations are affected.
func NewStallableInjector(opFilter func(errorfs.Op) bool) *StallableInjector {
	s := &StallableInjector{
		opFilter: opFilter,
	}
	s.mu.blockedCond = sync.NewCond(&s.mu.RWMutex)
	s.mu.blockCh = make(chan struct{})
	return s
}

// String implements fmt.Stringer and errorfs.Injector.
func (s *StallableInjector) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("StallableInjector(enabled=%v, mode=%s)", s.enabled.Load(), s.mu.mode)
}

// MaybeError implements errorfs.Injector.
//
// This method is called before each VFS operation. It may block, delay, or
// throttle the operation depending on the current configuration.
func (s *StallableInjector) MaybeError(op errorfs.Op) error {
	// Check if this operation should be affected.
	if s.opFilter != nil && !s.opFilter(op) {
		return nil
	}

	// Controller mode: delegate to external controller.
	if s.controller != nil {
		if !s.controller.IsEnabled() {
			return nil
		}
		s.stalledOps.Add(1)
		s.controller.Stall()
		return nil
	}

	// Standalone mode: use internal state.
	// Fast path: if not enabled, return immediately.
	if !s.enabled.Load() {
		return nil
	}

	s.stalledOps.Add(1)

	s.mu.RLock()
	mode := s.mu.mode
	delay := s.mu.delay
	blockCh := s.mu.blockCh
	limiter := s.mu.limiter
	s.mu.RUnlock()

	switch mode {
	case StallModeNone:
		// No stalling.
		return nil

	case StallModeBlock:
		// Block until the channel is closed.
		s.incrementBlocked()
		<-blockCh
		s.decrementBlocked()
		return nil

	case StallModeDelay:
		// Sleep for the configured duration.
		time.Sleep(delay)
		return nil

	case StallModeThrottle:
		// Use rate limiter. We use a fixed "cost" per operation.
		// For more accurate throttling, we'd need to know the data size.
		if limiter != nil {
			_ = limiter.Wait(context.Background())
		}
		return nil

	default:
		return nil
	}
}

// incrementBlocked increments the blocked count and signals waiters.
func (s *StallableInjector) incrementBlocked() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.blockedCount++
	s.mu.blockedCond.Broadcast()
}

// decrementBlocked decrements the blocked count and signals waiters.
func (s *StallableInjector) decrementBlocked() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.blockedCount--
	s.mu.blockedCond.Broadcast()
}

// Enable enables the stall behavior.
func (s *StallableInjector) Enable() {
	s.enabled.Store(true)
}

// Disable disables all stall behavior. Operations will proceed normally.
func (s *StallableInjector) Disable() {
	s.enabled.Store(false)
}

// IsEnabled returns true if stalling is enabled.
func (s *StallableInjector) IsEnabled() bool {
	return s.enabled.Load()
}

// SetBlock configures the injector to block operations indefinitely until
// Release() is called.
func (s *StallableInjector) SetBlock() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.mode = StallModeBlock
	// Create a new blocking channel if the old one was closed.
	select {
	case <-s.mu.blockCh:
		// Channel is closed, create a new one.
		s.mu.blockCh = make(chan struct{})
	default:
		// Channel is still open, keep it.
	}
	s.enabled.Store(true)
}

// Release unblocks all currently blocked operations and sets mode to None.
func (s *StallableInjector) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Close the channel to release all blocked goroutines.
	select {
	case <-s.mu.blockCh:
		// Already closed.
	default:
		close(s.mu.blockCh)
	}
	s.mu.mode = StallModeNone
}

// SetDelay configures the injector to add a fixed delay to operations.
func (s *StallableInjector) SetDelay(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.mode = StallModeDelay
	s.mu.delay = d
	s.enabled.Store(true)
}

// SetThrottle configures the injector to limit throughput.
//
// The bytesPerSec parameter specifies the maximum bytes per second.
// Note: This is approximate since we don't know the actual size of each
// operation. Each operation is counted as consuming 1 "token" from the limiter.
func (s *StallableInjector) SetThrottle(bytesPerSec int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.mode = StallModeThrottle
	s.mu.bytesPerSec = bytesPerSec
	// Create a rate limiter. We use 1 token per operation as an approximation.
	// For more accurate throttling, we'd need data size information.
	s.mu.limiter = rate.NewLimiter(rate.Limit(bytesPerSec), int(bytesPerSec))
	s.enabled.Store(true)
}

// WaitForBlocked blocks until at least n goroutines are blocked on I/O.
// This is useful for synchronizing test assertions.
func (s *StallableInjector) WaitForBlocked(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.mu.blockedCount < n {
		s.mu.blockedCond.Wait()
	}
}

// BlockedCount returns the number of currently blocked goroutines.
func (s *StallableInjector) BlockedCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.blockedCount
}

// StalledOps returns the total number of operations that were stalled.
func (s *StallableInjector) StalledOps() int64 {
	return s.stalledOps.Load()
}

// Mode returns the current stall mode.
func (s *StallableInjector) Mode() StallMode {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.mode
}

// Ensure StallableInjector implements errorfs.Injector.
var _ errorfs.Injector = (*StallableInjector)(nil)

// StallController is the interface that external controllers must implement
// to drive a StallableInjector's behavior.
type StallController interface {
	// IsEnabled returns whether stalling is currently enabled.
	IsEnabled() bool
	// Stall is called for each operation that should be stalled.
	// The implementation should block, delay, or otherwise affect the operation.
	Stall()
}

// NewStallableInjectorWithController creates a StallableInjector that delegates
// its stall behavior to an external controller.
//
// This is useful when you want centralized control over multiple types of faults
// (disk, RPC, etc.) from a single controller.
//
// The opFilter function, if non-nil, determines which operations are subject
// to stalling. If nil, all operations are affected.
func NewStallableInjectorWithController(controller StallController, opFilter func(errorfs.Op) bool) *StallableInjector {
	s := &StallableInjector{
		opFilter:   opFilter,
		controller: controller,
	}
	s.mu.blockedCond = sync.NewCond(&s.mu.RWMutex)
	s.mu.blockCh = make(chan struct{})
	return s
}
