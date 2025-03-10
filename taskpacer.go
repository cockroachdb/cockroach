// Package taskpacer provides an interface for pacing task execution.
package taskpacer

import (
	"context"
	"sync"
	"time"
)

// WorkPacer is an interface that controls the pacing of tasks.
// It provides methods to get work, get configuration, and wait for configuration changes.
type WorkPacer interface {
	// GetWork returns a channel that will be closed when work should be performed.
	// The implementation is provided by the DefaultWorkPacer.
	GetWork(ctx context.Context) <-chan struct{}

	// GetConf returns the current configuration for the task pacer.
	// The specific return type depends on the implementation.
	GetConf() interface{}

	// WaitConf blocks until the configuration changes or the context is canceled.
	// Returns true if the configuration changed, false if the context was canceled.
	WaitConf(ctx context.Context) bool
}

// DefaultWorkPacer provides a default implementation of the GetWork method
// that can be embedded in concrete WorkPacer implementations.
type DefaultWorkPacer struct {
	mu      sync.Mutex
	workCh  chan struct{}
	closeCh chan struct{}
}

// GetWork returns a channel that will be closed when work should be performed.
// This is the known implementation that can be used by all WorkPacer implementations.
func (d *DefaultWorkPacer) GetWork(ctx context.Context) <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()

	// If we already have a work channel, return it.
	if d.workCh != nil {
		select {
		case <-d.workCh:
			// Channel is already closed, create a new one.
		default:
			// Channel is still open, reuse it.
			return d.workCh
		}
	}

	// Create a new work channel.
	d.workCh = make(chan struct{})

	// Create a close channel if it doesn't exist.
	if d.closeCh == nil {
		d.closeCh = make(chan struct{})
	}

	// Start a goroutine to close the work channel when the context is done.
	go func() {
		select {
		case <-ctx.Done():
			d.mu.Lock()
			if d.workCh != nil {
				close(d.workCh)
				d.workCh = nil
			}
			d.mu.Unlock()
		case <-d.closeCh:
			// The DefaultWorkPacer is being closed.
			return
		}
	}()

	return d.workCh
}

// Close closes the DefaultWorkPacer and releases any resources.
func (d *DefaultWorkPacer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closeCh != nil {
		close(d.closeCh)
		d.closeCh = nil
	}

	if d.workCh != nil {
		close(d.workCh)
		d.workCh = nil
	}
}

// Example implementations of WorkPacer might include:

// TimerWorkPacer implements WorkPacer with a timer-based approach.
type TimerWorkPacer struct {
	DefaultWorkPacer
	mu       sync.Mutex
	interval time.Duration
	confCh   chan struct{}
}

// GetConf returns the current interval configuration.
func (t *TimerWorkPacer) GetConf() interface{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.interval
}

// WaitConf blocks until the configuration changes or the context is canceled.
func (t *TimerWorkPacer) WaitConf(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-t.confCh:
		return true
	}
}

// SetInterval updates the timer interval and signals a configuration change.
func (t *TimerWorkPacer) SetInterval(interval time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.interval == interval {
		return
	}

	t.interval = interval

	// Signal configuration change
	select {
	case <-t.confCh:
		// Channel already closed, create a new one
	default:
		close(t.confCh)
	}
	t.confCh = make(chan struct{})
}

// NewTimerWorkPacer creates a new TimerWorkPacer with the given interval.
func NewTimerWorkPacer(interval time.Duration) *TimerWorkPacer {
	return &TimerWorkPacer{
		interval: interval,
		confCh:   make(chan struct{}),
	}
}
