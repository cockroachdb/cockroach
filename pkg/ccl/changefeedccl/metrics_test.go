package changefeedccl

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type condWaiter struct {
	condition func(val int64) bool
	ch        chan struct{}
}

func newCondWaiter(condition func(val int64) bool) *condWaiter {
	return &condWaiter{
		condition: condition,
		ch:        make(chan struct{}),
	}
}

// Wait waits for the condition to be true or for the timeout to expire.
func (w *condWaiter) Wait(timeout time.Duration) error {
	select {
	case <-w.ch:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for condition")
	}
}

func (w *condWaiter) eval(val int64) (matched bool) {
	if w == nil {
		return false
	}
	if w.condition(val) {
		close(w.ch)
		return true
	}
	return false
}

// testMetric is a mock for metrics.Histogram/Gauge/Counter that lets you set a watch condition and wait for it to become true. It can function as a Gauge, Counter, or Histogram.
type testMetric struct {
	mu     syncutil.Mutex
	val    int64
	waiter *condWaiter
}

func (h *testMetric) RecordValue(v int64) {
	h.applyAndEval(func() { h.val = v })
}

func (h *testMetric) Update(v int64) {
	h.RecordValue(v)
}

func (h *testMetric) Dec(n int64) {
	h.applyAndEval(func() { h.val -= n })
}

func (h *testMetric) Inc(n int64) {
	h.applyAndEval(func() { h.val += n })
}

// applyAndEval is a little helper that applies a function to the metric value and evaluates the condition.
func (h *testMetric) applyAndEval(fn func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fn()
	if h.waiter.eval(h.val) {
		h.waiter = nil
	}
}

// SetWaiter sets a condition to wait for. Note that only one condition can be set at a time.
func (h *testMetric) SetWaiter(condition func(val int64) bool) *condWaiter {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.waiter != nil {
		panic("waiter already set")
	}
	h.waiter = newCondWaiter(condition)
	return h.waiter
}

var _ histogram = &testMetric{}
var _ gauge = &testMetric{}
var _ counter = &testMetric{}

// waitMultiple waits for multiple conditions to become true or for the timeout to expire.
func waitMultiple(timeout time.Duration, waiters ...*condWaiter) error {
	for _, w := range waiters {
		if err := w.Wait(timeout); err != nil {
			return err
		}
	}
	return nil
}
