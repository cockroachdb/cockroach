package changefeedccl

import "github.com/cockroachdb/cockroach/pkg/util/syncutil"

type testHistogram struct {
	mu  syncutil.Mutex
	val int64

	condition func(val int64) bool
	waiter    chan struct{}
}

func (h *testHistogram) RecordValue(v int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.val = v

	if h.condition != nil && h.condition(h.val) && h.waiter != nil {
		close(h.waiter)
	}
}

func (h *testHistogram) setWaiter(condition func(val int64) bool) chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.condition = condition
	h.waiter = make(chan struct{})
	return h.waiter
}

var _ histogram = &testHistogram{}

type testGauge struct {
	mu  syncutil.Mutex
	val int64

	condition func(val int64) bool
	waiter    chan struct{}
}

func (h *testGauge) Update(v int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.val = v
	if h.condition != nil && h.condition(h.val) && h.waiter != nil {
		close(h.waiter)
	}
}

func (h *testGauge) Dec(n int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.val -= n
	if h.condition != nil && h.condition(h.val) && h.waiter != nil {
		close(h.waiter)
	}
}

func (h *testGauge) Inc(n int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.val += n
	if h.condition != nil && h.condition(h.val) && h.waiter != nil {
		close(h.waiter)
	}
}

func (h *testGauge) setWaiter(condition func(val int64) bool) chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.condition = condition
	h.waiter = make(chan struct{})
	return h.waiter
}

var _ gauge = &testGauge{}
