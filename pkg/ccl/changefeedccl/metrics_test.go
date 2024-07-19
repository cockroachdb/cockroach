package changefeedccl

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

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

	fmt.Printf("testHistogram.RecordValue: h.val=%d, waiter=%v\n", h.val, h.waiter)

	if h.condition != nil && h.condition(h.val) && h.waiter != nil {
		close(h.waiter)
		h.waiter = nil
		h.condition = nil
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

	fmt.Printf("testGauge.Update: h.val=%d, waiter=%v\n", h.val, h.waiter)

	if h.condition != nil && h.condition(h.val) && h.waiter != nil {
		close(h.waiter)
		h.waiter = nil
		h.condition = nil
	}
}

func (h *testGauge) Dec(n int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.val -= n

	fmt.Printf("testGauge.Dec: h.val=%d, waiter=%v\n", h.val, h.waiter)

	if h.condition != nil && h.condition(h.val) && h.waiter != nil {
		close(h.waiter)
		h.waiter = nil
		h.condition = nil
	}
}

func (h *testGauge) Inc(n int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.val += n

	fmt.Printf("testGauge.Inc: h.val=%d, waiter=%v\n", h.val, h.waiter)

	if h.condition != nil && h.condition(h.val) && h.waiter != nil {
		close(h.waiter)
		h.waiter = nil
		h.condition = nil
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
