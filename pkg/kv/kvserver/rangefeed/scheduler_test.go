// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestStopEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{Workers: 1})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	s.Stop()

	assertStopsWithinTimeout(t, s)
}

func TestStopNonEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{Workers: 1})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s)
	s.StopProcessor(c.id)
	assertStopsWithinTimeout(t, s)
	c.requireStopped(t, time.Second*30)
}

type schedulerConsumer struct {
	c  chan processorEventType
	mu struct {
		syncutil.RWMutex
		wait    chan interface{}
		waiting chan interface{}
	}
	reschedule chan processorEventType
	flat       []processorEventType
	sched      *Scheduler
	id         int64
}

func createAndRegisterConsumerOrFail(t *testing.T, scheduler *Scheduler) *schedulerConsumer {
	t.Helper()
	c := &schedulerConsumer{
		c:          make(chan processorEventType, 1000),
		reschedule: make(chan processorEventType, 1),
		sched:      scheduler,
	}
	id, err := c.sched.Register(c.process)
	require.NoError(t, err, "failed to register processor")
	c.id = id
	return c
}

func (c *schedulerConsumer) process(ev processorEventType) processorEventType {
	c.c <- ev
	c.mu.RLock()
	w, ww := c.mu.wait, c.mu.waiting
	c.mu.RUnlock()
	if w != nil {
		close(ww)
		<-w
	}
	select {
	case r := <-c.reschedule:
		// Tests don't try to do reschedule and stop at the same time, so it's ok
		// not to fall through.
		return r
	default:
	}
	if ev&Stopped != 0 {
		c.sched.Unregister(c.id)
	}
	return 0
}

func (c *schedulerConsumer) pause() {
	c.mu.Lock()
	c.mu.wait = make(chan interface{})
	c.mu.waiting = make(chan interface{})
	c.mu.Unlock()
}

func (c *schedulerConsumer) waitPaused() {
	<-c.mu.waiting
}

// Close waiter channel. Test should track state itself and don't use resume if
// pause was not issued.
func (c *schedulerConsumer) resume() {
	c.mu.Lock()
	w := c.mu.wait
	c.mu.wait, c.mu.waiting = nil, nil
	c.mu.Unlock()
	close(w)
}

func (c *schedulerConsumer) rescheduleNext(e processorEventType) {
	c.reschedule <- e
}

func (c *schedulerConsumer) assertTill(
	t *testing.T, timeout time.Duration, assert func(flat []processorEventType) bool,
) bool {
	t.Helper()
	till := time.After(timeout)
	for {
		if assert(c.flat) {
			return true
		}
		select {
		case <-till:
			return false
		case e := <-c.c:
			c.flat = append(c.flat, e)
		}
	}
}

func (c *schedulerConsumer) requireEvent(
	t *testing.T, timeout time.Duration, event processorEventType, count ...int,
) {
	t.Helper()
	min, max := 0, 0
	l := len(count)
	switch {
	case l == 1:
		min, max = count[0], count[0]
	case l == 2:
		min, max = count[0], count[1]
	default:
		t.Fatal("event count limits must be 1 (exact) or 2 [mix, max]")
	}
	var lastHist []processorEventType
	if !c.assertTill(t, timeout, func(flat []processorEventType) bool {
		lastHist = flat
		match := 0
		for _, e := range lastHist {
			if e&event != 0 {
				match++
			}
		}
		return match >= min && match <= max
	}) {
		t.Fatalf("failed to find event %08b between %d and %d times in history %08b", event, min, max,
			lastHist)
	}
}

func (c *schedulerConsumer) requireHistory(
	t *testing.T, timeout time.Duration, history []processorEventType,
) {
	t.Helper()
	var lastHist []processorEventType
	if !c.assertTill(t, timeout, func(flat []processorEventType) bool {
		lastHist = flat
		return slices.Equal(history, lastHist)
	}) {
		t.Fatalf("expected history %08b found %08b", history, lastHist)
	}
}

func (c *schedulerConsumer) requireStopped(t *testing.T, timeout time.Duration) {
	t.Helper()
	lastEvent := processorEventType(0)
	if !c.assertTill(t, timeout, func(flat []processorEventType) bool {
		t.Helper()
		if len(c.flat) == 0 {
			return false
		}
		lastEvent = c.flat[len(c.flat)-1]
		return lastEvent&Stopped != 0
	}) {
		t.Fatalf("failed to find Stopped event at the end of history after %s, lastEvent=%08b", timeout,
			lastEvent)
	}
}

const (
	te1 = 1 << 2
	te2 = 1 << 3
	te3 = 1 << 4
)

func TestDeliverEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{Workers: 1})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s)
	s.Enqueue(c.id, te1)
	c.requireEvent(t, time.Second*30000, te1, 1)
	assertStopsWithinTimeout(t, s)
}

func TestNoParallel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{Workers: 2})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s)
	c.pause()
	s.Enqueue(c.id, te1)
	c.waitPaused()
	s.Enqueue(c.id, te2)
	c.resume()
	c.requireHistory(t, time.Second*30, []processorEventType{te1, te2})
	assertStopsWithinTimeout(t, s)
}

func TestProcessOtherWhilePaused(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{Workers: 2})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c1 := createAndRegisterConsumerOrFail(t, s)
	c2 := createAndRegisterConsumerOrFail(t, s)
	c1.pause()
	s.Enqueue(c1.id, te1)
	c1.waitPaused()
	s.Enqueue(c2.id, te1)
	c2.requireHistory(t, time.Second*30, []processorEventType{te1})
	c1.resume()
	c1.requireHistory(t, time.Second*30, []processorEventType{te1})
	assertStopsWithinTimeout(t, s)
	c1.requireStopped(t, time.Second*30)
	c2.requireStopped(t, time.Second*30)
}

func TestEventsCombined(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{Workers: 2})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s)
	c.pause()
	s.Enqueue(c.id, te1)
	c.waitPaused()
	s.Enqueue(c.id, te2)
	s.Enqueue(c.id, te3)
	c.resume()
	c.requireHistory(t, time.Second*30, []processorEventType{te1, te2 | te3})
	assertStopsWithinTimeout(t, s)
}

func TestRescheduleEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{Workers: 2})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s)
	c.pause()
	s.Enqueue(c.id, te1)
	c.waitPaused()
	s.Enqueue(c.id, te1)
	c.resume()
	c.requireHistory(t, time.Second*30, []processorEventType{te1, te1})
	assertStopsWithinTimeout(t, s)
}

func TestClientScheduler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{Workers: 2})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	cs := NewClientScheduler(s)
	// Manually create consumer as we don't want it to start, but want to use it
	// via client scheduler.
	c := &schedulerConsumer{
		c:          make(chan processorEventType, 1000),
		reschedule: make(chan processorEventType, 1),
		sched:      s,
		id:         1,
	}
	require.NoError(t, cs.Register(c.process), "failed to register consumer")
	require.Error(t,
		cs.Register(func(event processorEventType) (remaining processorEventType) { return 0 }),
		"reregistration must fail")
	c.pause()
	cs.Enqueue(te2)
	c.waitPaused()
	cs.Unregister()
	c.resume()
	c.requireHistory(t, time.Second*30, []processorEventType{te2})
	assertStopsWithinTimeout(t, s)
}

func TestScheduleMultiple(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{Workers: 2, BulkChunkSize: 2})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	const consumerNumber = 10
	consumers := make([]*schedulerConsumer, consumerNumber)
	ids := make([]int64, consumerNumber)
	for i := 0; i < consumerNumber; i++ {
		consumers[i] = createAndRegisterConsumerOrFail(t, s)
		ids[i] = consumers[i].id
	}
	s.EnqueueAll(ids, te1)
	for _, c := range consumers {
		c.requireEvent(t, time.Second*30000, te1, 1)
	}
	assertStopsWithinTimeout(t, s)
}

func TestPartialProcessing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{Workers: 1})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s)
	// Set process response to trigger process once again.
	c.rescheduleNext(te1)
	s.Enqueue(c.id, te1)
	c.requireHistory(t, time.Second*30, []processorEventType{te1, te1})
	assertStopsWithinTimeout(t, s)
}

func assertStopsWithinTimeout(t *testing.T, s *Scheduler) {
	stopC := make(chan interface{})
	go func() {
		s.Stop()
		close(stopC)
	}()
	select {
	case <-stopC:
	case <-time.After(30 * time.Second):
		t.Fatalf("scheduler failed to stop after 30 seconds")
	}
}

func TestUnregisterWithoutStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{Workers: 1})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s)
	s.Enqueue(c.id, te1)
	c.requireHistory(t, time.Second*30, []processorEventType{te1})
	s.Unregister(c.id)
	assertStopsWithinTimeout(t, s)
	// Ensure that we didn't send stop after callback was removed.
	c.requireHistory(t, time.Second*30, []processorEventType{te1})
}

func TestStartupFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{Workers: 1})
	require.Error(t, s.Start(ctx, stopper), "started despite stopper stopped")
}

func TestSchedulerShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{Workers: 1})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c1 := createAndRegisterConsumerOrFail(t, s)
	c2 := createAndRegisterConsumerOrFail(t, s)
	s.StopProcessor(c2.id)
	s.Stop()
	// Ensure that we are not stopped twice.
	c1.requireHistory(t, time.Second*30, []processorEventType{Stopped})
	c2.requireHistory(t, time.Second*30, []processorEventType{Stopped})
}

func TestQueueReadWrite1By1(t *testing.T) {
	q := newIDQueue()
	val := int64(7)
	for i := 0; i < idQueueChunkSize*3; i++ {
		q.pushBack(val)
		require.Equal(t, 1, q.Len(), "queue size")
		v, ok := q.popFront()
		require.True(t, ok, "value not found after writing")
		require.Equal(t, val, v, "read different from write")
		val = val*3 + 7
	}
	_, ok := q.popFront()
	require.False(t, ok, "unexpected value after tail")
}

func TestQueueReadWriteFull(t *testing.T) {
	q := newIDQueue()
	val := int64(7)
	for i := 0; i < idQueueChunkSize*3; i++ {
		require.Equal(t, i, q.Len(), "queue size")
		q.pushBack(val)
		val = val*3 + 7
	}
	val = int64(7)
	for i := 0; i < idQueueChunkSize*3; i++ {
		require.Equal(t, idQueueChunkSize*3-i, q.Len(), "queue size")
		v, ok := q.popFront()
		require.True(t, ok, "value not found after writing")
		require.Equal(t, val, v, "read different from write")
		val = val*3 + 7
	}
	require.Equal(t, 0, q.Len(), "queue size")
	_, ok := q.popFront()
	require.False(t, ok, "unexpected value after tail")
}

func TestQueueReadEmpty(t *testing.T) {
	q := newIDQueue()
	_, ok := q.popFront()
	require.False(t, ok, "unexpected value in empty queue")
}
