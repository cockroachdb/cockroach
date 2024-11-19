// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func TestStopEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := newTestScheduler(1)
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	s.Stop()

	assertStopsWithinTimeout(t, s)
}

func TestStopNonEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := newTestScheduler(1)
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s, 1, false /* priority */)
	s.stopProcessor(c.id)
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

func createAndRegisterConsumerOrFail(
	t *testing.T, scheduler *Scheduler, id int64, priority bool,
) *schedulerConsumer {
	t.Helper()
	c := &schedulerConsumer{
		c:          make(chan processorEventType, 1000),
		reschedule: make(chan processorEventType, 1),
		sched:      scheduler,
		id:         id,
	}
	err := c.sched.register(id, c.process, priority)
	require.NoError(t, err, "failed to register processor")
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
		c.sched.unregister(c.id)
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

	s := newTestScheduler(1)
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s, 1, false /* priority */)
	s.enqueue(c.id, te1)
	c.requireEvent(t, time.Second*30000, te1, 1)
	assertStopsWithinTimeout(t, s)
}

func TestNoParallel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := newTestScheduler(2)
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s, 1, false /* priority */)
	c.pause()
	s.enqueue(c.id, te1)
	c.waitPaused()
	s.enqueue(c.id, te2)
	c.resume()
	c.requireHistory(t, time.Second*30, []processorEventType{te1, te2})
	assertStopsWithinTimeout(t, s)
}

func TestProcessOtherWhilePaused(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := newTestScheduler(2)
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c1 := createAndRegisterConsumerOrFail(t, s, 1, false /* priority */)
	c2 := createAndRegisterConsumerOrFail(t, s, 2, false /* priority */)
	c1.pause()
	s.enqueue(c1.id, te1)
	c1.waitPaused()
	s.enqueue(c2.id, te1)
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

	s := newTestScheduler(2)
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s, 1, false /* priority */)
	c.pause()
	s.enqueue(c.id, te1)
	c.waitPaused()
	s.enqueue(c.id, te2)
	s.enqueue(c.id, te3)
	c.resume()
	c.requireHistory(t, time.Second*30, []processorEventType{te1, te2 | te3})
	assertStopsWithinTimeout(t, s)
}

func TestRescheduleEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := newTestScheduler(2)
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s, 1, false /* priority */)
	c.pause()
	s.enqueue(c.id, te1)
	c.waitPaused()
	s.enqueue(c.id, te1)
	c.resume()
	c.requireHistory(t, time.Second*30, []processorEventType{te1, te1})
	assertStopsWithinTimeout(t, s)
}

func TestClientScheduler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := newTestScheduler(2)
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	cs := s.NewClientScheduler()
	// Manually create consumer as we don't want it to start, but want to use it
	// via client scheduler.
	c := &schedulerConsumer{
		c:          make(chan processorEventType, 1000),
		reschedule: make(chan processorEventType, 1),
		sched:      s,
		id:         1,
	}
	require.NoError(t, cs.Register(c.process, false), "failed to register consumer")
	require.Error(t,
		cs.Register(func(event processorEventType) (remaining processorEventType) { return 0 }, false),
		"reregistration must fail")
	c.pause()
	cs.Enqueue(te2)
	c.waitPaused()
	cs.Unregister()
	c.resume()
	c.requireHistory(t, time.Second*30, []processorEventType{te2})
	assertStopsWithinTimeout(t, s)
}

func TestScheduleBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{
		Workers:       2,
		BulkChunkSize: 2,
		ShardSize:     2,
		Metrics:       NewSchedulerMetrics(time.Minute),
	})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	const consumerNumber = 100
	consumers := make([]*schedulerConsumer, consumerNumber)
	batch := s.NewEnqueueBatch()
	defer batch.Close()
	for i := 0; i < consumerNumber; i++ {
		consumers[i] = createAndRegisterConsumerOrFail(t, s, int64(i+1), false /* priority */)
		batch.Add(consumers[i].id)
	}
	s.EnqueueBatch(batch, te1)
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

	s := newTestScheduler(1)
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s, 1, false /* priority */)
	// Set process response to trigger process once again.
	c.rescheduleNext(te1)
	s.enqueue(c.id, te1)
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

	s := newTestScheduler(1)
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c := createAndRegisterConsumerOrFail(t, s, 1, false /* priority */)
	s.enqueue(c.id, te1)
	c.requireHistory(t, time.Second*30, []processorEventType{te1})
	s.unregister(c.id)
	assertStopsWithinTimeout(t, s)
	// Ensure that we didn't send stop after callback was removed.
	c.requireHistory(t, time.Second*30, []processorEventType{te1})
}

func TestStartupFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	stopper.Stop(ctx)

	s := newTestScheduler(1)
	require.Error(t, s.Start(ctx, stopper), "started despite stopper stopped")
}

func TestSchedulerShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{
		Workers: 2, ShardSize: 1, Metrics: NewSchedulerMetrics(time.Minute),
	})
	require.NoError(t, s.Start(ctx, stopper), "failed to start")
	c1 := createAndRegisterConsumerOrFail(t, s, 1, false /* priority */)
	c2 := createAndRegisterConsumerOrFail(t, s, 2, false /* priority */)
	s.stopProcessor(c2.id)
	s.Stop()
	// Ensure that we are not stopped twice.
	c1.requireHistory(t, time.Second*30, []processorEventType{Stopped})
	c2.requireHistory(t, time.Second*30, []processorEventType{Stopped})
}

func TestQueueReadWrite1By1(t *testing.T) {
	q := newIDQueue()
	val := int64(7)
	for i := 0; i < idQueueChunkSize*3; i++ {
		q.pushBack(queueEntry{id: val})
		require.Equal(t, 1, q.Len(), "queue size")
		v, ok := q.popFront()
		require.True(t, ok, "value not found after writing")
		require.Equal(t, val, v.id, "read different from write")
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
		q.pushBack(queueEntry{id: val})
		val = val*3 + 7
	}
	val = int64(7)
	for i := 0; i < idQueueChunkSize*3; i++ {
		require.Equal(t, idQueueChunkSize*3-i, q.Len(), "queue size")
		v, ok := q.popFront()
		require.True(t, ok, "value not found after writing")
		require.Equal(t, val, v.id, "read different from write")
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

func newTestScheduler(workers int) *Scheduler {
	return NewScheduler(SchedulerConfig{Workers: workers, Metrics: NewSchedulerMetrics(time.Minute)})
}

func TestNewSchedulerShards(t *testing.T) {
	defer leaktest.AfterTest(t)()

	pri := 2 // priority workers

	testcases := []struct {
		priorityWorkers int
		workers         int
		shardSize       int
		expectShards    []int
	}{
		// We always assign at least 1 priority worker to the priority shard.
		{-1, 1, 1, []int{1, 1}},
		{0, 1, 1, []int{1, 1}},
		{2, 1, 1, []int{2, 1}},

		// We balance workers across shards instead of filling up shards. We assume
		// ranges are evenly distributed across shards, and want ranges to have
		// about the same number of workers available on average.
		{pri, -1, -1, []int{pri, 1}},
		{pri, 0, 0, []int{pri, 1}},
		{pri, 1, -1, []int{pri, 1}},
		{pri, 1, 0, []int{pri, 1}},
		{pri, 1, 1, []int{pri, 1}},
		{pri, 1, 2, []int{pri, 1}},
		{pri, 2, 2, []int{pri, 2}},
		{pri, 3, 2, []int{pri, 2, 1}},
		{pri, 1, 3, []int{pri, 1}},
		{pri, 2, 3, []int{pri, 2}},
		{pri, 3, 3, []int{pri, 3}},
		{pri, 4, 3, []int{pri, 2, 2}},
		{pri, 5, 3, []int{pri, 3, 2}},
		{pri, 6, 3, []int{pri, 3, 3}},
		{pri, 7, 3, []int{pri, 3, 2, 2}},
		{pri, 8, 3, []int{pri, 3, 3, 2}},
		{pri, 9, 3, []int{pri, 3, 3, 3}},
		{pri, 10, 3, []int{pri, 3, 3, 2, 2}},
		{pri, 11, 3, []int{pri, 3, 3, 3, 2}},
		{pri, 12, 3, []int{pri, 3, 3, 3, 3}},

		// Typical examples, using 4 workers per CPU core and 8 workers per shard.
		// Note that we cap workers at 64 by default.
		{pri, 1 * 4, 8, []int{pri, 4}},
		{pri, 2 * 4, 8, []int{pri, 8}},
		{pri, 3 * 4, 8, []int{pri, 6, 6}},
		{pri, 4 * 4, 8, []int{pri, 8, 8}},
		{pri, 6 * 4, 8, []int{pri, 8, 8, 8}},
		{pri, 8 * 4, 8, []int{pri, 8, 8, 8, 8}},
		{pri, 12 * 4, 8, []int{pri, 8, 8, 8, 8, 8, 8}},
		{pri, 16 * 4, 8, []int{pri, 8, 8, 8, 8, 8, 8, 8, 8}}, // 64 workers
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("workers=%d/shardSize=%d", tc.workers, tc.shardSize), func(t *testing.T) {
			s := NewScheduler(SchedulerConfig{
				Workers:         tc.workers,
				PriorityWorkers: tc.priorityWorkers,
				ShardSize:       tc.shardSize,
				Metrics:         NewSchedulerMetrics(time.Minute),
			})

			var shardWorkers []int
			for _, shard := range s.shards {
				shardWorkers = append(shardWorkers, shard.numWorkers)
			}
			require.Equal(t, tc.expectShards, shardWorkers)
		})
	}
}

// TestSchedulerPriority tests that the scheduler correctly registers
// and enqueues events for priority processors.
func TestSchedulerPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	s := NewScheduler(SchedulerConfig{
		Workers:         1,
		PriorityWorkers: 1,
		ShardSize:       1,
		BulkChunkSize:   1,
		Metrics:         NewSchedulerMetrics(time.Minute),
	})
	require.NoError(t, s.Start(ctx, stopper))
	defer s.Stop()

	// Create one regular and one priority consumer.
	c := createAndRegisterConsumerOrFail(t, s, 1, false /* priority */)
	cPri := createAndRegisterConsumerOrFail(t, s, 2, true /* priority */)

	// Block the regular consumer.
	c.pause()
	s.enqueue(c.id, te1)
	c.waitPaused()

	// The priority consumer should be able to process events.
	s.enqueue(cPri.id, te1)
	cPri.requireHistory(t, 5*time.Second, []processorEventType{te1})

	// Resuming the regular consumer should process its queued event.
	c.resume()
	c.requireHistory(t, 5*time.Second, []processorEventType{te1})
	assertStopsWithinTimeout(t, s)
	c.requireStopped(t, 5*time.Second)
	cPri.requireStopped(t, 5*time.Second)
}

func TestMetricThrottling(t *testing.T) {
	const workers = 100
	const iterations = 1000
	const freq = 3
	s := schedulerShard{histogramFrequency: freq}
	var wg sync.WaitGroup
	ticks := make([]int, workers)
	for i := 0; i < workers; i++ {
		worker := i
		wg.Add(1)
		go func() {
			for j := 0; j < iterations; j++ {
				if s.maybeEnqueueStartTime() != 0 {
					ticks[worker]++
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	var sum int
	for _, v := range ticks {
		sum += v
	}
	require.Equal(t, workers*iterations/freq, sum, "total number of ticks")
}
