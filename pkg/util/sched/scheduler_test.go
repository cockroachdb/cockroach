// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sched

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestStopEmpty(t *testing.T) {
	stopper := stop.NewStopper()
	s := NewScheduler(SchedConfig{Name: "test-s", Workers: 1})
	require.NoError(t, s.Start(stopper), "failed to start")
	require.NoError(t, s.Close(time.Second*30), "failed to stop")
}

func TestStopNonEmpty(t *testing.T) {
	stopper := stop.NewStopper()
	s := NewScheduler(SchedConfig{Name: "test-s", Workers: 1})
	require.NoError(t, s.Start(stopper), "failed to start")
	c := newConsumer()
	require.NoError(t, s.Register(1, c.process))
	require.NoError(t, s.Close(time.Second*30), "failed to stop")
	c.requireStopped(t, time.Second*30)
}

type consumer struct {
	c  chan int
	mu struct {
		syncutil.RWMutex
		wait    chan interface{}
		waiting chan interface{}
	}
	flat []int
}

func newConsumer() *consumer {
	c := &consumer{}
	c.c = make(chan int, 1000)
	return c
}

func (c *consumer) process(ev int) int {
	c.c <- ev
	c.mu.RLock()
	w, ww := c.mu.wait, c.mu.waiting
	c.mu.RUnlock()
	if w != nil {
		close(ww)
		<-w
	}
	return 0
}

func (c *consumer) pause() {
	c.mu.Lock()
	c.mu.wait = make(chan interface{})
	c.mu.waiting = make(chan interface{})
	c.mu.Unlock()
}

func (c *consumer) waitPaused() {
	<-c.mu.waiting
}

// Close waiter channel. Test should track state itself and don't use resume if
// pause was not issued.
func (c *consumer) resume() {
	c.mu.Lock()
	w := c.mu.wait
	c.mu.wait, c.mu.waiting = nil, nil
	c.mu.Unlock()
	close(w)
}

func (c *consumer) assertTill(
	t *testing.T, timeout time.Duration, assert func(flat []int) bool,
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

func (c *consumer) requireEvent(t *testing.T, timeout time.Duration, event int, count ...int) {
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
	var lastHist []int
	if !c.assertTill(t, timeout, func(flat []int) bool {
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

func (c *consumer) requireHistory(t *testing.T, timeout time.Duration, history []int) {
	t.Helper()
	var lastHist []int
	if !c.assertTill(t, timeout, func(flat []int) bool {
		lastHist = flat
		return slices.Equal(history, lastHist)
	}) {
		t.Fatalf("expected history %08b found %08b", history, lastHist)
	}
}

func (c *consumer) requireStopped(t *testing.T, timeout time.Duration) {
	t.Helper()
	lastEvent := 0
	if !c.assertTill(t, timeout, func(flat []int) bool {
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
	stopper := stop.NewStopper()
	s := NewScheduler(SchedConfig{Name: "test-s", Workers: 1})
	require.NoError(t, s.Start(stopper), "failed to start")
	c := newConsumer()
	require.NoError(t, s.Register(2, c.process), "failed to register consumer")
	require.NoError(t, s.Enqueue(2, te1), "failed to enqueue")
	c.requireEvent(t, time.Second*30000, te1, 1)
	require.NoError(t, s.Close(time.Second*30), "failed to stop")
}

func TestNoParallel(t *testing.T) {
	stopper := stop.NewStopper()
	s := NewScheduler(SchedConfig{Name: "test-s", Workers: 2})
	require.NoError(t, s.Start(stopper), "failed to start")
	c := newConsumer()
	require.NoError(t, s.Register(2, c.process), "failed to register consumer")
	c.pause()
	require.NoError(t, s.Enqueue(2, te1), "failed to enqueue")
	c.waitPaused()
	require.NoError(t, s.Enqueue(2, te2), "failed to enqueue")
	c.resume()
	c.requireHistory(t, time.Second*30, []int{te1, te2})
	require.NoError(t, s.Close(time.Second*30), "failed to stop")
}

func TestProcessOtherWhilePaused(t *testing.T) {
	stopper := stop.NewStopper()
	s := NewScheduler(SchedConfig{Name: "test-s", Workers: 2})
	require.NoError(t, s.Start(stopper), "failed to start")
	c1 := newConsumer()
	require.NoError(t, s.Register(2, c1.process), "failed to register consumer 1")
	c2 := newConsumer()
	require.NoError(t, s.Register(3, c2.process), "failed to register consumer 2")
	c1.pause()
	require.NoError(t, s.Enqueue(2, te1), "failed to enqueue")
	c1.waitPaused()
	require.NoError(t, s.Enqueue(3, te1), "failed to enqueue")
	c2.requireHistory(t, time.Second*30, []int{te1})
	c1.resume()
	c1.requireHistory(t, time.Second*30, []int{te1})
	require.NoError(t, s.Close(time.Second*30), "failed to stop")
	c1.requireStopped(t, time.Second*30)
	c2.requireStopped(t, time.Second*30)
}

func TestEventsCombined(t *testing.T) {
	stopper := stop.NewStopper()
	s := NewScheduler(SchedConfig{Name: "test-s", Workers: 2})
	require.NoError(t, s.Start(stopper), "failed to start")
	c := newConsumer()
	require.NoError(t, s.Register(2, c.process), "failed to register consumer")
	c.pause()
	require.NoError(t, s.Enqueue(2, te1), "failed to enqueue")
	c.waitPaused()
	require.NoError(t, s.Enqueue(2, te2), "failed to enqueue")
	require.NoError(t, s.Enqueue(2, te3), "failed to enqueue")
	c.resume()
	c.requireHistory(t, time.Second*30, []int{te1, te2 | te3})
	require.NoError(t, s.Close(time.Second*30), "failed to stop")
}

func TestRescheduleEvent(t *testing.T) {
	stopper := stop.NewStopper()
	s := NewScheduler(SchedConfig{Name: "test-s", Workers: 2})
	require.NoError(t, s.Start(stopper), "failed to start")
	c := newConsumer()
	require.NoError(t, s.Register(2, c.process), "failed to register consumer")
	c.pause()
	require.NoError(t, s.Enqueue(2, te1), "failed to enqueue")
	c.waitPaused()
	require.NoError(t, s.Enqueue(2, te1), "failed to enqueue")
	c.resume()
	c.requireHistory(t, time.Second*30, []int{te1, te1})
	require.NoError(t, s.Close(time.Second*30), "failed to stop")
}

func TestClientScheduler(t *testing.T) {
	stopper := stop.NewStopper()
	s := NewScheduler(SchedConfig{Name: "test-s", Workers: 2})
	require.NoError(t, s.Start(stopper), "failed to start")
	cs := NewClientScheduler(1, s)
	require.Error(t, cs.Schedule(te1), "schedule prior to registration must fail")
	c := newConsumer()
	require.NoError(t, cs.Register(c.process), "failed to register consumer")
	require.Error(t, cs.Register(func(event int) (remaining int) { return 0 }),
		"reregistration must fail")
	c.pause()
	require.NoError(t, cs.Schedule(te2), "failed to schedule")
	c.waitPaused()
	cs.Stop()
	c.resume()
	c.requireHistory(t, time.Second*30, []int{te2, Stopped})
	require.Error(t, cs.Schedule(te1), "schedule after stop must fail")
	require.NoError(t, s.Close(time.Second*30), "failed to stop")
}
