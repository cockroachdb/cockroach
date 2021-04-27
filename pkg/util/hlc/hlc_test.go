// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package hlc

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Event uint8

const (
	SEND = iota
	RECV
)

// ExampleNewClock shows how to create a new
// hybrid logical clock based on the local machine's
// physical clock. The sanity checks in this example
// will, of course, not fail and the output will be
// the age of the Unix epoch in nanoseconds.
func ExampleNewClock() {
	// Initialize a new clock, using the local
	// physical clock.
	c := NewClock(UnixNano, time.Nanosecond)
	// Update the state of the hybrid clock.
	s := c.Now()
	time.Sleep(50 * time.Nanosecond)
	t := Timestamp{WallTime: UnixNano()}
	// The sanity checks below will usually never be triggered.

	if s.Less(t) || !t.Less(s) {
		log.Fatalf(context.Background(), "The later timestamp is smaller than the earlier one")
	}

	if t.WallTime-s.WallTime > 0 {
		log.Fatalf(context.Background(), "HLC timestamp %s deviates from physical clock %s", s, t)
	}

	if s.Logical > 0 {
		log.Fatalf(context.Background(), "Trivial timestamp has logical component")
	}

	fmt.Printf("The Unix Epoch is now approximately %dns old.\n", t.WallTime)
}

func TestHLCLess(t *testing.T) {
	m := NewManualClock(1)
	c := NewClock(m.UnixNano, time.Nanosecond)
	a := c.Now()
	b := a
	if a.Less(b) || b.Less(a) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	m.Increment(1)
	b = c.Now()
	if !a.Less(b) {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = c.Now() // add one to logical clock from b
	if !b.Less(a) {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

func TestHLCLessEq(t *testing.T) {
	m := NewManualClock(1)
	c := NewClock(m.UnixNano, time.Nanosecond)
	a := c.Now()
	b := a
	if !a.LessEq(b) || !b.LessEq(a) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	m.Increment(1)
	b = c.Now()
	if !a.LessEq(b) || b.LessEq(a) {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = c.Now() // add one to logical clock from b
	if !b.LessEq(a) || a.LessEq(b) {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

func TestHLCEqual(t *testing.T) {
	m := NewManualClock(1)
	c := NewClock(m.UnixNano, time.Nanosecond)
	a := c.Now()
	b := a
	if a != b {
		t.Errorf("expected %+v == %+v", a, b)
	}
	m.Increment(1)
	b = c.Now()
	if a == b {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = c.Now() // add one to logical clock from b
	if b == a {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

// isErrSimilar returns true of the expected error is similar to the
// actual error
func isErrSimilar(expected *regexp.Regexp, actual error) bool {
	if actual == nil {
		return expected == nil
	}
	// actual != nil
	return expected != nil && expected.FindString(actual.Error()) != ""
}

func TestHLCPhysicalClockJump(t *testing.T) {
	var fatal bool
	defer log.ResetExitFunc()
	log.SetExitFunc(true /* hideStack */, func(r exit.Code) {
		if r == exit.FatalError() {
			fatal = true
		}
	})

	testCases := []struct {
		name       string
		actualJump time.Duration
		maxOffset  time.Duration
		isFatal    bool
	}{
		{
			name:       "small forward jump",
			actualJump: 50 * time.Millisecond,
			maxOffset:  500 * time.Millisecond,
			isFatal:    false,
		},
		{
			name:       "half max offset jump",
			actualJump: 250 * time.Millisecond,
			maxOffset:  500 * time.Millisecond,
			isFatal:    true,
		},
		{
			name:       "large forward jump",
			actualJump: 400 * time.Millisecond,
			maxOffset:  500 * time.Millisecond,
			isFatal:    true,
		},
		{
			name:       "large forward jump large thresh",
			actualJump: 400 * time.Millisecond,
			maxOffset:  900 * time.Millisecond,
			isFatal:    false,
		},
		{
			name:       "small backward jump",
			actualJump: -40 * time.Millisecond,
			maxOffset:  500 * time.Millisecond,
			isFatal:    false,
		},
		{
			name:       "large backward jump",
			actualJump: -700 * time.Millisecond,
			maxOffset:  500 * time.Millisecond,
			isFatal:    false,
		},
		{
			name:       "large backward jump large thresh",
			actualJump: -700 * time.Millisecond,
			maxOffset:  900 * time.Millisecond,
			isFatal:    false,
		},
	}
	ctx := context.Background()
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			a := assert.New(t)

			m := NewManualClock(1)
			c := NewClock(m.UnixNano, test.maxOffset)
			var tickerDuration time.Duration
			tickerCh := make(chan time.Time)
			tickProcessedCh := make(chan struct{})
			forwardJumpCheckEnabledCh := make(chan bool, 1)
			defer close(forwardJumpCheckEnabledCh)

			if err := c.StartMonitoringForwardClockJumps(
				ctx,
				forwardJumpCheckEnabledCh,
				func(d time.Duration) *time.Ticker {
					tickerDuration = d
					ticker := time.NewTicker(d)
					ticker.Stop()
					ticker.C = tickerCh
					return ticker
				},
				func() {
					tickProcessedCh <- struct{}{}
				},
			); err != nil {
				t.Error(err)
				return
			}

			if err := c.StartMonitoringForwardClockJumps(
				ctx,
				forwardJumpCheckEnabledCh,
				time.NewTicker,
				nil, /* tick callback */
			); !isErrSimilar(regexp.MustCompile("already being monitored"), err) {
				t.Error("expected an error when starting monitor goroutine twice")
			}

			fatal = false
			t0 := c.Now()
			a.Equal(false, fatal)

			// forward jump check should be disabled unless set to true. This should
			// not fatal even though it is a large jump
			m.Increment(int64(test.maxOffset))
			fatal = false
			t1 := c.Now()
			a.True(t0.Less(t1), fmt.Sprintf("expected %+v < %+v", t0, t1))
			a.Equal(false, fatal)

			forwardJumpCheckEnabledCh <- true
			<-tickProcessedCh

			m.Increment(int64(test.actualJump))
			tickerCh <- timeutil.Now()
			<-tickProcessedCh

			fatal = false
			t2 := c.Now()
			a.True(t1.Less(t2), fmt.Sprintf("expected %+v < %+v", t1, t2))
			// This should not fatal as tickerCh has ticked
			a.Equal(false, fatal)
			// After ticker ticks, last physical time should be equal to physical now
			lastPhysicalTime := atomic.LoadInt64(&c.lastPhysicalTime)
			physicalNow := c.PhysicalNow()
			a.Equal(lastPhysicalTime, physicalNow)

			// Potentially a fatal jump depending on the test case
			fatal = false
			m.Increment(int64(test.actualJump))
			t3 := c.Now()
			a.True(t2.Less(t3), fmt.Sprintf("expected %+v < %+v", t2, t3))
			a.Equal(test.isFatal, fatal)

			a.True(
				tickerDuration <= test.maxOffset,
				fmt.Sprintf(
					"ticker duration %+v should be less than max jump %+v",
					tickerDuration,
					test.maxOffset,
				),
			)

			// A jump by maxOffset is surely fatal
			fatal = false
			m.Increment(int64(test.maxOffset))
			t4 := c.Now()
			a.True(t3.Less(t4), fmt.Sprintf("expected %+v < %+v", t3, t4))
			a.Equal(true, fatal)

			// disable forward jump check
			forwardJumpCheckEnabledCh <- false
			<-tickProcessedCh
			fatal = false
			m.Increment(int64(test.actualJump))
			t5 := c.Now()
			a.True(t4.Less(t5), fmt.Sprintf("expected %+v < %+v", t4, t5))
			a.Equal(false, fatal)
		})
	}
}

// TestHLCClock performs a complete test of all basic phenomena,
// including backward jumps in local physical time and clock offset.
func TestHLCClock(t *testing.T) {
	m := NewManualClock(1)
	c := NewClock(m.UnixNano, 1000*time.Nanosecond)
	expectedHistory := []struct {
		// The physical time that this event should take place at.
		wallClock int64
		event     Event
		// If this is a receive event, this holds the "input" timestamp.
		input *ClockTimestamp
		// The expected timestamp generated from the input.
		expected ClockTimestamp
	}{
		// A few valid steps to warm up.
		{5, SEND, nil, ClockTimestamp{WallTime: 5, Logical: 0}},
		{6, SEND, nil, ClockTimestamp{WallTime: 6, Logical: 0}},
		{10, RECV, &ClockTimestamp{WallTime: 10, Logical: 5}, ClockTimestamp{WallTime: 10, Logical: 6}},
		// Our clock mysteriously jumps back.
		{7, SEND, nil, ClockTimestamp{WallTime: 10, Logical: 7}},
		// Wall clocks coincide, but the local logical clock wins.
		{8, RECV, &ClockTimestamp{WallTime: 10, Logical: 4}, ClockTimestamp{WallTime: 10, Logical: 9}},
		// Wall clocks coincide, but the remote logical clock wins.
		{10, RECV, &ClockTimestamp{WallTime: 10, Logical: 99}, ClockTimestamp{WallTime: 10, Logical: 100}},
		// The physical clock has caught up and takes over.
		{11, RECV, &ClockTimestamp{WallTime: 10, Logical: 31}, ClockTimestamp{WallTime: 11, Logical: 1}},
		{11, SEND, nil, ClockTimestamp{WallTime: 11, Logical: 2}},
	}

	var current ClockTimestamp
	for i, step := range expectedHistory {
		m.Set(step.wallClock)
		switch step.event {
		case SEND:
			current = c.NowAsClockTimestamp()
		case RECV:
			fallthrough
		default:
			previous := c.NowAsClockTimestamp()
			c.Update(*step.input)
			current = c.NowAsClockTimestamp()
			if current == previous {
				t.Errorf("%d: clock not updated", i)
			}
		}
		if current != step.expected {
			t.Fatalf("HLC error: %d expected %v, got %v", i, step.expected, current)
		}
	}
}

// TestExampleManualClock shows how a manual clock can be
// used as a physical clock. This is useful for testing.
func TestExampleManualClock(t *testing.T) {
	m := NewManualClock(10)
	c := NewClock(m.UnixNano, time.Nanosecond)
	if wallNanos := c.Now().WallTime; wallNanos != 10 {
		t.Fatalf("unexpected wall time: %d", wallNanos)
	}
	m.Increment(10)
	if wallNanos := c.Now().WallTime; wallNanos != 20 {
		t.Fatalf("unexpected wall time: %d", wallNanos)
	}
}

// TestHybridManualClock test the basic functionality of the
// TestHybridManualClock.
func TestHybridManualClock(t *testing.T) {
	m := NewHybridManualClock()
	c := NewClock(m.UnixNano, time.Nanosecond)

	// We do a two sided test to make sure that the physical clock matches
	// the hybrid value. Since we cant pull a value off both clocks at the same
	// time, we use two LessOrEqual comparisons with reverse order, to establish
	// that the values are roughly equal.
	require.LessOrEqual(t, c.Now().WallTime, UnixNano())
	require.LessOrEqual(t, UnixNano(), c.Now().WallTime)

	m.Increment(10)
	require.LessOrEqual(t, c.Now().WallTime, UnixNano()+10)
	require.LessOrEqual(t, UnixNano()+10, c.Now().WallTime)
}

// TestHybridManualClockPause test the Pause() functionality of the
// HybridManualClock.
func TestHybridManualClockPause(t *testing.T) {
	m := NewHybridManualClock()
	c := NewClock(m.UnixNano, time.Nanosecond)
	now := c.Now().WallTime
	time.Sleep(10 * time.Millisecond)
	require.Less(t, now, c.Now().WallTime)
	m.Pause()
	now = c.Now().WallTime
	require.Equal(t, now, c.Now().WallTime)
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, now, c.Now().WallTime)
	m.Increment(10)
	require.Equal(t, now+10, c.Now().WallTime)
}

func TestHLCMonotonicityCheck(t *testing.T) {
	m := NewManualClock(100000)
	c := NewClock(m.UnixNano, 100*time.Nanosecond)

	// Update the state of the hybrid clock.
	firstTime := c.Now()
	m.Increment((-110 * time.Nanosecond).Nanoseconds())
	secondTime := c.Now()

	{
		errCount := atomic.LoadInt32(&c.monotonicityErrorsCount)

		if errCount != 1 {
			t.Fatalf("clock backward jump was not detected by the monotonicity checker (from %s to %s)", firstTime, secondTime)
		}
	}

	m.Increment((-10 * time.Nanosecond).Nanoseconds())
	thirdTime := c.Now()

	{
		errCount := atomic.LoadInt32(&c.monotonicityErrorsCount)

		if errCount != 1 {
			t.Fatalf("clock backward jump below threshold was incorrectly detected by the monotonicity checker (from %s to %s)", secondTime, thirdTime)
		}
	}
}

func TestHLCEnforceWallTimeWithinBoundsInNow(t *testing.T) {
	var fatal bool
	defer log.ResetExitFunc()
	log.SetExitFunc(true /* hideStack */, func(r exit.Code) {
		defer log.Flush()
		if r == exit.FatalError() {
			fatal = true
		}
	})

	testCases := []struct {
		name               string
		physicalTime       int64
		wallTimeUpperBound int64
		isFatal            bool
	}{
		{
			name:               "physical time > upper bound",
			physicalTime:       1000,
			wallTimeUpperBound: 100,
			isFatal:            true,
		},
		{
			name:               "physical time < upper bound",
			physicalTime:       1000,
			wallTimeUpperBound: 1010,
			isFatal:            false,
		},
		{
			name:               "physical time = upper bound",
			physicalTime:       1000,
			wallTimeUpperBound: 1000,
			isFatal:            false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			a := assert.New(t)
			m := NewManualClock(test.physicalTime)
			c := NewClock(m.UnixNano, time.Nanosecond)
			c.mu.wallTimeUpperBound = test.wallTimeUpperBound
			fatal = false
			c.Now()
			a.Equal(test.isFatal, fatal)
		})
	}
}

func TestHLCEnforceWallTimeWithinBoundsInUpdate(t *testing.T) {
	var fatal bool
	defer log.ResetExitFunc()
	log.SetExitFunc(true /* hideStack */, func(r exit.Code) {
		defer log.Flush()
		if r == exit.FatalError() {
			fatal = true
		}
	})

	testCases := []struct {
		name               string
		messageWallTime    int64
		wallTimeUpperBound int64
		isFatal            bool
	}{
		{
			name:               "message time > upper bound",
			messageWallTime:    1000,
			wallTimeUpperBound: 100,
			isFatal:            true,
		},
		{
			name:               "message time < upper bound",
			messageWallTime:    1000,
			wallTimeUpperBound: 1010,
			isFatal:            false,
		},
		{
			name:               "message time = upper bound",
			messageWallTime:    1000,
			wallTimeUpperBound: 1000,
			isFatal:            false,
		},
	}

	ctx := context.Background()
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			a := assert.New(t)
			m := NewManualClock(test.messageWallTime)
			c := NewClock(m.UnixNano, time.Nanosecond)
			c.mu.wallTimeUpperBound = test.wallTimeUpperBound
			fatal = false
			err := c.UpdateAndCheckMaxOffset(ctx, ClockTimestamp{WallTime: test.messageWallTime})
			a.Nil(err)
			a.Equal(test.isFatal, fatal)
		})
	}
}

// Ensure that an appropriately structured error is returned when trying to
// update a clock using a timestamp too far in the future.
func TestClock_UpdateAndCheckMaxOffset_UntrustworthyValue(t *testing.T) {
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	m := NewManualClock(t0.UnixNano())
	c := NewClock(m.UnixNano, 500*time.Millisecond)
	require.NoError(t, c.UpdateAndCheckMaxOffset(context.Background(), ClockTimestamp{
		WallTime: t0.Add(499 * time.Millisecond).UnixNano(),
	}))
	err := c.UpdateAndCheckMaxOffset(context.Background(), ClockTimestamp{
		WallTime: t0.Add(time.Second).UnixNano(),
	})
	require.True(t, IsUntrustworthyRemoteWallTimeError(err), err)

	// Test that the error properly round-trips through protobuf encoding.
	t.Run("encoding", func(t *testing.T) {
		err := errors.Wrapf(err, "wrapping")
		encoded := errors.EncodeError(context.Background(), err)
		marshaled, err := protoutil.Marshal(&encoded)
		require.NoError(t, err)
		var unmarshaled errors.EncodedError
		require.NoError(t, protoutil.Unmarshal(marshaled, &unmarshaled))
		decoded := errors.DecodeError(context.Background(), unmarshaled)
		require.True(t, IsUntrustworthyRemoteWallTimeError(decoded), decoded)
	})
}

func TestResetAndRefreshHLCUpperBound(t *testing.T) {
	testCases := []struct {
		name        string
		delta       int64
		persistErr  error
		expectedErr *regexp.Regexp
	}{
		{
			name:  "positive delta",
			delta: 100,
		},
		{
			name:        "negative delta",
			delta:       -100,
			expectedErr: regexp.MustCompile("HLC upper bound delta -100 should be positive"),
		},
		{
			name:        "persist error",
			delta:       100,
			persistErr:  errors.New("test error"),
			expectedErr: regexp.MustCompile("test error"),
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			a := assert.New(t)
			var persistedUpperBound int64

			persistFn := func(d int64) error {
				if test.persistErr != nil {
					return test.persistErr
				}
				persistedUpperBound = d
				return nil
			}
			m := NewManualClock(1)
			c := NewClock(m.UnixNano, time.Nanosecond)
			// Test Refresh Upper Bound
			err := c.RefreshHLCUpperBound(persistFn, test.delta)
			a.True(
				isErrSimilar(test.expectedErr, err),
				fmt.Sprintf(
					"expected err %v not equal to actual err %v",
					test.persistErr,
					err,
				),
			)
			if err == nil {
				a.Equal(c.Now().WallTime+test.delta, persistedUpperBound)
			}

			// Test Reset Upper Bound
			err = c.ResetHLCUpperBound(persistFn)
			a.True(
				errors.Is(test.persistErr, err),
				fmt.Sprintf(
					"expected err %v not equal to actual err %v",
					test.persistErr,
					err,
				),
			)
			if err == nil {
				a.Equal(int64(0), persistedUpperBound)
			}
		})
	}
}

func TestLateStartForwardClockJump(t *testing.T) {
	// Regression test for https://github.com/cockroachdb/cockroach/issues/28367
	//
	// Previously, if the clock offset monitor were started a long time
	// after the last call to hlc.Clock.Now, that time would register as
	// a forward clock jump (because the background goroutine to keep
	// the HLC clock fresh was not yet running).
	m := NewManualClock(1)
	c := NewClock(m.UnixNano, 500*time.Millisecond)
	c.Now()
	m.Increment(int64(time.Second))

	// Control channels for the clock monitor: active it immediately,
	// then wait for the first tick. We use a real ticker because the
	// interfaces involved are not very mock-friendly.
	activeCh := make(chan bool, 1)
	activeCh <- true
	tickedCh := make(chan struct{}, 1)
	ticked := func() {
		tickedCh <- struct{}{}
	}
	ctx := context.Background()
	if err := c.StartMonitoringForwardClockJumps(ctx, activeCh, time.NewTicker, ticked); err != nil {
		t.Fatal(err)
	}
	<-tickedCh
	c.Now()
}

func TestSleepUntil(t *testing.T) {
	m := NewManualClock(100000)
	c := NewClock(m.UnixNano, 0)

	before := c.Now()
	waitDur := int64(1000)
	waitUntil := before.Add(waitDur, 0)

	doneC := make(chan struct{}, 1)
	go func() {
		_ = c.SleepUntil(context.Background(), waitUntil)
		doneC <- struct{}{}
	}()

	step := waitDur / 25
	for waitLeft := waitDur; waitLeft > 0; waitLeft -= step {
		require.Empty(t, doneC)

		m.Increment(step)
		time.Sleep(1 * time.Millisecond)
	}
	<-doneC
}

func TestSleepUntilContextCancellation(t *testing.T) {
	m := NewManualClock(100000)
	c := NewClock(m.UnixNano, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	waitUntil := c.Now().Add(100000, 0)

	err := c.SleepUntil(ctx, waitUntil)
	require.Equal(t, context.DeadlineExceeded, err)
}

func BenchmarkUpdate(b *testing.B) {
	b.StopTimer()

	concurrency := 32    // number of concurrent updaters
	updates := int(10e6) // total number of updates to perform
	advanceChance := 0.2 // chance for each worker to advance time
	advanceMax := 5      // max amount to advance time by per update

	// We pre-generate random timestamps for each worker, to avoid it skewing
	// the benchmark.
	//
	// This benchmark may not be entirely realistic, since each worker advances
	// its own clock independent of other workers, which probably leads to
	// having one front-runner. However, synchronizing them while running ends up
	// benchmarking the contention of the benchmark synchronization rather than
	// the HLC.
	r := rand.New(rand.NewSource(34704832098))
	timestamps := make([][]ClockTimestamp, concurrency)
	for w := 0; w < concurrency; w++ {
		timestamps[w] = make([]ClockTimestamp, updates/concurrency)
		wallTime := 0
		for i := 0; i < updates/concurrency; i++ {
			if r.Float64() < advanceChance {
				wallTime += r.Intn(advanceMax + 1)
			}
			timestamps[w][i] = ClockTimestamp{WallTime: int64(wallTime)}
		}
	}

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		clock := NewClock(func() int64 { return 0 }, time.Second)
		wg := sync.WaitGroup{}
		for w := 0; w < concurrency; w++ {
			w := w // make sure we don't close over the loop variable
			wg.Add(1)
			go func() {
				for _, timestamp := range timestamps[w] {
					clock.Update(timestamp)
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
