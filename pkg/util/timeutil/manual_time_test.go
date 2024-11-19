// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestManualTime(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(1 * time.Second)
	t2 := t0.Add(2 * time.Second)
	t3 := t0.Add(3 * time.Second)
	t5 := t0.Add(5 * time.Second)

	t.Run("Advance", func(t *testing.T) {
		mt := timeutil.NewManualTime(t0)
		require.Equal(t, t0, mt.Now())
		mt.Advance(time.Second)
		require.Equal(t, t1, mt.Now())
	})

	t.Run("Backwards", func(t *testing.T) {
		mt := timeutil.NewManualTime(t1)
		require.Equal(t, t1, mt.Now())
		mt.Backwards(time.Second)
		require.Equal(t, t0, mt.Now())
	})

	t.Run("AdvanceTo", func(t *testing.T) {
		mt := timeutil.NewManualTime(t0)
		mt.AdvanceTo(t2)
		require.Equal(t, t2, mt.Now())
		mt.AdvanceTo(t1)
		require.Equal(t, t2, mt.Now())
	})

	t.Run("Timer.Stop on unset", func(t *testing.T) {
		mt := timeutil.NewManualTime(t0)
		timer := mt.NewTimer()
		require.False(t, timer.Stop())
	})

	ensureNoSend := func(t *testing.T, ch <-chan time.Time) {
		t.Helper()
		select {
		case <-ch:
			t.Fatalf("expected not to receive")
		default:
		}
	}

	// ensureSend verifies that the given channel can receive immediately and that
	// the value equals the given time (relative to t0).
	ensureSend := func(t *testing.T, ch <-chan time.Time, expected time.Duration) {
		t.Helper()
		select {
		case tm := <-ch:
			if exp := t0.Add(expected); tm != exp {
				t.Errorf("expected to receive %s, got %s", exp, tm)
			}
		default:
			t.Fatalf("expected to receive")
		}
	}

	t.Run("Timer basics", func(t *testing.T) {
		mt := timeutil.NewManualTime(t0)
		mkTimer := func(d time.Duration) timeutil.TimerI {
			timer := mt.NewTimer()
			timer.Reset(d)
			return timer
		}
		timers := []timeutil.TimerI{
			mkTimer(0 * time.Second),
			mkTimer(1 * time.Second),
			mkTimer(2 * time.Second),
			mkTimer(3 * time.Second),
			mkTimer(4 * time.Second),
			mkTimer(5 * time.Second),
		}
		ensureNoSendForTimers := func(t *testing.T, timers []timeutil.TimerI) {
			t.Helper()
			for _, timer := range timers {
				ensureNoSend(t, timer.Ch())
			}
		}
		ensureSend(t, timers[0].Ch(), 0)
		require.Len(t, timers[1:], len(mt.Timers()))
		ensureNoSendForTimers(t, timers[1:])

		mt.AdvanceTo(t1.Add(-time.Millisecond))
		ensureNoSendForTimers(t, timers[1:])

		// Advance to t1 and ensure it sends.
		mt.Advance(time.Millisecond)
		ensureSend(t, timers[1].Ch(), 1*time.Second)

		// Stop timers[3] and ensure it stops successfully.
		require.True(t, timers[3].Stop())

		// Advance past t3 and ensure that timers[2] sends and timers[3:] don't
		// because we stopped 3 and the rest haven't come due just yet.
		mt.AdvanceTo(t3.Add(time.Millisecond))
		ensureSend(t, timers[2].Ch(), 2*time.Second)
		ensureNoSendForTimers(t, timers[3:])
		require.Len(t, timers[4:], len(mt.Timers()))

		// Advance to t5 and ensure that timers[4] and timers[5] send.
		mt.AdvanceTo(t5)
		require.Len(t, timers[6:], len(mt.Timers()))
	})

	t.Run("Ticker basics", func(t *testing.T) {
		mt := timeutil.NewManualTime(t0)
		advanceTo := func(d time.Duration) {
			mt.AdvanceTo(t0.Add(d))
		}
		t1 := mt.NewTicker(1 * time.Second)
		t2 := mt.NewTicker(5 * time.Second)

		advanceTo(100 * time.Millisecond)
		ensureNoSend(t, t1.Ch())
		ensureNoSend(t, t2.Ch())

		advanceTo(1 * time.Second)
		ensureSend(t, t1.Ch(), 1*time.Second)
		ensureNoSend(t, t2.Ch())

		advanceTo(2*time.Second + time.Millisecond)
		ensureSend(t, t1.Ch(), 2*time.Second)
		ensureNoSend(t, t2.Ch())

		advanceTo(6 * time.Second)

		ensureSend(t, t1.Ch(), 3*time.Second)
		ensureSend(t, t1.Ch(), 4*time.Second)
		ensureSend(t, t1.Ch(), 5*time.Second)
		ensureSend(t, t1.Ch(), 6*time.Second)

		ensureSend(t, t2.Ch(), 5*time.Second)

		t1.Stop()
		advanceTo(12 * time.Second)
		ensureNoSend(t, t1.Ch())
		ensureSend(t, t2.Ch(), 10*time.Second)

		t2.Stop()
		advanceTo(100 * time.Second)
		ensureNoSend(t, t1.Ch())
		ensureNoSend(t, t2.Ch())
	})
}
