// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	ensureDontSend := func(t *testing.T, timers ...timeutil.TimerI) {
		for _, timer := range timers {
			select {
			case <-timer.Ch():
				t.Fatalf("expected not to receive")
			case <-time.After(time.Nanosecond):
			}
		}
	}
	ensureSends := func(t *testing.T, timer timeutil.TimerI) {
		select {
		case <-timer.Ch():
		default:
			t.Fatalf("expected to receive from tm0")
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
		ensureSends(t, timers[0])
		require.Len(t, timers[1:], len(mt.Timers()))
		ensureDontSend(t, timers[1:]...)

		mt.AdvanceTo(t1.Add(-time.Millisecond))
		ensureDontSend(t, timers[1:]...)

		// Advance to t1 and ensure it sends.
		mt.Advance(time.Millisecond)
		ensureSends(t, timers[1])

		// Stop timers[3] and ensure it stops successfully.
		require.True(t, timers[3].Stop())

		// Advance past t3 and ensure that timers[2] sends and timers[3:] don't
		// because we stopped 3 and the rest haven't come due just yet.
		mt.AdvanceTo(t3.Add(time.Millisecond))
		ensureSends(t, timers[2])
		ensureDontSend(t, timers[3:]...)
		require.Len(t, timers[4:], len(mt.Timers()))

		// Advance to t5 and ensure that timers[4] and timers[5] send.
		mt.AdvanceTo(t5)
		require.Len(t, timers[6:], len(mt.Timers()))
	})
}
