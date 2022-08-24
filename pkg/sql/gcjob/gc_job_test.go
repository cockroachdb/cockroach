// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcjob

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWaitForWork tests that the WaitForWork function properly responds to
// events and properly calls the MarkIdle callback.
func TestWaitForWork(t *testing.T) {
	t0 := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	waitForTimers := func(t *testing.T, ts *timeutil.ManualTime, exp int) {
		testutils.SucceedsSoon(t, func() error {
			if timers := ts.Timers(); len(timers) != exp {
				return errors.Errorf("expected %d timers, found %d (%v)", exp, len(timers), timers)
			}
			return nil
		})
	}
	waitForCalls := func(t *testing.T, b *setIdleCalls, exp ...bool) {
		testutils.SucceedsSoon(t, func() error {
			if calls := b.calls(); !assert.Equal(noopT{}, exp, calls) {
				return errors.Errorf("expected %v, found %v", exp, calls)
			}
			return nil
		})
	}
	t.Run("idle not called immediately", func(t *testing.T) {
		ts := timeutil.NewManualTime(t0)
		var b setIdleCalls
		ctx := context.Background()
		require.NoError(t, waitForWork(ctx, b.set, ts, 0, time.Second, nil))
		require.Equal(t, []bool{}, b.calls())
	})
	t.Run("idle not called if timer triggerred first", func(t *testing.T) {
		ts := timeutil.NewManualTime(t0)
		var b setIdleCalls
		ctx := context.Background()
		errCh := make(chan error)
		go func() {
			errCh <- waitForWork(ctx, b.set, ts, time.Millisecond, time.Second, nil)
		}()
		waitForTimers(t, ts, 2)
		ts.Advance(time.Millisecond)
		require.NoError(t, <-errCh)
		require.Equal(t, []bool{}, b.calls())
	})
	t.Run("idle called", func(t *testing.T) {
		ts := timeutil.NewManualTime(t0)
		var b setIdleCalls
		ctx := context.Background()
		errCh := make(chan error, 1)
		go func() {
			errCh <- waitForWork(ctx, b.set, ts, 2*time.Second, time.Second, nil)
		}()
		waitForTimers(t, ts, 2)
		ts.Advance(time.Second)
		waitForCalls(t, &b, true)
		require.Len(t, errCh, 0)
		ts.Advance(time.Second)
		require.NoError(t, <-errCh)
		require.Equal(t, []bool{true, false}, b.calls())
	})
	t.Run("cancellation leads to error", func(t *testing.T) {
		ts := timeutil.NewManualTime(t0)
		var b setIdleCalls
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- waitForWork(ctx, b.set, ts, 2*time.Second, time.Second, nil)
		}()
		waitForTimers(t, ts, 2)
		ts.Advance(time.Second)
		waitForCalls(t, &b, true)
		cancel()
		require.Equal(t, context.Canceled, <-errCh)
		require.Equal(t, []bool{true, false}, b.calls())
	})
	t.Run("gossip channel works", func(t *testing.T) {
		ts := timeutil.NewManualTime(t0)
		var b setIdleCalls
		ctx := context.Background()
		errCh := make(chan error, 1)
		gossipC := make(chan struct{})
		go func() {
			errCh <- waitForWork(ctx, b.set, ts, 2*time.Second, time.Second, gossipC)
		}()
		waitForTimers(t, ts, 2)
		ts.Advance(time.Second)
		waitForCalls(t, &b, true)
		gossipC <- struct{}{}
		require.NoError(t, <-errCh)
		require.Equal(t, []bool{true, false}, b.calls())
	})
}

type setIdleCalls struct {
	syncutil.Mutex
	v []bool
}

func (s *setIdleCalls) set(v bool) {
	s.Lock()
	defer s.Unlock()
	s.v = append(s.v, v)
}

func (s *setIdleCalls) calls() []bool {
	s.Lock()
	defer s.Unlock()
	return append([]bool{}, s.v...)
}

type noopT struct{}

func (n noopT) Errorf(format string, args ...interface{}) {}
func (n noopT) FailNow()                                  {}

var _ require.TestingT = noopT{}
