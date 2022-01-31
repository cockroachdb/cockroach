// Copyright 2022 The Cockroach Authors.
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
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

func TestFeedBudget(t *testing.T) {
	makeBudgetWithSize := func(size int64) (*feedBudget, *mon.BoundAccount) {
		m := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, nil)
		m.Start(context.Background(), nil, mon.MakeStandaloneBudget(size))
		b := m.MakeBoundAccount()

		f := feedBudget{}
		f.init(&b, NewMetrics())
		return &f, &b
	}
	ctx := context.Background()
	stop := make(chan struct{})

	t.Run("allocate one", func(t *testing.T) {
		f, b := makeBudgetWithSize(40)
		// Basic case of getting and returning allocation
		a, _, err := f.Get(ctx, 13, time.Microsecond, stop)
		require.NoError(t, err)
		require.Equal(t, int64(13), b.Used(), "")
		if a.release() {
			f.Return(ctx, a.size)
		}
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	t.Run("allocate multiple", func(t *testing.T) {
		f, b := makeBudgetWithSize(40)
		// Multiple allocations returned out of order.
		// Basic case of getting and returning allocation
		a1, _, err := f.Get(ctx, 13, time.Microsecond, stop)
		require.NoError(t, err)
		a2, _, err := f.Get(ctx, 20, time.Microsecond, stop)
		require.NoError(t, err)
		require.Equal(t, int64(33), b.Used(), "allocated budget")
		if a2.release() {
			f.Return(ctx, a2.size)
		}
		if a1.release() {
			f.Return(ctx, a1.size)
		}
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	// Wait for allocation to return some budget.
	t.Run("wait for replenish", func(t *testing.T) {
		f, b := makeBudgetWithSize(40)
		// Multiple allocations returned out of order.
		// Basic case of getting and returning allocation
		a1, _, err := f.Get(ctx, 30, time.Microsecond, stop)
		require.NoError(t, err)
		started := make(chan interface{})
		result := make(chan error)
		go func() {
			started <- struct{}{}
			a, _, err := f.Get(ctx, 20, time.Minute, stop)
			if err != nil {
				result <- err
			} else {
				f.Return(ctx, a.size)
				result <- nil
			}
		}()
		<-started
		require.Equal(t, int64(30), b.Used(), "allocated budget")
		f.Return(ctx, a1.size)
		err = <-result
		require.NoError(t, err)
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	// Fail allocation if used all budget
	t.Run("fail over budget", func(t *testing.T) {
		f, b := makeBudgetWithSize(40)
		a1, _, err := f.Get(ctx, 30, time.Microsecond, stop)
		require.NoError(t, err)
		_, _, err = f.Get(ctx, 20, time.Microsecond, stop)
		require.Error(t, err)
		require.Equal(t, int64(30), b.Used(), "allocated budget")
		if a1.release() {
			f.Return(ctx, a1.size)
		}
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	// Timeout allocation if used all budget
	t.Run("timeout over budget", func(t *testing.T) {
		f, b := makeBudgetWithSize(40)
		a1, _, err := f.Get(ctx, 30, time.Microsecond, stop)
		require.NoError(t, err)
		_, _, err = f.Get(ctx, 20, time.Microsecond, stop)
		require.Error(t, err)
		require.Equal(t, int64(30), b.Used(), "allocated budget")
		if a1.release() {
			f.Return(ctx, a1.size)
		}
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	// Get one over budget allocation when no others are present.
	t.Run("pass single oversized", func(t *testing.T) {
		f, b := makeBudgetWithSize(40)
		// Basic case of getting and returning allocation
		a, _, err := f.Get(ctx, 50, time.Microsecond, stop)
		require.NoError(t, err)
		// We don't allocate anything for now
		require.Equal(t, int64(0), b.Used(), "")
		if a.release() {
			f.Return(ctx, a.size)
		}
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	// Fail if oversized message in progress.
	t.Run("fail while oversized", func(t *testing.T) {
		f, b := makeBudgetWithSize(40)
		// Basic case of getting and returning allocation
		a, _, err := f.Get(ctx, 50, time.Microsecond, stop)
		require.NoError(t, err)

		// Allocations should fail now that oversized object is in progress.
		_, _, err = f.Get(ctx, 20, time.Microsecond, stop)
		require.Error(t, err)

		if a.release() {
			f.Return(ctx, a.size)
		}
		require.Equal(t, int64(0), b.Used(), "used budget after free")

		// Now that oversized object is released, proceed as normal.
		_, _, err = f.Get(ctx, 30, time.Microsecond, stop)
		require.NoError(t, err)
		require.Equal(t, int64(30), b.Used(), "used budget after free")
	})

	t.Run("wait with zero timeout", func(t *testing.T) {
		f, b := makeBudgetWithSize(40)
		a1, _, err := f.Get(ctx, 30, time.Microsecond, stop)
		require.NoError(t, err)
		started := make(chan interface{})
		result := make(chan error)
		go func() {
			started <- struct{}{}
			a, _, err := f.Get(ctx, 20, 0, stop)
			if err != nil {
				result <- err
			} else {
				f.Return(ctx, a.size)
				result <- nil
			}
		}()
		<-started
		f.Return(ctx, a1.size)
		err = <-result
		require.NoError(t, err, "waiting for budget with indefinite timeout")
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	t.Run("abort wait on stopper", func(t *testing.T) {
		f, b := makeBudgetWithSize(40)
		stop2 := make(chan struct{})

		a1, _, err := f.Get(ctx, 30, time.Microsecond, stop2)
		require.NoError(t, err)
		started := make(chan interface{})
		result := make(chan error)
		go func() {
			started <- struct{}{}
			a, _, err := f.Get(ctx, 20, 0, stop2)
			if err != nil {
				result <- err
			} else {
				f.Return(ctx, a.size)
				result <- nil
			}
		}()
		<-started
		close(stop2)

		err = <-result
		require.NoError(t, err, "waiting for budget with indefinite timeout")

		f.Return(ctx, a1.size)
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	t.Run("timer returned if wait was initiated", func(t *testing.T) {
		f, b := makeBudgetWithSize(40)
		a1, _, err := f.Get(ctx, 30, time.Microsecond, stop)
		require.NoError(t, err)
		started := make(chan interface{})
		result := make(chan (<-chan time.Time))
		go func() {
			started <- struct{}{}
			a, timer, err := f.Get(ctx, 20, time.Hour, stop)
			if err != nil {
				result <- nil
			} else {
				f.Return(ctx, a.size)
				result <- timer
			}
		}()
		<-started
		// Fire replenish channel twice to ensure that we entered waiting loop.
		// First event will be non-blocking, while second one should block until
		// retry loop runs once.
		f.replenishC <- struct{}{}
		f.replenishC <- struct{}{}
		f.Return(ctx, a1.size)

		timer := <-result
		require.NotNil(t, timer, "waiting for budget with indefinite timeout")

		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})
}
