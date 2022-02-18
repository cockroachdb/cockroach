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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

func TestFeedBudget(t *testing.T) {
	makeBudgetWithSize := func(poolSize, budgetSize int64) (*FeedBudget, *mon.BytesMonitor, *mon.BoundAccount) {
		m := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, nil)
		m.Start(context.Background(), nil, mon.MakeStandaloneBudget(poolSize))
		b := m.MakeBoundAccount()

		f := NewFeedBudget(&b, budgetSize, NewMetrics())
		return f, m, &b
	}
	ctx := context.Background()

	t.Run("allocate one", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		// Basic case of getting and returning allocation
		a, _, err := f.Get(ctx, 13, time.Microsecond)
		require.NoError(t, err)
		require.Equal(t, int64(13), b.Used(), "")
		a.Release(ctx)
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	t.Run("allocate multiple", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		// Multiple allocations returned out of order.
		// Basic case of getting and returning allocation
		a1, _, err := f.Get(ctx, 13, time.Microsecond)
		require.NoError(t, err)
		a2, _, err := f.Get(ctx, 20, time.Microsecond)
		require.NoError(t, err)
		require.Equal(t, int64(33), b.Used(), "allocated budget")
		a2.Release(ctx)
		a1.Release(ctx)
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	// Wait for allocation to return some budget.
	t.Run("wait for replenish", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		// Multiple allocations returned out of order.
		// Basic case of getting and returning allocation
		a1, _, err := f.Get(ctx, 30, time.Microsecond)
		require.NoError(t, err)
		started := make(chan interface{})
		result := make(chan error)
		go func() {
			started <- struct{}{}
			a, _, err := f.Get(ctx, 20, time.Minute)
			if err != nil {
				result <- err
			} else {
				f.returnAllocation(ctx, a.size)
				result <- nil
			}
		}()
		<-started
		require.Equal(t, int64(30), b.Used(), "allocated budget")
		f.returnAllocation(ctx, a1.size)
		err = <-result
		require.NoError(t, err)
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	// Fail allocation if used all budget
	t.Run("fail over budget", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		a1, _, err := f.Get(ctx, 30, time.Microsecond)
		require.NoError(t, err)
		_, _, err = f.Get(ctx, 20, time.Microsecond)
		require.Error(t, err)
		require.Equal(t, int64(30), b.Used(), "allocated budget")
		a1.Release(ctx)
		require.Equal(t, int64(0), b.Used(), "used budget after free")
		require.Equal(t, int64(1), f.metrics.RangeFeedBudgetExhausted.Count())
	})

	// Timeout allocation if used all budget
	t.Run("timeout over budget", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		a1, _, err := f.Get(ctx, 30, time.Microsecond)
		require.NoError(t, err)
		_, _, err = f.Get(ctx, 20, time.Microsecond)
		require.Error(t, err)
		require.Equal(t, int64(30), b.Used(), "allocated budget")
		a1.Release(ctx)
		require.Equal(t, int64(0), b.Used(), "used budget after free")
		require.Equal(t, int64(1), f.metrics.RangeFeedBudgetExhausted.Count())
	})

	// Get one over budget allocation when no others are present.
	t.Run("pass single oversized", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		// Basic case of getting and returning allocation
		a, _, err := f.Get(ctx, 50, time.Microsecond)
		require.NoError(t, err)
		// We don't allocate anything for now
		require.Equal(t, int64(0), b.Used(), "")
		require.Equal(t, int64(50), f.metrics.RangeFeedOverBudgetAllocation.Value())
		a.Release(ctx)
		require.Equal(t, int64(0), b.Used(), "used budget after free")
		require.Equal(t, int64(1), f.metrics.RangeFeedOverBudgetEvents.Count())
	})

	// Fail if oversized message in progress.
	t.Run("fail while oversized", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		// Basic case of getting and returning allocation
		a, _, err := f.Get(ctx, 50, time.Microsecond)
		require.NoError(t, err)

		// Allocations should fail now that oversized object is in progress.
		_, _, err = f.Get(ctx, 20, time.Microsecond)
		require.Error(t, err)

		a.Release(ctx)
		require.Equal(t, int64(0), b.Used(), "used budget after free")

		// Now that oversized object is released, proceed as normal.
		_, _, err = f.Get(ctx, 30, time.Microsecond)
		require.NoError(t, err)
		require.Equal(t, int64(30), b.Used(), "used budget after free")
		require.Equal(t, int64(1), f.metrics.RangeFeedBudgetExhausted.Count())
	})

	t.Run("wait with zero timeout", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		a1, _, err := f.Get(ctx, 30, time.Microsecond)
		require.NoError(t, err)
		started := make(chan interface{})
		result := make(chan error)
		go func() {
			started <- struct{}{}
			a, _, err := f.Get(ctx, 20, 0)
			if err != nil {
				result <- err
			} else {
				f.returnAllocation(ctx, a.size)
				result <- nil
			}
		}()
		<-started
		f.returnAllocation(ctx, a1.size)
		err = <-result
		require.NoError(t, err, "waiting for budget with indefinite timeout")
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	t.Run("abort wait on stopper", func(t *testing.T) {
		f, m, _ := makeBudgetWithSize(40, 0)

		a1, _, err := f.Get(ctx, 30, time.Microsecond)
		require.NoError(t, err)
		started := make(chan interface{})
		result := make(chan error)
		go func() {
			started <- struct{}{}
			a, _, err := f.Get(ctx, 20, 0)
			if err != nil {
				result <- err
			} else {
				a.Release(ctx)
				result <- nil
			}
		}()
		<-started
		f.Close(ctx)

		err = <-result
		require.NoError(t, err, "waiting for budget with indefinite timeout")

		f.returnAllocation(ctx, a1.size)
		require.Equal(t, int64(0), m.AllocBytes(), "used budget after free")
	})

	t.Run("timer returned if wait was initiated", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		a1, _, err := f.Get(ctx, 30, time.Microsecond)
		require.NoError(t, err)
		started := make(chan interface{})
		result := make(chan (<-chan time.Time))
		go func() {
			started <- struct{}{}
			a, timer, err := f.Get(ctx, 20, time.Hour)
			if err != nil {
				result <- nil
			} else {
				f.returnAllocation(ctx, a.size)
				result <- timer
			}
		}()
		<-started
		// Fire replenish channel twice to ensure that we entered waiting loop.
		// First event will be non-blocking, while second one should block until
		// retry loop runs once.
		f.replenishC <- struct{}{}
		f.replenishC <- struct{}{}
		f.returnAllocation(ctx, a1.size)

		timer := <-result
		require.NotNil(t, timer, "waiting for budget with indefinite timeout")

		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	t.Run("fail when reaching hard limit", func(t *testing.T) {
		f, _, _ := makeBudgetWithSize(1000, 30)
		_, _, err := f.Get(ctx, 20, time.Microsecond)
		require.NoError(t, err)
		// Try to send second object that would exceed hard limit and see it if fit.
		_, _, err = f.Get(ctx, 20, time.Microsecond)
		require.Error(t, err)
	})

	t.Run("send oversized with hard limit", func(t *testing.T) {
		f, _, _ := makeBudgetWithSize(1000, 30)
		a1, _, err := f.Get(ctx, 40, time.Microsecond)
		require.NoError(t, err)
		// Try to send second object while in overfill on hard limit.
		_, _, err = f.Get(ctx, 10, time.Microsecond)
		require.Error(t, err)
		a1.Release(ctx)
	})
}

func TestBudgetFactory(t *testing.T) {
	rootMon := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, nil)
	rootMon.Start(context.Background(), nil, mon.MakeStandaloneBudget(10000000))
	bf := NewBudgetFactory(context.Background(), rootMon, 10000, time.Second * 5)

	// Verify system ranges use own budget.
	bSys := bf.CreateBudget(keys.MustAddr(keys.Meta1Prefix), NewMetrics())
	_, _, e := bSys.Get(context.Background(), 199, time.Millisecond)
	require.NoError(t, e, "failed to obtain system range budget")
	require.Equal(t, int64(0), rootMon.AllocBytes(), "System feeds should borrow from own budget")
	require.Equal(t, int64(199), bf.Metrics().SystemCurBytesCount.Value(), "Metric was not updated")

	// Verify user feeds use shared root budget.
	bUsr := bf.CreateBudget(keys.MustAddr(keys.TableDataMin), NewMetrics())
	_, _, e = bUsr.Get(context.Background(), 99, time.Millisecond)
	require.NoError(t, e, "failed to obtain non-system budget")
	require.Equal(t, int64(99), rootMon.AllocBytes(), "Non-system feeds should borrow from shared budget")
	require.Equal(t, int64(99), bf.Metrics().SharedCurBytesCount.Value(), "Metric was not updated")
}
