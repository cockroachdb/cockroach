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

		f := NewFeedBudget(&b, budgetSize)
		return f, m, &b
	}
	ctx := context.Background()

	t.Run("allocate one", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		// Basic case of getting and returning allocation
		a, err := f.TryGet(ctx, 13)
		require.NoError(t, err)
		require.Equal(t, int64(13), b.Used(), "")
		a.Release(ctx)
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	t.Run("allocate multiple", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		// Multiple allocations returned out of order.
		// Basic case of getting and returning allocation
		a1, err := f.TryGet(ctx, 13)
		require.NoError(t, err)
		a2, err := f.TryGet(ctx, 20)
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
		a1, err := f.TryGet(ctx, 30)
		require.NoError(t, err)
		started := make(chan interface{})
		result := make(chan error)
		go func() {
			started <- struct{}{}
			ctx2, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()
			a, err := f.WaitAndGet(ctx2, 20)
			if err != nil {
				result <- err
			} else {
				f.returnAllocation(ctx2, a.size)
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
	t.Run("fail to get over budget", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		_, err := f.TryGet(ctx, 50)
		require.Error(t, err)
		a1, err := f.TryGet(ctx, 30)
		require.NoError(t, err)
		_, err = f.TryGet(ctx, 20)
		require.Error(t, err)
		require.Equal(t, int64(30), b.Used(), "allocated budget")
		a1.Release(ctx)
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	// Timeout allocation if used all budget
	t.Run("fail to wait over budget", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		ctx2, cancel := context.WithTimeout(ctx, time.Microsecond)
		defer cancel()
		a1, err := f.TryGet(ctx2, 30)
		require.NoError(t, err)
		_, err = f.WaitAndGet(ctx2, 20)
		require.Error(t, err)
		require.Equal(t, int64(30), b.Used(), "allocated budget")
		a1.Release(ctx)
		require.Equal(t, int64(0), b.Used(), "used budget after free")
	})

	t.Run("wait with no timeout", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		a1, err := f.TryGet(ctx, 30)
		require.NoError(t, err)
		started := make(chan interface{})
		result := make(chan error)
		go func() {
			started <- struct{}{}
			a, err := f.WaitAndGet(ctx, 20)
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
		a1, err := f.TryGet(ctx, 30)
		require.NoError(t, err)
		started := make(chan interface{})
		result := make(chan error)
		go func() {
			started <- struct{}{}
			a, err := f.WaitAndGet(ctx, 20)
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

	t.Run("fail when reaching hard limit", func(t *testing.T) {
		f, _, _ := makeBudgetWithSize(1000, 30)
		_, err := f.TryGet(ctx, 20)
		require.NoError(t, err)
		// Try to send second object that would exceed hard limit and see it if fit.
		_, err = f.TryGet(ctx, 20)
		require.Error(t, err)
	})
}

func TestBudgetFactory(t *testing.T) {
	rootMon := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, nil)
	rootMon.Start(context.Background(), nil, mon.MakeStandaloneBudget(10000000))
	bf := NewBudgetFactory(context.Background(), rootMon, 10000, time.Second*5)

	// Verify system ranges use own budget.
	bSys := bf.CreateBudget(keys.MustAddr(keys.Meta1Prefix))
	_, e := bSys.TryGet(context.Background(), 199)
	require.NoError(t, e, "failed to obtain system range budget")
	require.Equal(t, int64(0), rootMon.AllocBytes(), "System feeds should borrow from own budget")
	require.Equal(t, int64(199), bf.Metrics().SystemBytesCount.Value(), "Metric was not updated")

	// Verify user feeds use shared root budget.
	bUsr := bf.CreateBudget(keys.MustAddr(keys.SystemSQLCodec.TablePrefix(keys.MaxReservedDescID + 1)))
	_, e = bUsr.TryGet(context.Background(), 99)
	require.NoError(t, e, "failed to obtain non-system budget")
	require.Equal(t, int64(99), rootMon.AllocBytes(), "Non-system feeds should borrow from shared budget")
	require.Equal(t, int64(99), bf.Metrics().SharedBytesCount.Value(), "Metric was not updated")
}
