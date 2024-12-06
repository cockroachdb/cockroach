// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

func getMemoryMonitor(s *cluster.Settings) *mon.BytesMonitor {
	return mon.NewMonitor(mon.Options{
		Name:      mon.MakeMonitorName("rangefeed"),
		Increment: 1,
		Settings:  s,
	})
}

func TestFeedBudget(t *testing.T) {
	makeBudgetWithSize := func(poolSize, budgetSize int64) (
		*FeedBudget, *mon.BytesMonitor, *mon.BoundAccount,
	) {
		s := cluster.MakeTestingClusterSettings()
		m := getMemoryMonitor(s)
		m.Start(context.Background(), nil, mon.NewStandaloneBudget(poolSize))
		b := m.MakeBoundAccount()

		f := NewFeedBudget(&b, budgetSize, &s.SV)
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

func budgetLowThresholdFn(minSize int64) func(int64) int64 {
	return func(size int64) int64 {
		if minSize > size {
			return minSize
		}
		return size
	}
}

func TestBudgetFactory(t *testing.T) {
	s := cluster.MakeTestingClusterSettings()

	rootMon := getMemoryMonitor(s)
	rootMon.Start(context.Background(), nil, mon.NewStandaloneBudget(10000000))
	bf := NewBudgetFactory(context.Background(),
		CreateBudgetFactoryConfig(rootMon, 10000, time.Second*5, budgetLowThresholdFn(10000), &s.SV))

	// Verify system ranges use own budget.
	bSys := bf.CreateBudget(true)
	_, e := bSys.TryGet(context.Background(), 199)
	require.NoError(t, e, "failed to obtain system range budget")
	require.Equal(t, int64(0), rootMon.AllocBytes(), "System feeds should borrow from own budget")
	require.Equal(t, int64(199), bf.Metrics().SystemBytesCount.Value(), "Metric was not updated")

	// Verify user feeds use shared root budget.
	bUsr := bf.CreateBudget(false)
	_, e = bUsr.TryGet(context.Background(), 99)
	require.NoError(t, e, "failed to obtain non-system budget")
	require.Equal(t, int64(99), rootMon.AllocBytes(),
		"Non-system feeds should borrow from shared budget")
	require.Equal(t, int64(99), bf.Metrics().SharedBytesCount.Value(), "Metric was not updated")
}

func TestDisableBudget(t *testing.T) {
	s := cluster.MakeTestingClusterSettings()

	rootMon := getMemoryMonitor(s)
	rootMon.Start(context.Background(), nil, mon.NewStandaloneBudget(10000000))
	bf := NewBudgetFactory(context.Background(),
		CreateBudgetFactoryConfig(rootMon, 10000, time.Second*5, func(_ int64) int64 {
			return 0
		}, &s.SV))

	bUsr := bf.CreateBudget(false)
	require.Nil(t, bUsr, "Range budget when budgets are disabled.")
}

func TestDisableBudgetOnTheFly(t *testing.T) {
	s := cluster.MakeTestingClusterSettings()

	m := getMemoryMonitor(s)
	m.Start(context.Background(), nil, mon.NewStandaloneBudget(100000))
	bf := NewBudgetFactory(context.Background(),
		CreateBudgetFactoryConfig(
			m,
			10000000,
			time.Second*5,
			func(l int64) int64 {
				return l
			},
			&s.SV))

	f := bf.CreateBudget(false)

	objectSize := int64(1000)
	alloc, err := f.TryGet(context.Background(), objectSize)
	require.NoError(t, err)
	require.NotNil(t, alloc, "can't get budget")
	// Disable budget using settings and verify that budget will stop creating new
	// allocations.
	RangefeedBudgetsEnabled.Override(context.Background(), &s.SV, false)
	alloc2, err := f.TryGet(context.Background(), 1000)
	require.NoError(t, err)
	require.Nil(t, alloc2, "budget was not disabled")

	// Release should not crash or cause any anomalies after budget is disabled.
	alloc.Release(context.Background())
	// When budget is released it keeps as much as budget increment amount
	// allocated for caching purposes which we can't release until the factory
	// is destroyed, but we can check that it is less than object size (because
	// allocation increment is low).
	require.Less(t, bf.Metrics().SharedBytesCount.Value(), objectSize,
		"budget was not released")
}

func TestConfigFactory(t *testing.T) {
	s := cluster.MakeTestingClusterSettings()
	rootMon := getMemoryMonitor(s)
	rootMon.Start(context.Background(), nil, mon.NewStandaloneBudget(10000000))

	// Check provisionalFeedLimit is computed.
	config := CreateBudgetFactoryConfig(rootMon, 100000, time.Second*5, budgetLowThresholdFn(10000),
		&s.SV)
	require.Less(t, config.provisionalFeedLimit, int64(100000),
		"provisional range limit should be lower than whole memory pool")
	require.NotZerof(t, config.provisionalFeedLimit, "provisional range feed limit must not be zero")

	// Check if global disable switch works.
	useBudgets = false
	defer func() { useBudgets = true }()
	config = CreateBudgetFactoryConfig(rootMon, 100000, time.Second*5, budgetLowThresholdFn(10000),
		&s.SV)
	require.True(t, config.empty(), "config not empty despite disabled factory")
}

func TestBudgetLimits(t *testing.T) {
	s := cluster.MakeTestingClusterSettings()
	rootMon := getMemoryMonitor(s)
	rootMon.Start(context.Background(), nil, mon.NewStandaloneBudget(10000000))

	provisionalSize := int64(10000)
	adjustedSize := int64(1000)

	bf := NewBudgetFactory(context.Background(), BudgetFactoryConfig{
		rootMon:              rootMon,
		provisionalFeedLimit: provisionalSize,
		adjustLimit: func(size int64) int64 {
			require.Equal(t, provisionalSize, size)
			return adjustedSize
		},
		totalRangeFeedBudget:    100000,
		histogramWindowInterval: time.Second * 5,
		settings:                &s.SV,
	})

	b := bf.CreateBudget(false)
	require.NotNil(t, b, "budget is disabled")
	require.Equal(t, b.limit, adjustedSize, "budget limit is not adjusted")

	// Verify that zero limit is disabling feed budget for range.
	bf = NewBudgetFactory(context.Background(), BudgetFactoryConfig{
		rootMon:              rootMon,
		provisionalFeedLimit: provisionalSize,
		adjustLimit: func(int64) int64 {
			return 0
		},
		totalRangeFeedBudget:    100000,
		histogramWindowInterval: time.Second * 5,
		settings:                &s.SV,
	})
	b = bf.CreateBudget(false)
	require.Nil(t, b, "budget is disabled")
}
