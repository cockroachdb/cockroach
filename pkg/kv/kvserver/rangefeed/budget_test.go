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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

func TestMemoryAdjuster(t *testing.T) {
	ctx := context.Background()
	t.Run("empty initialization", func(t *testing.T) {
		a := newMemoryAdjuster(1000)
		require.Equal(t, int64(1000), a.allocated())
		require.Equal(t, int64(1000), a.freeUnusedMem())
		require.Equal(t, int64(0), a.used)
	})
	t.Run("track usage", func(t *testing.T) {
		a := newMemoryAdjuster(1000)
		a.increaseUsage(ctx, 200)
		require.Equal(t, int64(1000), a.allocated())
		require.Equal(t, int64(800), a.freeUnusedMem())
		require.Equal(t, int64(200), a.used)
	})
	t.Run("track more than allocated", func(t *testing.T) {
		a := newMemoryAdjuster(1000)
		a.increaseUsage(ctx, 2000)
		require.Equal(t, int64(1000), a.allocated())
		require.Equal(t, int64(1000), a.used)
		require.Equal(t, int64(0), a.unused)
		require.Equal(t, int64(0), a.freeUnusedMem())
		require.Equal(t, int64(1000), a.used)
		require.Equal(t, int64(0), a.unused)
		require.Equal(t, int64(1000), a.allocated())
	})
	t.Run("release but no usage yet", func(t *testing.T) {
		a := newMemoryAdjuster(1000)
		a.decreaseUsage(ctx, 200)
		require.Equal(t, int64(0), a.used) // no effect due to no usage
		require.Equal(t, int64(1000), a.unused)
		require.Equal(t, int64(1000), a.allocated())
		require.Equal(t, int64(1000), a.freeUnusedMem())
		require.Equal(t, int64(0), a.used)
		require.Equal(t, int64(0), a.unused)
		require.Equal(t, int64(0), a.allocated())
	})
	t.Run("release more than used", func(t *testing.T) {
		a := newMemoryAdjuster(1000)
		a.increaseUsage(ctx, 300)
		require.Equal(t, int64(300), a.used)
		require.Equal(t, int64(700), a.unused)
		a.decreaseUsage(ctx, 200)
		require.Equal(t, int64(100), a.used)
		require.Equal(t, int64(900), a.unused)
		require.Equal(t, int64(1000), a.allocated())
		require.Equal(t, int64(900), a.freeUnusedMem())
		require.Equal(t, int64(100), a.allocated())
	})
	t.Run("release less than used", func(t *testing.T) {
		a := newMemoryAdjuster(1000)
		a.increaseUsage(ctx, 300)
		require.Equal(t, int64(300), a.used)
		require.Equal(t, int64(700), a.unused)
		a.decreaseUsage(ctx, 200)
		require.Equal(t, int64(100), a.used)
		require.Equal(t, int64(900), a.unused)
		require.Equal(t, int64(1000), a.allocated())
	})
	t.Run("release more than allocated", func(t *testing.T) {
		a := newMemoryAdjuster(1000)
		a.decreaseUsage(ctx, 2000) // no effect since no usage yet
		require.Equal(t, int64(1000), a.allocated())
		require.Equal(t, int64(0), a.used)
		require.Equal(t, int64(1000), a.unused)
	})
	t.Run("free unused memory", func(t *testing.T) {
		a := newMemoryAdjuster(1000)
		a.increaseUsage(ctx, 200)
		require.Equal(t, int64(1000), a.allocated())
		require.Equal(t, int64(800), a.freeUnusedMem())
		require.Equal(t, int64(200), a.used)
		a.freeUnusedMem()                    // should free 800
		require.Equal(t, int64(0), a.unused) // 0 unused
		require.Equal(t, int64(200), a.used)
		require.Equal(t, int64(200), a.allocated())
		a.freeUnusedMem()
		require.Equal(t, int64(200), a.used)
	})
	t.Run("overuse memory", func(t *testing.T) {
		a := newMemoryAdjuster(1000)
		a.increaseUsage(ctx, 1200)
		require.Equal(t, int64(1000), a.allocated())
		require.Equal(t, int64(0), a.freeUnusedMem())
		require.Equal(t, int64(1000), a.used) // overused but do not overflow used over the budget
		a.freeUnusedMem()
		require.Equal(t, int64(0), a.freeUnusedMem())
		require.Equal(t, int64(1000), a.used)
		require.Equal(t, int64(1000), a.allocated())
	})
	t.Run("concurrent access handling for increaseUsage", func(t *testing.T) {
		a := newMemoryAdjuster(1000)
		wg := sync.WaitGroup{}
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				a.increaseUsage(ctx, 10)
			}()
		}
		wg.Wait()
		require.Equal(t, int64(500), a.used)
	})
	t.Run("concurrent access handling for decreaseUsage", func(t *testing.T) {
		a := newMemoryAdjuster(1000)
		a.increaseUsage(ctx, 50)
		require.Equal(t, int64(50), a.used)
		require.Equal(t, int64(950), a.unused)
		wg := sync.WaitGroup{}
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				a.decreaseUsage(ctx, 1)
			}()
		}
		wg.Wait()
		// decrease usage by 20
		require.Equal(t, int64(30), a.used)    // 50 - 20 = 30
		require.Equal(t, int64(970), a.unused) // 950 + 20 = 970
	})
	t.Run("concurrent access handling for unused", func(t *testing.T) {
		a := newMemoryAdjuster(1000)
		a.increaseUsage(ctx, 200) // 800 unused
		wg := sync.WaitGroup{}
		unusedTotals := make(chan int64, 100)

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				unused := a.freeUnusedMem()
				unusedTotals <- unused
			}()
		}

		wg.Wait()
		close(unusedTotals)

		unusedTotal := int64(0)
		for unused := range unusedTotals {
			unusedTotal += unused
		}
		require.Equal(t, int64(800), unusedTotal)
	})
	t.Run("mixed of increase and decrease", func(t *testing.T) {
		ma := newMemoryAdjuster(10000)
		var wg sync.WaitGroup

		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ma.increaseUsage(ctx, 5)
				ma.decreaseUsage(ctx, 5)
			}()
		}

		wg.Wait()

		require.Equal(t, ma.used, 0)
		require.Equal(t, ma.unused, 10000)
	})
}

func TestFeedBudgetMemoryAdjustment(t *testing.T) {
	makeBudgetWithSize := func(poolSize, budgetSize int64) (
		*FeedBudget, *mon.BytesMonitor, *mon.BoundAccount,
	) {
		m := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, nil)
		m.Start(context.Background(), nil, mon.NewStandaloneBudget(poolSize))
		b := m.MakeBoundAccount()

		s := cluster.MakeTestingClusterSettings()
		f := NewFeedBudget(&b, budgetSize, &s.SV)
		return f, m, &b
	}
	ctx := context.Background()

	t.Run("allocate and adjust memory usage", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(40, 0)
		a, err := f.TryGet(ctx, 13)
		require.NoError(t, err)
		_, err = f.TryGet(ctx, 20)
		require.NoError(t, err)
		require.Equal(t, int64(33), b.Used())
		a.AdjustMemUsage(ctx) // should free 13
		require.Equal(t, int64(20), b.Used())
	})

	t.Run("track usage with event", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(10000, 0)
		ev := event{ops: []enginepb.MVCCLogicalOp{
			{
				WriteValue: &enginepb.MVCCWriteValueOp{
					Key:       roachpb.Key("/db1"),
					Timestamp: hlc.Timestamp{WallTime: 10, Logical: 4},
				},
			},
		}}
		evMemUsage := EventMemUsage(&ev)
		// Check evMemUsage < 1000.
		require.Greater(t, int64(1000), evMemUsage)
		a, err := f.TryGet(ctx, int64(1000))
		require.NoError(t, err)
		a.TrackUsage(ctx, &ev, nil)
		require.Equal(t, int64(1000), b.Used())
		a.AdjustMemUsage(ctx) // free 1000-evMemUsage
		require.Equal(t, evMemUsage, b.Used())
	})

	t.Run("overuse alloc", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(10000, 0)
		ev := event{ops: []enginepb.MVCCLogicalOp{
			{
				WriteValue: &enginepb.MVCCWriteValueOp{
					Key:       roachpb.Key("/db1"),
					Timestamp: hlc.Timestamp{WallTime: 10, Logical: 4},
				},
			},
		}}
		evMemUsage := EventMemUsage(&ev)
		require.Less(t, int64(100), evMemUsage)

		a, err := f.TryGet(ctx, 100)
		require.NoError(t, err)

		a.TrackUsage(ctx, &ev, nil) // overused by evMemUsage
		a.AdjustMemUsage(ctx)       // should free 0
		require.Equal(t, int64(100), b.Used())
	})

	t.Run("track usage with rangefeed event", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(10000, 0)
		ev := event{ops: []enginepb.MVCCLogicalOp{
			{
				WriteValue: &enginepb.MVCCWriteValueOp{
					Key:       roachpb.Key("/db1"),
					Timestamp: hlc.Timestamp{WallTime: 10, Logical: 4},
				},
			},
		}}
		evMemUsage := EventMemUsage(&ev)

		var futureEvent kvpb.RangeFeedEvent
		futureEvent.MustSetValue(&kvpb.RangeFeedValue{
			Key: testKey,
			Value: roachpb.Value{
				Timestamp: testTs,
			},
		})
		futureEventMemUsage := RangefeedEventMemUsage(&futureEvent)
		require.Greater(t, int64(700), evMemUsage+futureEventMemUsage)

		a, err := f.TryGet(ctx, 700)
		require.Equal(t, int64(700), b.Used())
		require.NoError(t, err)

		a.TrackUsage(ctx, &ev, nil)
		require.Equal(t, int64(700), b.Used())
		a.TrackUsage(ctx, nil, &futureEvent)
		require.Equal(t, int64(700), b.Used())
		a.AdjustMemUsage(ctx) // should free 600-evMemUsage-futureEventMemUsage
		require.Equal(t, evMemUsage+futureEventMemUsage, b.Used())
	})
	t.Run("concurrent access handling for TrackUsage", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(math.MaxInt64, 0)
		a, err := f.TryGet(ctx, 1000000)
		require.NoError(t, err)
		ev := event{ops: []enginepb.MVCCLogicalOp{{WriteValue: &enginepb.MVCCWriteValueOp{}}}}
		evMemUsage := EventMemUsage(&ev)

		var futureEvent kvpb.RangeFeedEvent
		futureEvent.MustSetValue(&kvpb.RangeFeedValue{})
		futureEventMemUsage := RangefeedEventMemUsage(&futureEvent)

		wg := sync.WaitGroup{}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				a.TrackUsage(ctx, &ev, nil)
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				a.TrackUsage(ctx, nil, &futureEvent)
			}()
		}

		wg.Wait()
		require.Greater(t, int64(1000000), evMemUsage*100+futureEventMemUsage*100)
		a.AdjustMemUsage(ctx) // should free 10000-evMemUsage*100-futureEventMemUsage*100
		require.Equal(t, evMemUsage*100+futureEventMemUsage*100, b.Used())
	})
	t.Run("concurrent access handling for AdjustMemUsage", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(10000, 0)
		ev := event{ops: []enginepb.MVCCLogicalOp{
			{
				WriteValue: &enginepb.MVCCWriteValueOp{
					Key:       roachpb.Key("/db1"),
					Timestamp: hlc.Timestamp{WallTime: 10, Logical: 4},
				},
			},
		}}
		evMemUsage := EventMemUsage(&ev)
		require.Greater(t, int64(600), evMemUsage)

		a, err := f.TryGet(ctx, 600)
		require.NoError(t, err)

		a.TrackUsage(ctx, &ev, nil)
		wg := sync.WaitGroup{}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				a.AdjustMemUsage(ctx) // should free 600-evMemUsage
			}()
		}
		require.Equal(t, evMemUsage, b.Used())
	})
	t.Run("concurrent access handling for Release", func(t *testing.T) {
		f, _, b := makeBudgetWithSize(10000, 0)
		ev := event{ops: []enginepb.MVCCLogicalOp{
			{
				WriteValue: &enginepb.MVCCWriteValueOp{
					Key:       roachpb.Key("/db1"),
					Timestamp: hlc.Timestamp{WallTime: 10, Logical: 4},
				},
			},
		}}
		evMemUsage := EventMemUsage(&ev)
		require.Greater(t, int64(600), evMemUsage)

		a, err := f.TryGet(ctx, 600)
		require.NoError(t, err)

		a.TrackUsage(ctx, &ev, nil) // overused by evMemUsage
		require.Equal(t, int64(600), b.Used())
		wg := sync.WaitGroup{}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				a.Release(ctx) // should free once
			}()
		}
		require.Equal(t, int64(0), b.Used())
	})
}

func TestFeedBudget(t *testing.T) {
	makeBudgetWithSize := func(poolSize, budgetSize int64) (
		*FeedBudget, *mon.BytesMonitor, *mon.BoundAccount,
	) {
		m := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, nil)
		m.Start(context.Background(), nil, mon.NewStandaloneBudget(poolSize))
		b := m.MakeBoundAccount()

		s := cluster.MakeTestingClusterSettings()
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
				f.returnAllocation(ctx2, a.memoryAdjuster.allocated())
				result <- nil
			}
		}()
		<-started
		require.Equal(t, int64(30), b.Used(), "allocated budget")
		f.returnAllocation(ctx, a1.memoryAdjuster.allocated())
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
				f.returnAllocation(ctx, a.memoryAdjuster.allocated())
				result <- nil
			}
		}()
		<-started
		f.returnAllocation(ctx, a1.memoryAdjuster.allocated())
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

		f.returnAllocation(ctx, a1.memoryAdjuster.allocated())
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

	rootMon := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, s)
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

	rootMon := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, s)
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

	m := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, nil)
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
	rootMon := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, nil)
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
	rootMon := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, nil)
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
