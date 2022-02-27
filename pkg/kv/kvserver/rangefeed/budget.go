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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// useBudgets controls if RangeFeed memory budgets are enabled. Overridable by
// environment variable.
var useBudgets = envutil.EnvOrDefaultBool("COCKROACH_USE_RANGEFEED_MEM_BUDGETS", true)

// totalSharedFeedBudgetFraction is maximum percentage of SQL memory pool that
// could be used by all feed budgets together. Overridable by environment
// variable.
var totalSharedFeedBudgetFraction = envutil.EnvOrDefaultFloat64("COCKROACH_RANGEFEED_FEED_MEM_FRACTION",
	0.5)

// maxFeedFraction is maximum percentage of feed memory pool that could be
// allocated to a single feed budget. Overridable by environment variable.
// With 32 GB node and 0.25 sql memory budget (8 GB) each range would get max of
// 8 * 0.5 * 0.05 = 200 MB budget limit.
// With 8 GB node and 0.25 sql memory budget (2 GB) each range would get max of
// 2 * 0.5 * 0.05 = 50 MB budget limit.
var maxFeedFraction = envutil.EnvOrDefaultFloat64("COCKROACH_RANGEFEED_TOTAL_MEM_FRACTION", 0.05)

// Pre allocated memory limit for system RangeFeeds. Each event should never
// exceed 64 MB as it would fail to write to raft log. We don't expect system
// ranges to have such objects, but we'll have a multiple of those just in case.
var systemRangeFeedBudget = envutil.EnvOrDefaultInt64("COCKROACH_RANGEFEED_SYSTEM_BUDGET",
	2*64*1024*1024 /* 128MB */)

var budgetAllocationSyncPool = sync.Pool{
	New: func() interface{} {
		return new(SharedBudgetAllocation)
	},
}

func getPooledBudgetAllocation(ba SharedBudgetAllocation) *SharedBudgetAllocation {
	b := budgetAllocationSyncPool.Get().(*SharedBudgetAllocation)
	*b = ba
	return b
}

func putPooledBudgetAllocation(ba *SharedBudgetAllocation) {
	*ba = SharedBudgetAllocation{}
	budgetAllocationSyncPool.Put(ba)
}

// FeedBudget is memory budget for RangeFeed that wraps BoundAccount
// and provides ability to wait for downstream to release budget and
// to send individual events that exceed total budget size.
// FeedBudget doesn't provide any fairness when acquiring as it is only
// supposed to be used by a single caller.
// When owning component is destroyed, budget must be closed, in that
// case all budget allocation is returned immediately and no further
// allocations are possible.
// In the typical case processor will get allocations from budget which
// would be in turn borrowed from underlying account. Once event is
// processed, allocation would be returned.
// To use the budget first try to obtain allocation with TryGet and if
// it fails because budget is exhausted use WaitAndGet and use context with
// a deadline to stop. It is not safe to just call WaitAndGet because it
// doesn't check for available budget before waiting.
// NB: Resource release notifications only work within context of a single
// feed. If we start contending for memory with other feeds in the same
// BytesMonitor pool we won't see if memory is released there and will
// time-out if memory is not allocated.
type FeedBudget struct {
	mu struct {
		syncutil.Mutex
		// Bound account that provides budget with resource.
		memBudget *mon.BoundAccount
		// If true, budget was released and no more allocations could take place.
		closed bool
	}
	// Maximum amount of memory to use by feed. We use separate limit here to
	// avoid creating BytesMontior with a limit per feed.
	limit int64
	// Channel to notify that memory was returned to the budget.
	replenishC chan interface{}
	// Budget cancellation request
	stopC chan interface{}

	closed sync.Once
}

// NewFeedBudget creates a FeedBudget to be used with RangeFeed. If nil account
// is passed, function will return nil which is safe to use with RangeFeed as
// it effectively disables memory accounting for that feed.
func NewFeedBudget(budget *mon.BoundAccount, limit int64) *FeedBudget {
	if budget == nil {
		return nil
	}
	// If limit is not specified, use large enough value.
	if limit <= 0 {
		limit = (1 << 63) - 1
	}
	f := &FeedBudget{
		replenishC: make(chan interface{}, 1),
		stopC:      make(chan interface{}),
		limit:      limit,
	}
	f.mu.memBudget = budget
	return f
}

// TryGet allocates amount from budget. If there's not enough budget available
// returns error immediately.
// Returned allocation has its use counter set to 1.
func (f *FeedBudget) TryGet(ctx context.Context, amount int64) (*SharedBudgetAllocation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mu.closed {
		log.Info(ctx, "trying to get allocation from already closed budget")
		return nil, errors.Errorf("budget unexpectedly closed")
	}
	var err error
	if f.mu.memBudget.Used()+amount > f.limit {
		return nil, errors.Wrap(f.mu.memBudget.Monitor().Resource().NewBudgetExceededError(amount,
			f.mu.memBudget.Used(),
			f.limit), "rangefeed budget")
	}
	if err = f.mu.memBudget.Grow(ctx, amount); err != nil {
		return nil, err
	}
	return getPooledBudgetAllocation(SharedBudgetAllocation{size: amount, refCount: 1, feed: f}), nil
}

// WaitAndGet waits for replenish channel to return any allocations back to the
// budget and then tries to get allocation. Waiting stops when context is
// cancelled. Context should be used to set up a timeout as needed.
func (f *FeedBudget) WaitAndGet(
	ctx context.Context, amount int64,
) (*SharedBudgetAllocation, error) {
	for {
		select {
		case <-f.replenishC:
			alloc, err := f.TryGet(ctx, amount)
			if err == nil {
				return alloc, nil
			}
		case <-ctx.Done():
			// Since we share budget with other components, it is also possible that
			// it was returned and we are not notified by our feed channel, so we try
			// for the last time.
			return f.TryGet(ctx, amount)
		case <-f.stopC:
			// We are already stopped, current allocation is already freed so, do
			// nothing.
			return nil, nil
		}
	}
}

// Return returns amount to budget.
func (f *FeedBudget) returnAllocation(ctx context.Context, amount int64) {
	f.mu.Lock()
	if f.mu.closed {
		f.mu.Unlock()
		return
	}
	if amount > 0 {
		f.mu.memBudget.Shrink(ctx, amount)
	}
	f.mu.Unlock()
	select {
	case f.replenishC <- struct{}{}:
	default:
	}
}

// Close frees up all allocated budget and prevents any further allocations.
// Safe to call on nil budget.
func (f *FeedBudget) Close(ctx context.Context) {
	if f == nil {
		return
	}
	f.closed.Do(func() {
		f.mu.Lock()
		f.mu.closed = true
		f.mu.memBudget.Close(ctx)
		close(f.stopC)
		f.mu.Unlock()
	})
}

// SharedBudgetAllocation is a token that is passed around with range events
// to registrations to maintain RangeFeed memory budget across shared queues.
type SharedBudgetAllocation struct {
	refCount int32
	size     int64
	feed     *FeedBudget
}

// Use increases usage count for the allocation. It should be called by each
// new consumer that plans to retain allocation after returning to a caller
// that passed this allocation.
func (a *SharedBudgetAllocation) Use() {
	if a != nil {
		if atomic.AddInt32(&a.refCount, 1) == 1 {
			panic("unexpected shared memory allocation usage increase after free")
		}
	}
}

// Release decreases ref count and returns true if budget could be released.
func (a *SharedBudgetAllocation) Release(ctx context.Context) {
	if a != nil && atomic.AddInt32(&a.refCount, -1) == 0 {
		a.feed.returnAllocation(ctx, a.size)
		putPooledBudgetAllocation(a)
	}
}

// BudgetFactory creates memory budget for rangefeed according to system
// settings.
type BudgetFactory struct {
	limit              int64
	feedBytesMon       *mon.BytesMonitor
	systemFeedBytesMon *mon.BytesMonitor

	metrics *FeedBudgetPoolMetrics
}

// NewBudgetFactory creates a factory callback that would create RangeFeed
// memory budget according to system policy.
func NewBudgetFactory(
	ctx context.Context,
	rootMon *mon.BytesMonitor,
	memoryPoolSize int64,
	histogramWindowInterval time.Duration,
) *BudgetFactory {
	if !useBudgets || rootMon == nil {
		return nil
	}
	totalRangeReedBudget := int64(float64(memoryPoolSize) * totalSharedFeedBudgetFraction)
	feedSizeLimit := int64(float64(totalRangeReedBudget) * maxFeedFraction)

	metrics := NewFeedBudgetMetrics(histogramWindowInterval)
	systemRangeMonitor := mon.NewMonitorInheritWithLimit("rangefeed-system-monitor",
		systemRangeFeedBudget, rootMon)
	systemRangeMonitor.SetMetrics(metrics.SystemBytesCount, nil /* maxHist */)
	systemRangeMonitor.Start(ctx, rootMon,
		mon.MakeStandaloneBudget(systemRangeFeedBudget))

	rangeFeedPoolMonitor := mon.NewMonitorInheritWithLimit("rangefeed-monitor", totalRangeReedBudget,
		rootMon)
	rangeFeedPoolMonitor.SetMetrics(metrics.SharedBytesCount, nil /* maxHist */)
	rangeFeedPoolMonitor.Start(ctx, rootMon, mon.BoundAccount{})

	return &BudgetFactory{
		limit:              feedSizeLimit,
		feedBytesMon:       rangeFeedPoolMonitor,
		systemFeedBytesMon: systemRangeMonitor,
		metrics:            metrics,
	}
}

// Stop stops underlying memory monitors used by factory.
// Safe to call on nil factory.
func (f *BudgetFactory) Stop(ctx context.Context) {
	if f == nil {
		return
	}
	f.systemFeedBytesMon.Stop(ctx)
	f.feedBytesMon.Stop(ctx)
}

// CreateBudget creates feed budget using memory pools configured in the
// factory. It is safe to call on nil factory as it will produce nil budget
// which in turn disables memory accounting on range feed.
func (f *BudgetFactory) CreateBudget(key roachpb.RKey) *FeedBudget {
	if f == nil {
		return nil
	}
	// We use any table with reserved ID in system tenant as system case.
	if key.Less(roachpb.RKey(keys.SystemSQLCodec.TablePrefix(keys.MaxReservedDescID + 1))) {
		acc := f.systemFeedBytesMon.MakeBoundAccount()
		return NewFeedBudget(&acc, 0)
	}
	acc := f.feedBytesMon.MakeBoundAccount()
	return NewFeedBudget(&acc, f.limit)
}

// Metrics exposes Metrics for BudgetFactory so that they could be registered
// in the metric registry.
func (f *BudgetFactory) Metrics() *FeedBudgetPoolMetrics {
	return f.metrics
}
