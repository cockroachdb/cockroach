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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// useBudgets controls if RangeFeed memory budgets are enabled. Overridable by
// environment variable.
var useBudgets = envutil.EnvOrDefaultBool("COCKROACH_USE_RANGEFEED_MEM_BUDGETS", true)

// maxFeedFraction is maximum percentage of feed memory pool that could be
// allocated to a single feed budget. Overridable by environment variable.
var maxFeedFraction = envutil.EnvOrDefaultFloat64("COCKROACH_RANGEFEED_MEM_FRACTION", 0.1)

// Pre allocated memory limit for system RangeFeeds.
var systemRangeFeedBudget = envutil.EnvOrDefaultInt64("COCKROACH_RANGEFEED_SYSTEM_BUDGET",
	64*1024*1024 /* 64MB */)

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
// If budget is exhausted, allocations would block until more is available
// or until an optional timeout expires.
// NB: Resource release notifications only work within context of a single
// feed. If we start contending for memory with other feeds in the same
// BytesMonitor pool we won't see if memory is released there and will
// time-out if memory is not allocated.
type FeedBudget struct {
	mu struct {
		syncutil.Mutex
		// Bound account that provides budget with resource.
		memBudget *mon.BoundAccount
		// Whenever we pass oversized message this would go to true to prevent
		// other attempts to acquire more memory until the event goes through.
		oversize bool
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

	closed  sync.Once
	metrics *Metrics
}

// NewFeedBudget creates a FeedBudget to be used with RangeFeed. If nil account
// is passed, function will return nil which is safe to use with RangeFeed as
// it effectively disables memory accounting for that feed.
func NewFeedBudget(budget *mon.BoundAccount, limit int64, metrics *Metrics) *FeedBudget {
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
		metrics:    metrics,
		limit:      limit,
	}
	f.mu.memBudget = budget
	return f
}

// Get allocates amount from budget.
// If budget has less memory than requested it could either wait, if we have any
// in flight allocations, or it could send a single message through regardless
// of its size. In latter case, memory is not requested from underlying budget,
// but we won't be able to do another Get until this message is returned. To
// track such events, FeedBudget uses a counter metric.
//
// timeout argument indicates how long we could wait for the budget to replenish
// if there is not enough budget available. Budget is replenished by Return
// calls.
// timeout value of 0 means wait indefinitely.
// Whenever we use timeout, we return its wait chan to caller, so it could be
// used subsequently by the chained wait for other resources thus keeping total
// timeout in check without adding to it on every op. If we didn't initiate
// a wait or if timeout was 0 then wait channel will be nil.
// Returned allocation has its use counter set to 1.
func (f *FeedBudget) Get(
	ctx context.Context, amount int64, timeout time.Duration,
) (*SharedBudgetAllocation, <-chan time.Time, error) {
	alloc, err := f.tryAlloc(ctx, amount)
	if err == nil {
		return getPooledBudgetAllocation(alloc), nil, nil
	}
	// If we failed to allocate immediately, then we need to wait on replenish
	// channel to track Return calls from consumed events.
	var timeC <-chan time.Time
	if timeout > 0 {
		timeC = time.After(timeout)
	}
	for {
		select {
		case <-f.replenishC:
			alloc, err = f.tryAlloc(ctx, amount)
			if err == nil {
				return getPooledBudgetAllocation(alloc), timeC, nil
			}
		case <-timeC:
			// TODO(oleg): should we try to allocate again in case we are faced
			// exhausted memory pool and someone returned memory to the pool?
			f.metrics.RangeFeedBudgetExhausted.Inc(1)
			return nil, timeC, errors.New("failed to allocate mem budget")
		case <-ctx.Done():
			return nil, timeC, nil
		case <-f.stopC:
			// We are already stopped, current allocation is already freed so, do
			// nothing.
			return nil, timeC, nil
		}
	}
}

// Function to allocate from budget or if there's nothing there borrow from
// emergency ether.
func (f *FeedBudget) tryAlloc(ctx context.Context, amount int64) (SharedBudgetAllocation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mu.closed {
		return SharedBudgetAllocation{}, errors.New("budget is already released")
	}
	if f.mu.oversize {
		return SharedBudgetAllocation{}, errors.New("failed to allocate mem budget")
	}
	var err error
	if f.mu.memBudget.Used()+amount < f.limit {
		err = f.mu.memBudget.Grow(ctx, amount)
		if err == nil {
			return SharedBudgetAllocation{size: amount, refCount: 1, feed: f}, nil
		}
	} else {
		err = errors.Wrap(f.mu.memBudget.Monitor().Resource().NewBudgetExceededError(amount,
			f.mu.memBudget.Used(),
			f.limit), "rangefeed budget")
	}
	// If we didn't use any budget yet and not currently sending an oversized
	// event then we can send one.
	if f.mu.memBudget.Used() == 0 {
		f.metrics.RangeFeedOverBudgetEvents.Inc(1)
		f.metrics.RangeFeedOverBudgetAllocation.Inc(amount)
		f.mu.oversize = true
		// We are in override mode, we return 0 size to avoid returning it to
		// budget later as we didn't borrow anything.
		return SharedBudgetAllocation{size: 0, refCount: 1, feed: f}, nil
	}
	return SharedBudgetAllocation{}, err
}

// Return returns amount to budget.
func (f *FeedBudget) returnAllocation(ctx context.Context, amount int64) {
	f.mu.Lock()
	if f.mu.closed {
		f.mu.Unlock()
		return
	}
	if f.mu.oversize {
		f.metrics.RangeFeedOverBudgetAllocation.Inc(-amount)
	}
	f.mu.oversize = false
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
	feedSizeLimit := int64(float64(memoryPoolSize) * maxFeedFraction)

	metrics := NewFeedBudgetMetrics(histogramWindowInterval)
	systemRangeMonitor := mon.NewMonitorInheritWithLimit("rangefeed-system-monitor",
		systemRangeFeedBudget, rootMon)
	systemRangeMonitor.SetMetrics(metrics.SystemCurBytesCount, metrics.SystemMaxBytesHist)
	systemRangeMonitor.Start(ctx, rootMon,
		mon.MakeStandaloneBudget(systemRangeFeedBudget))

	rangeFeedPoolMonitor := mon.NewMonitorInheritWithLimit("rangefeed-monitor", 0, rootMon)
	rangeFeedPoolMonitor.SetMetrics(metrics.SharedCurBytesCount, metrics.SharedMaxBytesHist)
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
func (f *BudgetFactory) CreateBudget(key roachpb.RKey, metrics *Metrics) *FeedBudget {
	if f == nil {
		return nil
	}
	if rpc.ConnectionClassForKey(key) == rpc.SystemClass {
		acc := f.systemFeedBytesMon.MakeBoundAccount()
		return NewFeedBudget(&acc, 0, metrics)
	}
	acc := f.feedBytesMon.MakeBoundAccount()
	return NewFeedBudget(&acc, f.limit, metrics)
}

// Metrics exposes Metrics for BudgetFactory so that they could be registered
// in the metric registry.
func (f *BudgetFactory) Metrics() *FeedBudgetPoolMetrics {
	return f.metrics
}
