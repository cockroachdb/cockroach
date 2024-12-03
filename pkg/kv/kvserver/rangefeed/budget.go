// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
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
// Note that regardless of calculated value, memory budged can't go below max
// raft command size since we should be able to send any command through
// rangefeed. See kvserver.MaxCommandSize for details on raft command size.
var maxFeedFraction = envutil.EnvOrDefaultFloat64("COCKROACH_RANGEFEED_TOTAL_MEM_FRACTION", 0.05)

// Pre allocated memory limit for system RangeFeeds. Each event should never
// exceed 64 MB as it would fail to write to raft log. We don't expect system
// ranges to have such objects, but we'll have a multiple of those just in case.
var systemRangeFeedBudget = envutil.EnvOrDefaultInt64("COCKROACH_RANGEFEED_SYSTEM_BUDGET",
	2*64*1024*1024 /* 128MB */)

// RangefeedBudgetsEnabled is a cluster setting that enables rangefeed memory
// budgets. This is meant to be an escape hatch to disable budgets if they cause
// feeds to fail unexpectedly despite nodes have plenty of memory or if bugs in
// budgeting are discovered and mitigation is required.
// With budgets disabled, rangefeeds will behave as they did prior to 22.1.
// Note that disabling this feature will not disable budgets on already running
// rangefeeds, but will affect feeds that are created after the option was
// changed. You need to restart all feeds to ensure that the budget is fully
// disabled.
// This feature is also globally controlled by useBudgets environment option
// defined above. If budgets are disabled by environment variable, cluster
// setting has no effect.
var RangefeedBudgetsEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.rangefeed.memory_budgets.enabled",
	"if set, rangefeed memory budgets are enabled",
	true,
)

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

var budgetClosedError = errors.Errorf("budget closed")

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
	// avoid creating BytesMonitor with a limit per feed.
	limit int64
	// Channel to notify that memory was returned to the budget.
	replenishC chan interface{}
	// Budget cancellation request.
	stopC chan interface{}
	// Cluster settings to be able to check RangefeedBudgetsEnabled on the fly.
	// When budgets are disabled, all new messages should pass through, all
	// previous allocations should still be returned as memory pool is shared with
	// SQL.
	settings *settings.Values

	closed sync.Once
}

// NewFeedBudget creates a FeedBudget to be used with RangeFeed. If nil account
// is passed, function will return nil which is safe to use with RangeFeed as
// it effectively disables memory accounting for that feed.
func NewFeedBudget(budget *mon.BoundAccount, limit int64, settings *settings.Values) *FeedBudget {
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
		settings:   settings,
	}
	f.mu.memBudget = budget
	return f
}

// TryGet allocates amount from budget. If there's not enough budget available
// returns error immediately.
// Returned allocation has its use counter set to 1.
func (f *FeedBudget) TryGet(ctx context.Context, amount int64) (*SharedBudgetAllocation, error) {
	if !RangefeedBudgetsEnabled.Get(f.settings) {
		return nil, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mu.closed {
		return nil, budgetClosedError
	}
	var err error
	if f.mu.memBudget.Used()+amount > f.limit {
		return nil, errors.Wrap(mon.NewMemoryBudgetExceededError(amount,
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
			err := ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				return f.TryGet(ctx, amount)
			}
			return nil, err
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
		f.mu.memBudget.Clear(ctx)
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
func (a *SharedBudgetAllocation) Use(ctx context.Context) {
	if a != nil {
		if atomic.AddInt32(&a.refCount, 1) == 1 {
			log.Fatalf(ctx, "unexpected shared memory allocation usage increase after free")
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
	adjustLimit        func(int64) int64
	feedBytesMon       *mon.BytesMonitor
	systemFeedBytesMon *mon.BytesMonitor

	settings *settings.Values

	metrics *FeedBudgetPoolMetrics
}

// BudgetFactoryConfig is a config for a BudgetFactory. It's main purpose is to
// decouple derived parameter calculation from the function that creates
// factories so that it could be tested independently.
type BudgetFactoryConfig struct {
	rootMon                 *mon.BytesMonitor
	provisionalFeedLimit    int64
	adjustLimit             func(int64) int64
	totalRangeFeedBudget    int64
	histogramWindowInterval time.Duration
	settings                *settings.Values
}

func (b BudgetFactoryConfig) empty() bool {
	return b.rootMon == nil
}

// CreateBudgetFactoryConfig creates new config using system defaults set by
// environment variables. If budgets are disabled, factory created from the
// config will be nil and will disable all feed memory budget accounting.
func CreateBudgetFactoryConfig(
	rootMon *mon.BytesMonitor,
	memoryPoolSize int64,
	histogramWindowInterval time.Duration,
	adjustLimit func(int64) int64,
	settings *settings.Values,
) BudgetFactoryConfig {
	if rootMon == nil || !useBudgets {
		return BudgetFactoryConfig{}
	}
	totalRangeFeedBudget := int64(float64(memoryPoolSize) * totalSharedFeedBudgetFraction)
	feedSizeLimit := int64(float64(totalRangeFeedBudget) * maxFeedFraction)
	return BudgetFactoryConfig{
		rootMon:                 rootMon,
		provisionalFeedLimit:    feedSizeLimit,
		adjustLimit:             adjustLimit,
		totalRangeFeedBudget:    totalRangeFeedBudget,
		histogramWindowInterval: histogramWindowInterval,
		settings:                settings,
	}
}

// NewBudgetFactory creates a factory callback that would create RangeFeed
// memory budget according to system policy.
func NewBudgetFactory(ctx context.Context, config BudgetFactoryConfig) *BudgetFactory {
	if config.empty() {
		return nil
	}
	metrics := NewFeedBudgetMetrics(config.histogramWindowInterval)
	systemRangeMonitor := mon.NewMonitorInheritWithLimit(
		"rangefeed-system-monitor", systemRangeFeedBudget, config.rootMon, true, /* longLiving */
	)
	systemRangeMonitor.SetMetrics(metrics.SystemBytesCount, nil /* maxHist */)
	systemRangeMonitor.Start(ctx, config.rootMon,
		mon.NewStandaloneBudget(systemRangeFeedBudget))

	rangeFeedPoolMonitor := mon.NewMonitorInheritWithLimit(
		"rangefeed-monitor", config.totalRangeFeedBudget, config.rootMon, true, /* longLiving */
	)
	rangeFeedPoolMonitor.SetMetrics(metrics.SharedBytesCount, nil /* maxHist */)
	rangeFeedPoolMonitor.StartNoReserved(ctx, config.rootMon)

	return &BudgetFactory{
		limit:              config.provisionalFeedLimit,
		adjustLimit:        config.adjustLimit,
		feedBytesMon:       rangeFeedPoolMonitor,
		systemFeedBytesMon: systemRangeMonitor,
		settings:           config.settings,
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
func (f *BudgetFactory) CreateBudget(isSystem bool) *FeedBudget {
	if f == nil {
		return nil
	}
	rangeLimit := f.adjustLimit(f.limit)
	if rangeLimit == 0 {
		return nil
	}
	// We use any table with reserved ID in system tenant as system case.
	if isSystem {
		acc := f.systemFeedBytesMon.MakeBoundAccount()
		return NewFeedBudget(&acc, 0, f.settings)
	}
	acc := f.feedBytesMon.MakeBoundAccount()
	return NewFeedBudget(&acc, rangeLimit, f.settings)
}

// Metrics exposes Metrics for BudgetFactory so that they could be registered
// in the metric registry.
func (f *BudgetFactory) Metrics() *FeedBudgetPoolMetrics {
	return f.metrics
}
