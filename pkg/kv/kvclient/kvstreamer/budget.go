// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstreamer

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// budget abstracts the memory budget that is provided to the Streamer by its
// client.
//
// This struct is a wrapper on top of mon.BoundAccount because we want to
// support the notion of budget "going in debt". This can occur in a degenerate
// case when a single large row exceeds the provided limit. The Streamer is
// expected to have only a single request in progress in this case.
// Additionally, the budget provides blocking until some memory is returned to
// the budget or, if the budget is already in debt, until it gets out of debt.
type budget struct {
	mu struct {
		// If the Streamer's mutex also needs to be locked, the budget's mutex
		// must be acquired first.
		syncutil.Mutex
		// acc represents the current reservation of this budget against the
		// root memory pool.
		acc *mon.BoundAccount
		// waitForBudget is used by the main loop of the workerCoordinator to
		// block until the next release() call or, if the budget is currently in
		// debt, until it gets out of debt.
		waitForBudget *sync.Cond
	}
	// limitBytes is the maximum amount of bytes that this budget should reserve
	// against acc, i.e. acc.Used() should not exceed limitBytes. However, in a
	// degenerate case of a single large row, the budget can go into debt and
	// acc.Used() might exceed limitBytes.
	//
	// Available budget can be calculated as limitBytes - mu.acc.Used().
	limitBytes int64
}

// newBudget creates a new budget with the specified limit. The limit determines
// the maximum amount of memory this budget is allowed to use (i.e. it'll be
// used lazily, as needed).
//
// The budget itself is responsible for staying under the limit, so acc should
// be bound to an unlimited memory monitor. This is needed in order to support
// the case of budget going into debt. Note that although it is an "unlimited
// memory monitor", the monitor is still limited by --max-sql-memory in size
// eventually because all monitors are descendants of the root SQL monitor.
//
// The budget takes ownership of the memory account, and the caller is allowed
// to interact with the account only after canceling the Streamer (because
// memory accounts are not thread-safe).
func newBudget(acc *mon.BoundAccount, limitBytes int64) *budget {
	b := budget{limitBytes: limitBytes}
	b.mu.acc = acc
	b.mu.waitForBudget = sync.NewCond(&b.mu.Mutex)
	return &b
}

// consume draws bytes from the available budget. An error is returned if the
// root pool budget is used up such that the budget's limit cannot be fully
// reserved.
// - allowDebt indicates whether the budget is allowed to go into debt on this
// consumption. In other words, if allowDebt is true, then acc's reservation is
// allowed to exceed limitBytes (but the error is still returned if the root
// pool budget is exceeded). Note that allowDebt value applies only to this
// consume() call and is not carried forward.
//
// b's mutex should not be held when calling this method.
func (b *budget) consume(ctx context.Context, bytes int64, allowDebt bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.consumeLocked(ctx, bytes, allowDebt)
}

// consumeLocked is the same as consume but assumes that the b's lock is held.
func (b *budget) consumeLocked(ctx context.Context, bytes int64, allowDebt bool) error {
	b.mu.AssertHeld()
	// If we're asked to not exceed the limit (and the limit is greater than
	// twenty bytes - limits of twenty bytes or less are treated as a special
	// case for "forced disk spilling" scenarios like in logic tests), we have
	// to check whether we'll stay within the budget.
	if !allowDebt && b.limitBytes > 20 {
		if b.mu.acc.Used()+bytes > b.limitBytes {
			return errors.Wrap(
				mon.NewMemoryBudgetExceededError(bytes, b.mu.acc.Used(), b.limitBytes),
				"streamer budget",
			)
		}
	}
	return b.mu.acc.Grow(ctx, bytes)
}

// release returns bytes to the available budget. The budget's mutex must not be
// held.
func (b *budget) release(ctx context.Context, bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.releaseLocked(ctx, bytes)
}

// releaseLocked is the same as release but assumes that the budget's mutex is
// already being held.
func (b *budget) releaseLocked(ctx context.Context, bytes int64) {
	b.mu.AssertHeld()
	b.mu.acc.Shrink(ctx, bytes)
	if b.limitBytes > b.mu.acc.Used() {
		// Since we now have some available budget, signal the worker
		// coordinator.
		b.mu.waitForBudget.Signal()
	}
}
