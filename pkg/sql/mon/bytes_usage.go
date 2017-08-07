// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mon

import (
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"golang.org/x/net/context"
)

// BytesAccount and BytesMonitor together form the mechanism by which
// allocations are tracked and constrained (e.g. memory allocations by the
// server on behalf of db clients). The primary motivation is to avoid common
// cases of memory or disk blow-ups due to user error or unoptimized queries; a
// secondary motivation in the longer term is to offer more detailed metrics to
// users to track and explain memory/disk usage.
//
// The overall mechanism functions as follows:
//
// - components in CockroachDB that wish to have their allocations tracked
//   declare/register their allocations to an instance of BytesMonitor. To do
//   this, each component maintains one or more instances of BytesAccount, one
//   per "category" of allocation, and issue requests to Grow, Resize or Close
//   to their monitor. Grow/Resize requests can be denied (return an error),
//   which indicates the budget has been reached.
//
// - different instances of BytesAccount are associated to different usage
//   categories in components, in principle to track different object lifetimes.
//   Each account tracks the total amount of bytes allocated in that category
//   and enables declaring all the bytes as released at once using Close().
//
// - BytesMonitor checks the total sum of allocations across accounts, but also
//   serves as endpoint for statistics. Therefore each db client connection
//   should use a separate monitor for its allocations, so that statistics can
//   be separated per connection.
//
// - a BytesMonitor can be used standalone, and operate independently from other
//   monitors; however, since we also want to constrain global bytes usage
//   across all connections, multiple instance of BytesMonitors can coordinate
//   with each other by referring to a shared BytesMonitor, also known as
//   "pool". When operating in that mode, each BytesMonitor reports allocations
//   declared to it by component accounts also to the pool; and refusal by the
//   pool is reported back to the component. In addition, allocations are
//   "buffered" to reduce pressure on the mutex of the shared pool.
//
// General use cases:
//
//   component1 -+- account1 ---\
//               |              |
//               \- account2 ---+--- monitor (standalone)
//                              |
//   component2 --- account3 ---/
//
//
// Client connection A:
//   component1 -+- account1 ---\
//               |              |
//               \- account2 ---+--- monitorA --\
//                              |               |
//   component2 --- account3 ---/               |
//                                              +---- pool (shared monitor)
// Client connection B:                         |
//   component1 -+- account1 ---\               |
//               |              |               |
//               \- account2 ---+--- monitorB --/
//                              |
//   component2 --- account3 ---/
//
//
// In CockroachDB this is integrated as follows:
//
// For the internal executor:
//
//   internal executor ------------------------------owns-- session --owns-- monitor (standalone)
//        |                                                   |
//        \--- (sql run by internal executor) -- (accounts) --/
//
// Every use of the internal executor talks to a monitor that is not
// connected to a pool and does not constrain allocations (it just
// performs tracking).
//
// For admin commands:
//
//   admin server ---------------------------------------------------owns-- pool1 (shared monitor)
//     |                                                                       |
//     +-- admin conn1 --owns------------------ session --owns-- monitor --\   |
//     |     |                                     |                       |   |
//     |     \-- (sql for conn) -- (accounts) -----/                       +---/
//     |                                                                   |
//     +-- admin conn2 --owns------------------ session --owns-- monitor --/
//           |                                     |
//           \-- (sql for conn) -- (accounts) -----/
//
// All admin endpoints have a monitor per connection, held by the SQL
// session object, and all admin monitors talk to a single pool in the
// adminServer. This pool is (currently) unconstrained; it merely
// serves to track global memory usage by admin commands.
//
// The reason why the monitor object is held by the session object and tracks
// allocation that may span the entire lifetime of the session is detailed
// in a comment in the Session struct (cf. session.go).
//
// For regular SQL client connections:
//
//   executor --------------------------------------------------------owns-- pool2 (shared monitor)
//                                                                             |
//   pgwire server ---------------------------------------owns-- monitor --\   |
//     |                                                           |       |   |
//     +-- conn1 -- base account-----------------------------------+       +---/
//     |     |                                                     |       |
//     |     |                                                    ```      |
//     |     |                                                             |
//     |     +-----owns------------------------ session --owns-- monitor --+
//     |     |                                     |                       |
//     |     \-- (sql for conn) -- (accounts) -----/              ...      |
//     |                                                           |       |
//     |                                                           |       |
//     +-- conn2 -- base account-----------------------------------/       |
//           |                                                             |
//           |                                                             |
//           +-----owns------------------------ session --owns-- monitor --/
//           |                                     |
//           \-- (sql for conn) -- (accounts) -----/
//
// This is similar to the situation with admin commands with two deviations:
//
// - in this use case the shared pool is constrained; the maximum is
//   configured to be 1/4 of RAM size by default, and can be
//   overridden from the command-line.
//
// - in addition to the per-connection monitors, the pgwire server
//   owns and uses an additional shared monitor. This is an
//   optimization: when a pgwire connection is opened, the server
//   pre-reserves some bytes (`baseSQLMemoryBudget`) using a
//   per-connection "base account" to the shared server monitor. By
//   doing so, the server capitalizes on the fact that a monitor
//   "buffers" allocations from the pool and thus each connection
//   receives a starter bytes budget without needing to hit the
//   shared pool and its mutex.
//
// Finally, a simplified API is provided in session_mem_usage.go
// (WrappedMemoryAccount) to simplify the interface offered to SQL components
// using accounts linked to the session-bound monitor.

// BytesMonitor defines an object that can track and limit memory/disk usage by
// other CockroachDB components. The monitor must be set up via Start/Stop
// before and after use.
// The various counters express sizes in bytes.
type BytesMonitor struct {
	mu struct {
		syncutil.Mutex

		// curAllocated tracks the current amount of bytes allocated at this
		// monitor by its client components.
		curAllocated int64

		// maxAllocated tracks the high water mark of allocations. Used for
		// monitoring.
		maxAllocated int64

		// curBudget represents the budget allocated at the pool on behalf of
		// this monitor.
		curBudget BytesAccount
	}

	// name identifies this monitor in logging messages.
	name string

	// resource specifies what kind of resource the monitor is tracking
	// allocations for. Specific behavior is delegated to this resource (e.g.
	// budget exceeded errors).
	resource Resource

	// reserved indicates how many bytes were already reserved for this
	// monitor before it was instantiated. Allocations registered to
	// this monitor are first deducted from this budget. If there is no
	// pool, reserved determines the maximum allocation capacity of this
	// monitor. The reserved bytes are released to their owner monitor
	// upon Stop.
	reserved BoundAccount

	// limit specifies a hard limit on the number of bytes a monitor allows to
	// be allocated. Note that this limit will not be observed if allocations
	// hit constraints on the owner monitor. This is useful to limit allocations
	// when an owner monitor has a larger capacity than wanted but should still
	// keep track of allocations made through this monitor. Note that child
	// monitors are affected by this limit.
	limit int64

	// pool specifies where to send requests to increase or decrease curBudget.
	// May be nil for a standalone monitor.
	pool *BytesMonitor

	// poolAllocationSize specifies the allocation unit for requests to the
	// pool.
	poolAllocationSize int64

	// noteworthyUsageBytes is the size beyond which total allocations start to
	// become reported in the logs.
	noteworthyUsageBytes int64

	curBytesCount *metric.Counter
	maxBytesHist  *metric.Histogram
}

// maxAllocatedButUnusedBlocks determines the maximum difference between the
// amount of bytes used by a monitor and the amount of bytes reserved at the
// upstream pool before the monitor relinquishes the bytes back to the pool.
// This is useful so that a monitor currently at the boundary of a block does
// not cause contention when accounts cause its allocation counter to grow and
// shrink slightly beyond and beneath an allocation block boundary. The
// difference is expressed as a number of blocks of size `poolAllocationSize`.
var maxAllocatedButUnusedBlocks = envutil.EnvOrDefaultInt("COCKROACH_MAX_ALLOCATED_UNUSED_BLOCKS", 10)

// DefaultPoolAllocationSize specifies the unit of allocation used by a monitor
// to reserve and release bytes to a pool.
var DefaultPoolAllocationSize = envutil.EnvOrDefaultInt64("COCKROACH_ALLOCATION_CHUNK_SIZE", 10*1024)

// MakeMonitor creates a new monitor.
// Arguments:
// - name is used to annotate log messages, can be used to distinguish
//   monitors.
//
// - resource specifies what kind of resource the monitor is tracking
//   allocations for (e.g. memory or disk).
//
// - curCount and maxHist are the metric objects to update with usage
//   statistics.
//
// - increment is the block size used for upstream allocations from
//   the pool. Note: if set to 0 or lower, the default pool allocation
//   size is used.
//
// - noteworthy determines the minimum total allocated size beyond
//   which the monitor starts to log increases. Use 0 to always log
//   or math.MaxInt64 to never log.
func MakeMonitor(
	name string,
	res Resource,
	curCount *metric.Counter,
	maxHist *metric.Histogram,
	increment int64,
	noteworthy int64,
) BytesMonitor {
	return MakeMonitorWithLimit(name, res, math.MaxInt64, curCount, maxHist, increment, noteworthy)
}

// MakeMonitorWithLimit creates a new monitor with a limit local to this
// monitor.
func MakeMonitorWithLimit(
	name string,
	res Resource,
	limit int64,
	curCount *metric.Counter,
	maxHist *metric.Histogram,
	increment int64,
	noteworthy int64,
) BytesMonitor {
	if increment <= 0 {
		increment = DefaultPoolAllocationSize
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	return BytesMonitor{
		name:                 name,
		resource:             res,
		limit:                limit,
		noteworthyUsageBytes: noteworthy,
		curBytesCount:        curCount,
		maxBytesHist:         maxHist,
		poolAllocationSize:   increment,
	}
}

// MakeMonitorInheritWithLimit creates a new monitor with a limit local to this
// monitor with all other attributes inherited from the passed in monitor.
func MakeMonitorInheritWithLimit(name string, limit int64, m *BytesMonitor) BytesMonitor {
	return MakeMonitorWithLimit(
		name,
		m.resource,
		limit,
		m.curBytesCount,
		m.maxBytesHist,
		m.poolAllocationSize,
		m.noteworthyUsageBytes,
	)
}

// Start begins a monitoring region.
// Arguments:
// - pool is the upstream monitor that provision allocations exceeding the
//   pre-reserved budget. If pool is nil, no upstream allocations are possible
//   and the pre-reserved budget determines the entire capacity of this monitor.
//
// - reserved is the pre-reserved budget (see above).
func (mm *BytesMonitor) Start(ctx context.Context, pool *BytesMonitor, reserved BoundAccount) {
	if mm.mu.curAllocated != 0 {
		panic(fmt.Sprintf("%s: started with %d bytes left over", mm.name, mm.mu.curAllocated))
	}
	if mm.pool != nil {
		panic(fmt.Sprintf("%s: already started with pool %s", mm.name, mm.pool.name))
	}
	mm.pool = pool
	mm.mu.curAllocated = 0
	mm.mu.maxAllocated = 0
	mm.mu.curBudget.curAllocated = 0
	mm.reserved = reserved
	if log.V(2) {
		poolname := "(none)"
		if pool != nil {
			poolname = pool.name
		}
		log.InfofDepth(ctx, 1, "%s: starting monitor, reserved %s, pool %s",
			mm.name,
			humanizeutil.IBytes(mm.reserved.curAllocated),
			poolname)
	}
}

// MakeUnlimitedMonitor creates a new monitor and starts the monitor in
// "detached" mode without a pool and without a maximum budget.
func MakeUnlimitedMonitor(
	ctx context.Context,
	name string,
	res Resource,
	curCount *metric.Counter,
	maxHist *metric.Histogram,
	noteworthy int64,
) BytesMonitor {
	if log.V(2) {
		log.InfofDepth(ctx, 1, "%s: starting unlimited monitor", name)

	}
	return BytesMonitor{
		name:                 name,
		resource:             res,
		limit:                math.MaxInt64,
		noteworthyUsageBytes: noteworthy,
		curBytesCount:        curCount,
		maxBytesHist:         maxHist,
		poolAllocationSize:   DefaultPoolAllocationSize,
		reserved:             MakeStandaloneBudget(math.MaxInt64),
	}
}

// EmergencyStop completes a monitoring region, and disables checking
// that all accounts have been closed.
func (mm *BytesMonitor) EmergencyStop(ctx context.Context) {
	mm.doStop(ctx, false)
}

// Stop completes a monitoring region.
func (mm *BytesMonitor) Stop(ctx context.Context) {
	mm.doStop(ctx, true)
}

func (mm *BytesMonitor) doStop(ctx context.Context, check bool) {
	// NB: No need to lock mm.mu here, when StopMonitor() is called the
	// monitor is not shared any more.
	if log.V(1) {
		log.InfofDepth(ctx, 1, "%s, bytes usage max %s",
			mm.name,
			humanizeutil.IBytes(mm.mu.maxAllocated))
	}

	if check && mm.mu.curAllocated != 0 {
		panic(fmt.Sprintf("%s: unexpected %d leftover bytes",
			mm.name,
			mm.mu.curAllocated))
	}

	mm.releaseBudget(ctx)

	if mm.maxBytesHist != nil && mm.mu.maxAllocated > 0 {
		// TODO(knz): We record the logarithm because the UI doesn't know
		// how to do logarithmic y-axes yet. See the explanatory comments
		// in sql/mem_metrics.go.
		val := int64(1000 * math.Log(float64(mm.mu.maxAllocated)) / math.Ln10)
		mm.maxBytesHist.RecordValue(val)
	}

	// Disable the pool for further allocations, so that further
	// uses outside of monitor control get errors.
	mm.pool = nil

	// Release the reserved budget to its original pool, if any.
	mm.reserved.Clear(ctx)
}

// BytesAccount tracks the cumulated allocations for one client of a pool or
// monitor. BytesMonitor has an account to its pool; BytesMonitor clients have
// an account to the monitor. This allows each client to release all the bytes
// at once when it completes its work.
//
// See the comments in bytes_usage.go for a fuller picture of how these accounts
// are used in CockroachDB.
type BytesAccount struct {
	curAllocated int64
}

// CurrentlyAllocated returns the number of bytes currently allocated through
// this account.
func (acc BytesAccount) CurrentlyAllocated() int64 {
	return acc.curAllocated
}

// OpenAccount creates a new empty account.
func (mm *BytesMonitor) OpenAccount(_ *BytesAccount) {
	// TODO(knz): conditionally track accounts in the monitor (#9122).
}

// OpenAndInitAccount creates a new account and pre-allocates some
// initial amount of bytes.
func (mm *BytesMonitor) OpenAndInitAccount(
	ctx context.Context, acc *BytesAccount, initialAllocation int64,
) error {
	mm.OpenAccount(acc)
	return mm.GrowAccount(ctx, acc, initialAllocation)
}

// GrowAccount requests a new allocation in an account.
func (mm *BytesMonitor) GrowAccount(ctx context.Context, acc *BytesAccount, extraSize int64) error {
	if err := mm.reserveBytes(ctx, extraSize); err != nil {
		return err
	}
	acc.curAllocated += extraSize
	return nil
}

// CloseAccount releases all the cumulated allocations of an account at once.
func (mm *BytesMonitor) CloseAccount(ctx context.Context, acc *BytesAccount) {
	if acc.curAllocated == 0 {
		// Fast path so as to avoid locking the monitor.
		return
	}
	mm.releaseBytes(ctx, acc.curAllocated)
}

// ClearAccount releases all the cumulated allocations of an account at once
// and primes it for reuse.
func (mm *BytesMonitor) ClearAccount(ctx context.Context, acc *BytesAccount) {
	mm.CloseAccount(ctx, acc)
	acc.curAllocated = 0
}

// ShrinkAccount releases part of the cumulated allocations by the specified size.
func (mm *BytesMonitor) ShrinkAccount(ctx context.Context, acc *BytesAccount, delta int64) {
	if acc.curAllocated < delta {
		panic(fmt.Sprintf("%s: no bytes in account to release, current %d, free %d",
			mm.name, acc.curAllocated, delta))
	}
	mm.releaseBytes(ctx, delta)
	acc.curAllocated -= delta
}

// ResizeItem requests a size change for an object already registered
// in an account. The reservation is not modified if the new allocation is
// refused, so that the caller can keep using the original item
// without an accounting error. This is better than calling ClearAccount
// then GrowAccount because if the Clear succeeds and the Grow fails
// the original item becomes invisible from the perspective of the
// monitor.
func (mm *BytesMonitor) ResizeItem(
	ctx context.Context, acc *BytesAccount, oldSize, newSize int64,
) error {
	delta := newSize - oldSize
	switch {
	case delta > 0:
		return mm.GrowAccount(ctx, acc, delta)
	case delta < 0:
		mm.ShrinkAccount(ctx, acc, -delta)
	}
	return nil
}

// BoundAccount implements a BytesAccount attached to a specific monitor.
type BoundAccount struct {
	BytesAccount
	mon *BytesMonitor
}

// MakeStandaloneBudget creates a BoundAccount suitable for root
// monitors.
func MakeStandaloneBudget(capacity int64) BoundAccount {
	return BoundAccount{BytesAccount: BytesAccount{curAllocated: capacity}}
}

// MakeBoundAccount greates a BoundAccount connected to the given monitor.
func (mm *BytesMonitor) MakeBoundAccount() BoundAccount {
	return BoundAccount{mon: mm}
}

// Clear is an accessor for b.mon.ClearAccount.
func (b *BoundAccount) Clear(ctx context.Context) {
	if b.mon == nil {
		// An account created by MakeStandaloneBudget is disconnected from any
		// monitor -- "bytes out of the aether". This needs not be closed.
		return
	}
	b.mon.ClearAccount(ctx, &b.BytesAccount)
}

// Close is an accessor for b.mon.CloseAccount.
func (b *BoundAccount) Close(ctx context.Context) {
	if b.mon == nil {
		// An account created by MakeStandaloneBudget is disconnected from any
		// monitor -- "bytes out of the aether". This needs not be closed.
		return
	}
	b.mon.CloseAccount(ctx, &b.BytesAccount)
}

// ResizeItem is an accessor for b.mon.ResizeItem.
func (b *BoundAccount) ResizeItem(ctx context.Context, oldSz, newSz int64) error {
	return b.mon.ResizeItem(ctx, &b.BytesAccount, oldSz, newSz)
}

// Grow is an accessor for b.mon.GrowAccount.
func (b *BoundAccount) Grow(ctx context.Context, x int64) error {
	return b.mon.GrowAccount(ctx, &b.BytesAccount, x)
}

// Shrink is an accessor for b.mon.ShrinkAccount.
func (b *BoundAccount) Shrink(ctx context.Context, x int64) {
	b.mon.ShrinkAccount(ctx, &b.BytesAccount, x)
}

// reserveBytes declares an allocation to this monitor. An error is returned if
// the allocation is denied.
func (mm *BytesMonitor) reserveBytes(ctx context.Context, x int64) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	// Check the local limit first.
	if mm.mu.curAllocated+x > mm.limit {
		return pgerror.AnnotateError(mm.name, mm.resource.NewBudgetExceededError(x, mm.limit))
	}
	// Check whether we need to request an increase of our budget.
	if mm.mu.curAllocated > mm.mu.curBudget.curAllocated+mm.reserved.curAllocated-x {
		if err := mm.increaseBudget(ctx, x); err != nil {
			return err
		}
	}
	mm.mu.curAllocated += x
	if mm.curBytesCount != nil {
		mm.curBytesCount.Inc(x)
	}
	if mm.mu.maxAllocated < mm.mu.curAllocated {
		mm.mu.maxAllocated = mm.mu.curAllocated
	}

	// Report "large" queries to the log for further investigation.
	if mm.mu.curAllocated > mm.noteworthyUsageBytes {
		// We only report changes in binary magnitude of the size. This is to
		// limit the amount of log messages when a size blowup is caused by
		// many small allocations.
		if util.RoundUpPowerOfTwo(mm.mu.curAllocated) != util.RoundUpPowerOfTwo(mm.mu.curAllocated-x) {
			log.Infof(ctx, "%s: bytes usage increases to %s (+%d)",
				mm.name,
				humanizeutil.IBytes(mm.mu.curAllocated), x)
		}
	}

	if log.V(2) {
		// We avoid VEventf here because we want to avoid computing the
		// trace string if there is nothing to log.
		log.Infof(ctx, "%s: now at %d bytes (+%d) - %s",
			mm.name, mm.mu.curAllocated, x, util.GetSmallTrace(3))
	}
	return nil
}

// releaseBytes releases bytes previously successfully registered via
// reserveBytes().
func (mm *BytesMonitor) releaseBytes(ctx context.Context, sz int64) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if mm.mu.curAllocated < sz {
		panic(fmt.Sprintf("%s: no bytes to release, current %d, free %d",
			mm.name, mm.mu.curAllocated, sz))
	}
	mm.mu.curAllocated -= sz
	if mm.curBytesCount != nil {
		mm.curBytesCount.Dec(sz)
	}
	mm.adjustBudget(ctx)

	if log.V(2) {
		// We avoid VEventf here because we want to avoid computing the
		// trace string if there is nothing to log.
		log.Infof(ctx, "%s: now at %d bytes (-%d) - %s",
			mm.name, mm.mu.curAllocated, sz, util.GetSmallTrace(3))
	}
}

// increaseBudget requests more bytes from the pool.
func (mm *BytesMonitor) increaseBudget(ctx context.Context, minExtra int64) error {
	// NB: mm.mu Already locked by reserveBytes().
	if mm.pool == nil {
		return pgerror.AnnotateError(
			mm.name, mm.resource.NewBudgetExceededError(minExtra, mm.reserved.curAllocated),
		)
	}
	minExtra = mm.roundSize(minExtra)
	if log.V(2) {
		log.Infof(ctx, "%s: requesting %d bytes from the pool", mm.name, minExtra)
	}

	return mm.pool.GrowAccount(ctx, &mm.mu.curBudget, minExtra)
}

// roundSize rounds its argument to the smallest greater or equal
// multiple of `poolAllocationSize`.
func (mm *BytesMonitor) roundSize(sz int64) int64 {
	chunks := (sz + mm.poolAllocationSize - 1) / mm.poolAllocationSize
	return chunks * mm.poolAllocationSize
}

// releaseBudget relinquishes all the monitor's allocated bytes back to the
// pool.
func (mm *BytesMonitor) releaseBudget(ctx context.Context) {
	// NB: mm.mu need not be locked here, as this is only called from StopMonitor().
	if log.V(2) {
		log.Infof(ctx, "%s: releasing %d bytes to the pool", mm.name, mm.mu.curBudget.curAllocated)
	}
	mm.pool.ClearAccount(ctx, &mm.mu.curBudget)
}

// adjustBudget ensures that the monitor does not keep many more bytes reserved
// from the pool than it currently has allocated. Bytes are relinquished when
// there are at least maxAllocatedButUnusedBlocks*poolAllocationSize bytes
// reserved but unallocated.
func (mm *BytesMonitor) adjustBudget(ctx context.Context) {
	// NB: mm.mu Already locked by releaseBytes().
	margin := mm.poolAllocationSize * int64(maxAllocatedButUnusedBlocks)

	neededBytes := mm.mu.curAllocated
	if neededBytes <= mm.reserved.curAllocated {
		neededBytes = 0
	} else {
		neededBytes = mm.roundSize(neededBytes - mm.reserved.curAllocated)
	}
	if neededBytes <= mm.mu.curBudget.curAllocated-margin {
		mm.pool.ShrinkAccount(ctx, &mm.mu.curBudget, mm.mu.curBudget.curAllocated-neededBytes)
	}
}

// GetCurrentAllocationForTesting returns the number of bytes that have
// currently been allocated in the BytesMonitor. Intended for use in testing.
func (mm *BytesMonitor) GetCurrentAllocationForTesting() int64 {
	return mm.mu.curAllocated
}
