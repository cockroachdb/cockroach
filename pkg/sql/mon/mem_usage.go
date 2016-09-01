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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package mon

import (
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// MemoryAccount and MemoryMonitor together form the
// mechanism by which memory consumption by the server on behalf of db
// clients is tracked and constrained. The primary motivation is to
// avoid common cases of memory blow-ups due to user error or
// unoptimized queries; a secondary motivation in the longer term is
// to offer more detailed metrics to users to track and explain memory
// consumption.
//
// The overall mechanism functions as follows:
//
// - components in CockroachDB that wish to have their allocations
//   tracked declare/register their allocations to an instance of
//   MemoryMonitor. To do this, each component maintains one or
//   more instances of MemoryAccount, one per "category" of
//   allocation, and issue requests to Grow, Resize or Close to their
//   monitor. Grow/Resize requests can be denied (return an error),
//   which indicates the memory budget has been reached.
//
// - different instances of MemoryAccount are associated to different
//   usage categories in components, in principle to track different
//   object lifetimes. Each account tracks the total amount of memory
//   allocated in that category and enables declaring all the memory
//   as released at once using Close() when all objects in that
//   category are released back to Go's heap.
//
// - MemoryMonitor checks the total sum of allocations across
//   accounts, but also serves as endpoint for statistics. Therefore
//   each db client connection should use a separate monitor for its
//   allocations, so that statistics can be separated per connection.
//
// - a MemoryMonitor can be used standalone, and operate
//   independently from other monitors; however, since we also want to
//   constrain global memory usage across all connections, multiple
//   instance of MemoryMonitors can coordinate with each other by
//   referring to a shared MemoryMonitor. When operating in that mode,
//   each MemoryMonitor reports allocations declared to it by
//   component accounts also to the pool; and refusal by the pool
//   is reported back to the component. In addition, allocations are
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
//   pre-reserves some memory (`baseSQLMemoryBudget`) using a
//   per-connection "base account" to the shared server monitor. By
//   doing so, the server capitalizes on the fact that a monitor
//   "buffers" allocations from the pool and thus each connection
//   receives a starter memory budget without needing to hit the
//   shared pool and its mutex.
//
// Finally, a simplified API is provided in session_mem_usage.go
// (WrappedMemoryAccount) to simplify the interface offered to SQL
// components using memory accounts linked to the session-bound
// monitor.

// MemoryMonitor defines an object that can track and limit
// memory usage by other CockroachDB components.
// The monitor must be set up via StartMonitor/StopMonitor before
// and after use.
// The various counters express sizes in bytes.
type MemoryMonitor struct {
	mu struct {
		syncutil.Mutex

		// curAllocated tracks the current amount of memory allocated at
		// this monitor by its client components.
		curAllocated int64

		// maxAllocated tracks the high water mark of allocations.
		// Used for monitoring.
		maxAllocated int64

		// curBudget represents the budget allocated at the pool on behalf
		// of this monitor.
		curBudget MemoryAccount
	}

	// name identifies this monitor in logging messages.
	name string

	// reservedBytes indicates how much memory was already reserved for this
	// monitor before it was instantiated. Allocations registered to
	// this monitor are first deducted from this budget. If there is
	// no pool, reservedBytes determines the maximum allocation capacity
	// of this monitor. The reservedBytes are never released to the pool
	// by the monitor.
	reservedBytes int64

	// pool specifies where to send requests to increase or decrease
	// curBudget. May be nil for a standalone monitor.
	pool *MemoryMonitor

	// poolAllocationSize specifies the allocation unit for requests to
	// the pool.
	poolAllocationSize int64

	curBytesCount *metric.Counter
	maxBytesHist  *metric.Histogram
}

// noteworthyMemoryUsageBytes is the minimum size tracked by a monitor
// before the monitor starts explicitly logging overall usage growth in the log.
var noteworthyMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_MEMORY_USAGE", 10000)

// maxAllocatedButUnusedMemoryBlocks determines the maximum difference
// between the amount of memory used by a monitor and the amount of
// memory reserved at the upstream pool before the monitor
// relinquishes the memory back to the pool. This is useful so that a
// monitor currently at the boundary of a block does not cause
// contention when accounts cause its allocation counter to grow and
// shrink slightly beyond and beneath an allocation block
// boundary. The difference is expressed as a number of blocks of size
// `poolAllocationSize`.
var maxAllocatedButUnusedMemoryBlocks = envutil.EnvOrDefaultInt("COCKROACH_MAX_ALLOCATED_UNUSED_BLOCKS", 10)

// defaultPoolAllocationSize specifies the unit of allocation used by
// a monitor to reserve and release memory to a pool.
var defaultPoolAllocationSize = envutil.EnvOrDefaultInt64("COCKROACH_MEMORY_ALLOCATION_CHUNK_SIZE", 10*1024)

// StartMonitor begins a monitoring region.
func (mm *MemoryMonitor) StartMonitor(
	name string,
	curCount *metric.Counter,
	maxHist *metric.Histogram,
	pool *MemoryMonitor,
	reserved, increment int64,
) {
	if mm.mu.curAllocated != 0 {
		panic(fmt.Sprintf("%s: started with %d bytes left over", mm.name, mm.mu.curAllocated))
	}
	mm.name = name
	mm.pool = pool
	if increment == -1 {
		increment = defaultPoolAllocationSize
	}
	mm.poolAllocationSize = increment
	mm.mu.curAllocated = 0
	mm.mu.maxAllocated = 0
	mm.mu.curBudget.curAllocated = 0
	mm.reservedBytes = reserved
	mm.curBytesCount = curCount
	mm.maxBytesHist = maxHist
}

// StartUnlimitedMonitor starts the monitor in "detached" mode without
// a pool and without a maximum budget.
func (mm *MemoryMonitor) StartUnlimitedMonitor(
	name string, curCount *metric.Counter, maxHist *metric.Histogram,
) {
	mm.StartMonitor(name, curCount, maxHist, nil, math.MaxInt64, 1)
}

// StopMonitor completes a monitoring region.
func (mm *MemoryMonitor) StopMonitor(ctx context.Context) {
	// NB: No need to lock mm.mu here, when StopMonitor() is called the
	// monitor is not shared any more.
	if log.V(1) {
		log.InfofDepth(ctx, 1, "%s, memory usage max %s",
			mm.name,
			humanizeutil.IBytes(mm.mu.maxAllocated))
	}

	if mm.mu.curAllocated != 0 {
		panic(fmt.Sprintf("%s: unexpected leftover memory: %d bytes",
			mm.name,
			mm.mu.curAllocated))
	}

	mm.releaseBudget(ctx)

	if mm.maxBytesHist != nil && mm.mu.maxAllocated > 0 {
		// TODO(knz) We record the logarithm because the UI doesn't know
		// how to do logarithmic y-axes yet. See the explanatory comments
		// in sql/mem_metrics.go.
		val := int64(1000 * math.Log(float64(mm.mu.maxAllocated)) / math.Ln10)
		mm.maxBytesHist.RecordValue(val)
	}

	// Disable the pool for further allocations, so that further
	// uses outside of monitor control get errors.
	mm.pool = nil
}

// MemoryAccount tracks the cumulated allocations for one client of
// MemoryPool or MemoryMonitor. MemoryMonitor has an account
// to its pool; MemoryMonitor clients have an account to the
// monitor. This allows each client to release all the memory at once
// when it completes its work.
//
// See the comments in mem_usage.go for a fuller picture of how
// these accounts are used in CockroachDB.
type MemoryAccount struct {
	curAllocated int64
}

// OpenAccount creates a new empty account.
func (mm *MemoryMonitor) OpenAccount(_ context.Context, acc *MemoryAccount) {
	// TODO(knz): conditionally track accounts in the memory monitor
	// (#9122).
}

// OpenAndInitAccount creates a new account and pre-allocates some
// initial amount of memory.
func (mm *MemoryMonitor) OpenAndInitAccount(
	ctx context.Context, acc *MemoryAccount, initialAllocation int64,
) error {
	mm.OpenAccount(ctx, acc)
	return mm.GrowAccount(ctx, acc, initialAllocation)
}

// GrowAccount requests a new allocation in an account.
func (mm *MemoryMonitor) GrowAccount(
	ctx context.Context, acc *MemoryAccount, extraSize int64,
) error {
	if err := mm.reserveMemory(ctx, extraSize); err != nil {
		return err
	}
	acc.curAllocated += extraSize
	return nil
}

// CloseAccount releases all the cumulated allocations of an account at once.
func (mm *MemoryMonitor) CloseAccount(ctx context.Context, acc *MemoryAccount) {
	mm.releaseMemory(ctx, acc.curAllocated)
}

// ClearAccount releases all the cumulated allocations of an account at once
// and primes it for reuse.
func (mm *MemoryMonitor) ClearAccount(ctx context.Context, acc *MemoryAccount) {
	mm.releaseMemory(ctx, acc.curAllocated)
	acc.curAllocated = 0
}

// ShrinkAccount releases part of the cumulated allocations by the specified size.
func (mm *MemoryMonitor) ShrinkAccount(ctx context.Context, acc *MemoryAccount, delta int64) {
	if acc.curAllocated < delta {
		panic(fmt.Sprintf("%s: no memory in account to release, current %d, free %d",
			mm.name, acc.curAllocated, delta))
	}
	mm.releaseMemory(ctx, delta)
	acc.curAllocated -= delta
}

// ResizeItem requests a size change for an object already registered
// in an account. The reservation is not modified if the new allocation is
// refused, so that the caller can keep using the original item
// without an accounting error. This is better than calling ClearAccount
// then GrowAccount because if the Clear succeeds and the Grow fails
// the original item becomes invisible from the perspective of the
// monitor.
func (mm *MemoryMonitor) ResizeItem(
	ctx context.Context, acc *MemoryAccount, oldSize, newSize int64,
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

// reserveMemory declares an allocation to this monitor. An error is
// returned if the allocation is denied.
func (mm *MemoryMonitor) reserveMemory(ctx context.Context, x int64) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if mm.mu.curAllocated > mm.mu.curBudget.curAllocated+mm.reservedBytes-x {
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
	if mm.mu.curAllocated > noteworthyMemoryUsageBytes {
		// We only report changes in binary magnitude of the size.  This
		// is to limit the amount of log messages when a size blowup is
		// caused by many small allocations.
		if util.RoundUpPowerOfTwo(mm.mu.curAllocated) != util.RoundUpPowerOfTwo(mm.mu.curAllocated-x) {
			log.Infof(ctx, "%s: memory usage increases to %s (+%d)",
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

// releaseMemory releases memory previously successfully registered
// via reserveMemory().
func (mm *MemoryMonitor) releaseMemory(ctx context.Context, sz int64) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if mm.mu.curAllocated < sz {
		panic(fmt.Sprintf("%s: no memory to release, current %d, free %d",
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

// increaseBudget requests more memory from the pool.
func (mm *MemoryMonitor) increaseBudget(ctx context.Context, minExtra int64) error {
	// NB: mm.mu Already locked by reserveMemory().
	if mm.pool == nil {
		return errors.Errorf("%s: memory budget exceeded: %d bytes requested, %d bytes in budget",
			mm.name, minExtra, mm.reservedBytes)
	}
	minExtra = mm.roundSize(minExtra)
	log.VEventf(2, ctx, "%s: requesting %d bytes from the pool",
		mm.name, minExtra)

	return mm.pool.GrowAccount(ctx, &mm.mu.curBudget, minExtra)
}

// roundSize rounds its argument to the smallest greater or equal
// multiple of `poolAllocationSize`.
func (mm *MemoryMonitor) roundSize(sz int64) int64 {
	chunks := (sz + mm.poolAllocationSize - 1) / mm.poolAllocationSize
	return chunks * mm.poolAllocationSize
}

// releaseBudget relinquishes all the monitor's memory back to the
// pool.
func (mm *MemoryMonitor) releaseBudget(ctx context.Context) {
	// NB: mm.mu need not be locked here, as this is only called from StopMonitor().
	if mm.mu.curBudget.curAllocated == 0 {
		return
	}
	log.VEventf(2, ctx, "%s: releasing %d bytes to the pool", mm.name, mm.mu.curBudget.curAllocated)
	mm.pool.ClearAccount(ctx, &mm.mu.curBudget)
}

// adjustBudget ensures that the monitor does not keep much more
// memory reserved from the pool than it currently has allocated.
// Memory is relinquished when there are at least
// maxAllocatedButUnusedMemoryBlocks*poolAllocationSize bytes reserved
// but unallocated.
func (mm *MemoryMonitor) adjustBudget(ctx context.Context) {
	// NB: mm.mu Already locked by releaseMemory().
	margin := mm.poolAllocationSize * int64(maxAllocatedButUnusedMemoryBlocks)

	neededBytes := mm.mu.curAllocated
	if neededBytes <= mm.reservedBytes {
		neededBytes = 0
	} else {
		neededBytes = mm.roundSize(neededBytes - mm.reservedBytes)
	}
	if neededBytes <= mm.mu.curBudget.curAllocated-margin {
		mm.pool.ShrinkAccount(ctx, &mm.mu.curBudget, mm.mu.curBudget.curAllocated-neededBytes)
	}
}
