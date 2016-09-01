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

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/humanizeutil"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// MemoryPool, MemoryAccount and MemoryUsageMonitor together form the
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
//   MemoryUsageMonitor. To do this, each component maintains one or
//   more instances of MemoryAccount, one per "category" of
//   allocation, and issue requests to Grow, Resize or Close to their
//   monitor.  Grow/Resize requests can be denied (return an error),
//   which indicates the memory budget has been reached.
//
// - different instances of MemoryAccount are associated to different
//   usage categories in components, in principle to track different
//   object lifetimes.  Each account tracks the total amount of memory
//   allocated in that category and enables declaring all the memory
//   as released at once using Close() when all objects in that
//   category are released back to Go's heap.
//
// - MemoryUsageMonitor checks the total sum of allocations across
//   accounts, but also serves as endpoint for statistics. Therefore
//   each db client connection should use a separate monitor for its
//   allocations, so that statistics can be separated per connection.
//
// - MemoryUsageMonitor is not thread-safe by default, as its most
//   common use is by a single goroutine. For a thread-safe version,
//   see MemoryUsageMonitorWithMutex.
//
// - a MemoryUsageMonitor can be used standalone, and operate
//   independently from other monitors; however, since we also want to
//   constrain global memory usage across all connections, multiple
//   instance of MemoryUsageMonitors can coordinate with each other by
//   referring to a shared MemoryPool. When operating in that mode,
//   each MemoryUsageMonitor reports allocations declared to it by
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
//                                              +---- pool (shared)
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
//   admin server ---------------------------------------------------owns-- pool1 (shared)
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
//   executor --------------------------------------------------------owns-- pool2 (shared)
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
//   configured to be 1/4 of RAM size by default, and can be overridden
//   from the command-line.
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
// Finally, a simplified API is provided in session_mem_usage.go to
// simplify the interface offered to SQL components using memory
// accounts linked to the session-bound monitor.

// MemoryUsageMonitor defines an object that can track and limit
// memory usage by other CockroachDB components.
// The monitor must be set up via StartMonitor/StopMonitor before
// and after use.
// The various counters express sizes in bytes.
//
// This structure is not thread-safe, as its most common use is
// envisioned to be for a single goroutine. For a thread-safe version,
// see MemoryUsageMonitorWithMutex in mem_usage_mutex.go.
type MemoryUsageMonitor struct {
	// curAllocated tracks the current amount of memory allocated at
	// this monitor by its client components.
	curAllocated int64

	// totalAllocated tracks the cumulated amount of memory allocated
	// without taking releases into account. Used in monitoring.
	totalAllocated int64

	// maxAllocated tracks the high water mark of allocations.
	// Used for monitoring.
	maxAllocated int64

	// curBudget represents the budget pre-allocated at the pool, if
	// any.
	curBudget MemoryAccount

	// preBudget indicates how much memory was already reserved for this
	// monitor before it was instantiated. Allocations registered to
	// this monitor are first deducted from the preBudget. If there is
	// no pool, the preBudget determines the maximum allocation capacity
	// of this monitor. The preBudget is never released to the pool
	// by the monitor.
	preBudget MemoryAccount

	// pool specifies where to send requests to increase or decrease
	// curBudget. May be nil for a standalone monitor.
	pool *MemoryPool

	// poolAllocationSize specifies the allocation unit for requests to
	// the pool.
	poolAllocationSize int64
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

// roundSize rounds its argument to the smallest greater or equal
// multiple of `poolAllocationSize`.
func (mm *MemoryUsageMonitor) roundSize(sz int64) int64 {
	chunks := (sz + mm.poolAllocationSize - 1) / mm.poolAllocationSize
	return chunks * mm.poolAllocationSize
}

// increaseBudget requests more memory from the pool.
func (mm *MemoryUsageMonitor) increaseBudget(ctx context.Context, minExtra int64) error {
	if mm.pool == nil {
		return errors.Errorf("memory budget exceeded: %d bytes requested, %d bytes in budget",
			minExtra, mm.preBudget.curAllocated)
	}
	minExtra = mm.roundSize(minExtra)
	if log.V(2) {
		log.Infof(ctx, "requesting %d bytes from the pool", minExtra)
	}
	return mm.pool.growAccount(ctx, &mm.curBudget, minExtra)
}

// releaseBudget relinquishes all the monitor's memory back to the
// pool.
func (mm *MemoryUsageMonitor) releaseBudget(ctx context.Context) {
	if mm.curBudget.curAllocated == 0 {
		return
	}
	if log.V(2) {
		log.Infof(ctx, "releasing %d bytes to the pool", mm.curBudget.curAllocated)
	}
	mm.pool.closeAccount(&mm.curBudget)
}

// adjustBudget ensures that the monitor does not keep much more
// memory reserved from the pool than it currently has allocated.
// Memory is relinquished when there are at least
// maxAllocatedButUnusedMemoryBlocks*poolAllocationSize bytes reserved
// but unallocated.
func (mm *MemoryUsageMonitor) adjustBudget() {
	margin := mm.poolAllocationSize * int64(maxAllocatedButUnusedMemoryBlocks)

	neededBytes := mm.curAllocated
	if neededBytes <= mm.preBudget.curAllocated {
		neededBytes = 0
	} else {
		neededBytes = mm.roundSize(neededBytes - mm.preBudget.curAllocated)
	}
	if neededBytes <= mm.curBudget.curAllocated-margin {
		mm.pool.shrinkAccount(&mm.curBudget, neededBytes)
	}
}

// reserveMemory declares an allocation to this monitor. An error is
// returned if the allocation is denied.
func (mm *MemoryUsageMonitor) reserveMemory(ctx context.Context, x int64) error {
	if mm.curAllocated > mm.curBudget.curAllocated+mm.preBudget.curAllocated-x {
		if err := mm.increaseBudget(ctx, x); err != nil {
			return err
		}
	}
	mm.curAllocated += x
	mm.totalAllocated += x
	if mm.maxAllocated < mm.curAllocated {
		mm.maxAllocated = mm.curAllocated
	}

	// Report "large" queries to the log for further investigation.
	if mm.curAllocated > noteworthyMemoryUsageBytes {
		// We only report changes in binary magnitude of the size.  This
		// is to limit the amount of log messages when a size blowup is
		// caused by many small allocations.
		if util.RoundUpPowerOfTwo(mm.curAllocated) != util.RoundUpPowerOfTwo(mm.curAllocated-x) {
			log.Infof(ctx, "memory usage increases to %s (+%d)",
				humanizeutil.IBytes(mm.curAllocated), x)
		}
	}

	if log.V(2) {
		log.Infof(ctx, "now at %d bytes (+%d) - %s", mm.curAllocated, x, util.GetSmallTrace(2))
	}
	return nil
}

// releaseMemory releases memory previously successfully registered
// via reserveMemory().
func (mm *MemoryUsageMonitor) releaseMemory(ctx context.Context, sz int64) {
	if mm.curAllocated < sz {
		panic(fmt.Sprintf("no memory to release, current %d, free %d", mm.curAllocated, sz))
	}
	mm.curAllocated -= sz
	mm.adjustBudget()

	if log.V(2) {
		log.Infof(ctx, "now at %d bytes (-%d) - %s", mm.curAllocated, sz, util.GetSmallTrace(2))
	}
}

// StartMonitor begins a monitoring region.
func (mm *MemoryUsageMonitor) StartMonitor(pool *MemoryPool, preBudget MemoryAccount) {
	if mm.curAllocated != 0 {
		panic(fmt.Sprintf("monitor started with %d bytes left over", mm.curAllocated))
	}
	mm.pool = pool
	mm.poolAllocationSize = defaultPoolAllocationSize
	mm.curAllocated = 0
	mm.maxAllocated = 0
	mm.totalAllocated = 0
	mm.preBudget = preBudget
}

// StartUnlimitedMonitor starts the monitor in "detached" mode without
// a pool and without a maximum budget.
func (mm *MemoryUsageMonitor) StartUnlimitedMonitor() {
	mm.StartMonitor(nil, MemoryAccount{curAllocated: math.MaxInt64})
}

// StopMonitor completes a monitoring region.
func (mm *MemoryUsageMonitor) StopMonitor(ctx context.Context) {
	if log.V(1) {
		log.InfofDepth(ctx, 1, "memory usage max %s total %s",
			humanizeutil.IBytes(mm.maxAllocated),
			humanizeutil.IBytes(mm.totalAllocated))
	}

	if mm.curAllocated != 0 {
		panic(fmt.Sprintf("unexpected leftover memory: %d bytes", mm.curAllocated))
	}

	mm.releaseBudget(ctx)

	// Disable the pool for further allocations, so that further
	// uses outside of monitor control get errors.
	mm.pool = nil
}
