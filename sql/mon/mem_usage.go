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
	"golang.org/x/net/context"
)

// MemoryUsageMonitor defines an object that can track and limit
// memory usage by other CockroachDB components.
//
// This is currently used by the SQL session, see sql/session_mem_usage.go.
//
// The monitor must be set up via StartMonitor/StopMonitor just before
// and after a processing context.
// The various counters express sizes in bytes.
type MemoryUsageMonitor struct {
	// curAllocated tracks the current amount of memory allocated for
	// in-memory row storage.
	curAllocated int64

	// totalAllocated tracks the cumulated amount of memory allocated.
	totalAllocated int64

	// maxAllocated tracks the high water mark of allocations.
	maxAllocated int64

	// curBudget sets the allowable upper limit for allocations,
	// representing the budget pre-allocated at the pool.
	curBudget MemoryAccount

	// preBudget indicates how much memory was already reserved for this
	// monitor before it was instantiated.
	preBudget MemoryAccount

	// pool specifies where to send requests to increase or decrease
	// curBudget.
	pool *MemoryPool

	// poolAllocationSize specifies the allocation unit for requests to
	// the pool.
	poolAllocationSize int64
}

// noteworthyMemoryUsageBytes is the minimum size tracked by a monitor
// before the monitor starts explicitly logging overall usage growth in the log.
var noteworthyMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_MEMORY_USAGE", 10000)

// allocHysteresisFactor determines the maximum difference between the
// amount of memory used by a monitor and the amount of memory
// reserved at the upstream pool before the monitor relinquishes the
// memory back to the pool.  This is useful so that a monitor
// currently at the boundary of a block does not cause contention when
// accounts cause its allocation counter to grow and shrink slightly
// beyond and beneath an allocation block boundary.  The difference is
// expressed as a number of blocks of size `poolAllocationSize`.
var allocHysteresisFactor = envutil.EnvOrDefaultInt("COCKROACH_MEMORY_FREE_FACTOR", 10)

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
// allocHysteresisFactor*poolAllocationSize bytes reserved but
// unallocated.
func (mm *MemoryUsageMonitor) adjustBudget() {
	margin := mm.poolAllocationSize * int64(allocHysteresisFactor)

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
func (mm *MemoryUsageMonitor) releaseMemory(ctx context.Context, x int64) {
	if mm.curAllocated < x {
		panic(fmt.Sprintf("no memory to release, current %d, free %d", mm.curAllocated, x))
	}
	mm.curAllocated -= x
	mm.adjustBudget()

	if log.V(2) {
		log.Infof(ctx, "now at %d bytes (-%d) - %s", mm.curAllocated, x, util.GetSmallTrace(2))
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
