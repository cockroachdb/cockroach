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
	"runtime"
	"strings"

	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/humanizeutil"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// noteworthyMemoryUsage is the minimum size tracked by a monitor
// before the monitor starts explicitly logging overall usage growth in the log.
var noteworthyMemoryUsage = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_MEMORY_USAGE", 10000)

// MemoryUsageMonitor defines an object that can track and limit
// memory usage by other CockroachDB components.
//
// This should be instantiated:
// - once per statement, to track SQL row memory usage;
// - once per transaction, to track write intents;
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

	// maxAllocatedBudget sets the allowable upper limit for allocations.
	// Set to MaxInt64 to indicate no limit.
	maxAllocatedBudget int64
}

// AllocationSpan tracks the cumulated allocations for one client of
// MemoryUsageMonitor. This allows a client to release all the memory
// at once when it completes its work.
type AllocationSpan struct {
	curAllocated int64
}

// CloseSpan releases all the cumulated allocations of a span at once.
func (mm *MemoryUsageMonitor) CloseSpan(span *AllocationSpan, ctx context.Context) {
	mm.releaseMemory(span.curAllocated, ctx)
}

// ResetSpan releases all the cumulated allocations of a span at once
// and primes it for reuse.
func (mm *MemoryUsageMonitor) ResetSpan(span *AllocationSpan, ctx context.Context) {
	mm.releaseMemory(span.curAllocated, ctx)
	span.curAllocated = 0
}

// ResetSpanAndAlloc releases all the cumulated allocations of a span
// at once and primes it for reuse, starting with a first allocation
// of the given size. The span is always closed even if the new
// allocation is refused.
func (mm *MemoryUsageMonitor) ResetSpanAndAlloc(
	span *AllocationSpan, newSize int64, ctx context.Context,
) error {
	mm.releaseMemory(span.curAllocated, ctx)
	if err := mm.reserveMemory(newSize, ctx); err != nil {
		return err
	}
	span.curAllocated = newSize
	return nil
}

// ResizeItem requests a size change for an object already registered
// in a span. Nothing is changed if the new allocation is refused.
func (mm *MemoryUsageMonitor) ResizeItem(
	span *AllocationSpan, oldSize, newSize int64, ctx context.Context,
) error {
	if err := mm.reserveMemory(newSize, ctx); err != nil {
		return err
	}
	mm.releaseMemory(oldSize, ctx)
	span.curAllocated += newSize - oldSize
	return nil
}

// ExtendSpan requests a new allocation in a span.
func (mm *MemoryUsageMonitor) ExtendSpan(
	span *AllocationSpan, extraSize int64, ctx context.Context,
) error {
	if err := mm.reserveMemory(extraSize, ctx); err != nil {
		return err
	}
	span.curAllocated += extraSize
	return nil
}

// reserveMemory declares the intent to allocate a given number of
// bytes to this monitor. An error is returned if the intent is
// denied.
func (mm *MemoryUsageMonitor) reserveMemory(x int64, ctx context.Context) error {
	// TODO(knz): This is here that a policy will restrict how large a
	// query can become.
	if mm.curAllocated > mm.maxAllocatedBudget-x {
		err := errors.Errorf("memory budget exceeded: %d requested, %s already allocated",
			x, humanizeutil.IBytes(mm.curAllocated))
		if log.V(2) {
			log.Errorf(ctx, "%v", err)
		}
		return err
	}
	mm.curAllocated += x
	mm.totalAllocated += x
	if mm.maxAllocated < mm.curAllocated {
		mm.maxAllocated = mm.curAllocated
	}

	// Report "large" queries to the log for further investigation.
	if mm.curAllocated > noteworthyMemoryUsage {
		if int(math.Log2(float64(mm.curAllocated))) !=
			int(math.Log2(float64(mm.curAllocated-x))) {
			log.Infof(ctx, "memory usage increases to %s (+%d)",
				humanizeutil.IBytes(mm.curAllocated), x)
		}
	}

	if log.V(2) {
		log.Infof(ctx, "now at %d bytes (+%d) - %s", mm.curAllocated, x, getSmallTrace(2))
	}
	return nil
}

// releaseMemory releases memory previously successfully registered
// via reserveMemory().
func (mm *MemoryUsageMonitor) releaseMemory(x int64, ctx context.Context) {
	if mm.curAllocated < x {
		panic(fmt.Sprintf("no memory to release, current %d, free %d", mm.curAllocated, x))
	}
	mm.curAllocated -= x

	if log.V(2) {
		log.Infof(ctx, "now at %d bytes (-%d) - %s", mm.curAllocated, x, getSmallTrace(2))
	}
}

// StartMonitor begins a monitoring region. It (re-)initializes the
// context-specific counters. Then until StopMonitor() is called this
// monitor will use the provided context arguments for logging.
func (mm *MemoryUsageMonitor) StartMonitor() {
	if mm.curAllocated != 0 {
		panic(fmt.Sprintf("monitor started with %d bytes left over", mm.curAllocated))
	}
	mm.curAllocated = 0
	mm.maxAllocated = 0
	mm.totalAllocated = 0
	// TODO(knz): this is where we will use a policy to set a maximum.
	mm.maxAllocatedBudget = math.MaxInt64
}

// StopMonitor completes a monitoring region.
func (mm *MemoryUsageMonitor) StopMonitor(ctx context.Context) {
	if log.V(1) {
		log.Infof(ctx, "memory usage max %s total %s",
			humanizeutil.IBytes(mm.maxAllocated),
			humanizeutil.IBytes(mm.totalAllocated))
	}

	if mm.curAllocated != 0 {
		panic(fmt.Sprintf("unexpected leftover memory: %d bytes", mm.curAllocated))
	}

	// Disable the budget for further allocations, so that further SQL
	// uses outside of monitor control get errors.
	mm.maxAllocatedBudget = 0
}

// getTop5Callers populates an array with the names of the topmost 5
// caller functions in the stack after skipping a given number of
// frames. We use this to provide context to allocations in the logs
// with high verbosity.
func getTop5Callers(callers *[5]string, skip int) {
	for i := 0; i < 5; i++ {
		pc, _, _, ok := runtime.Caller(i + skip + 1)
		if !ok {
			break
		}
		name := runtime.FuncForPC(pc).Name()
		const crl = "github.com/cockroachdb/cockroach/"
		if strings.HasPrefix(name, crl) {
			name = name[len(crl):]
		}
		callers[i] = name
	}
}

// getSmallTrace produces a ":"-separated single line containing the
// topmost 5 callers from a given skip level.
func getSmallTrace(skip int) string {
	var callers [5]string
	getTop5Callers(&callers, skip+1)
	return strings.Join(callers[0:], ":")
}
