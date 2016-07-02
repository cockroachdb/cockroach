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

// noteworthyMemoryUsageBytes is the minimum size tracked by a monitor
// before the monitor starts explicitly logging overall usage growth in the log.
var noteworthyMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_MEMORY_USAGE", 10000)

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

	// maxAllocatedBudget sets the allowable upper limit for allocations.
	// Set to MaxInt64 to indicate no limit.
	maxAllocatedBudget int64
}

// MemoryAccount tracks the cumulated allocations for one client of
// MemoryUsageMonitor. This allows a client to release all the memory
// at once when it completes its work.
type MemoryAccount struct {
	curAllocated int64
}

// CloseAccount releases all the cumulated allocations of an account at once.
func (mm *MemoryUsageMonitor) CloseAccount(ctx context.Context, acc *MemoryAccount) {
	mm.releaseMemory(ctx, acc.curAllocated)
}

// ClearAccount releases all the cumulated allocations of an account at once
// and primes it for reuse.
func (mm *MemoryUsageMonitor) ClearAccount(ctx context.Context, acc *MemoryAccount) {
	mm.releaseMemory(ctx, acc.curAllocated)
	acc.curAllocated = 0
}

// ClearAccountAndAlloc releases all the cumulated allocations of an account
// at once and primes it for reuse, starting with a first allocation
// of the given size. The account is always closed even if the new
// allocation is refused.
func (mm *MemoryUsageMonitor) ClearAccountAndAlloc(
	ctx context.Context, acc *MemoryAccount, newSize int64,
) error {
	mm.releaseMemory(ctx, acc.curAllocated)
	if err := mm.reserveMemory(ctx, newSize); err != nil {
		return err
	}
	acc.curAllocated = newSize
	return nil
}

// ResizeItem requests a size change for an object already registered
// in an account. The reservation is not modified if the new allocation is
// refused, so that the caller can keep using the original item
// without an accounting error. This is better than calling ClearAccount
// then GrowAccount because if the Clear succeeds and the Grow fails
// the original item becomes invisible from the perspective of the
// monitor.
func (mm *MemoryUsageMonitor) ResizeItem(
	ctx context.Context, acc *MemoryAccount, oldSize, newSize int64,
) error {
	if err := mm.reserveMemory(ctx, newSize); err != nil {
		return err
	}
	mm.releaseMemory(ctx, oldSize)
	acc.curAllocated += newSize - oldSize
	return nil
}

// GrowAccount requests a new allocation in an account.
func (mm *MemoryUsageMonitor) GrowAccount(
	ctx context.Context, acc *MemoryAccount, extraSize int64,
) error {
	if err := mm.reserveMemory(ctx, extraSize); err != nil {
		return err
	}
	acc.curAllocated += extraSize
	return nil
}

// magnitude returns the first power of 2 greater or equal to the number.
// Source: http://graphics.stanford.edu/%7Eseander/bithacks.html#RoundUpPowerOf2
func magnitude(x int64) int64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	return x + 1
}

// reserveMemory declares the intent to allocate a given number of
// bytes to this monitor. An error is returned if the intent is
// denied.
func (mm *MemoryUsageMonitor) reserveMemory(ctx context.Context, x int64) error {
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
	if mm.curAllocated > noteworthyMemoryUsageBytes {
		// We only report changes in binary magnitude of the size.  This
		// is to limit the amount of log messages when a size blowup is
		// caused by many small allocations.
		if magnitude(mm.curAllocated) != magnitude(mm.curAllocated-x) {
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
func (mm *MemoryUsageMonitor) releaseMemory(ctx context.Context, x int64) {
	if mm.curAllocated < x {
		panic(fmt.Sprintf("no memory to release, current %d, free %d", mm.curAllocated, x))
	}
	mm.curAllocated -= x

	if log.V(2) {
		log.Infof(ctx, "now at %d bytes (-%d) - %s", mm.curAllocated, x, getSmallTrace(2))
	}
}

// StartMonitor begins a monitoring region.
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
	var pc [5]uintptr
	nCallers := runtime.Callers(skip+2, pc[:len(pc)-1])
	for i := 0; i < nCallers; i++ {
		name := runtime.FuncForPC(pc[i]).Name()
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
