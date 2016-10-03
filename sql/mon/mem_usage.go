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
			log.Errorf(ctx, "%s - %s", err, util.GetSmallTrace(2))
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

	if log.V(2) {
		log.Infof(ctx, "now at %d bytes (-%d) - %s", mm.curAllocated, x, util.GetSmallTrace(2))
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
		log.InfofDepth(ctx, 1, "memory usage max %s total %s",
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
