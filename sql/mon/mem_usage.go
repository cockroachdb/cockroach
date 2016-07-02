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
	"bytes"
	"fmt"
	"math"
	"runtime"
	"strings"

	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/humanizeutil"
	"github.com/cockroachdb/cockroach/util/log"
	"golang.org/x/net/context"
)

// MemoryUsageMonitor defines and object that can track and limit
// memory usage by row data during the execution of SQL queries.
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

	// ctx carries the logging context.
	ctx context.Context
}

// ReserveMemory declares the intent to allocate a given number of
// bytes to this monitor. An error is returned if the intent is
// denied.
func (mm *MemoryUsageMonitor) ReserveMemory(x int64) error {
	// TODO(knz) This is here that a policy will restrict how large a
	// query can become.
	if mm.curAllocated > mm.maxAllocatedBudget-x {
		return fmt.Errorf("memory budget exceeded: %d requested, %s already allocated",
			x, humanizeutil.IBytes(mm.curAllocated))
	}
	mm.curAllocated += x
	mm.totalAllocated += x
	if mm.maxAllocated < mm.curAllocated {
		mm.maxAllocated = mm.curAllocated
	}

	// Report "large" queries to the log for further investigation.
	if mm.curAllocated > noteworthySQLQuerySize {
		if int(math.Log2(float64(mm.curAllocated))) !=
			int(math.Log2(float64(mm.curAllocated-x))) {
			log.Infof(mm.ctx, "memory usage increases to %s (+%d)",
				humanizeutil.IBytes(mm.curAllocated), x)
		}
	}

	if log.V(2) {
		log.Infof(mm.ctx, "now at %d (+%d) - %s", mm.curAllocated, x, getSmallTrace())
	}
	return nil
}

// ReleaseMemory releases memory previously successfully registered
// via ReserveMemory().
func (mm *MemoryUsageMonitor) ReleaseMemory(x int64) {
	if mm.curAllocated < x {
		log.Errorf(mm.ctx, "no memory to release, current %d, free %d", mm.curAllocated, x)
		panic("memory usage error, see details in log")
	}
	mm.curAllocated -= x
	if log.V(2) {
		log.Infof(mm.ctx, "now at %d bytes (-%d) - %s", mm.curAllocated, x, getSmallTrace())
	}
}

// StartMonitor begins a monitoring region. It (re-)initializes the
// query-specific counters. Then until StopMonitor() is called this
// monitor will use the provided context arguments for logging.
func (mm *MemoryUsageMonitor) StartMonitor(ctx context.Context, details interface{}) {
	mm.ctx = log.WithLogTag(ctx, "mon", details)
	mm.maxAllocated = 0
	mm.totalAllocated = 0
	// TODO(knz) this is where we will use a policy to set a maximum.
	mm.maxAllocatedBudget = math.MaxInt64

	if log.V(1) && mm.curAllocated != 0 {
		log.Infof(mm.ctx, "starts with %d bytes left over (%p)", mm.curAllocated, mm)
	}
}

// StopMonitor completes a monitoring region.
func (mm *MemoryUsageMonitor) StopMonitor() {
	if log.V(1) {
		log.Infof(mm.ctx, "memory usage %s max %s total %s",
			humanizeutil.IBytes(mm.curAllocated),
			humanizeutil.IBytes(mm.maxAllocated),
			humanizeutil.IBytes(mm.totalAllocated))
	}
	if mm.curAllocated != 0 && log.V(1) {
		log.Infof(mm.ctx, "leaves %d bytes over (%p)", mm.curAllocated, mm)
	}

	// Disable the budget for further allocations, so that further SQL
	// uses outside of monitor control get errors.
	mm.maxAllocatedBudget = 0
}

var noteworthySQLQuerySize = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_SQL_QUERY_SIZE", 10000)

func getSmallTrace() string {
	var buf bytes.Buffer
	for i := 0; i < 5; i++ {
		if i > 0 {
			buf.WriteByte(':')
		}
		pc, _, _, ok := runtime.Caller(i + 2)
		if ok {
			name := runtime.FuncForPC(pc).Name()
			// Skip "github.com/cockroachdb/cockroach/" at the beginning.
			if strings.HasPrefix(name, "github.com/cockroachdb/cockroach/") {
				name = name[37:]
			}
			buf.WriteString(name)
		} else {
			buf.WriteByte('?')
		}
	}
	return buf.String()
}
