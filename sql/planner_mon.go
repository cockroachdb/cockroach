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

package sql

import (
	"bytes"
	"fmt"
	"math"
	"runtime"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/humanizeutil"
	"github.com/cockroachdb/cockroach/util/log"
)

type plannerMonitor struct {
	// curAllocated tracks the current amount of memory allocated for
	// in-memory row storage.
	curAllocated int64

	// totalAllocated tracks the cumulated amount of memory allocated.
	totalAllocated int64

	// maxAllocated tracks the high water mark of allocations.
	maxAllocated int64

	// lastStmt tracks the current/last executed SQL statement to
	// provide context in logging messages.
	lastStmt parser.Statement
}

func (pm *plannerMonitor) lastStmtString() string {
	if pm.lastStmt == nil {
		return "(unknown statement)"
	}
	return pm.lastStmt.String()
}

// ReserveMemory implements the parser.MemoryUsageMonitor interface.
func (p *planner) ReserveMemory(x int64) error {
	// TODO(knz) This is here that a policy will restrict how large a
	// query can become. For now we use a fixed threshold (1GB). It seems
	// large enough for reasonable workloads.
	if p.mon.curAllocated+x > 1000000000 {
		return fmt.Errorf("memory budget exceeded: %d requested, %s already allocated",
			x, humanizeutil.IBytes(p.mon.curAllocated))
	}
	p.mon.curAllocated += x
	p.mon.totalAllocated += x
	if p.mon.maxAllocated < p.mon.curAllocated {
		p.mon.maxAllocated = p.mon.curAllocated
	}

	// Report large queries to the log for further investigation.
	if p.mon.curAllocated > 10000 {
		if int(math.Log2(float64(p.mon.curAllocated))) !=
			int(math.Log2(float64(p.mon.curAllocated-x))) {
			log.Infof(p.ctx(), "[%s] memory usage increases to %s (+%d)",
				p.mon.lastStmtString(), humanizeutil.IBytes(p.mon.curAllocated), x)
		}
	}

	if log.V(2) {
		log.Infof(p.ctx(), "[%s] now at %d (+%d) - %s",
			p.mon.lastStmtString(), p.mon.curAllocated, x, getSmallTrace())
	}
	return nil
}

// ReleaseMemory implepments the parser.MemoryUsageMonitor interface.
func (p *planner) ReleaseMemory(x int64) {
	if p.mon.curAllocated < x {
		panic(fmt.Sprintf("[%s] no memory to release, current %d, free %d",
			p.mon.lastStmtString(), p.mon.curAllocated, x))
	}
	p.mon.curAllocated -= x
	if log.V(2) {
		log.Infof(p.ctx(), "[%s] now at %d bytes (-%d) - %s",
			p.mon.lastStmtString(), p.mon.curAllocated, x, getSmallTrace())
	}
}

func (p *planner) stopStatementMonitor() {
	if log.V(1) {
		log.Infof(p.ctx(), "memory usage %s max %s total %s by [%s]",
			humanizeutil.IBytes(p.mon.curAllocated),
			humanizeutil.IBytes(p.mon.maxAllocated),
			humanizeutil.IBytes(p.mon.totalAllocated),
			p.mon.lastStmtString())
	}
	p.mon.lastStmt = nil
}

func (p *planner) startStatementMonitor(stmt parser.Statement) {
	if log.V(1) && p.mon.curAllocated != 0 {
		prevStmt := ""
		if p.mon.lastStmt != nil {
			prevStmt = " from " + p.mon.lastStmtString()
		}
		log.Infof(p.ctx(), "[%s] starts with %d bytes left over%s",
			stmt.String(), p.mon.curAllocated, prevStmt)
	}
	p.mon.maxAllocated = 0
	p.mon.totalAllocated = 0
	p.mon.lastStmt = stmt
}

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
			buf.WriteString(name[37:])
		} else {
			buf.WriteByte('?')
		}
	}
	return buf.String()
}
