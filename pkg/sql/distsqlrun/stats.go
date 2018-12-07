// Copyright 2018 The Cockroach Authors.
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

package distsqlrun

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// InputStatCollector wraps a RowSource and collects stats from it.
type InputStatCollector struct {
	RowSource
	InputStats
}

var _ RowSource = &InputStatCollector{}

// NewInputStatCollector creates a new InputStatCollector that wraps the given
// input.
func NewInputStatCollector(input RowSource) *InputStatCollector {
	return &InputStatCollector{RowSource: input}
}

// Next implements the RowSource interface. It calls Next on the embedded
// RowSource and collects stats.
func (isc *InputStatCollector) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	start := timeutil.Now()
	row, meta := isc.RowSource.Next()
	if row != nil {
		isc.NumRows++
	}
	isc.StallTime += timeutil.Since(start)
	return row, meta
}

const (
	rowsReadTagSuffix  = "input.rows"
	stallTimeTagSuffix = "stalltime"
	maxMemoryTagSuffix = "mem.max"
	maxDiskTagSuffix   = "disk.max"
)

// Stats is a utility method that returns a map of the InputStats` stats to
// output to a trace as tags. The given prefix is prefixed to the keys.
func (is InputStats) Stats(prefix string) map[string]string {
	return map[string]string{
		prefix + rowsReadTagSuffix:  fmt.Sprintf("%d", is.NumRows),
		prefix + stallTimeTagSuffix: fmt.Sprintf("%v", is.RoundStallTime()),
	}
}

const (
	rowsReadQueryPlanSuffix  = "rows read"
	stallTimeQueryPlanSuffix = "stall time"
	maxMemoryQueryPlanSuffix = "max memory used"
	maxDiskQueryPlanSuffix   = "max disk used"
)

// StatsForQueryPlan is a utility method that returns a list of the InputStats'
// stats to output on a query plan. The given prefix is prefixed to each element
// in the returned list.
func (is InputStats) StatsForQueryPlan(prefix string) []string {
	return []string{
		fmt.Sprintf("%s%s: %d", prefix, rowsReadQueryPlanSuffix, is.NumRows),
		fmt.Sprintf("%s%s: %v", prefix, stallTimeQueryPlanSuffix, is.RoundStallTime()),
	}
}

// RoundStallTime returns the InputStats' StallTime rounded to the nearest
// time.Millisecond.
func (is InputStats) RoundStallTime() time.Duration {
	return is.StallTime.Round(time.Microsecond)
}
