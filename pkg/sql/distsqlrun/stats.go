// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package distsqlrun

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
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
func (isc *InputStatCollector) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
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
	bytesReadTagSuffix = "bytes.read"
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
	bytesReadQueryPlanSuffix = "bytes read"
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
