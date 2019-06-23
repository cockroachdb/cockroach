// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execpb

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var _ tracing.SpanStats = &VectorizedStats{}
var _ distsqlpb.DistSQLSpanStats = &VectorizedStats{}

const (
	batchesOutputTagSuffix = "output.batches"
	tuplesOutputTagSuffix  = "output.tuples"
	selectivityTagSuffix   = "selectivity"
	stallTimeTagSuffix     = "time.stall"
	executionTimeTagSuffix = "time.execution"
)

// Stats is part of SpanStats interface.
func (vs *VectorizedStats) Stats() map[string]string {
	var timeSuffix string
	if vs.Stall {
		timeSuffix = stallTimeTagSuffix
	} else {
		timeSuffix = executionTimeTagSuffix
	}
	return map[string]string{
		batchesOutputTagSuffix: fmt.Sprintf("%d", vs.NumBatches),
		tuplesOutputTagSuffix:  fmt.Sprintf("%d", vs.NumTuples),
		selectivityTagSuffix:   fmt.Sprintf("%.2f", float64(vs.NumTuples)/float64(coldata.BatchSize*vs.NumBatches)),
		timeSuffix:             fmt.Sprintf("%v", vs.Time.Round(time.Microsecond)),
	}
}

const (
	batchesOutputQueryPlanSuffix = "batches output"
	tuplesOutputQueryPlanSuffix  = "tuples output"
	selectivityQueryPlanSuffix   = "selectivity"
	stallTimeQueryPlanSuffix     = "stall time"
	executionTimeQueryPlanSuffix = "execution time"
)

// StatsForQueryPlan is part of DistSQLSpanStats interface.
func (vs *VectorizedStats) StatsForQueryPlan() []string {
	var timeSuffix string
	if vs.Stall {
		timeSuffix = stallTimeQueryPlanSuffix
	} else {
		timeSuffix = executionTimeQueryPlanSuffix
	}
	return []string{
		fmt.Sprintf("%s: %d", batchesOutputQueryPlanSuffix, vs.NumBatches),
		fmt.Sprintf("%s: %d", tuplesOutputQueryPlanSuffix, vs.NumTuples),
		fmt.Sprintf("%s: %.2f", selectivityQueryPlanSuffix, float64(vs.NumTuples)/float64(coldata.BatchSize*vs.NumBatches)),
		fmt.Sprintf("%s: %v", timeSuffix, vs.Time.Round(time.Microsecond)),
	}
}
