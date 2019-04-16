// Copyright 2019 The Cockroach Authors.
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
