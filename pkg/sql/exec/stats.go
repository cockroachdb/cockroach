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

package exec

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// VectorizedStatsCollector collects VectorizedStats on Operators.
//
// If two Operators are connected (i.e. one is an input to another), the
// corresponding VectorizedStatsCollectors are also "connected" by sharing a
// StopWatch.
type VectorizedStatsCollector struct {
	Operator
	VectorizedStats

	// inputWatch is a single stop watch that is shared with all the input
	// Operators. If the Operator doesn't have any inputs (like colBatchScan),
	// it is not shared with anyone. It is used by the wrapped Operator to
	// measure its stall or execution time.
	inputWatch *timeutil.StopWatch
	// outputWatch is a stop watch that is shared with the Operator that the
	// wrapped Operator is feeding into. It must be started right before
	// returning a batch when Nexted. It is used by the "output" Operator.
	outputWatch *timeutil.StopWatch
}

var _ Operator = &VectorizedStatsCollector{}

// NewVectorizedStatsCollector creates a new VectorizedStatsCollector which
// wraps op that corresponds to a processor with ProcessorID id. isStall
// indicates whether stall or execution time is being measured. inputWatch must
// be non-nil.
func NewVectorizedStatsCollector(
	op Operator, id int32, isStall bool, inputWatch *timeutil.StopWatch,
) *VectorizedStatsCollector {
	if inputWatch == nil {
		panic("input watch for VectorizedStatsCollector is nil")
	}
	return &VectorizedStatsCollector{
		Operator:        op,
		VectorizedStats: VectorizedStats{Id: id, Stall: isStall},
		inputWatch:      inputWatch,
	}
}

// SetOutputWatch vsc.outputWatch to outputWatch. It is used to "connect" this
// VectorizedStatsCollector to the next one in the chain.
func (vsc *VectorizedStatsCollector) SetOutputWatch(outputWatch *timeutil.StopWatch) {
	vsc.outputWatch = outputWatch
}

// Next is part of Operator interface.
func (vsc *VectorizedStatsCollector) Next() coldata.Batch {
	var batch coldata.Batch
	if vsc.VectorizedStats.Stall {
		// We're measuring stall time, so there are no inputs into the wrapped
		// Operator, and we need to start the stop watch ourselves.
		vsc.inputWatch.Start()
		batch = vsc.Operator.Next()
		vsc.Time += vsc.inputWatch.Stop()
	} else {
		// We're measuring execution time, so there is at least one input into the
		// wrapped Operator, so the stop watch must have already been started.
		batch = vsc.Operator.Next()
		vsc.Time += vsc.inputWatch.Stop()
	}
	if batch.Length() > 0 {
		vsc.NumBatches++
		vsc.NumInputTuples += int64(batch.Length())
	}
	sel := batch.Selection()
	if sel == nil {
		vsc.NumOutputTuples += int64(batch.Length())
	} else {
		vsc.NumOutputTuples += int64(len(sel))
	}
	if vsc.outputWatch != nil {
		vsc.outputWatch.Start()
	}
	return batch
}

var _ tracing.SpanStats = &VectorizedStats{}
var _ distsqlpb.DistSQLSpanStats = &VectorizedStats{}

const (
	batchesReadTagSuffix   = "input.batches"
	tuplesReadTagSuffix    = "input.tuples"
	selectivityTagSuffix   = "selectivity"
	stallTimeTagSuffix     = "stalltime"
	executionTimeTagSuffix = "executiontime"
)

// Stats is part of SpanStats interface.
func (vs *VectorizedStats) Stats() map[string]string {
	var suffix string
	if vs.Stall {
		suffix = stallTimeTagSuffix
	} else {
		suffix = executionTimeTagSuffix
	}
	return map[string]string{
		batchesReadTagSuffix: fmt.Sprintf("%d", vs.NumBatches),
		tuplesReadTagSuffix:  fmt.Sprintf("%d", vs.NumInputTuples),
		selectivityTagSuffix: fmt.Sprintf("%.2f", float64(vs.NumInputTuples)/float64(vs.NumOutputTuples)),
		suffix:               fmt.Sprintf("%v", vs.Time.Round(time.Microsecond)),
	}
}

const (
	batchesReadQueryPlanSuffix   = "batches read"
	tuplesReadQueryPlanSuffix    = "tuples read"
	selectivityQueryPlanSuffix   = "selectivity"
	stallTimeQueryPlanSuffix     = "stall time"
	executionTimeQueryPlanSuffix = "execution time"
)

// StatsForQueryPlan is part of DistSQLSpanStats interface.
func (vs *VectorizedStats) StatsForQueryPlan() []string {
	var suffix string
	if vs.Stall {
		suffix = stallTimeQueryPlanSuffix
	} else {
		suffix = executionTimeQueryPlanSuffix
	}
	return []string{
		fmt.Sprintf("%s: %d", batchesReadQueryPlanSuffix, vs.NumBatches),
		fmt.Sprintf("%s: %d", tuplesReadQueryPlanSuffix, vs.NumInputTuples),
		fmt.Sprintf("%s: %.2f", selectivityQueryPlanSuffix, float64(vs.NumInputTuples)/float64(vs.NumOutputTuples)),
		fmt.Sprintf("%s: %v", suffix, vs.Time.Round(time.Microsecond)),
	}
}
