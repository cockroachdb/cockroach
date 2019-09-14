// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// VectorizedStatsCollector collects VectorizedStats on Operators.
//
// If two Operators are connected (i.e. one is an input to another), the
// corresponding VectorizedStatsCollectors are also "connected" by sharing a
// StopWatch.
type VectorizedStatsCollector struct {
	Operator
	NonExplainable
	execpb.VectorizedStats

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
		execerror.VectorizedInternalPanic("input watch for VectorizedStatsCollector is nil")
	}
	return &VectorizedStatsCollector{
		Operator:        op,
		VectorizedStats: execpb.VectorizedStats{ID: id, Stall: isStall},
		inputWatch:      inputWatch,
	}
}

// SetOutputWatch sets vsc.outputWatch to outputWatch. It is used to "connect"
// this VectorizedStatsCollector to the next one in the chain.
func (vsc *VectorizedStatsCollector) SetOutputWatch(outputWatch *timeutil.StopWatch) {
	vsc.outputWatch = outputWatch
}

// Next is part of Operator interface.
func (vsc *VectorizedStatsCollector) Next(ctx context.Context) coldata.Batch {
	if vsc.outputWatch != nil {
		// vsc.outputWatch is non-nil which means that this Operator is outputting
		// the batches into another one. In order to avoid double counting the time
		// actually spent in the current "input" Operator, we're stopping the stop
		// watch of the other "output" Operator before doing any computations here.
		vsc.outputWatch.Stop()
	}

	var batch coldata.Batch
	if vsc.VectorizedStats.Stall {
		// We're measuring stall time, so there are no inputs into the wrapped
		// Operator, and we need to start the stop watch ourselves.
		vsc.inputWatch.Start()
	}
	batch = vsc.Operator.Next(ctx)
	if batch.Length() > 0 {
		vsc.NumBatches++
		vsc.NumTuples += int64(batch.Length())
	}
	vsc.inputWatch.Stop()
	if vsc.outputWatch != nil {
		// vsc.outputWatch is non-nil which means that this Operator is outputting
		// the batches into another one. To allow for measuring the execution time
		// of that other Operator, we're starting the stop watch right before
		// returning batch.
		vsc.outputWatch.Start()
	}
	return batch
}

// FinalizeStats records the time measured by the stop watch into the stats.
func (vsc *VectorizedStatsCollector) FinalizeStats() {
	vsc.Time = vsc.inputWatch.Elapsed()
}
