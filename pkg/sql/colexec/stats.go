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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// VectorizedStatsCollector collects VectorizedStats on Operators.
//
// If two Operators are connected (i.e. one is an input to another), the
// corresponding VectorizedStatsCollectors are also "connected" by sharing a
// StopWatch.
type VectorizedStatsCollector struct {
	colexecbase.Operator
	NonExplainable
	execpb.VectorizedStats
	idTagKey string

	// inputWatch is a single stop watch that is shared with all the input
	// Operators. If the Operator doesn't have any inputs (like colBatchScan),
	// it is not shared with anyone. It is used by the wrapped Operator to
	// measure its stall or execution time.
	inputWatch *timeutil.StopWatch
	// outputWatch is a stop watch that is shared with the Operator that the
	// wrapped Operator is feeding into. It must be started right before
	// returning a batch when Nexted. It is used by the "output" Operator.
	outputWatch *timeutil.StopWatch

	memMonitors  []*mon.BytesMonitor
	diskMonitors []*mon.BytesMonitor
}

var _ colexecbase.Operator = &VectorizedStatsCollector{}

// NewVectorizedStatsCollector creates a new VectorizedStatsCollector which
// wraps 'op' that corresponds to a component with either ProcessorID or
// StreamID 'id' (with 'idTagKey' distinguishing between the two). 'isStall'
// indicates whether stall or execution time is being measured. 'inputWatch'
// must be non-nil.
func NewVectorizedStatsCollector(
	op colexecbase.Operator,
	id int32,
	idTagKey string,
	isStall bool,
	inputWatch *timeutil.StopWatch,
	memMonitors []*mon.BytesMonitor,
	diskMonitors []*mon.BytesMonitor,
) *VectorizedStatsCollector {
	if inputWatch == nil {
		colexecerror.InternalError("input watch for VectorizedStatsCollector is nil")
	}
	return &VectorizedStatsCollector{
		Operator:        op,
		VectorizedStats: execpb.VectorizedStats{ID: id, Stall: isStall},
		idTagKey:        idTagKey,
		inputWatch:      inputWatch,
		memMonitors:     memMonitors,
		diskMonitors:    diskMonitors,
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

// finalizeStats records the time measured by the stop watch into the stats as
// well as the memory and disk usage.
func (vsc *VectorizedStatsCollector) finalizeStats() {
	vsc.Time = vsc.inputWatch.Elapsed()
	for _, memMon := range vsc.memMonitors {
		vsc.MaxAllocatedMem += memMon.MaximumBytes()
	}
	for _, diskMon := range vsc.diskMonitors {
		vsc.MaxAllocatedDisk += diskMon.MaximumBytes()
	}
}

// OutputStats outputs the vectorized stats collected by vsc into ctx.
func (vsc *VectorizedStatsCollector) OutputStats(
	ctx context.Context, flowID string, deterministicStats bool,
) {
	if vsc.ID < 0 {
		// Ignore this stats collector since it is not associated with any
		// component.
		return
	}
	// We're creating a new span for every component setting the appropriate
	// tag so that it is displayed correctly on the flow diagram.
	// TODO(yuzefovich): these spans are created and finished right away which
	// is not the way they are supposed to be used, so this should be fixed.
	_, span := tracing.ChildSpan(ctx, fmt.Sprintf("%T", vsc.Operator))
	span.SetTag(execinfrapb.FlowIDTagKey, flowID)
	span.SetTag(vsc.idTagKey, vsc.ID)
	vsc.finalizeStats()
	if deterministicStats {
		vsc.VectorizedStats.Time = 0
		vsc.MaxAllocatedMem = 0
		vsc.MaxAllocatedDisk = 0
		vsc.NumBatches = 0
	}
	tracing.SetSpanStats(span, &vsc.VectorizedStats)
	span.Finish()
}
