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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
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

	ioReader execinfra.IOReader

	// stopwatch keeps track of the amount of time the wrapped operator spent
	// doing work. Note that this will include all of the time that the operator's
	// inputs spent doing work - this will be corrected when stats are reported
	// in finalizeStats.
	stopwatch *timeutil.StopWatch

	// childStatsCollectors contains the stats collectors for all of the inputs
	// to the wrapped operator.
	childStatsCollectors []*VectorizedStatsCollector

	memMonitors  []*mon.BytesMonitor
	diskMonitors []*mon.BytesMonitor
}

var _ colexecbase.Operator = &VectorizedStatsCollector{}

// NewVectorizedStatsCollector creates a new VectorizedStatsCollector which
// wraps 'op' that corresponds to a component with either ProcessorID or
// StreamID 'id' (with 'idTagKey' distinguishing between the two). 'ioReader'
// is a component (either an operator or a wrapped processor) that performs
// IO reads that is present in the chain of operators rooted at 'op'.
func NewVectorizedStatsCollector(
	op colexecbase.Operator,
	ioReader execinfra.IOReader,
	id int32,
	idTagKey string,
	inputWatch *timeutil.StopWatch,
	memMonitors []*mon.BytesMonitor,
	diskMonitors []*mon.BytesMonitor,
	inputStatsCollectors []*VectorizedStatsCollector,
) *VectorizedStatsCollector {
	if inputWatch == nil {
		colexecerror.InternalError(errors.AssertionFailedf("input watch for VectorizedStatsCollector is nil"))
	}
	// ioTime indicates whether the time should be displayed as "IO time" on
	// the diagram.
	var ioTime bool
	if ioReader != nil {
		ioTime = true
		if _, isProcessor := ioReader.(execinfra.Processor); isProcessor {
			// We have a wrapped processor that performs IO reads. Most likely
			// it is a rowexec.joinReader, so we want to display "execution
			// time" and not "IO time". In the less likely case that it is a
			// wrapped rowexec.tableReader showing "execution time" is also
			// acceptable.
			ioTime = false
		}
	}
	return &VectorizedStatsCollector{
		Operator:             op,
		VectorizedStats:      execpb.VectorizedStats{ID: id, IO: ioTime},
		idTagKey:             idTagKey,
		ioReader:             ioReader,
		stopwatch:            inputWatch,
		memMonitors:          memMonitors,
		diskMonitors:         diskMonitors,
		childStatsCollectors: inputStatsCollectors,
	}
}

// Next is part of the Operator interface.
func (vsc *VectorizedStatsCollector) Next(ctx context.Context) coldata.Batch {
	var batch coldata.Batch
	vsc.stopwatch.Start()
	batch = vsc.Operator.Next(ctx)
	if batch.Length() > 0 {
		vsc.NumBatches++
		vsc.NumTuples += int64(batch.Length())
	}
	vsc.stopwatch.Stop()
	return batch
}

// finalizeStats records the time measured by the stop watch into the stats as
// well as the memory and disk usage.
func (vsc *VectorizedStatsCollector) finalizeStats() {
	vsc.Time = vsc.stopwatch.Elapsed()
	// Subtract the time spent in each of the child stats collectors, to produce
	// the amount of time that the wrapped operator spent doing work itself, not
	// including time spent waiting on its inputs.
	for _, statsCollectors := range vsc.childStatsCollectors {
		vsc.Time -= statsCollectors.stopwatch.Elapsed()
	}
	for _, memMon := range vsc.memMonitors {
		vsc.MaxAllocatedMem += memMon.MaximumBytes()
	}
	for _, diskMon := range vsc.diskMonitors {
		vsc.MaxAllocatedDisk += diskMon.MaximumBytes()
	}
	if vsc.ioReader != nil {
		vsc.BytesRead = vsc.ioReader.GetBytesRead()
	}
	if vsc.IO {
		// Note that vsc.IO is true only for ColBatchScans, and this is the
		// only case when we want to add the number of rows read (because the
		// wrapped joinReaders and tableReaders will add that statistic
		// themselves).
		vsc.RowsRead = vsc.ioReader.GetRowsRead()
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
		vsc.BytesRead = 0
	}
	tracing.SetSpanStats(span, &vsc.VectorizedStats)
	span.Finish()
}
