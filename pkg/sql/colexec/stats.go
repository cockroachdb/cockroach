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
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats/execstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// VectorizedStatsCollector exists so that the vectorizedStatsCollectorsQueue
// in the colflow.vectorizedFlowCreator can hold both
// VectorizedStatsCollectorBase and NetworkVectorizedStatsCollector types.
type VectorizedStatsCollector interface {
	OutputStats(ctx context.Context, flowID string, deterministicStats bool)
}

// ChildStatsCollector gives access to the stopwatches of a
// VectorizedStatsCollector's childStatsCollectors.
type ChildStatsCollector interface {
	getElapsedTime() time.Duration
}

// VectorizedStatsCollectorBase collects VectorizedStats on Operators.
type VectorizedStatsCollectorBase struct {
	colexecbase.Operator
	NonExplainable
	idTagKey string

	stats execstatspb.OperatorStats
	// If ioTime is true, time stats are recorded in stats.IOTime, and in
	// stats.ExecTime otherwise.
	ioTime bool

	ioReader execinfra.IOReader

	// stopwatch keeps track of the amount of time the wrapped operator spent
	// doing work. Note that this will include all of the time that the operator's
	// inputs spent doing work - this will be corrected when stats are reported
	// in finalizeStats.
	stopwatch *timeutil.StopWatch

	// childStatsCollectors contains the stats collectors for all of the inputs
	// to the wrapped operator.
	childStatsCollectors []ChildStatsCollector

	memMonitors  []*mon.BytesMonitor
	diskMonitors []*mon.BytesMonitor
}

// NetworkVectorizedStatsCollector collects VectorizedInboxStats on Inbox.
type NetworkVectorizedStatsCollector struct {
	*VectorizedStatsCollectorBase
}

var _ colexecbase.Operator = &VectorizedStatsCollectorBase{}
var _ colexecbase.Operator = &NetworkVectorizedStatsCollector{}

// initVectorizedStatsCollectorBase initializes the common fields
// of VectorizedStatsCollectorBase for all VectorizedStatsCollectors.
func initVectorizedStatsCollectorBase(
	op colexecbase.Operator,
	ioReader execinfra.IOReader,
	id int32,
	idTagKey string,
	inputWatch *timeutil.StopWatch,
) *VectorizedStatsCollectorBase {
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

	return &VectorizedStatsCollectorBase{
		Operator: op,
		ioTime:   ioTime,
		stats: execstatspb.OperatorStats{
			OperatorID: id,
		},
		idTagKey:  idTagKey,
		ioReader:  ioReader,
		stopwatch: inputWatch,
	}
}

// NewVectorizedStatsCollectorBase creates a new VectorizedStatsCollectorBase
// which wraps 'op' that corresponds to a component with either ProcessorID or
// StreamID 'id' (with 'idTagKey' distinguishing between the two). 'ioReader'
// is a component (either an operator or a wrapped processor) that performs
// IO reads that is present in the chain of operators rooted at 'op'.
func NewVectorizedStatsCollectorBase(
	op colexecbase.Operator,
	ioReader execinfra.IOReader,
	id int32,
	idTagKey string,
	inputWatch *timeutil.StopWatch,
	memMonitors []*mon.BytesMonitor,
	diskMonitors []*mon.BytesMonitor,
	inputStatsCollectors []ChildStatsCollector,
) *VectorizedStatsCollectorBase {
	vsc := initVectorizedStatsCollectorBase(op, ioReader, id, idTagKey, inputWatch)

	// TODO(cathymw): Refactor to have specialized stats collectors for
	// memory/disk stats and IO operators.
	vsc.memMonitors = memMonitors
	vsc.diskMonitors = diskMonitors
	vsc.childStatsCollectors = inputStatsCollectors
	return vsc
}

// NewNetworkVectorizedStatsCollector creates a new VectorizedStatsCollector
// for streams. In addition to the base stats, NewNetworkVectorizedStatsCollector
// collects the network latency for a stream.
func NewNetworkVectorizedStatsCollector(
	op colexecbase.Operator,
	ioReader execinfra.IOReader,
	id int32,
	idTagKey string,
	inputWatch *timeutil.StopWatch,
	latency int64,
) *NetworkVectorizedStatsCollector {
	vscBase := initVectorizedStatsCollectorBase(op, ioReader, id, idTagKey, inputWatch)
	return &NetworkVectorizedStatsCollector{
		VectorizedStatsCollectorBase: vscBase,
	}
}

// Next is part of the Operator interface.
func (vsc *VectorizedStatsCollectorBase) Next(ctx context.Context) coldata.Batch {
	var batch coldata.Batch
	vsc.stopwatch.Start()
	batch = vsc.Operator.Next(ctx)
	if batch.Length() > 0 {
		vsc.stats.NumBatches.Add(1)
		vsc.stats.NumTuples.Add(int64(batch.Length()))
	}
	vsc.stopwatch.Stop()
	return batch
}

// finalizeStats records the time measured by the stop watch into the stats as
// well as the memory and disk usage.
func (vsc *VectorizedStatsCollectorBase) finalizeStats() {
	tm := vsc.stopwatch.Elapsed()
	// Subtract the time spent in each of the child stats collectors, to produce
	// the amount of time that the wrapped operator spent doing work itself, not
	// including time spent waiting on its inputs.
	for _, statsCollectors := range vsc.childStatsCollectors {
		tm -= statsCollectors.getElapsedTime()
	}
	if vsc.ioTime {
		vsc.stats.IOTime = tm
		// Note that ioTime is true only for ColBatchScans, and this is the
		// only case when we want to add the number of rows read (because the
		// wrapped joinReaders and tableReaders will add that statistic
		// themselves).
		vsc.stats.RowsRead.Set(uint64(vsc.ioReader.GetRowsRead()))
	} else {
		vsc.stats.ExecTime = tm
	}
	for _, memMon := range vsc.memMonitors {
		vsc.stats.MaxAllocatedMem.Add(memMon.MaximumBytes())
	}
	for _, diskMon := range vsc.diskMonitors {
		vsc.stats.MaxAllocatedDisk.Add(diskMon.MaximumBytes())
	}
	if vsc.ioReader != nil {
		vsc.stats.BytesRead.Set(uint64(vsc.ioReader.GetBytesRead()))
	}
}

func (vsc *VectorizedStatsCollectorBase) createSpan(
	ctx context.Context, flowID string,
) *tracing.Span {
	// We're creating a new span for every component setting the appropriate
	// tag so that it is displayed correctly on the flow diagram.
	// TODO(yuzefovich): these spans are created and finished right away which
	// is not the way they are supposed to be used, so this should be fixed.
	_, span := tracing.ChildSpan(ctx, fmt.Sprintf("%T", vsc.Operator))
	span.SetTag(execinfrapb.FlowIDTagKey, flowID)
	span.SetTag(vsc.idTagKey, vsc.stats.OperatorID)
	return span
}

// clearNonDeterministicStats clears non-deterministic stats collected by vsc to
// make tests deterministic.
func (vsc *VectorizedStatsCollectorBase) clearNonDeterministicStats() {
	vsc.stats.IOTime = 0
	vsc.stats.ExecTime = 0
	vsc.stats.MaxAllocatedMem.Clear()
	vsc.stats.MaxAllocatedDisk.Clear()
	vsc.stats.NumBatches.Clear()
	// BytesRead is overridden to a useful value for tests.
	vsc.stats.BytesRead.Set(8 * vsc.stats.NumTuples.Value())
	vsc.stats.NetworkLatency = 0
}

// OutputStats outputs the vectorized stats collected by vsc into ctx.
func (vsc *VectorizedStatsCollectorBase) OutputStats(
	ctx context.Context, flowID string, deterministicStats bool,
) {
	if vsc.stats.OperatorID < 0 {
		// Ignore this stats collector since it is not associated with any
		// component.
		return
	}
	span := vsc.createSpan(ctx, flowID)
	vsc.finalizeStats()
	if deterministicStats {
		vsc.clearNonDeterministicStats()
	}
	span.SetSpanStats(&vsc.stats)
	span.Finish()
}

// OutputStats outputs the vectorized stats collected by nvsc into ctx.
func (nvsc *NetworkVectorizedStatsCollector) OutputStats(
	ctx context.Context, flowID string, deterministicStats bool,
) {
	if nvsc.stats.OperatorID < 0 {
		// Ignore this stats collector since it is not associated with any
		// component.
		return
	}
	span := nvsc.createSpan(ctx, flowID)
	nvsc.finalizeStats()
	if deterministicStats {
		nvsc.clearNonDeterministicStats()
	}
	span.SetSpanStats(&nvsc.stats)
	span.Finish()
}

// getElapsedTime is a getter that returns the elapsed time in
// childStatsCollectors.
func (vsc *VectorizedStatsCollectorBase) getElapsedTime() time.Duration {
	return vsc.stopwatch.Elapsed()
}
