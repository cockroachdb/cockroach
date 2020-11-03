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
	childStatsCollectors []ChildStatsCollector

	memMonitors  []*mon.BytesMonitor
	diskMonitors []*mon.BytesMonitor
}

// NetworkVectorizedStatsCollector collects VectorizedInboxStats on Inbox.
type NetworkVectorizedStatsCollector struct {
	*VectorizedStatsCollectorBase
	execpb.VectorizedInboxStats
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
		Operator:        op,
		VectorizedStats: execpb.VectorizedStats{ID: id, IO: ioTime},
		idTagKey:        idTagKey,
		ioReader:        ioReader,
		stopwatch:       inputWatch,
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
		VectorizedInboxStats: execpb.VectorizedInboxStats{
			BaseVectorizedStats: &vscBase.VectorizedStats,
			NetworkLatency:      latency,
		},
	}
}

// Next is part of the Operator interface.
func (vsc *VectorizedStatsCollectorBase) Next(ctx context.Context) coldata.Batch {
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
func (vsc *VectorizedStatsCollectorBase) finalizeStats() {
	vsc.Time = vsc.stopwatch.Elapsed()
	// Subtract the time spent in each of the child stats collectors, to produce
	// the amount of time that the wrapped operator spent doing work itself, not
	// including time spent waiting on its inputs.
	for _, statsCollectors := range vsc.childStatsCollectors {
		vsc.Time -= statsCollectors.getElapsedTime()
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

func (vsc *VectorizedStatsCollectorBase) createSpan(
	ctx context.Context, flowID string,
) *tracing.Span {
	// We're creating a new span for every component setting the appropriate
	// tag so that it is displayed correctly on the flow diagram.
	// TODO(yuzefovich): these spans are created and finished right away which
	// is not the way they are supposed to be used, so this should be fixed.
	_, span := tracing.ChildSpan(ctx, fmt.Sprintf("%T", vsc.Operator))
	span.SetTag(execinfrapb.FlowIDTagKey, flowID)
	span.SetTag(vsc.idTagKey, vsc.ID)
	return span
}

// setNonDeterministicStats sets non-deterministic stats collected by vsc to reduce
// non-determinism in tests.
func (vsc *VectorizedStatsCollectorBase) setNonDeterministicStats() {
	vsc.Time = 0
	vsc.MaxAllocatedMem = 0
	vsc.MaxAllocatedDisk = 0
	vsc.NumBatches = 0
	// BytesRead is overridden to a useful value for tests.
	vsc.BytesRead = 8 * vsc.NumTuples
}

// setNonDeterministicStats sets non-deterministic stats collected by nvsc to reduce
// non-determinism in tests.
func (nvsc *NetworkVectorizedStatsCollector) setNonDeterministicStats() {
	nvsc.VectorizedStatsCollectorBase.setNonDeterministicStats()
	nvsc.NetworkLatency = 0
}

// OutputStats outputs the vectorized stats collected by vsc into ctx.
func (vsc *VectorizedStatsCollectorBase) OutputStats(
	ctx context.Context, flowID string, deterministicStats bool,
) {
	if vsc.ID < 0 {
		// Ignore this stats collector since it is not associated with any
		// component.
		return
	}
	span := vsc.createSpan(ctx, flowID)
	vsc.finalizeStats()
	if deterministicStats {
		vsc.setNonDeterministicStats()
	}
	span.SetSpanStats(&vsc.VectorizedStats)
	span.Finish()
}

// OutputStats outputs the vectorized stats collected by nvsc into ctx.
func (nvsc *NetworkVectorizedStatsCollector) OutputStats(
	ctx context.Context, flowID string, deterministicStats bool,
) {
	if nvsc.ID < 0 {
		// Ignore this stats collector since it is not associated with any
		// component.
		return
	}
	span := nvsc.createSpan(ctx, flowID)
	nvsc.finalizeStats()
	if deterministicStats {
		nvsc.setNonDeterministicStats()
	}
	span.SetSpanStats(&nvsc.VectorizedInboxStats)
	span.Finish()
}

// getElapsedTime is a getter that returns the elapsed time in
// childStatsCollectors.
func (vsc *VectorizedStatsCollectorBase) getElapsedTime() time.Duration {
	return vsc.stopwatch.Elapsed()
}
