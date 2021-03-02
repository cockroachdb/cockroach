// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflow

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow/colrpc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// vectorizedStatsCollector is the common interface implemented by collectors.
type vectorizedStatsCollector interface {
	colexecop.Operator
	outputStats(ctx context.Context)
}

// childStatsCollector gives access to the stopwatches of a
// vectorizedStatsCollector's childStatsCollectors.
type childStatsCollector interface {
	getElapsedTime() time.Duration
}

// batchInfoCollector is a helper used by collector implementations.
//
// It wraps an Operator and keeps track of how much time was spent while calling
// Next on the underlying Operator and how many batches and tuples were
// returned.
type batchInfoCollector struct {
	colexecop.Operator
	colexecop.NonExplainable
	componentID execinfrapb.ComponentID

	numBatches, numTuples uint64

	// stopwatch keeps track of the amount of time the wrapped operator spent
	// doing work. Note that this will include all of the time that the operator's
	// inputs spent doing work - this will be corrected when stats are reported
	// in finish().
	stopwatch *timeutil.StopWatch

	// childStatsCollectors contains the stats collectors for all of the inputs
	// to the wrapped operator.
	childStatsCollectors []childStatsCollector
}

var _ colexecop.Operator = &batchInfoCollector{}

func makeBatchInfoCollector(
	op colexecop.Operator,
	id execinfrapb.ComponentID,
	inputWatch *timeutil.StopWatch,
	childStatsCollectors []childStatsCollector,
) batchInfoCollector {
	if inputWatch == nil {
		colexecerror.InternalError(errors.AssertionFailedf("input watch is nil"))
	}
	return batchInfoCollector{
		Operator:             op,
		componentID:          id,
		stopwatch:            inputWatch,
		childStatsCollectors: childStatsCollectors,
	}
}

// Next is part of the Operator interface.
func (bic *batchInfoCollector) Next(ctx context.Context) coldata.Batch {
	var batch coldata.Batch
	bic.stopwatch.Start()
	batch = bic.Operator.Next(ctx)
	if batch.Length() > 0 {
		bic.numBatches++
		bic.numTuples += uint64(batch.Length())
	}
	bic.stopwatch.Stop()
	return batch
}

// finish calculates the final statistics.
func (bic *batchInfoCollector) finish() (numBatches, numTuples uint64, time time.Duration) {
	tm := bic.stopwatch.Elapsed()
	// Subtract the time spent in each of the child stats collectors, to produce
	// the amount of time that the wrapped operator spent doing work itself, not
	// including time spent waiting on its inputs.
	for _, statsCollectors := range bic.childStatsCollectors {
		tm -= statsCollectors.getElapsedTime()
	}
	return bic.numBatches, bic.numTuples, tm
}

// getElapsedTime implements the childStatsCollector interface.
func (bic *batchInfoCollector) getElapsedTime() time.Duration {
	return bic.stopwatch.Elapsed()
}

// newVectorizedStatsCollector creates a vectorizedStatsCollector which wraps
// 'op' that corresponds to a component with either ProcessorID or StreamID 'id'
// (with 'idTagKey' distinguishing between the two). 'kvReader' is a component
// (either an operator or a wrapped processor) that performs KV reads that is
// present in the chain of operators rooted at 'op'.
func newVectorizedStatsCollector(
	op colexecop.Operator,
	kvReader execinfra.KVReader,
	id execinfrapb.ComponentID,
	inputWatch *timeutil.StopWatch,
	memMonitors []*mon.BytesMonitor,
	diskMonitors []*mon.BytesMonitor,
	inputStatsCollectors []childStatsCollector,
) vectorizedStatsCollector {
	// TODO(cathymw): Refactor to have specialized stats collectors for
	// memory/disk stats and IO operators.
	return &vectorizedStatsCollectorImpl{
		batchInfoCollector: makeBatchInfoCollector(op, id, inputWatch, inputStatsCollectors),
		kvReader:           kvReader,
		memMonitors:        memMonitors,
		diskMonitors:       diskMonitors,
	}
}

// vectorizedStatsCollectorImpl is the implementation behind
// newVectorizedStatsCollector.
type vectorizedStatsCollectorImpl struct {
	batchInfoCollector

	kvReader     execinfra.KVReader
	memMonitors  []*mon.BytesMonitor
	diskMonitors []*mon.BytesMonitor
}

// finish returns the collected stats.
func (vsc *vectorizedStatsCollectorImpl) finish() *execinfrapb.ComponentStats {
	numBatches, numTuples, time := vsc.batchInfoCollector.finish()

	s := &execinfrapb.ComponentStats{Component: vsc.componentID}

	for _, memMon := range vsc.memMonitors {
		s.Exec.MaxAllocatedMem.Add(memMon.MaximumBytes())
	}
	for _, diskMon := range vsc.diskMonitors {
		s.Exec.MaxAllocatedDisk.Add(diskMon.MaximumBytes())
	}

	// Depending on kvReader, the accumulated time spent by the wrapped operator
	// inside Next() is reported as either execution time or KV time.
	kvTime := false
	if vsc.kvReader != nil {
		kvTime = true
		if _, isProcessor := vsc.kvReader.(execinfra.Processor); isProcessor {
			// We have a wrapped processor that performs KV reads. Most likely
			// it is a rowexec.joinReader, so we want to display "execution
			// time" and not "KV time". In the less likely case that it is a
			// wrapped rowexec.tableReader showing "execution time" is also
			// acceptable.
			kvTime = false
		}
	}

	if kvTime {
		s.KV.KVTime.Set(time)
		// Note that kvTime is true only for ColBatchScans, and this is the
		// only case when we want to add the number of rows read (because the
		// wrapped joinReaders and tableReaders will add that statistic
		// themselves).
		s.KV.TuplesRead.Set(uint64(vsc.kvReader.GetRowsRead()))
		s.KV.BytesRead.Set(uint64(vsc.kvReader.GetBytesRead()))
		s.KV.ContentionTime.Set(vsc.kvReader.GetCumulativeContentionTime())
	} else {
		s.Exec.ExecTime.Set(time)
	}

	s.Output.NumBatches.Set(numBatches)
	s.Output.NumTuples.Set(numTuples)
	return s
}

// outputStats is part of the vectorizedStatsCollector interface.
func (vsc *vectorizedStatsCollectorImpl) outputStats(ctx context.Context) {
	s := vsc.finish()
	createStatsSpan(ctx, fmt.Sprintf("%T", vsc.Operator), s)
}

// newNetworkVectorizedStatsCollector creates a new vectorizedStatsCollector
// for streams. In addition to the base stats, newNetworkVectorizedStatsCollector
// collects the network latency for a stream.
func newNetworkVectorizedStatsCollector(
	op colexecop.Operator,
	id execinfrapb.ComponentID,
	inputWatch *timeutil.StopWatch,
	inbox *colrpc.Inbox,
	latency time.Duration,
) vectorizedStatsCollector {
	return &networkVectorizedStatsCollectorImpl{
		batchInfoCollector: makeBatchInfoCollector(op, id, inputWatch, nil /* childStatsCollectors */),
		inbox:              inbox,
		latency:            latency,
	}
}

// networkVectorizedStatsCollectorImpl is the implementation behind
// newNetworkVectorizedStatsCollector.
type networkVectorizedStatsCollectorImpl struct {
	batchInfoCollector

	inbox   *colrpc.Inbox
	latency time.Duration
}

// finish returns the collected stats.
func (nvsc *networkVectorizedStatsCollectorImpl) finish() *execinfrapb.ComponentStats {
	numBatches, numTuples, time := nvsc.batchInfoCollector.finish()

	s := &execinfrapb.ComponentStats{Component: nvsc.componentID}

	s.NetRx.Latency.Set(nvsc.latency)
	s.NetRx.WaitTime.Set(time)
	s.NetRx.DeserializationTime.Set(nvsc.inbox.GetDeserializationTime())
	s.NetRx.TuplesReceived.Set(uint64(nvsc.inbox.GetRowsRead()))
	s.NetRx.BytesReceived.Set(uint64(nvsc.inbox.GetBytesRead()))
	s.NetRx.MessagesReceived.Set(uint64(nvsc.inbox.GetNumMessages()))

	s.Output.NumBatches.Set(numBatches)
	s.Output.NumTuples.Set(numTuples)

	return s
}

// outputStats is part of the vectorizedStatsCollector interface.
func (nvsc *networkVectorizedStatsCollectorImpl) outputStats(ctx context.Context) {
	s := nvsc.finish()
	createStatsSpan(ctx, fmt.Sprintf("%T", nvsc.Operator), s)
}

func createStatsSpan(ctx context.Context, opName string, stats *execinfrapb.ComponentStats) {
	// We're creating a new span for every component setting the appropriate
	// tag so that it is displayed correctly on the flow diagram.
	// TODO(yuzefovich): these spans are created and finished right away which
	// is not the way they are supposed to be used, so this should be fixed.
	_, span := tracing.ChildSpan(ctx, opName)
	span.RecordStructured(stats)
	span.Finish()
}
