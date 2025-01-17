// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colflow

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow/colrpc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// childStatsCollector gives access to the stopwatches of a
// colexecop.VectorizedStatsCollector's childStatsCollectors.
type childStatsCollector interface {
	getElapsedTime() time.Duration
	getElapsedCPUTime() time.Duration
}

// batchInfoCollector is a helper used by collector implementations.
//
// It wraps a colexecop.Operator (inside of the colexecop.OneInputNode) and
// keeps track of how much time was spent while calling Next on the underlying
// Operator and how many batches and tuples were returned.
type batchInfoCollector struct {
	colexecop.OneInputNode
	colexecop.NonExplainable
	componentID execinfrapb.ComponentID

	mu struct {
		// We need a mutex because finishAndGetStats() and Next() might be
		// called from different goroutines.
		syncutil.Mutex
		initialized           bool
		numBatches, numTuples uint64
	}

	// ctx is used only by the init() adapter.
	ctx context.Context

	// batch is the last batch returned by the wrapped operator.
	batch coldata.Batch

	// rowCountFastPath is set to indicate that the input is expected to produce
	// a single batch with a single column with the row count value.
	rowCountFastPath bool

	// stopwatch keeps track of the amount of time the wrapped operator spent
	// doing work. Note that this will include all of the time that the operator's
	// inputs spent doing work - this will be corrected when stats are reported
	// in finishAndGetStats().
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
		OneInputNode:         colexecop.NewOneInputNode(op),
		componentID:          id,
		rowCountFastPath:     colexec.IsColumnarizerAroundFastPathNode(op),
		stopwatch:            inputWatch,
		childStatsCollectors: childStatsCollectors,
	}
}

func (bic *batchInfoCollector) init() {
	bic.Input.Init(bic.ctx)
}

// Init is part of the colexecop.Operator interface.
func (bic *batchInfoCollector) Init(ctx context.Context) {
	bic.ctx = ctx
	bic.stopwatch.Start()
	// Wrap the call to Init() with a panic catcher in order to get the correct
	// execution time (e.g. in the statement bundle).
	err := colexecerror.CatchVectorizedRuntimeError(bic.init)
	bic.stopwatch.Stop()
	if err != nil {
		colexecerror.InternalError(err)
	}
	// Unset the context so that it's not used outside of the init() function.
	bic.ctx = nil
	bic.mu.Lock()
	// If we got here, then Init above succeeded, so the wrapped operator has
	// been properly initialized.
	bic.mu.initialized = true
	bic.mu.Unlock()
}

func (bic *batchInfoCollector) next() {
	bic.batch = bic.Input.Next()
}

// Next is part of the colexecop.Operator interface.
func (bic *batchInfoCollector) Next() coldata.Batch {
	bic.stopwatch.Start()
	// Wrap the call to Next() with a panic catcher in order to get the correct
	// execution time (e.g. in the statement bundle).
	err := colexecerror.CatchVectorizedRuntimeError(bic.next)
	bic.stopwatch.Stop()
	if err != nil {
		colexecerror.InternalError(err)
	}
	if bic.batch.Length() > 0 {
		bic.mu.Lock()
		defer bic.mu.Unlock()
		bic.mu.numBatches++
		if bic.rowCountFastPath {
			// We have a special case where the batch has exactly one column
			// with exactly one row in which we have the row count.
			if buildutil.CrdbTestBuild {
				if bic.mu.numBatches != 1 {
					colexecerror.InternalError(errors.AssertionFailedf("saw second batch in fast path:\n%s", bic.batch))
				}
				if bic.batch.Width() != 1 {
					colexecerror.InternalError(errors.AssertionFailedf("batch width is not 1:\n%s", bic.batch))
				}
				if bic.batch.Length() != 1 {
					colexecerror.InternalError(errors.AssertionFailedf("batch length is not 1:\n%s", bic.batch))
				}
				if !bic.batch.ColVec(0).Type().Equal(types.Int) {
					colexecerror.InternalError(errors.AssertionFailedf("single vector is not int:\n%s", bic.batch))
				}
			}
			if ints, ok := bic.batch.ColVec(0).Col().(coldata.Int64s); ok {
				bic.mu.numTuples = uint64(ints[0])
			}
		} else {
			bic.mu.numTuples += uint64(bic.batch.Length())
		}
	}
	return bic.batch
}

// finishAndGetStats calculates the final execution statistics for the wrapped
// operator. ok indicates whether the stats collection was successful.
func (bic *batchInfoCollector) finishAndGetStats() (
	numBatches, numTuples uint64,
	time, cpuTime time.Duration,
	ok bool,
) {
	tm := bic.stopwatch.Elapsed()
	cpuTm := bic.stopwatch.ElapsedCPU()
	// Subtract the time spent in each of the child stats collectors, to produce
	// the amount of time that the wrapped operator spent doing work itself, not
	// including time spent waiting on its inputs.
	for _, statsCollectors := range bic.childStatsCollectors {
		tm -= statsCollectors.getElapsedTime()
		cpuTm -= statsCollectors.getElapsedCPUTime()
	}
	if buildutil.CrdbTestBuild {
		if tm < 0 {
			colexecerror.InternalError(errors.AssertionFailedf("unexpectedly execution time is negative"))
		}
	}
	if cpuTm < 0 {
		// The internal clock used by grunning is non-monotonic, so in rare cases we
		// can end up measuring a larger duration for the child operators than for
		// the parent.
		cpuTm = 0
	}
	bic.mu.Lock()
	defer bic.mu.Unlock()
	return bic.mu.numBatches, bic.mu.numTuples, tm, cpuTm, bic.mu.initialized
}

// getElapsedTime implements the childStatsCollector interface.
func (bic *batchInfoCollector) getElapsedTime() time.Duration {
	return bic.stopwatch.Elapsed()
}

// getElapsedCPUTime implements the childStatsCollector interface.
func (bic *batchInfoCollector) getElapsedCPUTime() time.Duration {
	return bic.stopwatch.ElapsedCPU()
}

// newVectorizedStatsCollector creates a colexecop.VectorizedStatsCollector
// which wraps 'op' that corresponds to a component with either ProcessorID or
// StreamID 'id' (with 'idTagKey' distinguishing between the two). 'kvReader' is
// a component (either an operator or a wrapped processor) that performs KV
// reads that is present in the chain of operators rooted at 'op'.
func newVectorizedStatsCollector(
	op colexecop.Operator,
	kvReader colexecop.KVReader,
	columnarizer colexecop.VectorizedStatsCollector,
	id execinfrapb.ComponentID,
	inputWatch *timeutil.StopWatch,
	memMonitors []*mon.BytesMonitor,
	diskMonitors []*mon.BytesMonitor,
	inputStatsCollectors []childStatsCollector,
) colexecop.VectorizedStatsCollector {
	// TODO(cathymw): Refactor to have specialized stats collectors for
	// memory/disk stats and IO operators.
	return &vectorizedStatsCollectorImpl{
		batchInfoCollector: makeBatchInfoCollector(op, id, inputWatch, inputStatsCollectors),
		kvReader:           kvReader,
		columnarizer:       columnarizer,
		memMonitors:        memMonitors,
		diskMonitors:       diskMonitors,
	}
}

// vectorizedStatsCollectorImpl is the implementation behind
// newVectorizedStatsCollector.
type vectorizedStatsCollectorImpl struct {
	batchInfoCollector

	kvReader     colexecop.KVReader
	columnarizer colexecop.VectorizedStatsCollector
	memMonitors  []*mon.BytesMonitor
	diskMonitors []*mon.BytesMonitor
}

// GetStats is part of the colexecop.VectorizedStatsCollector interface.
func (vsc *vectorizedStatsCollectorImpl) GetStats() *execinfrapb.ComponentStats {
	numBatches, numTuples, time, cpuTime, ok := vsc.batchInfoCollector.finishAndGetStats()
	if !ok {
		// The stats collection wasn't successful for some reason, so we will
		// return an empty object (since nil is not allowed by the contract of
		// GetStats).
		//
		// Such scenario can occur, for example, if the operator wrapped by the
		// batchInfoCollector wasn't properly initialized, yet the stats
		// retrieval is attempted. In many places we assume that if an operator
		// is interacted with in any way, it must have been successfully
		// initialized. Having such a check in the vectorizedStatsCollectorImpl
		// makes sure that we never violate those assumptions.
		return &execinfrapb.ComponentStats{}
	}

	var s *execinfrapb.ComponentStats
	if vsc.columnarizer != nil {
		s = vsc.columnarizer.GetStats()

		// If the columnarizer is wrapping an operator that performs KV operations,
		// we must subtract the CPU time spent performing KV work on a SQL goroutine
		// from the measured CPU time. If the wrapped operator does not perform KV
		// operations, this value will be zero.
		cpuTime -= s.KV.KVCPUTime.Value()
	} else {
		// There was no root columnarizer, so create a new stats object.
		s = &execinfrapb.ComponentStats{Component: vsc.componentID}
	}

	for _, memMon := range vsc.memMonitors {
		s.Exec.MaxAllocatedMem.Add(memMon.MaximumBytes())
	}
	for _, diskMon := range vsc.diskMonitors {
		s.Exec.MaxAllocatedDisk.Add(diskMon.MaximumBytes())
	}

	if vsc.kvReader != nil {
		// Note that kvReader is non-nil only for vectorized operators that perform
		// kv operations, and this is the only case when we want to add the number
		// of rows read, bytes read, and the contention time (because the wrapped
		// row-execution KV reading processors - joinReaders, tableReaders,
		// zigzagJoiners, and invertedJoiners - will add these statistics
		// themselves). Similarly, for those wrapped processors it is ok to show the
		// time as "execution time" since "KV time" would only make sense for
		// tableReaders, and they are less likely to be wrapped than others.
		s.KV.KVTime.Set(time)
		s.KV.BytesRead.Set(uint64(vsc.kvReader.GetBytesRead()))
		s.KV.KVPairsRead.Set(uint64(vsc.kvReader.GetKVPairsRead()))
		s.KV.TuplesRead.Set(uint64(vsc.kvReader.GetRowsRead()))
		s.KV.BatchRequestsIssued.Set(uint64(vsc.kvReader.GetBatchRequestsIssued()))
		s.KV.ContentionTime.Set(vsc.kvReader.GetContentionTime())
		s.KV.UsedStreamer = vsc.kvReader.UsedStreamer()
		scanStats := vsc.kvReader.GetScanStats()
		execstats.PopulateKVMVCCStats(&s.KV, &scanStats)
		s.Exec.ConsumedRU.Set(vsc.kvReader.GetConsumedRU())

		// In order to account for SQL CPU time, we have to subtract the CPU time
		// spent while serving KV requests on a SQL goroutine.
		cpuTime -= vsc.kvReader.GetKVCPUTime()
	} else {
		s.Exec.ExecTime.Set(time)
	}
	if cpuTime > 0 && grunning.Supported() {
		// Note that in rare cases, the measured CPU time can be less than zero
		// grunning uses a non-monotonic clock. This should only happen rarely when
		// the actual CPU time is very small, so it seems OK to not set the value in
		// that case.
		s.Exec.CPUTime.Set(cpuTime)
	}

	s.Output.NumBatches.Set(numBatches)
	s.Output.NumTuples.Set(numTuples)
	return s
}

// newNetworkVectorizedStatsCollector creates a new
// colexecop.VectorizedStatsCollector for streams. In addition to the base stats,
// newNetworkVectorizedStatsCollector collects the network latency for a stream.
func newNetworkVectorizedStatsCollector(
	op colexecop.Operator,
	id execinfrapb.ComponentID,
	inputWatch *timeutil.StopWatch,
	inbox *colrpc.Inbox,
	latency time.Duration,
) colexecop.VectorizedStatsCollector {
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

// GetStats is part of the colexecop.VectorizedStatsCollector interface.
func (nvsc *networkVectorizedStatsCollectorImpl) GetStats() *execinfrapb.ComponentStats {
	numBatches, numTuples, time, _, ok := nvsc.batchInfoCollector.finishAndGetStats()
	if !ok {
		// The stats collection wasn't successful for some reason, so we will
		// return an empty object (since nil is not allowed by the contract of
		// GetStats).
		//
		// Such scenario can occur, for example, if the operator wrapped by the
		// batchInfoCollector wasn't properly initialized, yet the stats
		// retrieval is attempted. In many places we assume that if an operator
		// is interacted with in any way, it must have been successfully
		// initialized. Having such a check in the vectorizedStatsCollectorImpl
		// makes sure that we never violate those assumptions.
		return &execinfrapb.ComponentStats{}
	}

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

// maybeAddStatsInvariantChecker will add a statsInvariantChecker to both
// StatsCollectors and MetadataSources of op if crdb_test build tag is
// specified. See comment on statsInvariantChecker for the kind of invariants
// checked.
func maybeAddStatsInvariantChecker(op *colexecargs.OpWithMetaInfo) {
	if buildutil.CrdbTestBuild {
		c := &statsInvariantChecker{}
		op.StatsCollectors = append(op.StatsCollectors, c)
		op.MetadataSources = append(op.MetadataSources, c)
	}
}

// statsInvariantChecker is a dummy colexecop.VectorizedStatsCollector as well
// as colexecop.MetadataSource which asserts that GetStats is called before
// DrainMeta. It should only be used in the test environment.
type statsInvariantChecker struct {
	colexecop.ZeroInputNode

	statsRetrieved bool
}

var _ colexecop.VectorizedStatsCollector = &statsInvariantChecker{}
var _ colexecop.MetadataSource = &statsInvariantChecker{}

func (i *statsInvariantChecker) Init(context.Context) {}

func (i *statsInvariantChecker) Next() coldata.Batch {
	return coldata.ZeroBatch
}

func (i *statsInvariantChecker) GetStats() *execinfrapb.ComponentStats {
	i.statsRetrieved = true
	return &execinfrapb.ComponentStats{}
}

func (i *statsInvariantChecker) DrainMeta() []execinfrapb.ProducerMetadata {
	if !i.statsRetrieved {
		return []execinfrapb.ProducerMetadata{{Err: errors.AssertionFailedf("GetStats wasn't called before DrainMeta")}}
	}
	return nil
}
