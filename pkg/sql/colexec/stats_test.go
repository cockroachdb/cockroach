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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestNumBatches is a unit test for NumBatches field of VectorizedStats.
func TestNumBatches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	nBatches := 10
	noop := NewNoop(makeFiniteChunksSourceWithBatchSize(nBatches, coldata.BatchSize()))
	vsc := NewVectorizedStatsCollectorBase(
		noop, nil /* ioReader */, 0 /* id */, execinfrapb.ProcessorIDTagKey,
		timeutil.NewStopWatch(), nil /* memMonitors */, nil, /* diskMonitors */
		nil, /* inputStatsCollectors */
	)
	vsc.Init()
	for {
		b := vsc.Next(context.Background())
		if b.Length() == 0 {
			break
		}
	}
	require.Equal(t, nBatches, int(vsc.NumBatches))
}

// TestNumTuples is a unit test for NumTuples field of VectorizedStats.
func TestNumTuples(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	nBatches := 10
	for _, batchSize := range []int{1, 16, 1024} {
		noop := NewNoop(makeFiniteChunksSourceWithBatchSize(nBatches, batchSize))
		vsc := NewVectorizedStatsCollectorBase(
			noop, nil /* ioReader */, 0 /* id */, execinfrapb.ProcessorIDTagKey,
			timeutil.NewStopWatch(), nil /* memMonitors */, nil, /* diskMonitors */
			nil, /* inputStatsCollectors */
		)
		vsc.Init()
		for {
			b := vsc.Next(context.Background())
			if b.Length() == 0 {
				break
			}
		}
		require.Equal(t, nBatches*batchSize, int(vsc.NumTuples))
	}
}

// TestVectorizedStatsCollector is an integration test for the
// VectorizedStatsCollector. It creates two inputs and feeds them into the
// merge joiner and makes sure that all the stats measured on the latter are as
// expected.
func TestVectorizedStatsCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	for nBatches := 1; nBatches < 5; nBatches++ {
		timeSource := timeutil.NewTestTimeSource()
		mjInputWatch := timeutil.NewTestStopWatch(timeSource.Now)
		leftSource := &timeAdvancingOperator{
			OneInputNode: NewOneInputNode(makeFiniteChunksSourceWithBatchSize(nBatches, coldata.BatchSize())),
			timeSource:   timeSource,
		}
		leftInput := NewVectorizedStatsCollectorBase(
			leftSource, nil /* ioReader */, 0 /* id */, execinfrapb.ProcessorIDTagKey,
			timeutil.NewTestStopWatch(timeSource.Now), nil /* memMonitors */, nil, /* diskMonitors */
			nil, /* inputStatsCollectors */
		)
		rightSource := &timeAdvancingOperator{
			OneInputNode: NewOneInputNode(makeFiniteChunksSourceWithBatchSize(nBatches, coldata.BatchSize())),
			timeSource:   timeSource,
		}
		rightInput := NewVectorizedStatsCollectorBase(
			rightSource, nil /* ioReader */, 1 /* id */, execinfrapb.ProcessorIDTagKey,
			timeutil.NewTestStopWatch(timeSource.Now), nil /* memMonitors */, nil, /* diskMonitors */
			nil, /* inputStatsCollectors */
		)
		mergeJoiner, err := NewMergeJoinOp(
			testAllocator, defaultMemoryLimit, queueCfg,
			colexecbase.NewTestingSemaphore(4), descpb.InnerJoin, leftInput, rightInput,
			[]*types.T{types.Int}, []*types.T{types.Int},
			[]execinfrapb.Ordering_Column{{ColIdx: 0}},
			[]execinfrapb.Ordering_Column{{ColIdx: 0}},
			testDiskAcc,
		)
		if err != nil {
			t.Fatal(err)
		}
		timeAdvancingMergeJoiner := &timeAdvancingOperator{
			OneInputNode: NewOneInputNode(mergeJoiner),
			timeSource:   timeSource,
		}

		mjStatsCollector := NewVectorizedStatsCollectorBase(
			timeAdvancingMergeJoiner, nil /* ioReader */, 2 /* id */, execinfrapb.ProcessorIDTagKey,
			mjInputWatch, nil /* memMonitors */, nil, /* diskMonitors */
			[]ChildStatsCollector{leftInput, rightInput},
		)

		// The inputs are identical, so the merge joiner should output
		// nBatches x coldata.BatchSize() tuples.
		mjStatsCollector.Init()
		batchCount, tupleCount := 0, 0
		for {
			b := mjStatsCollector.Next(context.Background())
			if b.Length() == 0 {
				break
			}
			batchCount++
			tupleCount += b.Length()
		}
		mjStatsCollector.finalizeStats()

		require.Equal(t, nBatches*coldata.BatchSize(), int(mjStatsCollector.NumTuples))
		// Two inputs are advancing the time source for a total of 2 * nBatches
		// advances, but these do not count towards merge joiner execution time.
		// Merge joiner advances the time on its every non-empty batch totaling
		// batchCount advances that should be accounted for in stats.
		require.Equal(t, time.Duration(batchCount), mjStatsCollector.Time)
	}
}

func makeFiniteChunksSourceWithBatchSize(nBatches int, batchSize int) colexecbase.Operator {
	typs := []*types.T{types.Int}
	batch := testAllocator.NewMemBatchWithFixedCapacity(typs, batchSize)
	vec := batch.ColVec(0).Int64()
	for i := 0; i < batchSize; i++ {
		vec[i] = int64(i)
	}
	batch.SetLength(batchSize)
	return newFiniteChunksSource(batch, typs, nBatches, 1 /* matchLen */)
}

// timeAdvancingOperator is an Operator that advances the time source upon
// receiving a non-empty batch from its input. It is used for testing only.
type timeAdvancingOperator struct {
	OneInputNode

	timeSource *timeutil.TestTimeSource
}

var _ colexecbase.Operator = &timeAdvancingOperator{}

func (o *timeAdvancingOperator) Init() {
	o.input.Init()
}

func (o *timeAdvancingOperator) Next(ctx context.Context) coldata.Batch {
	b := o.input.Next(ctx)
	if b.Length() > 0 {
		o.timeSource.Advance()
	}
	return b
}
