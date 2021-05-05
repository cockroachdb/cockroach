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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecjoin"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestNumBatches is a unit test for the NumBatches field.
func TestNumBatches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tu := newTestUtils(ctx)
	defer tu.cleanup(ctx)
	nBatches := 10
	noop := colexecop.NewNoop(makeFiniteChunksSourceWithBatchSize(tu.testAllocator, nBatches, coldata.BatchSize()))
	vsc := newVectorizedStatsCollector(
		noop, nil /* kvReader */, nil /* columnarizer */, execinfrapb.ComponentID{},
		timeutil.NewStopWatch(), nil /* memMonitors */, nil, /* diskMonitors */
		nil, /* inputStatsCollectors */
	)
	vsc.Init(ctx)
	for {
		b := vsc.Next()
		if b.Length() == 0 {
			break
		}
	}
	s := vsc.(*vectorizedStatsCollectorImpl).GetStats()
	require.Equal(t, nBatches, int(s.Output.NumBatches.Value()))
}

// TestNumTuples is a unit test for NumTuples field.
func TestNumTuples(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tu := newTestUtils(ctx)
	defer tu.cleanup(ctx)
	nBatches := 10
	for _, batchSize := range []int{1, 16, 1024} {
		noop := colexecop.NewNoop(makeFiniteChunksSourceWithBatchSize(tu.testAllocator, nBatches, batchSize))
		vsc := newVectorizedStatsCollector(
			noop, nil /* kvReader */, nil /* columnarizer */, execinfrapb.ComponentID{},
			timeutil.NewStopWatch(), nil /* memMonitors */, nil, /* diskMonitors */
			nil, /* inputStatsCollectors */
		)
		vsc.Init(ctx)
		for {
			b := vsc.Next()
			if b.Length() == 0 {
				break
			}
		}
		s := vsc.(*vectorizedStatsCollectorImpl).GetStats()
		require.Equal(t, nBatches*batchSize, int(s.Output.NumTuples.Value()))
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
	ctx := context.Background()
	tu := newTestUtils(ctx)
	defer tu.cleanup(ctx)
	for nBatches := 1; nBatches < 5; nBatches++ {
		timeSource := timeutil.NewTestTimeSource()
		mjInputWatch := timeutil.NewTestStopWatch(timeSource.Now)
		leftSource := &timeAdvancingOperator{
			OneInputHelper: colexecop.MakeOneInputHelper(makeFiniteChunksSourceWithBatchSize(tu.testAllocator, nBatches, coldata.BatchSize())),
			timeSource:     timeSource,
		}
		leftInput := newVectorizedStatsCollector(
			leftSource, nil /* kvReader */, nil /* columnarizer */, execinfrapb.ComponentID{ID: 0},
			timeutil.NewTestStopWatch(timeSource.Now), nil /* memMonitors */, nil, /* diskMonitors */
			nil, /* inputStatsCollectors */
		)
		rightSource := &timeAdvancingOperator{
			OneInputHelper: colexecop.MakeOneInputHelper(makeFiniteChunksSourceWithBatchSize(tu.testAllocator, nBatches, coldata.BatchSize())),
			timeSource:     timeSource,
		}
		rightInput := newVectorizedStatsCollector(
			rightSource, nil /* kvReader */, nil /* columnarizer */, execinfrapb.ComponentID{ID: 1},
			timeutil.NewTestStopWatch(timeSource.Now), nil /* memMonitors */, nil, /* diskMonitors */
			nil, /* inputStatsCollectors */
		)
		mergeJoiner, err := colexecjoin.NewMergeJoinOp(
			tu.testAllocator, execinfra.DefaultMemoryLimit, queueCfg,
			colexecop.NewTestingSemaphore(4), descpb.InnerJoin, leftInput, rightInput,
			[]*types.T{types.Int}, []*types.T{types.Int},
			[]execinfrapb.Ordering_Column{{ColIdx: 0}},
			[]execinfrapb.Ordering_Column{{ColIdx: 0}},
			tu.testDiskAcc,
		)
		if err != nil {
			t.Fatal(err)
		}
		timeAdvancingMergeJoiner := &timeAdvancingOperator{
			OneInputHelper: colexecop.MakeOneInputHelper(mergeJoiner),
			timeSource:     timeSource,
		}

		mjStatsCollector := newVectorizedStatsCollector(
			timeAdvancingMergeJoiner, nil /* kvReader */, nil /* columnarizer */, execinfrapb.ComponentID{ID: 2},
			mjInputWatch, nil /* memMonitors */, nil, /* diskMonitors */
			[]childStatsCollector{leftInput.(childStatsCollector), rightInput.(childStatsCollector)},
		)

		// The inputs are identical, so the merge joiner should output
		// nBatches x coldata.BatchSize() tuples.
		mjStatsCollector.Init(ctx)
		batchCount, tupleCount := 0, 0
		for {
			b := mjStatsCollector.Next()
			if b.Length() == 0 {
				break
			}
			batchCount++
			tupleCount += b.Length()
		}
		s := mjStatsCollector.(*vectorizedStatsCollectorImpl).GetStats()

		require.Equal(t, nBatches*coldata.BatchSize(), int(s.Output.NumTuples.Value()))
		// Two inputs are advancing the time source for a total of 2 * nBatches
		// advances, but these do not count towards merge joiner execution time.
		// Merge joiner advances the time on its every non-empty batch totaling
		// batchCount advances that should be accounted for in stats.
		require.Equal(t, time.Duration(batchCount), s.Exec.ExecTime.Value())
	}
}

func makeFiniteChunksSourceWithBatchSize(
	testAllocator *colmem.Allocator, nBatches int, batchSize int,
) colexecop.Operator {
	typs := []*types.T{types.Int}
	batch := testAllocator.NewMemBatchWithFixedCapacity(typs, batchSize)
	vec := batch.ColVec(0).Int64()
	for i := 0; i < batchSize; i++ {
		vec[i] = int64(i)
	}
	batch.SetLength(batchSize)
	return colexectestutils.NewFiniteChunksSource(testAllocator, batch, typs, nBatches, 1 /* matchLen */)
}

// timeAdvancingOperator is an Operator that advances the time source upon
// receiving a non-empty batch from its input. It is used for testing only.
type timeAdvancingOperator struct {
	colexecop.OneInputHelper

	timeSource *timeutil.TestTimeSource
}

var _ colexecop.Operator = &timeAdvancingOperator{}

func (o *timeAdvancingOperator) Next() coldata.Batch {
	b := o.Input.Next()
	if b.Length() > 0 {
		o.timeSource.Advance()
	}
	return b
}
