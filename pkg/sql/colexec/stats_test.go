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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestNumBatches is a unit test for NumBatches field of VectorizedStats.
func TestNumBatches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nBatches := 10
	noop := NewNoop(makeFiniteChunksSourceWithBatchSize(nBatches, int(coldata.BatchSize())))
	vsc := NewVectorizedStatsCollector(noop, 0 /* id */, true /* isStall */, timeutil.NewStopWatch())
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
	nBatches := 10
	for _, batchSize := range []int{1, 16, 1024} {
		noop := NewNoop(makeFiniteChunksSourceWithBatchSize(nBatches, batchSize))
		vsc := NewVectorizedStatsCollector(noop, 0 /* id */, true /* isStall */, timeutil.NewStopWatch())
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
	for nBatches := 1; nBatches < 5; nBatches++ {
		timeSource := timeutil.NewTestTimeSource()
		mjInputWatch := timeutil.NewTestStopWatch(timeSource.Now)

		leftSource := &timeAdvancingOperator{
			OneInputNode: NewOneInputNode(makeFiniteChunksSourceWithBatchSize(nBatches, int(coldata.BatchSize()))),
			timeSource:   timeSource,
		}
		leftInput := NewVectorizedStatsCollector(leftSource, 0 /* id */, true /* isStall */, timeutil.NewTestStopWatch(timeSource.Now))
		leftInput.SetOutputWatch(mjInputWatch)

		rightSource := &timeAdvancingOperator{
			OneInputNode: NewOneInputNode(makeFiniteChunksSourceWithBatchSize(nBatches, int(coldata.BatchSize()))),
			timeSource:   timeSource,
		}
		rightInput := NewVectorizedStatsCollector(rightSource, 1 /* id */, true /* isStall */, timeutil.NewTestStopWatch(timeSource.Now))
		rightInput.SetOutputWatch(mjInputWatch)

		mergeJoiner, err := NewMergeJoinOp(
			testAllocator,
			sqlbase.InnerJoin,
			leftInput,
			rightInput,
			[]uint32{0},
			[]uint32{0},
			[]coltypes.T{coltypes.Int64},
			[]coltypes.T{coltypes.Int64},
			[]execinfrapb.Ordering_Column{{ColIdx: 0}},
			[]execinfrapb.Ordering_Column{{ColIdx: 0}},
			nil,   /* filterConstructor */
			false, /* filterOnlyOnLeft */
		)
		if err != nil {
			t.Fatal(err)
		}
		timeAdvancingMergeJoiner := &timeAdvancingOperator{
			OneInputNode: NewOneInputNode(mergeJoiner),
			timeSource:   timeSource,
		}
		mjStatsCollector := NewVectorizedStatsCollector(timeAdvancingMergeJoiner, 2 /* id */, false /* isStall */, mjInputWatch)

		// The inputs are identical, so the merge joiner should output nBatches
		// batches with each having coldata.BatchSize() tuples.
		mjStatsCollector.Init()
		batchCount := 0
		for {
			b := mjStatsCollector.Next(context.Background())
			if b.Length() == 0 {
				break
			}
			require.Equal(t, coldata.BatchSize(), b.Length())
			batchCount++
		}
		mjStatsCollector.FinalizeStats()

		require.Equal(t, nBatches, batchCount)
		require.Equal(t, nBatches, int(mjStatsCollector.NumBatches))
		require.Equal(t, nBatches*int(coldata.BatchSize()), int(mjStatsCollector.NumTuples))
		// Two inputs are advancing the time source for a total of 2 * nBatches
		// advances, but these do not count towards merge joiner execution time.
		// Merge joiner advances the time on its every non-empty batch totaling
		// nBatches advances that should be accounted for in stats.
		require.Equal(t, time.Duration(nBatches), mjStatsCollector.Time)
	}
}

func makeFiniteChunksSourceWithBatchSize(nBatches int, batchSize int) Operator {
	batch := testAllocator.NewMemBatchWithSize([]coltypes.T{coltypes.Int64}, batchSize)
	vec := batch.ColVec(0).Int64()
	for i := 0; i < batchSize; i++ {
		vec[i] = int64(i)
	}
	batch.SetLength(uint16(batchSize))
	return newFiniteChunksSource(batch, nBatches, 1 /* matchLen */)
}

// timeAdvancingOperator is an Operator that advances the time source upon
// receiving a non-empty batch from its input. It is used for testing only.
type timeAdvancingOperator struct {
	OneInputNode

	timeSource *timeutil.TestTimeSource
}

var _ Operator = &timeAdvancingOperator{}

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
