// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package exec

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestNumBatches is a unit test for NumBatches field of VectorizedStats.
func TestNumBatches(t *testing.T) {
	nBatches := 10
	noop := NewNoop(makeFiniteChunksSourceWithBatchSize(nBatches, coldata.BatchSize))
	vsc := NewVectorizedStatsCollector(noop, 0 /* id */, true /* isStall */, timeutil.NewStopWatch())
	vsc.Init()
	for {
		b := vsc.Next()
		if b.Length() == 0 {
			break
		}
	}
	require.Equal(t, nBatches, vsc.NumBatches)
}

// TestNumTuples is a unit test for NumTuples field of VectorizedStats.
func TestNumTuples(t *testing.T) {
	nBatches := 10
	for _, batchSize := range []int{1, 16, 1024} {
		noop := NewNoop(makeFiniteChunksSourceWithBatchSize(nBatches, batchSize))
		vsc := NewVectorizedStatsCollector(noop, 0 /* id */, true /* isStall */, timeutil.NewStopWatch())
		vsc.Init()
		for {
			b := vsc.Next()
			if b.Length() == 0 {
				break
			}
		}
		require.Equal(t, nBatches*int(batchSize), vsc.NumTuples)
	}
}

// TestVectorizedStatsCollector is an integration test for the
// VectorizedStatsCollector. It creates two inputs and feeds them into the
// merge joiner and makes sure that all the stats measured on the latter are as
// expected.
func TestVectorizedStatsCollector(t *testing.T) {
	for nBatches := 1; nBatches < 5; nBatches++ {
		timeSource := timeutil.NewTestTimeSource()
		mjInputWatch := timeutil.NewTestStopWatch(timeSource.Now)

		leftSource := &timeAdvancingNoop{
			input:      makeFiniteChunksSourceWithBatchSize(nBatches, coldata.BatchSize),
			timeSource: timeSource,
		}
		leftInput := NewVectorizedStatsCollector(leftSource, 0 /* id */, true /* isStall */, timeutil.NewTestStopWatch(timeSource.Now))
		leftInput.SetOutputWatch(mjInputWatch)

		rightSource := &timeAdvancingNoop{
			input:      makeFiniteChunksSourceWithBatchSize(nBatches, coldata.BatchSize),
			timeSource: timeSource,
		}
		rightInput := NewVectorizedStatsCollector(rightSource, 1 /* id */, true /* isStall */, timeutil.NewTestStopWatch(timeSource.Now))
		rightInput.SetOutputWatch(mjInputWatch)

		mergeJoiner, err := NewMergeJoinOp(
			leftInput,
			rightInput,
			[]uint32{0},
			[]uint32{0},
			[]types.T{types.Int64},
			[]types.T{types.Int64},
			[]uint32{0},
			[]uint32{0},
		)
		if err != nil {
			t.Fatal(err)
		}
		mjStatsCollector := NewVectorizedStatsCollector(mergeJoiner, 2 /* id */, false /* isStall */, mjInputWatch)

		// The inputs are identical, so the merge joiner should output nBatches
		// batches with each having coldata.BatchSize tuples.
		mjStatsCollector.Init()
		batchCount := 0
		for {
			b := mjStatsCollector.Next()
			if b.Length() == 0 {
				break
			}
			require.Equal(t, coldata.BatchSize, int(b.Length()))
			batchCount++
		}
		mjStatsCollector.FinalizeStats()

		require.Equal(t, nBatches, batchCount)
		require.Equal(t, nBatches, int(mjStatsCollector.NumBatches))
		require.Equal(t, nBatches*coldata.BatchSize, int(mjStatsCollector.NumTuples))
		// Merge joiner has two inputs with each advancing the time source on every
		// non-empty batch for total of 2 x nBatches advances.
		require.Equal(t, time.Duration(2*nBatches), mjStatsCollector.Time)
	}
}

func makeFiniteChunksSourceWithBatchSize(nBatches int, batchSize int) Operator {
	batch := coldata.NewMemBatchWithSize([]types.T{types.Int64}, batchSize)
	vec := batch.ColVec(0).Int64()
	for i := 0; i < batchSize; i++ {
		vec[i] = int64(i)
	}
	batch.SetLength(uint16(batchSize))
	return newFiniteChunksSource(batch, nBatches, 1 /* matchLen */)
}

// timeAdvancingNoop is a noop Operator that advances the time source upon
// receiving a non-empty batch from its input. It is used for testing only.
type timeAdvancingNoop struct {
	input Operator

	timeSource *timeutil.TestTimeSource
}

var _ Operator = &timeAdvancingNoop{}

func (o *timeAdvancingNoop) Init() {
	o.input.Init()
}

func (o *timeAdvancingNoop) Next() coldata.Batch {
	b := o.input.Next()
	if b.Length() > 0 {
		o.timeSource.Advance()
	}
	return b
}
