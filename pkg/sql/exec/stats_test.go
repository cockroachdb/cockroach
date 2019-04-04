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
)

// TestBatchesOutput is a unit test for NumBatches field of VectorizedStats.
func TestBatchesOutput(t *testing.T) {
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
	if nBatches != int(vsc.NumBatches) {
		t.Fatalf("batches output differ: %d expected, %d actual", nBatches, vsc.NumBatches)
	}
}

// TestTuplesOutput is a unit test for NumTuples field of VectorizedStats.
func TestTuplesOutput(t *testing.T) {
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
		if nBatches*int(batchSize) != int(vsc.NumTuples) {
			t.Fatalf("tuples output differ: %d expected, %d actual", nBatches*int(batchSize), vsc.NumTuples)
		}
	}
}

// TestVectorizedStatsCollector is an integration test for the
// VectorizedStatsCollector. It creates two inputs and feeds them into the
// merge joiner and makes sure that all the stats measured on the latter are as
// expected.
func TestVectorizedStatsCollector(t *testing.T) {
	for nBatches := 2; nBatches < 5; nBatches++ {
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
			if b.Length() != coldata.BatchSize {
				t.Fatalf("unexpectedly merge joiner output a batch with %d tuples (%d were expected)", b.Length(), coldata.BatchSize)
			}
			batchCount++
		}
		if batchCount != nBatches {
			t.Fatalf("merge joiner output %d batches while %d were expected", batchCount, nBatches)
		}
		if int(mjStatsCollector.NumBatches) != nBatches {
			t.Fatalf("wrong vectorized stats collected: expected %d output batches, actual %d", nBatches, mjStatsCollector.NumBatches)
		}
		if int(mjStatsCollector.NumTuples) != nBatches*coldata.BatchSize {
			t.Fatalf("wrong vectorized stats collected: expected %d output tuples, actual %d", nBatches*coldata.BatchSize, mjStatsCollector.NumTuples)
		}
		// Left input advances the time and starts the stop watch, then right input
		// advances the time. Now, merge joiner gets batches from both inputs
		// within one "advance" on the stop watch, so this explains "nBatches" part
		// of the relationship. Plus one comes (to the best of my understanding)
		// from the first batch produced by the merge joiner - at that initial
		// stage it reads two batches from both inputs in 3 advances to produce two
		// output batches.
		// Note: if nBatches is 1, then the measured time will be 1 (i.e. without
		// the plus one).
		if mjStatsCollector.Time != time.Duration(nBatches+1) {
			t.Fatalf("wrong execution time: expected %v, actual %v", time.Duration(nBatches+1), mjStatsCollector.Time)
		}
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
