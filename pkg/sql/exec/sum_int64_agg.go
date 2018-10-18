// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// sumInt64Agg takes two input columns, one bool column which specifies whether
// the current position is a new group and a corresponding batch of int64s. It
// outputs in one column the sum of each group.
type sumInt64Agg struct {
	input Operator

	scratch struct {
		// ColBatch is the batch to return.
		ColBatch
		// vec points at the only column in ColBatch that we are updating.
		vec []int64
	}

	// carry is non-zero if we have an accumulated sum from the last batch but we
	// were unable to output it to the output batch.
	carry int64

	// nextBatch is set when the batch we are returning hits the size limit. This
	// variable will point to the remaining upstream batch that needs to be
	// processed in the next iteration.
	nextBatch ColBatch
	resumeIdx uint16

	// outputBatchSize is ColBatchSize by default.
	outputBatchSize uint16
}

var _ Operator = &sumInt64Agg{}

func (a *sumInt64Agg) initWithBatchSize(batchSize uint16) {
	a.scratch.ColBatch = NewMemBatch([]types.T{types.Int64})
	a.scratch.vec = a.scratch.ColBatch.ColVec(0).Int64()
	a.outputBatchSize = batchSize
	a.input.Init()
}

func (a *sumInt64Agg) Init() {
	a.initWithBatchSize(ColBatchSize)
}

// next is a utility function that abstracts the behavior of resuming a batch
// if the aggregator's output capacity is hit.
func (a *sumInt64Agg) next() (uint16, ColBatch) {
	if a.nextBatch != nil {
		resumeIdx := a.resumeIdx
		nextBatch := a.nextBatch
		a.resumeIdx = 0
		a.nextBatch = nil
		return resumeIdx, nextBatch
	}
	a.resumeIdx = 0
	return a.resumeIdx, a.input.Next()
}

var zeroInt64Batch = make([]int64, ColBatchSize)

// Next returns the next ColBatch.
func (a *sumInt64Agg) Next() ColBatch {
	// curIdx acts as a cursor over the output batch.
	curIdx := -1
	// Re-initialize the output batch.
	copy(a.scratch.vec, zeroInt64Batch[:a.outputBatchSize])
	for curIdx < int(a.outputBatchSize) {
		resumeIdx, batch := a.next()
		if batch.Length() == 0 {
			// Finalize the last batch.
			curIdx++
			break
		}

		groups, ints, sel := batch.ColVec(0).Bool(), batch.ColVec(1).Int64(), batch.Selection()

		if curIdx == -1 {
			// When curIdx == -1, the output batch has not been touched. We need to
			// determine what state we are in and perform any initialization needed.
			if resumeIdx != 0 {
				// We hit a new group in the last iteration and had no capacity to
				// initialize the sum in the output batch. This initial value is set in
				// the carry value.
				a.scratch.vec[0] = a.carry
			}
			firstIdx := uint16(0)
			if len(sel) > 0 {
				firstIdx = sel[0]
			}
			// We overwrite the indicator of a new group if there is one in the groups
			// to be processed to avoid incrementing curIdx.
			if groups[firstIdx] {
				groups[firstIdx] = false
			}
			curIdx = 0
		}

		// toSum represents the last value read from the ints to sum. On every
		// iteration of the loop, the value that was previously read is added to the
		// current position in the output batch. The reason one cannot increment the
		// current index when finding a new group and then write the sum to it is
		// that in the case where we find a new group when we are at the end of the
		// current batch, incrementing the index and accessing the output buffer
		// would result in a panic. In these cases, we save the sum into a.carry
		// and add it to the new output buffer on the next call to Next().
		// TODO(asubiotto): Slicing the input column/selection vector would be more
		// efficient. We could also allocate 2*ColBatchSize for the output batch to
		// not have to worry about bounds checking the output batch.
		toSum := int64(0)
		if sel != nil {
			for ; resumeIdx < batch.Length() && curIdx < int(a.outputBatchSize); resumeIdx++ {
				i := sel[resumeIdx]
				a.scratch.vec[curIdx] += toSum
				// This if statement is optimized out. See
				// https://github.com/golang/go/issues/6011#issuecomment-254303032
				x := 0
				if groups[i] {
					x = 1
				}
				curIdx += x
				toSum = ints[i]
			}
		} else {
			i := resumeIdx
			for ; i < batch.Length() && curIdx < int(a.outputBatchSize); i++ {
				a.scratch.vec[curIdx] += toSum
				// This if statement is optimized out. See
				// https://github.com/golang/go/issues/6011#issuecomment-254303032
				x := 0
				if groups[i] {
					x = 1
				}
				curIdx += x
				toSum = ints[i]
			}
			resumeIdx = i
		}

		if curIdx == int(a.outputBatchSize) {
			// We hit a new group but do not have space in the output batch to add
			// it.
			a.resumeIdx = resumeIdx
			a.nextBatch = batch
			a.carry = toSum
		} else {
			// curIdx is valid, flush the sum and loop again.
			a.scratch.vec[curIdx] += toSum
		}
	}

	a.scratch.SetLength(uint16(curIdx))
	return a.scratch
}
