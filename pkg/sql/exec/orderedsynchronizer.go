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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// orderedSynchronizer receives rows from multiple inputs and produces a single
// stream of rows, ordered according to a set of columns. The rows in each input
// stream are assumed to be ordered according to the same set of columns.
type orderedSynchronizer struct {
	inputs      []Operator
	ordering    sqlbase.ColumnOrdering
	columnTypes []types.T

	// inputBatches stores the current batch for each input.
	inputBatches []coldata.Batch
	// inputIndices stores the current index into each input batch.
	inputIndices []uint16
	output       coldata.Batch
}

func (o *orderedSynchronizer) Next(ctx context.Context) coldata.Batch {
	if o.inputBatches == nil {
		o.inputBatches = make([]coldata.Batch, len(o.inputs))
		for i := range o.inputs {
			o.inputBatches[i] = o.inputs[i].Next(ctx)
		}
	}
	outputIdx := uint16(0)
	for outputIdx < coldata.BatchSize {
		// Determine the batch with the smallest row.
		minBatch := -1
		for i := range o.inputs {
			if o.inputBatches[i].Length() == 0 {
				// Input exhausted.
				continue
			}
			if minBatch == -1 || o.compareRow(i, minBatch) < 0 {
				minBatch = i
			}
		}
		if minBatch == -1 {
			// All inputs exhausted.
			break
		}

		// Copy the min row into the output.
		for i := range o.columnTypes {
			batch := o.inputBatches[minBatch]
			vec := batch.ColVec(i)
			srcStartIdx := o.inputIndices[minBatch]
			if sel := batch.Selection(); sel != nil {
				srcStartIdx = sel[srcStartIdx]
			}
			o.output.ColVec(i).AppendSlice(
				vec, o.columnTypes[i], uint64(outputIdx), srcStartIdx, srcStartIdx+1)
		}

		// Advance the input batch, fetching a new batch if necessary.
		if o.inputIndices[minBatch]+1 < o.inputBatches[minBatch].Length() {
			o.inputIndices[minBatch]++
		} else {
			o.inputBatches[minBatch] = o.inputs[minBatch].Next(ctx)
			o.inputIndices[minBatch] = 0
		}

		outputIdx++
	}
	o.output.SetLength(outputIdx)
	return o.output
}

func (o *orderedSynchronizer) Init() {
	o.inputIndices = make([]uint16, len(o.inputs))
	o.output = coldata.NewMemBatch(o.columnTypes)
	for i := range o.inputs {
		o.inputs[i].Init()
	}
}

func (o *orderedSynchronizer) compareRow(batch1 int, batch2 int) int {
	for i := range o.ordering {
		info := o.ordering[i]
		res := o.compareCol(batch1, batch2, info.ColIdx)
		if res != 0 {
			switch d := info.Direction; d {
			case encoding.Ascending:
				return res
			case encoding.Descending:
				return res * -1
			default:
				panic(fmt.Sprintf("unexpected direction value %d", d))
			}
		}
	}
	return 0
}

func (o *orderedSynchronizer) compareCol(batchIdx1 int, batchIdx2 int, colIdx int) int {
	batch1 := o.inputBatches[batchIdx1]
	batch2 := o.inputBatches[batchIdx2]
	rowIdx1 := o.inputIndices[batchIdx1]
	rowIdx2 := o.inputIndices[batchIdx2]
	if sel := batch1.Selection(); sel != nil {
		rowIdx1 = sel[rowIdx1]
	}
	if sel := batch2.Selection(); sel != nil {
		rowIdx2 = sel[rowIdx2]
	}

	// Handle null cases. Nulls sort before non-null values.
	nulls1 := batch1.ColVec(colIdx).Nulls()
	nulls2 := batch2.ColVec(colIdx).Nulls()
	n1 := nulls1.HasNulls() && nulls1.NullAt(rowIdx1)
	n2 := nulls2.HasNulls() && nulls2.NullAt(rowIdx2)
	if n1 && n2 {
		return 0
	} else if n1 {
		return -1
	} else if n2 {
		return 1
	}

	// Both values are non-null. Compare them according to type.
	// TODO(solon): Template this to handle all types.
	switch o.columnTypes[colIdx] {
	case types.Int64:
		left := batch1.ColVec(colIdx).Int64()[rowIdx1]
		right := batch2.ColVec(colIdx).Int64()[rowIdx2]
		if left < right {
			return -1
		} else if left > right {
			return 1
		} else {
			return 0
		}
	default:
		panic(fmt.Sprintf("unhandled type %v", o.columnTypes[colIdx]))
	}
}
