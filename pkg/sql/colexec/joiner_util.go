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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
)

// filterFeedOperator is used to feed the filter by manually setting the batch
// to be returned on the first Next() call with zero-length batches returned on
// all subsequent calls.
type filterFeedOperator struct {
	ZeroInputNode
	batch  coldata.Batch
	nexted bool
}

var _ Operator = &filterFeedOperator{}

func newFilterFeedOperator(allocator *Allocator, inputTypes []coltypes.T) *filterFeedOperator {
	return &filterFeedOperator{
		batch: allocator.NewMemBatchWithSize(inputTypes, 1 /* size */),
	}
}

func (o *filterFeedOperator) Init() {}

func (o *filterFeedOperator) Next(context.Context) coldata.Batch {
	if !o.nexted {
		o.nexted = true
		return o.batch
	}
	return coldata.ZeroBatch
}

func (o *filterFeedOperator) reset() {
	o.batch.ResetInternalBatch()
	o.nexted = false
}

func newJoinerFilter(
	allocator *Allocator,
	leftSourceTypes []coltypes.T,
	rightSourceTypes []coltypes.T,
	filterConstructor func(Operator) (Operator, error),
	filterOnlyOnLeft bool,
) (*joinerFilter, error) {
	input := newFilterFeedOperator(allocator, append(leftSourceTypes, rightSourceTypes...))
	filter, err := filterConstructor(input)
	return &joinerFilter{
		Operator:         filter,
		allocator:        allocator,
		leftSourceTypes:  leftSourceTypes,
		rightSourceTypes: rightSourceTypes,
		input:            input,
		onlyOnLeft:       filterOnlyOnLeft,
	}, err
}

// joinerFilter is a side chain of Operators needed to support ON expressions.
type joinerFilter struct {
	Operator

	allocator        *Allocator
	leftSourceTypes  []coltypes.T
	rightSourceTypes []coltypes.T
	input            *filterFeedOperator
	// onlyOnLeft indicates whether the ON expression is such that only columns
	// from the left input are used.
	onlyOnLeft bool
}

var _ Operator = &joinerFilter{}

// isLeftTupleFilteredOut returns whether a tuple lIdx from lBatch combined
// with all tuples in range [rStartIdx, rEndIdx) from rBatch does *not* satisfy
// the filter.
func (f *joinerFilter) isLeftTupleFilteredOut(
	ctx context.Context, lBatch, rBatch coldata.Batch, lIdx, rStartIdx, rEndIdx int,
) bool {
	if f.onlyOnLeft {
		f.input.reset()
		f.setInputBatch(lBatch, nil /* rBatch */, lIdx, 0 /* rIdx */)
		b := f.Next(ctx)
		return b.Length() == 0
	}
	for rIdx := rStartIdx; rIdx < rEndIdx; rIdx++ {
		f.input.reset()
		f.setInputBatch(lBatch, rBatch, lIdx, rIdx)
		b := f.Next(ctx)
		if b.Length() > 0 {
			return false
		}
	}
	return true
}

// setInputBatch sets the batch of filterFeedOperator, namely, it copies a
// single tuple from each of the batches at the specified indices and puts the
// combined "double" tuple into f.filterInput.
// Either lBatch or rBatch can be nil to indicate that the tuple from the
// respective side should not be set, i.e. the filter input batch will only be
// partially initialized.
func (f *joinerFilter) setInputBatch(lBatch, rBatch coldata.Batch, lIdx, rIdx int) {
	if lBatch == nil && rBatch == nil {
		execerror.VectorizedInternalPanic("only one of lBatch and rBatch can be nil")
	}
	setOneSide := func(colOffset int, batch coldata.Batch, sourceTypes []coltypes.T, idx int) {
		sel := batch.Selection()
		if sel != nil {
			idx = sel[idx]
		}
		f.allocator.PerformOperation(f.input.batch.ColVecs(), func() {
			for colIdx := 0; colIdx < batch.Width(); colIdx++ {
				f.input.batch.ColVec(colOffset + colIdx).Append(
					coldata.SliceArgs{
						Src:         batch.ColVec(colIdx),
						ColType:     sourceTypes[colIdx],
						DestIdx:     0,
						SrcStartIdx: idx,
						SrcEndIdx:   idx + 1,
					})
			}
		})
	}
	if lBatch != nil {
		setOneSide(0 /* colOffset */, lBatch, f.leftSourceTypes, lIdx)
	}
	if rBatch != nil {
		setOneSide(len(f.leftSourceTypes), rBatch, f.rightSourceTypes, rIdx)
	}
	f.input.batch.SetLength(1)
	f.input.batch.SetSelection(false)
}
