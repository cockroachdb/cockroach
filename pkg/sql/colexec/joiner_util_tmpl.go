// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for joiner_util.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
)

// {{/*
// Declarations to make the template compile properly.

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// */}}

// filterFeedOperator is used to feed the filter by manually setting the batch
// to be returned on the first Next() call with zero-length batches returned on
// all subsequent calls.
type filterFeedOperator struct {
	ZeroInputNode
	batch     coldata.Batch
	zeroBatch coldata.Batch
	nexted    bool
}

var _ Operator = &filterFeedOperator{}

func newFilterFeedOperator(inputTypes []coltypes.T) *filterFeedOperator {
	return &filterFeedOperator{
		batch:     coldata.NewMemBatchWithSize(inputTypes, 1 /* size */),
		zeroBatch: coldata.NewMemBatchWithSize([]coltypes.T{}, 0 /* size */),
	}
}

func (o *filterFeedOperator) Init() {}

func (o *filterFeedOperator) Next(context.Context) coldata.Batch {
	if !o.nexted {
		o.nexted = true
		return o.batch
	}
	o.zeroBatch.SetLength(0)
	return o.zeroBatch
}

func (o *filterFeedOperator) reset() {
	o.batch.ResetInternalBatch()
	o.nexted = false
}

func newJoinerFilter(
	leftSourceTypes []coltypes.T,
	rightSourceTypes []coltypes.T,
	filterConstructor func(Operator) (Operator, error),
	filterOnlyOnLeft bool,
) (*joinerFilter, error) {
	input := newFilterFeedOperator(append(leftSourceTypes, rightSourceTypes...))
	filter, err := filterConstructor(input)
	return &joinerFilter{
		Operator:         filter,
		leftSourceTypes:  leftSourceTypes,
		rightSourceTypes: rightSourceTypes,
		input:            input,
		onlyOnLeft:       filterOnlyOnLeft,
	}, err
}

// joinerFilter is a side chain of Operators needed to support ON expressions.
type joinerFilter struct {
	Operator

	leftSourceTypes  []coltypes.T
	rightSourceTypes []coltypes.T
	input            *filterFeedOperator
	// onlyOnLeft indicates whether the ON expression is such that only columns
	// from the left input are used.
	onlyOnLeft bool
}

var _ StaticMemoryOperator = &joinerFilter{}

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
			idx = int(sel[idx])
		}
		for colIdx := 0; colIdx < batch.Width(); colIdx++ {
			colType := sourceTypes[colIdx]
			col := batch.ColVec(colIdx)
			memCol := col.Slice(colType, uint64(idx), uint64(idx+1))
			switch colType {
			// {{ range . }}
			case _TYPES_T:
				f.input.batch.ColVec(colOffset + colIdx).SetCol(memCol._TemplateType())
				// {{ end }}
			default:
				execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", colType))
			}
			f.input.batch.ColVec(colOffset + colIdx).SetNulls(memCol.Nulls())
		}
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

// EstimateStaticMemoryUsage implements the StaticMemoryOperator interface.
func (f *joinerFilter) EstimateStaticMemoryUsage() int {
	filterMemUsage := 0
	if s, ok := f.Operator.(StaticMemoryOperator); ok {
		filterMemUsage = s.EstimateStaticMemoryUsage()
	}
	return filterMemUsage +
		EstimateBatchSizeBytes(f.leftSourceTypes, 1 /* batchLength */) +
		EstimateBatchSizeBytes(f.rightSourceTypes, 1 /* batchLength */)
}
