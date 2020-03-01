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
)

// isNullProjOp is an Operator that projects into outputIdx Vec whether the
// corresponding value in colIdx Vec is NULL (i.e. it performs IS NULL check).
// If negate is true, it does the opposite - it performs IS NOT NULL check.
type isNullProjOp struct {
	OneInputNode
	allocator *Allocator
	colIdx    int
	outputIdx int
	negate    bool
}

func newIsNullProjOp(
	allocator *Allocator, input Operator, colIdx, outputIdx int, negate bool,
) Operator {
	return &isNullProjOp{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		colIdx:       colIdx,
		outputIdx:    outputIdx,
		negate:       negate,
	}
}

var _ Operator = &isNullProjOp{}

func (o *isNullProjOp) Init() {
	o.input.Init()
}

func (o *isNullProjOp) Next(ctx context.Context) coldata.Batch {
	batch := o.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	o.allocator.MaybeAddColumn(batch, coltypes.Bool, o.outputIdx)
	vec := batch.ColVec(o.colIdx)
	nulls := vec.Nulls()
	projCol := batch.ColVec(o.outputIdx).Bool()
	if nulls.MaybeHasNulls() {
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				projCol[i] = nulls.NullAt(i) != o.negate
			}
		} else {
			projCol = projCol[:n]
			for i := range projCol {
				projCol[i] = nulls.NullAt(i) != o.negate
			}
		}
	} else {
		// There are no NULLs, so we don't need to check each index for nullity.
		result := o.negate
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				projCol[i] = result
			}
		} else {
			projCol = projCol[:n]
			for i := range projCol {
				projCol[i] = result
			}
		}
	}
	return batch
}

// isNullSelOp is an Operator that selects all the tuples that have a NULL
// value in colIdx Vec. If negate is true, then it does the opposite -
// selecting all the tuples that have a non-NULL value in colIdx Vec.
type isNullSelOp struct {
	OneInputNode
	colIdx int
	negate bool
}

func newIsNullSelOp(input Operator, colIdx int, negate bool) Operator {
	return &isNullSelOp{
		OneInputNode: NewOneInputNode(input),
		colIdx:       colIdx,
		negate:       negate,
	}
}

var _ Operator = &isNullSelOp{}

func (o *isNullSelOp) Init() {
	o.input.Init()
}

func (o *isNullSelOp) Next(ctx context.Context) coldata.Batch {
	for {
		batch := o.input.Next(ctx)
		n := batch.Length()
		if n == 0 {
			return batch
		}
		var idx int
		vec := batch.ColVec(o.colIdx)
		nulls := vec.Nulls()
		if nulls.MaybeHasNulls() {
			// There might be NULLs in the Vec, so we'll need to iterate over all
			// tuples.
			if sel := batch.Selection(); sel != nil {
				sel = sel[:n]
				for _, i := range sel {
					if nulls.NullAt(i) != o.negate {
						sel[idx] = i
						idx++
					}
				}
			} else {
				batch.SetSelection(true)
				sel := batch.Selection()[:n]
				for i := range sel {
					if nulls.NullAt(i) != o.negate {
						sel[idx] = i
						idx++
					}
				}
			}
			if idx > 0 {
				batch.SetLength(idx)
				return batch
			}
		} else {
			// There are no NULLs, so we don't need to check each index for nullity.
			if o.negate {
				// o.negate is true, so we select all tuples, i.e. we don't need to
				// modify the batch and can just return it.
				return batch
			}
			// o.negate is false, so we omit all tuples from this batch and move onto
			// the next one.
		}
	}
}
