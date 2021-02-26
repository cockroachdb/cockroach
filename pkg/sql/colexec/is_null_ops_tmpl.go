// Copyright 2020 The Cockroach Authors.
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
// This file is the execgen template for is_null_ops.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input.
// This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type isNullProjBase struct {
	colexecop.OneInputHelper
	allocator *colmem.Allocator
	colIdx    int
	outputIdx int
	negate    bool
}

// NewIsNullProjOp returns a new isNullProjOp.
// - negate indicates whether a negative version is used (we either have IS NOT
// NULL or IS DISTINCT FROM).
// - isTupleNull indicates whether special "is tuple null" version is needed
// (we either have IS NULL or IS NOT NULL with tuple type as the input vector).
func NewIsNullProjOp(
	allocator *colmem.Allocator,
	input colexecop.Operator,
	colIdx, outputIdx int,
	negate bool,
	isTupleNull bool,
) colexecop.Operator {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, types.Bool, outputIdx)
	base := isNullProjBase{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		allocator:      allocator,
		colIdx:         colIdx,
		outputIdx:      outputIdx,
		negate:         negate,
	}
	if isTupleNull {
		return &isTupleNullProjOp{isNullProjBase: base}
	}
	return &isNullProjOp{isNullProjBase: base}
}

// {{range .}}

// is_KINDNullProjOp is an Operator that projects into outputIdx Vec whether
// the corresponding value in colIdx Vec is NULL (i.e. it performs IS NULL
// check). If negate is true, it does the opposite - it performs IS NOT NULL
// check. Tuples require special attention, so we have a separate struct for
// them.
type is_KINDNullProjOp struct {
	isNullProjBase
}

var _ colexecop.Operator = &is_KINDNullProjOp{}

func (o *is_KINDNullProjOp) Next() coldata.Batch {
	batch := o.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	vec := batch.ColVec(o.colIdx)
	nulls := vec.Nulls()
	// {{if .IsTuple}}
	datums := vec.Datum()
	// {{end}}
	projVec := batch.ColVec(o.outputIdx)
	projCol := projVec.Bool()
	if projVec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		projVec.Nulls().UnsetNulls()
	}
	if nulls.MaybeHasNulls() {
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				_COMPUTE_IS_NULL(o, nulls, i, true, _IS_TUPLE)
			}
		} else {
			projCol = projCol[:n]
			for i := range projCol {
				_COMPUTE_IS_NULL(o, nulls, i, true, _IS_TUPLE)
			}
		}
	} else {
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				_COMPUTE_IS_NULL(o, nulls, i, false, _IS_TUPLE)
			}
		} else {
			projCol = projCol[:n]
			for i := range projCol {
				_COMPUTE_IS_NULL(o, nulls, i, false, _IS_TUPLE)
			}
		}
	}
	return batch
}

// {{end}}

// {{/*
// _COMPUTE_IS_NULL is a code snippet that evaluates IS [NOT] NULL predicate on
// a single element of the input vector at position i and writes the result to
// projCol column.
func _COMPUTE_IS_NULL(
	o *is_KINDNullProjOp, nulls *coldata.Nulls, i int, _HAS_NULLS bool, _IS_TUPLE bool,
) { // */}}
	// {{define "computeIsNull" -}}

	// {{if .IsTuple}}
	if nulls.NullAt(i) {
		projCol[i] = !o.negate
	} else {
		projCol[i] = isTupleNull(datums.Get(i).(*coldataext.Datum).Datum, o.negate)
	}
	// {{else}}
	// {{if .HasNulls}}
	projCol[i] = nulls.NullAt(i) != o.negate
	// {{else}}
	// There are no NULLs, so we don't need to check each index for nullity.
	projCol[i] = o.negate
	// {{end}}
	// {{end}}

	// {{end}}
	// {{/*
} // */}}

type isNullSelBase struct {
	colexecop.OneInputHelper
	colIdx int
	negate bool
}

// NewIsNullSelOp returns a new isNullSelOp.
// - negate indicates whether a negative version is used (we either have IS NOT
// NULL or IS DISTINCT FROM).
// - isTupleNull indicates whether special "is tuple null" version is needed
// (we either have IS NULL or IS NOT NULL with tuple type as the input vector).
func NewIsNullSelOp(
	input colexecop.Operator, colIdx int, negate bool, isTupleNull bool,
) colexecop.Operator {
	base := isNullSelBase{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		colIdx:         colIdx,
		negate:         negate,
	}
	if isTupleNull {
		return &isTupleNullSelOp{isNullSelBase: base}
	}
	return &isNullSelOp{isNullSelBase: base}
}

// {{range .}}

// is_KINDNullSelOp is an Operator that selects all the tuples that have a NULL
// value in colIdx Vec. If negate is true, then it does the opposite -
// selecting all the tuples that have a non-NULL value in colIdx Vec.
type is_KINDNullSelOp struct {
	isNullSelBase
}

var _ colexecop.Operator = &is_KINDNullSelOp{}

func (o *is_KINDNullSelOp) Next() coldata.Batch {
	for {
		batch := o.Input.Next()
		n := batch.Length()
		if n == 0 {
			return batch
		}
		var idx int
		vec := batch.ColVec(o.colIdx)
		nulls := vec.Nulls()
		// {{if .IsTuple}}
		datums := vec.Datum()
		// {{else}}
		if nulls.MaybeHasNulls() {
			// {{end}}
			// There might be NULLs in the Vec, so we'll need to iterate over all
			// tuples.
			if sel := batch.Selection(); sel != nil {
				sel = sel[:n]
				for _, i := range sel {
					_MAYBE_SELECT(o, nulls, i, idx, sel, _IS_TUPLE)
				}
			} else {
				batch.SetSelection(true)
				sel := batch.Selection()[:n]
				for i := range sel {
					_MAYBE_SELECT(o, nulls, i, idx, sel, _IS_TUPLE)
				}
			}
			if idx > 0 {
				batch.SetLength(idx)
				return batch
			}
			// {{if not .IsTuple}}
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
		// {{end}}
	}
}

// {{end}}

// {{/*
// _MAYBE_SELECT is a code snippet that evaluates IS [NOT] NULL predicate on
// a single element of the input vector at position i and includes that element
// into sel if the predicate is true.
func _MAYBE_SELECT(
	o *is_KINDNullSelOp, nulls *coldata.Nulls, i, idx int, sel []int, _IS_TUPLE bool,
) { // */}}
	// {{define "maybeSelect" -}}

	// {{if .IsTuple}}
	selectTuple := nulls.NullAt(i) != o.negate
	if !selectTuple {
		selectTuple = isTupleNull(datums.Get(i).(*coldataext.Datum).Datum, o.negate)
	}
	if selectTuple {
		sel[idx] = i
		idx++
	}
	// {{else}}
	if nulls.NullAt(i) != o.negate {
		sel[idx] = i
		idx++
	}
	// {{end}}

	// {{end}}
	// {{/*
} // */}}

// isTupleNull returns the evaluation of IS [NOT] NULL predicate on datum
// (which is either a tree.DNull or tree.DTuple). negate differentiates between
// IS NULL and IS NOT NULL.
func isTupleNull(datum tree.Datum, negate bool) bool {
	if datum == tree.DNull {
		return !negate
	}
	// A tuple IS NULL if all elements are NULL.
	// A tuple IS NOT NULL if all elements are not NULL.
	for _, tupleDatum := range datum.(*tree.DTuple).D {
		datumIsNull := tupleDatum == tree.DNull
		if datumIsNull == negate {
			return false
		}
	}
	return true
}
