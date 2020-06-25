// Copyright 2018 The Cockroach Authors.
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
// This file is the execgen template for distinct.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// OrderedDistinctColsToOperators is a utility function that given an input and
// a slice of columns, creates a chain of distinct operators and returns the
// last distinct operator in that chain as well as its output column.
func OrderedDistinctColsToOperators(
	input colexecbase.Operator, distinctCols []uint32, typs []*types.T,
) (colexecbase.Operator, []bool, error) {
	distinctCol := make([]bool, coldata.BatchSize())
	// zero the boolean column on every iteration.
	input = fnOp{
		OneInputNode: NewOneInputNode(input),
		fn:           func() { copy(distinctCol, zeroBoolColumn) },
	}
	var (
		err error
		r   ResettableOperator
		ok  bool
	)
	for i := range distinctCols {
		input, err = newSingleDistinct(input, int(distinctCols[i]), distinctCol, typs[distinctCols[i]])
		if err != nil {
			return nil, nil, err
		}
	}
	if r, ok = input.(ResettableOperator); !ok {
		colexecerror.InternalError("unexpectedly an ordered distinct is not a resetter")
	}
	distinctChain := &distinctChainOps{
		ResettableOperator: r,
	}
	return distinctChain, distinctCol, nil
}

type distinctChainOps struct {
	ResettableOperator
}

var _ ResettableOperator = &distinctChainOps{}

// NewOrderedDistinct creates a new ordered distinct operator on the given
// input columns with the given types.
func NewOrderedDistinct(
	input colexecbase.Operator, distinctCols []uint32, typs []*types.T,
) (colexecbase.Operator, error) {
	op, outputCol, err := OrderedDistinctColsToOperators(input, distinctCols, typs)
	if err != nil {
		return nil, err
	}
	return &boolVecToSelOp{
		OneInputNode: NewOneInputNode(op),
		outputCol:    outputCol,
	}, nil
}

// {{/*

// Declarations to make the template compile properly.

// _GOTYPE is the template variable.
type _GOTYPE interface{}

// _GOTYPESLICE is the template variable.
type _GOTYPESLICE interface{}

// _ASSIGN_NE is the template equality function for assigning the first input
// to the result of the second input != the third input.
func _ASSIGN_NE(_ bool, _, _, _, _, _ _GOTYPE) bool {
	colexecerror.InternalError("")
}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// */}}

func newSingleDistinct(
	input colexecbase.Operator, distinctColIdx int, outputCol []bool, t *types.T,
) (colexecbase.Operator, error) {
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			return &distinct_TYPEOp{
				OneInputNode:   NewOneInputNode(input),
				distinctColIdx: distinctColIdx,
				outputCol:      outputCol,
			}, nil
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unsupported distinct type %s", t)
}

// partitioner is a simple implementation of sorted distinct that's useful for
// other operators that need to partition an arbitrarily-sized Vec.
type partitioner interface {
	// partition partitions the input colVec of size n, writing true to the
	// outputCol for every value that differs from the previous one.
	partition(colVec coldata.Vec, outputCol []bool, n int)

	// partitionWithOrder is like partition, except it performs the partitioning
	// on the input Vec as if it were ordered via the input order vector, which is
	// a selection vector. The output is written in absolute order, however. For
	// example, with an input vector [a,b,b] and an order vector [1,2,0], which
	// implies a reordered input vector [b,b,a], the resultant outputCol would be
	// [true, false, true], indicating a distinct value at the 0th and 2nd
	// elements.
	partitionWithOrder(colVec coldata.Vec, order []int, outputCol []bool, n int)
}

// newPartitioner returns a new partitioner on type t.
func newPartitioner(t *types.T) (partitioner, error) {
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			return partitioner_TYPE{}, nil
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unsupported partition type %s", t)
}

// {{range .}}
// {{range .WidthOverloads}}

// distinct_TYPEOp runs a distinct on the column in distinctColIdx, writing
// true to the resultant bool column for every value that differs from the
// previous one.
type distinct_TYPEOp struct {
	OneInputNode

	// distinctColIdx is the index of the column to distinct upon.
	distinctColIdx int

	// outputCol is the boolean output column. It is shared by all of the
	// other distinct operators in a distinct operator set.
	outputCol []bool

	// Set to true at runtime when we've seen the first row. Distinct always
	// outputs the first row that it sees.
	foundFirstRow bool

	// lastVal is the last value seen by the operator, so that the distincting
	// still works across batch boundaries.
	lastVal     _GOTYPE
	lastValNull bool
}

var _ ResettableOperator = &distinct_TYPEOp{}

func (p *distinct_TYPEOp) Init() {
	p.input.Init()
}

func (p *distinct_TYPEOp) reset(ctx context.Context) {
	p.foundFirstRow = false
	p.lastValNull = false
	if resetter, ok := p.input.(resetter); ok {
		resetter.reset(ctx)
	}
}

func (p *distinct_TYPEOp) Next(ctx context.Context) coldata.Batch {
	batch := p.input.Next(ctx)
	if batch.Length() == 0 {
		return batch
	}
	outputCol := p.outputCol
	vec := batch.ColVec(p.distinctColIdx)
	var nulls *coldata.Nulls
	if vec.MaybeHasNulls() {
		nulls = vec.Nulls()
	}
	col := vec.TemplateType()

	// We always output the first row.
	lastVal := p.lastVal
	lastValNull := p.lastValNull
	sel := batch.Selection()
	firstIdx := 0
	if sel != nil {
		firstIdx = sel[0]
	}
	if !p.foundFirstRow {
		outputCol[firstIdx] = true
		p.foundFirstRow = true
	} else if nulls == nil && lastValNull {
		// The last value of the previous batch was null, so the first value of this
		// non-null batch is distinct.
		outputCol[firstIdx] = true
		lastValNull = false
	}

	n := batch.Length()
	if sel != nil {
		// Bounds check elimination.
		sel = sel[:n]
		if nulls != nil {
			for _, idx := range sel {
				lastVal, lastValNull = checkDistinctWithNulls(idx, idx, lastVal, nulls, lastValNull, col, outputCol)
			}
		} else {
			// Eliminate bounds checks for outputCol[idx].
			_ = outputCol[n-1]
			// Eliminate bounds checks for col[idx].
			_ = execgen.UNSAFEGET(col, n-1)
			for _, idx := range sel {
				lastVal = checkDistinct(idx, idx, lastVal, col, outputCol)
			}
		}
	} else {
		if nulls != nil {
			for idx := 0; idx < n; idx++ {
				lastVal, lastValNull = checkDistinctWithNulls(idx, idx, lastVal, nulls, lastValNull, col, outputCol)
			}
		} else {
			// Eliminate bounds checks for outputCol[idx].
			_ = outputCol[n-1]
			// Eliminate bounds checks for col[idx].
			_ = execgen.UNSAFEGET(col, n-1)
			for idx := 0; idx < n; idx++ {
				lastVal = checkDistinct(idx, idx, lastVal, col, outputCol)
			}
		}
	}

	p.lastVal = lastVal
	p.lastValNull = lastValNull

	return batch
}

// partitioner_TYPE partitions an arbitrary-length colVec by running a distinct
// operation over it. It writes the same format to outputCol that sorted
// distinct does: true for every row that differs from the previous row in the
// input column.
type partitioner_TYPE struct{}

func (p partitioner_TYPE) partitionWithOrder(
	colVec coldata.Vec, order []int, outputCol []bool, n int,
) {
	var lastVal _GOTYPE
	var lastValNull bool
	var nulls *coldata.Nulls
	if colVec.MaybeHasNulls() {
		nulls = colVec.Nulls()
	}

	col := colVec.TemplateType()
	col = execgen.SLICE(col, 0, n)
	outputCol = outputCol[:n]
	// Eliminate bounds checks for outputcol[outputIdx].
	_ = outputCol[len(order)-1]
	// Eliminate bounds checks for col[outputIdx].
	_ = execgen.UNSAFEGET(col, len(order)-1)
	outputCol[0] = true
	if nulls != nil {
		for outputIdx, checkIdx := range order {
			lastVal, lastValNull = checkDistinctWithNulls(checkIdx, outputIdx, lastVal, nulls, lastValNull, col, outputCol)
		}
	} else {
		for outputIdx, checkIdx := range order {
			lastVal = checkDistinct(checkIdx, outputIdx, lastVal, col, outputCol)
		}
	}
}

func (p partitioner_TYPE) partition(colVec coldata.Vec, outputCol []bool, n int) {
	var (
		lastVal     _GOTYPE
		lastValNull bool
		nulls       *coldata.Nulls
	)
	if colVec.MaybeHasNulls() {
		nulls = colVec.Nulls()
	}

	col := colVec.TemplateType()
	_ = execgen.UNSAFEGET(col, n-1)
	_ = outputCol[n-1]
	outputCol[0] = true
	if nulls != nil {
		for idx := 0; idx < n; idx++ {
			lastVal, lastValNull = checkDistinctWithNulls(idx, idx, lastVal, nulls, lastValNull, col, outputCol)
		}
	} else {
		for idx := 0; idx < n; idx++ {
			lastVal = checkDistinct(idx, idx, lastVal, col, outputCol)
		}
	}
}

// {{end}}
// {{end}}

// checkDistinct retrieves the value at the ith index of col, compares it
// to the passed in lastVal, and sets the ith value of outputCol to true if the
// compared values were distinct. It presumes that the current batch has no null
// values.
// execgen:inline
func checkDistinct(
	checkIdx int, outputIdx int, lastVal _GOTYPE, col []_GOTYPE, outputCol []bool,
) _GOTYPE {
	v := execgen.UNSAFEGET(col, checkIdx)
	var unique bool
	_ASSIGN_NE(unique, v, lastVal, _, col, _)
	outputCol[outputIdx] = outputCol[outputIdx] || unique
	execgen.COPYVAL(lastVal, v)
	return lastVal
}

// checkDistinctWithNulls behaves the same as checkDistinct, but it also
// considers whether the previous and current values are null. It assumes that
// `nulls` is non-nil.
// execgen:inline
func checkDistinctWithNulls(
	checkIdx int,
	outputIdx int,
	lastVal _GOTYPE,
	nulls *coldata.Nulls,
	lastValNull bool,
	col []_GOTYPE,
	outputCol []bool,
) (lastVal _GOTYPE, lastValNull bool) {
	null := nulls.NullAt(checkIdx)
	if null {
		if !lastValNull {
			// The current value is null while the previous was not.
			outputCol[outputIdx] = true
		}
	} else {
		v := execgen.UNSAFEGET(col, checkIdx)
		if lastValNull {
			// The previous value was null while the current is not.
			outputCol[outputIdx] = true
		} else {
			// Neither value is null, so we must compare.
			var unique bool
			_ASSIGN_NE(unique, v, lastVal, _, col, _)
			outputCol[outputIdx] = outputCol[outputIdx] || unique
		}
		execgen.COPYVAL(lastVal, v)
	}
	return lastVal, null
}
