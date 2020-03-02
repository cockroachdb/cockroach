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
	"bytes"
	"context"
	"math"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	// */}}
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/pkg/errors"
)

// OrderedDistinctColsToOperators is a utility function that given an input and
// a slice of columns, creates a chain of distinct operators and returns the
// last distinct operator in that chain as well as its output column.
func OrderedDistinctColsToOperators(
	input Operator, distinctCols []uint32, typs []coltypes.T,
) (Operator, []bool, error) {
	distinctCol := make([]bool, coldata.BatchSize())
	// zero the boolean column on every iteration.
	input = fnOp{
		OneInputNode: NewOneInputNode(input),
		fn:           func() { copy(distinctCol, zeroBoolColumn) },
	}
	var (
		err error
		r   resettableOperator
		ok  bool
	)
	for i := range distinctCols {
		input, err = newSingleOrderedDistinct(input, int(distinctCols[i]), distinctCol, typs[distinctCols[i]])
		if err != nil {
			return nil, nil, err
		}
	}
	if r, ok = input.(resettableOperator); !ok {
		execerror.VectorizedInternalPanic("unexpectedly an ordered distinct is not a resetter")
	}
	distinctChain := &distinctChainOps{
		resettableOperator: r,
	}
	return distinctChain, distinctCol, nil
}

type distinctChainOps struct {
	resettableOperator
}

var _ resettableOperator = &distinctChainOps{}

// NewOrderedDistinct creates a new ordered distinct operator on the given
// input columns with the given coltypes.
func NewOrderedDistinct(
	input Operator, distinctCols []uint32, typs []coltypes.T,
) (Operator, error) {
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

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in coltypes.T, for example
// int64 for coltypes.Int64.
type _GOTYPE interface{}

// _GOTYPESLICE is the template Go type slice variable for this operator. It
// will be replaced by the Go slice representation for each type in coltypes.T, for
// example []int64 for coltypes.Int64.
type _GOTYPESLICE interface{}

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// _ASSIGN_NE is the template equality function for assigning the first input
// to the result of the second input != the third input.
func _ASSIGN_NE(_ bool, _, _ _GOTYPE) bool {
	execerror.VectorizedInternalPanic("")
}

// */}}

func newSingleOrderedDistinct(
	input Operator, distinctColIdx int, outputCol []bool, t coltypes.T,
) (Operator, error) {
	switch t {
	// {{range .}}
	case _TYPES_T:
		return &sortedDistinct_TYPEOp{
			OneInputNode:      NewOneInputNode(input),
			sortedDistinctCol: distinctColIdx,
			outputCol:         outputCol,
		}, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported distinct type %s", t)
	}
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
func newPartitioner(t coltypes.T) (partitioner, error) {
	switch t {
	// {{range .}}
	case _TYPES_T:
		return partitioner_TYPE{}, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported partition type %s", t)
	}
}

// {{range .}}

// sortedDistinct_TYPEOp runs a distinct on the column in sortedDistinctCol,
// writing true to the resultant bool column for every value that differs from
// the previous one.
// TODO(solon): Update this name to remove "sorted". The input values are not
// necessarily in sorted order.
type sortedDistinct_TYPEOp struct {
	OneInputNode

	// sortedDistinctCol is the index of the column to distinct upon.
	sortedDistinctCol int

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

var _ resettableOperator = &sortedDistinct_TYPEOp{}

func (p *sortedDistinct_TYPEOp) Init() {
	p.input.Init()
}

func (p *sortedDistinct_TYPEOp) reset() {
	p.foundFirstRow = false
	p.lastValNull = false
	if resetter, ok := p.input.(resetter); ok {
		resetter.reset()
	}
}

func (p *sortedDistinct_TYPEOp) Next(ctx context.Context) coldata.Batch {
	batch := p.input.Next(ctx)
	if batch.Length() == 0 {
		return batch
	}
	outputCol := p.outputCol
	vec := batch.ColVec(p.sortedDistinctCol)
	var nulls *coldata.Nulls
	if vec.MaybeHasNulls() {
		nulls = vec.Nulls()
	}
	col := vec._TemplateType()

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
			for _, checkIdx := range sel {
				outputIdx := checkIdx
				_CHECK_DISTINCT_WITH_NULLS(checkIdx, outputIdx, lastVal, nulls, lastValNull, col, outputCol)
			}
		} else {
			for _, checkIdx := range sel {
				outputIdx := checkIdx
				_CHECK_DISTINCT(checkIdx, outputIdx, lastVal, col, outputCol)
			}
		}
	} else {
		// Bounds check elimination.
		col = execgen.SLICE(col, 0, n)
		outputCol = outputCol[:n]
		_ = outputCol[n-1]
		if nulls != nil {
			for execgen.RANGE(checkIdx, col, 0, n) {
				outputIdx := checkIdx
				_CHECK_DISTINCT_WITH_NULLS(checkIdx, outputIdx, lastVal, nulls, lastValNull, col, outputCol)
			}
		} else {
			for execgen.RANGE(checkIdx, col, 0, n) {
				outputIdx := checkIdx
				_CHECK_DISTINCT(checkIdx, outputIdx, lastVal, col, outputCol)
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

	col := colVec._TemplateType()
	col = execgen.SLICE(col, 0, n)
	outputCol = outputCol[:n]
	outputCol[0] = true
	if nulls != nil {
		for outputIdx, checkIdx := range order {
			_CHECK_DISTINCT_WITH_NULLS(checkIdx, outputIdx, lastVal, nulls, lastValNull, col, outputCol)
		}
	} else {
		for outputIdx, checkIdx := range order {
			_CHECK_DISTINCT(checkIdx, outputIdx, lastVal, col, outputCol)
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

	col := colVec._TemplateType()
	col = execgen.SLICE(col, 0, n)
	outputCol = outputCol[:n]
	outputCol[0] = true
	if nulls != nil {
		for execgen.RANGE(checkIdx, col, 0, n) {
			outputIdx := checkIdx
			_CHECK_DISTINCT_WITH_NULLS(checkIdx, outputIdx, lastVal, nulls, lastValNull, col, outputCol)
		}
	} else {
		for execgen.RANGE(checkIdx, col, 0, n) {
			outputIdx := checkIdx
			_CHECK_DISTINCT(checkIdx, outputIdx, lastVal, col, outputCol)
		}
	}
}

// {{end}}

// {{/*
// _CHECK_DISTINCT retrieves the value at the ith index of col, compares it
// to the passed in lastVal, and sets the ith value of outputCol to true if the
// compared values were distinct. It presumes that the current batch has no null
// values.
func _CHECK_DISTINCT(
	checkIdx int, outputIdx int, lastVal _GOTYPE, col []_GOTYPE, outputCol []bool,
) { // */}}

	// {{define "checkDistinct" -}}
	v := execgen.UNSAFEGET(col, checkIdx)
	var unique bool
	_ASSIGN_NE(unique, v, lastVal)
	outputCol[outputIdx] = outputCol[outputIdx] || unique
	execgen.COPYVAL(lastVal, v)
	// {{end}}

	// {{/*
} // */}}

// {{/*
// _CHECK_DISTINCT_WITH_NULLS behaves the same as _CHECK_DISTINCT, but it also
// considers whether the previous and current values are null. It assumes that
// `nulls` is non-nil.
func _CHECK_DISTINCT_WITH_NULLS(
	checkIdx int,
	outputIdx int,
	lastVal _GOTYPE,
	nulls *coldata.Nulls,
	lastValNull bool,
	col []_GOTYPE,
	outputCol []bool,
) { // */}}

	// {{define "checkDistinctWithNulls" -}}
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
			_ASSIGN_NE(unique, v, lastVal)
			outputCol[outputIdx] = outputCol[outputIdx] || unique
		}
		execgen.COPYVAL(lastVal, v)
	}
	lastValNull = null
	// {{end}}

	// {{/*
} // */}}
