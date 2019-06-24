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

package exec

import (
	"bytes"
	"context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// OrderedDistinctColsToOperators is a utility function that given an input and
// a slice of columns, creates a chain of distinct operators and returns the
// last distinct operator in that chain as well as its output column.
func OrderedDistinctColsToOperators(
	input Operator, distinctCols []uint32, typs []types.T,
) (Operator, []bool, error) {
	distinctCol := make([]bool, coldata.BatchSize)
	// zero the boolean column on every iteration.
	input = fnOp{input: input, fn: func() { copy(distinctCol, zeroBoolColumn) }}
	var err error
	for i := range distinctCols {
		input, err = newSingleOrderedDistinct(input, int(distinctCols[i]), distinctCol, typs[distinctCols[i]])
		if err != nil {
			return nil, nil, err
		}
	}
	return input, distinctCol, nil
}

// NewOrderedDistinct creates a new ordered distinct operator on the given
// input columns with the given types.
func NewOrderedDistinct(input Operator, distinctCols []uint32, typs []types.T) (Operator, error) {
	op, outputCol, err := OrderedDistinctColsToOperators(input, distinctCols, typs)
	if err != nil {
		return nil, err
	}
	return &boolVecToSelOp{
		input:     op,
		outputCol: outputCol,
	}, nil
}

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "tree" package.
var _ tree.Datum

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in types.T, for example
// int64 for types.Int64.
type _GOTYPE interface{}

// _TYPES_T is the template type variable for types.T. It will be replaced by
// types.Foo for each type Foo in the types.T type.
const _TYPES_T = types.Unhandled

// _ASSIGN_NE is the template equality function for assigning the first input
// to the result of the second input != the third input.
func _ASSIGN_NE(_, _, _ string) bool {
	panic("")
}

// */}}

func newSingleOrderedDistinct(
	input Operator, distinctColIdx int, outputCol []bool, t types.T,
) (Operator, error) {
	switch t {
	// {{range .}}
	case _TYPES_T:
		return &sortedDistinct_TYPEOp{
			input:             input,
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
	partition(colVec coldata.Vec, outputCol []bool, n uint64)
}

// newPartitioner returns a new partitioner on type t.
func newPartitioner(t types.T) (partitioner, error) {
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

type sortedDistinct_TYPEOp struct {
	input Operator

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
	lastVal _GOTYPE
}

var _ Operator = &sortedDistinct_TYPEOp{}

func (p *sortedDistinct_TYPEOp) Init() {
	p.input.Init()
}

func (p *sortedDistinct_TYPEOp) reset() {
	p.foundFirstRow = false
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
	col := batch.ColVec(p.sortedDistinctCol)._TemplateType()

	// We always output the first row.
	lastVal := p.lastVal
	sel := batch.Selection()
	startIdx := uint16(0)
	if !p.foundFirstRow {
		if sel != nil {
			lastVal = col[sel[0]]
			outputCol[sel[0]] = true
		} else {
			lastVal = col[0]
			outputCol[0] = true
		}
		startIdx = 1
		p.foundFirstRow = true
		if batch.Length() == 1 {
			p.lastVal = lastVal
			return batch
		}
	}

	n := batch.Length()
	if sel != nil {
		// Bounds check elimination.
		sel = sel[startIdx:n]
		for _, i := range sel {
			_CHECK_DISTINCT(int(i), lastVal, col, outputCol)
		}
	} else {
		// Bounds check elimination.
		col = col[startIdx:n]
		outputCol = outputCol[startIdx:n]
		_ = outputCol[len(col)-1]
		for i := range col {
			_CHECK_DISTINCT(i, lastVal, col, outputCol)
		}
	}

	p.lastVal = lastVal

	return batch
}

// partitioner_TYPE partitions an arbitrary-length colVec by running a distinct
// operation over it. It writes the same format to outputCol that sorted
// distinct does: true for every row that differs from the previous row in the
// input column.
type partitioner_TYPE struct{}

func (p partitioner_TYPE) partition(colVec coldata.Vec, outputCol []bool, n uint64) {
	col := colVec._TemplateType()
	lastVal := col[0]
	outputCol[0] = true
	outputCol = outputCol[1:n]
	col = col[1:n]
	for i := range col {
		_CHECK_DISTINCT(i, lastVal, col, outputCol)
	}
}

// {{end}}

// {{/*
// _CHECK_DISTINCT retrieves the value at the ith index of col, compares it
// to the passed in lastVal, and sets the ith value of outputCol to true if the
// compared values were distinct.
func _CHECK_DISTINCT(i int, lastVal _GOTYPE, col []_GOTYPE, outputCol []bool) { // */}}

	// {{define "checkDistinct"}}
	v := col[i]
	var unique bool
	_ASSIGN_NE(unique, v, lastVal)
	outputCol[i] = outputCol[i] || unique
	lastVal = v
	// {{end}}

	// {{/*
} // */}}
