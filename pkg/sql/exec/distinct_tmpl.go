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

// {{/*
// +build execgen_template
//
// This file is the execgen template for distinct.og.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/pkg/errors"
)

// NewOrderedDistinct creates a new ordered distinct operator on the given
// input columns with the given types.
func NewOrderedDistinct(input Operator, distinctCols []uint32, typs []types.T) (Operator, error) {
	outputCol := make([]bool, ColBatchSize)
	op := input
	var err error
	for i := range distinctCols {
		op, err = newSingleOrderedDistinct(op, int(distinctCols[i]), outputCol, typs[i])
		if err != nil {
			return nil, err
		}
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

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in types.T, for example
// int64 for types.Int64.
type _GOTYPE interface{}

// _TYPES_T is the template type variable for types.T. It will be replaced by
// types.Foo for each type Foo in the types.T type.
const _TYPES_T = Unhandled

// _EQUALITY_FN is the template equality function for types that have an
// equality function and not an infix operator.
func _EQUALITY_FN(_ interface{}, _ interface{}) bool {
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

func (p *sortedDistinct_TYPEOp) Next() ColBatch {
	batch := p.input.Next()
	if batch.Length() == 0 {
		return batch
	}
	outputCol := p.outputCol
	col := batch.ColVec(p.sortedDistinctCol)._TemplateType()

	// We always output the first row.
	lastVal := p.lastVal
	sel := batch.Selection()
	if !p.foundFirstRow {
		if sel != nil {
			lastVal = col[sel[0]]
			outputCol[sel[0]] = true
		} else {
			lastVal = col[0]
			outputCol[0] = true
		}
	}

	startIdx := uint16(0)
	if !p.foundFirstRow {
		startIdx = 1
	}

	n := batch.Length()
	if sel != nil {
		// Bounds check elimination.
		sel = sel[startIdx:n]
		for _, i := range sel {
			v := col[i]
			// Note that not inlining this unique var actually makes a non-trivial
			// performance difference.
			var unique bool
			// {{if eq .EqualityFunction ""}}
			unique = v != lastVal
			// {{else}}
			unique = !_EQUALITY_FN(v, lastVal)
			// {{end}}
			outputCol[i] = outputCol[i] || unique
			lastVal = v
		}
	} else {
		// Bounds check elimination.
		col = col[startIdx:n]
		outputCol = outputCol[startIdx:n]
		for i := range col {
			v := col[i]
			// Note that not inlining this unique var actually makes a non-trivial
			// performance difference.
			var unique bool
			// {{if eq .EqualityFunction ""}}
			unique = v != lastVal
			// {{else}}
			unique = !_EQUALITY_FN(v, lastVal)
			// {{end}}
			outputCol[i] = outputCol[i] || unique
			lastVal = v
		}
	}

	p.lastVal = lastVal
	p.foundFirstRow = true

	return batch
}

// {{end}}
