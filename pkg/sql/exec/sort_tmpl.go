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
// This file is the execgen template for sort.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/apd"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

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

// _ASSIGN_LT is the template equality function for assigning the first input
// to the result of the second input < the third input.
func _ASSIGN_LT(_, _, _ string) bool {
	panic("")
}

// */}}

type sortSpec struct {
	sortCol    uint32
	inputTypes []types.T
}

// sortState represents the state of the processor.
type sortState int

const (
	sortSpooling sortState = iota

	sortEmitting
)

func NewSorter(input Operator, inputTypes []types.T, sortCol uint32) (Operator, error) {
	t := inputTypes[sortCol]
	switch t {
	// {{range .}}
	case _TYPES_T:
		return &sort_TYPEOp{
			input: input,
			spec: sortSpec{
				sortCol:    sortCol,
				inputTypes: inputTypes,
			},
		}, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported sort type %s", t)
	}
}

// {{range .}}

type sort_TYPEOp struct {
	spec sortSpec

	input Operator

	state sortState
	// nTuples is the number of tuples we've seen from our input.
	nTuples uint64
	// values stores all the values from the input
	values []ColVec
	order  []uint64

	sortCol []_GOTYPE

	// emitted is the index of the last value that was emitted by Next()
	emitted uint64
	output  ColBatch
}

var _ Operator = &sort_TYPEOp{}

func (p *sort_TYPEOp) Init() {
	p.input.Init()
	p.output = NewMemBatch(p.spec.inputTypes)
	p.values = make([]ColVec, len(p.spec.inputTypes))
	for i := 0; i < len(p.spec.inputTypes); i++ {
		p.values[i] = newMemColumn(p.spec.inputTypes[i], 0)
	}
}

func (p *sort_TYPEOp) Next() ColBatch {
	switch p.state {
	case sortSpooling:
		p.spool()
		p.state = sortEmitting
		fallthrough
	case sortEmitting:
		newEmitted := p.emitted + ColBatchSize
		if newEmitted > p.nTuples {
			newEmitted = p.nTuples
		}
		p.output.SetLength(uint16(newEmitted - p.emitted))
		if p.output.Length() == 0 {
			return p.output
		}

		sortColIdx := int(p.spec.sortCol)
		for j := 0; j < len(p.values); j++ {
			if j == sortColIdx {
				// the sortCol'th vec is already sorted, so just fill it directly.
				p.output.ColVec(j).Copy(p.values[j], p.emitted, newEmitted, p.spec.inputTypes[j])
			} else {
				p.output.ColVec(j).CopyWithSelInt64(p.values[j], p.order[p.emitted:], p.output.Length(), p.spec.inputTypes[j])
			}
		}
		p.emitted = newEmitted
		return p.output
	default:
		panic(fmt.Sprintf("invalid sorter state %d", p.state))
	}

}

func (p *sort_TYPEOp) spool() {
	batch := p.input.Next()
	for ; batch.Length() != 0; batch = p.input.Next() {
		// First, copy all vecs into values.
		for i := 0; i < len(p.values); i++ {
			if batch.Selection() == nil {
				p.values[i].Append(batch.ColVec(i),
					p.spec.inputTypes[i],
					p.nTuples,
					batch.Length(),
				)
			} else {
				p.values[i].AppendWithSel(batch.ColVec(i),
					batch.Selection(),
					batch.Length(),
					p.spec.inputTypes[i],
					p.nTuples,
				)
			}
		}
		p.nTuples += uint64(batch.Length())
	}

	p.order = make([]uint64, p.nTuples)
	for i := uint64(0); i < uint64(len(p.order)); i++ {
		p.order[i] = i
	}

	p.sortCol = p.values[p.spec.sortCol]._TemplateType()

	n := p.Len()
	p.quickSort(0, n, maxDepth(n))
}

func (s *sort_TYPEOp) Less(i, j int) bool {
	var lt bool
	_ASSIGN_LT("lt", "s.sortCol[i]", "s.sortCol[j]")
	return lt
}

func (s *sort_TYPEOp) Swap(i, j int) {
	// Swap needs to swap the values in the column being sorted, as otherwise
	// subsequent calls to Less would be incorrect.
	// We also store the swap order in s.order to swap all the other columns.
	s.sortCol[i], s.sortCol[j] = s.sortCol[j], s.sortCol[i]
	s.order[i], s.order[j] = s.order[j], s.order[i]
}

func (s *sort_TYPEOp) Len() int {
	return int(s.nTuples)
}

// {{end}}
