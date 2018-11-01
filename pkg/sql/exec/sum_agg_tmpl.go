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
// This file is the execgen template for sum_agg.og.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// {{/*
// Declarations to make the template compile properly

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "tree" package.
var _ tree.Datum

// _ASSIGN_ADD is the template addition function for assigning the first input
// to the result of the second input + the third input.
func _ASSIGN_ADD(_, _, _ string) {
	panic("")
}

// */}}

func newSumAgg(t types.T) (aggregateFunc, error) {
	switch t {
	// {{range .}}
	case _TYPES_T:
		return &sum_TYPEAgg{}, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported sum agg type %s", t)
	}
}

// {{range .}}

type sum_TYPEAgg struct {
	done bool

	groups  []bool
	scratch struct {
		curIdx int
		// vec points to the output vector we are updating.
		vec []_GOTYPE
	}
}

var _ aggregateFunc = &sum_TYPEAgg{}

// TODO(asubiotto): Have all these zero batches somewhere else templated
// separately.
var zero_TYPEBatch = make([]_GOTYPE, ColBatchSize)

func (a *sum_TYPEAgg) Init(groups []bool, v ColVec) {
	a.groups = groups
	a.scratch.vec = v._TemplateType()
	a.Reset()
}

func (a *sum_TYPEAgg) Reset() {
	copy(a.scratch.vec, zero_TYPEBatch)
	a.scratch.curIdx = -1
}

func (a *sum_TYPEAgg) CurrentOutputIndex() int {
	return a.scratch.curIdx
}

func (a *sum_TYPEAgg) SetOutputIndex(idx int) {
	if a.scratch.curIdx != -1 {
		a.scratch.curIdx = idx
		copy(a.scratch.vec[idx+1:], zero_TYPEBatch)
	}
}

func (a *sum_TYPEAgg) Compute(b ColBatch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		// The aggregation is finished. Flush the last value.
		a.scratch.curIdx++
		a.done = true
		return
	}
	col, sel := b.ColVec(int(inputIdxs[0]))._TYPE(), b.Selection()
	if sel != nil {
		sel = sel[:inputLen]
		for _, i := range sel {
			x := 0
			if a.groups[i] {
				x = 1
			}
			a.scratch.curIdx += x
			_ASSIGN_ADD("a.scratch.vec[a.scratch.curIdx]", "a.scratch.vec[a.scratch.curIdx]", "col[i]")
		}
	} else {
		col = col[:inputLen]
		for i := range col {
			x := 0
			if a.groups[i] {
				x = 1
			}
			a.scratch.curIdx += x
			_ASSIGN_ADD("a.scratch.vec[a.scratch.curIdx]", "a.scratch.vec[a.scratch.curIdx]", "col[i]")
		}
	}
}

// {{end}}
