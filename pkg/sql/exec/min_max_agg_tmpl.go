// Copyright 2019 The Cockroach Authors.
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
// This file is the execgen template for min_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"bytes"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// {{/*
// Declarations to make the template compile properly

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "tree" package.
var _ tree.Datum

// _ASSIGN_LT is the template equality function for assigning the first input
// to the result of the second input < the third input.
func _ASSIGN_LT(_, _, _ string) bool {
	panic("")
}

// */}}

// {{range .}} {{/* for each aggregation (min and max) */}}

// {{/* Capture the aggregation name so we can use it in the inner loop. */}}
// {{$agg := .AggNameLower}}

func new_AGG_TITLEAgg(t types.T) (aggregateFunc, error) {
	switch t {
	// {{range .Overloads}}
	case _TYPES_T:
		return &_AGG_TYPEAgg{}, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported min agg type %s", t)
	}
}

// {{range .Overloads}}

type _AGG_TYPEAgg struct {
	done   bool
	groups []bool
	curIdx int
	// vec points to the output vector we are updating.
	vec []_GOTYPE
}

var _ aggregateFunc = &_AGG_TYPEAgg{}

func (a *_AGG_TYPEAgg) Init(groups []bool, v coldata.Vec) {
	a.groups = groups
	a.vec = v._TYPE()
	a.Reset()
}

func (a *_AGG_TYPEAgg) Reset() {
	copy(a.vec, zero_TYPEColumn)
	a.curIdx = -1
	a.done = false
}

func (a *_AGG_TYPEAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *_AGG_TYPEAgg) SetOutputIndex(idx int) {
	if a.curIdx != -1 {
		a.curIdx = idx
		copy(a.vec[idx+1:], zero_TYPEColumn)
	}
}

func (a *_AGG_TYPEAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		// The aggregation is finished. Flush the last value.
		a.curIdx++
		a.done = true
		return
	}
	col, sel := b.ColVec(int(inputIdxs[0]))._TYPE(), b.Selection()
	if sel != nil {
		sel = sel[:inputLen]
		for _, i := range sel {
			if a.groups[i] {
				a.curIdx++
				a.vec[a.curIdx] = col[i]
			} else {
				var cmp bool
				_ASSIGN_CMP("cmp", "col[i]", "a.vec[a.curIdx]")
				if cmp {
					a.vec[a.curIdx] = col[i]
				}
			}
		}
	} else {
		col = col[:inputLen]
		for i := range col {
			if a.groups[i] {
				a.curIdx++
				a.vec[a.curIdx] = col[i]
			} else {
				var cmp bool
				_ASSIGN_CMP("cmp", "col[i]", "a.vec[a.curIdx]")
				if cmp {
					a.vec[a.curIdx] = col[i]
				}
			}
		}
	}
}

// {{end}}
// {{end}}
