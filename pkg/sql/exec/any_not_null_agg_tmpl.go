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
// This file is the execgen template for distinct.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/pkg/errors"
)

func newAnyNotNullAgg(t types.T) (aggregateFunc, error) {
	switch t {
	// {{range .}}
	case _TYPES_T:
		return &anyNotNull_TYPEAgg{}, nil
		// {{end}}
	default:
		return nil, errors.Errorf("unsupported any not null agg type %s", t)
	}
}

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in types.T, for example
// int64 for types.Int64.
type _GOTYPE interface{}

// _TYPES_T is the template type variable for types.T. It will be replaced by
// types.Foo for each type Foo in the types.T type.
const _TYPES_T = types.Unhandled

// */}}

// {{range .}}

// anyNotNull_TYPEAgg implements the ANY_NOT_NULL aggregate, returning the
// first non-null value in the input column.
// TODO(jordan): this needs to have null handling when it's added!
type anyNotNull_TYPEAgg struct {
	done   bool
	groups []bool
	vec    []_GOTYPE
	curIdx int
}

func (a *anyNotNull_TYPEAgg) Init(groups []bool, vec ColVec) {
	a.groups = groups
	a.vec = vec._TemplateType()
	a.Reset()
}

func (a *anyNotNull_TYPEAgg) Reset() {
	a.curIdx = -1
}

func (a *anyNotNull_TYPEAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *anyNotNull_TYPEAgg) SetOutputIndex(idx int) {
	if a.curIdx != -1 {
		a.curIdx = idx
	}
}

func (a *anyNotNull_TYPEAgg) Compute(b ColBatch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		a.curIdx++
		a.done = true
		return
	}
	col, sel := b.ColVec(int(inputIdxs[0]))._TemplateType(), b.Selection()
	if sel != nil {
		sel = sel[:inputLen]
		for _, i := range sel {
			if a.groups[i] {
				a.curIdx++
				a.vec[a.curIdx] = col[i]
			}
		}
	} else {
		col = col[:inputLen]
		for i := range col {
			if a.groups[i] {
				a.curIdx++
				a.vec[a.curIdx] = col[i]
			}
		}
	}
}

// {{end}}
