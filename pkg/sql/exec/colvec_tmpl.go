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
// This file is the execgen template for colvec.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// {{/*

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// */}}

func (m *memColumn) Append(vec ColVec, colType types.T, toLength uint64, fromLength uint16) {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		m.col = append(m._TemplateType()[:toLength], vec._TemplateType()[:fromLength]...)
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) AppendWithSel(
	vec ColVec, sel []uint16, batchSize uint16, colType types.T, toLength uint64,
) {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := append(m._TemplateType()[:toLength], make([]_GOTYPE, batchSize)...)
		fromCol := vec._TemplateType()

		for i := uint16(0); i < batchSize; i++ {
			toCol[uint64(i)+toLength] = fromCol[sel[i]]
		}

		m.col = toCol
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) Copy(src ColVec, srcStartIdx, srcEndIdx uint64, typ types.T) {
	switch typ {
	// {{range .}}
	case _TYPES_T:
		copy(m._TemplateType(), src._TemplateType()[srcStartIdx:srcEndIdx])
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", typ))
	}
}

func (m *memColumn) CopyWithSelInt64(vec ColVec, sel []uint64, nSel uint16, colType types.T) {
	// todo (changangela): handle the case when nSel > ColBatchSize
	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := m._TemplateType()
		fromCol := vec._TemplateType()

		for i := uint16(0); i < nSel; i++ {
			toCol[i] = fromCol[sel[i]]
		}
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) CopyWithSelInt16(vec ColVec, sel []uint16, nSel uint16, colType types.T) {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := m._TemplateType()
		fromCol := vec._TemplateType()

		for i := uint16(0); i < nSel; i++ {
			toCol[i] = fromCol[sel[i]]
		}
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}
