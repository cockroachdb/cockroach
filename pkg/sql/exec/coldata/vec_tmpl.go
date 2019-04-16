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

package coldata

import (
	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// {{/*

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// _TYPES_T is the template type variable for types.T. It will be replaced by
// types.Foo for each type Foo in the types.T type.
const _TYPES_T = types.Unhandled

// */}}

func (m *memColumn) Append(vec Vec, colType types.T, toLength uint64, fromLength uint16) {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		m.col = append(m._TemplateType()[:toLength], vec._TemplateType()[:fromLength]...)
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}

	if fromLength > 0 {
		m.nulls = append(m.nulls, make([]int64, (fromLength-1)>>6+1)...)

		if vec.HasNulls() {
			for i := uint16(0); i < fromLength; i++ {
				if vec.NullAt(i) {
					m.SetNull64(toLength + uint64(i))
				}
			}
		}
	}
}

func (m *memColumn) AppendSlice(
	vec Vec, colType types.T, destStartIdx uint64, srcStartIdx uint16, srcEndIdx uint16,
) {
	batchSize := srcEndIdx - srcStartIdx
	outputLen := destStartIdx + uint64(batchSize)

	switch colType {
	// {{range .}}
	case _TYPES_T:
		if outputLen > uint64(len(m._TemplateType())) {
			m.col = append(m._TemplateType()[:destStartIdx], vec._TemplateType()[srcStartIdx:srcEndIdx]...)
		} else {
			copy(m._TemplateType()[destStartIdx:], vec._TemplateType()[srcStartIdx:srcEndIdx])
		}
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}

	m.ExtendNulls(vec, destStartIdx, srcStartIdx, batchSize)
}

func (m *memColumn) AppendWithSel(
	vec Vec, sel []uint16, batchSize uint16, colType types.T, toLength uint64,
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

	if batchSize > 0 {
		m.nulls = append(m.nulls, make([]int64, (batchSize-1)>>6+1)...)
		for i := uint16(0); i < batchSize; i++ {
			if vec.NullAt(sel[i]) {
				m.SetNull64(toLength + uint64(i))
			}
		}
	}
}

func (m *memColumn) AppendSliceWithSel(
	vec Vec, colType types.T, destStartIdx uint64, srcStartIdx uint16, srcEndIdx uint16, sel []uint16,
) {
	batchSize := srcEndIdx - srcStartIdx
	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := append(m._TemplateType()[:destStartIdx], make([]_GOTYPE, batchSize)...)
		fromCol := vec._TemplateType()

		for i := 0; i < int(batchSize); i++ {
			toCol[uint64(i)+destStartIdx] = fromCol[sel[i+int(srcStartIdx)]]
		}

		m.col = toCol
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}

	m.ExtendNullsWithSel(vec, destStartIdx, srcStartIdx, batchSize, sel)
}

func (m *memColumn) Copy(src Vec, srcStartIdx, srcEndIdx uint64, typ types.T) {
	m.CopyAt(src, 0, srcStartIdx, srcEndIdx, typ)
}

func (m *memColumn) CopyAt(src Vec, destStartIdx, srcStartIdx, srcEndIdx uint64, typ types.T) {
	switch typ {
	// {{range .}}
	case _TYPES_T:
		copy(m._TemplateType()[destStartIdx:], src._TemplateType()[srcStartIdx:srcEndIdx])
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", typ))
	}
}

func (m *memColumn) CopyWithSelInt64(vec Vec, sel []uint64, nSel uint16, colType types.T) {
	m.UnsetNulls()

	// todo (changangela): handle the case when nSel > BatchSize
	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := m._TemplateType()
		fromCol := vec._TemplateType()

		if vec.HasNulls() {
			for i := uint16(0); i < nSel; i++ {
				if vec.NullAt64(sel[i]) {
					m.SetNull(i)
				} else {
					toCol[i] = fromCol[sel[i]]
				}
			}
		} else {
			for i := uint16(0); i < nSel; i++ {
				toCol[i] = fromCol[sel[i]]
			}
		}
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) CopyWithSelInt16(vec Vec, sel []uint16, nSel uint16, colType types.T) {
	m.UnsetNulls()

	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := m._TemplateType()
		fromCol := vec._TemplateType()

		if vec.HasNulls() {
			for i := uint16(0); i < nSel; i++ {
				if vec.NullAt(sel[i]) {
					m.SetNull(i)
				} else {
					toCol[i] = fromCol[sel[i]]
				}
			}
		} else {
			for i := uint16(0); i < nSel; i++ {
				toCol[i] = fromCol[sel[i]]
			}
		}
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) CopyWithSelAndNilsInt64(
	vec Vec, sel []uint64, nSel uint16, nils []bool, colType types.T,
) {
	m.UnsetNulls()

	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := m._TemplateType()
		fromCol := vec._TemplateType()

		if vec.HasNulls() {
			// TODO(jordan): copy the null arrays in batch.
			for i := uint16(0); i < nSel; i++ {
				if nils[i] {
					m.SetNull(i)
				} else {
					if vec.NullAt64(sel[i]) {
						m.SetNull(i)
					} else {
						toCol[i] = fromCol[sel[i]]
					}
				}
			}
		} else {
			for i := uint16(0); i < nSel; i++ {
				if nils[i] {
					m.SetNull(i)
				} else {
					toCol[i] = fromCol[sel[i]]
				}
			}
		}
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) Slice(colType types.T, start uint64, end uint64) Vec {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		col := m._TemplateType()
		var nulls []int64
		if m.hasNulls {
			mod := start % 64
			startIdx := start >> 6
			// end is exclusive, so translate that to an exclusive index in nulls by
			// figuring out which index the last accessible null should be in and add
			// 1.
			endIdx := (end-1)>>6 + 1
			nulls = m.nulls[startIdx:endIdx]
			if mod != 0 {
				// If start is not a multiple of 64, we need to shift over the bitmap
				// to have the first index correspond. Allocate new null bitmap as we
				// want to keep the original bitmap safe for reuse.
				nulls = make([]int64, len(nulls))
				for i, j := startIdx, 0; i < endIdx-1; i, j = i+1, j+1 {
					// Bring the first null to the beginning.
					nulls[j] = m.nulls[i] >> mod
					// And now bitwise or the remaining bits with the bits we want to
					// bring over from the next index, note that we handle endIdx-1
					// separately.
					nulls[j] |= (m.nulls[i+1] << (64 - mod))
				}
				// Get the first bits to where we want them for endIdx-1.
				nulls[len(nulls)-1] = m.nulls[endIdx-1] >> mod
			}
		}
		return &memColumn{
			col:      col[start:end],
			nulls:    nulls,
			hasNulls: m.hasNulls,
		}
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) PrettyValueAt(colIdx uint16, colType types.T) string {
	if m.NullAt(colIdx) {
		return "NULL"
	}
	switch colType {
	// {{range .}}
	case _TYPES_T:
		return fmt.Sprintf("%v", m._TemplateType()[colIdx])
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) ExtendNulls(vec Vec, destStartIdx uint64, srcStartIdx uint16, toAppend uint16) {
	outputLen := destStartIdx + uint64(toAppend)
	if uint64(cap(m.nulls)) < outputLen/64 {
		// (batchSize-1)>>6+1 is the number of Int64s needed to encode the additional elements/nulls in the Vec.
		// This is equivalent to ceil(batchSize/64).
		m.nulls = append(m.nulls, make([]int64, (toAppend-1)>>6+1)...)
	}
	if vec.HasNulls() {
		for i := uint16(0); i < toAppend; i++ {
			// TODO(yuzefovich): this can be done more efficiently with a bitwise OR:
			// like m.nulls[i] |= vec.nulls[i].
			if vec.NullAt(srcStartIdx + i) {
				m.SetNull64(destStartIdx + uint64(i))
			}
		}
	}
}

func (m *memColumn) ExtendNullsWithSel(
	vec Vec, destStartIdx uint64, srcStartIdx uint16, toAppend uint16, sel []uint16,
) {
	outputLen := destStartIdx + uint64(toAppend)
	if uint64(cap(m.nulls)) < outputLen/64 {
		// (batchSize-1)>>6+1 is the number of Int64s needed to encode the additional elements/nulls in the Vec.
		// This is equivalent to ceil(batchSize/64).
		m.nulls = append(m.nulls, make([]int64, (toAppend-1)>>6+1)...)
	}
	for i := uint16(0); i < toAppend; i++ {
		// TODO(yuzefovich): this can be done more efficiently with a bitwise OR:
		// like m.nulls[i] |= vec.nulls[i].
		if vec.NullAt(sel[srcStartIdx+i]) {
			m.SetNull64(destStartIdx + uint64(i))
		}
	}
}
