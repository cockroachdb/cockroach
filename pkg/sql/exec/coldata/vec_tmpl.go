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

func (m *memColumn) Append(args AppendArgs) {
	switch args.ColType {
	// {{range .}}
	case _TYPES_T:
		fromCol := args.Src._TemplateType()
		toCol := m._TemplateType()
		numToAppend := args.SrcEndIdx - args.SrcStartIdx
		if args.Sel == nil {
			toCol = append(toCol[:args.DestIdx], fromCol[args.SrcStartIdx:args.SrcEndIdx]...)
			m.nulls.Extend(args.Src.Nulls(), args.DestIdx, args.SrcStartIdx, numToAppend)
		} else {
			sel := args.Sel[args.SrcStartIdx:args.SrcEndIdx]
			appendVals := make([]_GOTYPE, len(sel))
			for i, selIdx := range sel {
				appendVals[i] = fromCol[selIdx]
			}
			toCol = append(toCol[:args.DestIdx], appendVals...)
			// TODO(asubiotto): Change Extend* signatures to allow callers to pass in
			// SrcEndIdx instead of numToAppend.
			m.nulls.ExtendWithSel(args.Src.Nulls(), args.DestIdx, args.SrcStartIdx, numToAppend, args.Sel)
		}
		m.col = toCol
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %s", args.ColType))
	}
}

func (m *memColumn) Copy(src Vec, srcStartIdx, srcEndIdx uint64, typ types.T) {
	m.CopyAt(src, 0, srcStartIdx, srcEndIdx, typ)
}

func (m *memColumn) CopyAt(src Vec, destStartIdx, srcStartIdx, srcEndIdx uint64, typ types.T) {
	switch typ {
	// {{range .}}
	case _TYPES_T:
		copy(m._TemplateType()[destStartIdx:], src._TemplateType()[srcStartIdx:srcEndIdx])
		// TODO(asubiotto): Improve this, there are cases where we don't need to
		// allocate a new bitmap.
		srcBitmap := src.Nulls().NullBitmap()
		m.nulls.nulls = make([]byte, len(srcBitmap))
		m.nulls.UnsetNulls()
		if !src.HasNulls() {
			return
		}
		if destStartIdx == 0 {
			m.nulls.hasNulls = true
			copy(m.nulls.nulls, srcBitmap)
		} else {
			// The above strategy to just copy will not work. Fall back to a loop.
			// TODO(asubiotto): This can be improved as well.
			srcNulls := src.Nulls()
			for curDestIdx, curSrcIdx := destStartIdx, srcStartIdx; curSrcIdx < srcEndIdx; curDestIdx, curSrcIdx = curDestIdx+1, curSrcIdx+1 {
				if srcNulls.NullAt64(curSrcIdx) {
					m.nulls.SetNull64(curDestIdx)
				}
			}
		}
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", typ))
	}
}

func (m *memColumn) CopyWithSelInt64(vec Vec, sel []uint64, nSel uint16, colType types.T) {
	m.nulls.UnsetNulls()

	// todo (changangela): handle the case when nSel > BatchSize
	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := m._TemplateType()
		fromCol := vec._TemplateType()

		if vec.HasNulls() {
			for i := uint16(0); i < nSel; i++ {
				if vec.Nulls().NullAt64(sel[i]) {
					m.nulls.SetNull(i)
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
	m.nulls.UnsetNulls()

	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := m._TemplateType()
		fromCol := vec._TemplateType()

		if vec.HasNulls() {
			for i := uint16(0); i < nSel; i++ {
				if vec.Nulls().NullAt(sel[i]) {
					m.nulls.SetNull(i)
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
	m.nulls.UnsetNulls()

	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := m._TemplateType()
		fromCol := vec._TemplateType()

		if vec.HasNulls() {
			// TODO(jordan): copy the null arrays in batch.
			for i := uint16(0); i < nSel; i++ {
				if nils[i] {
					m.nulls.SetNull(i)
				} else {
					if vec.Nulls().NullAt64(sel[i]) {
						m.nulls.SetNull(i)
					} else {
						toCol[i] = fromCol[sel[i]]
					}
				}
			}
		} else {
			for i := uint16(0); i < nSel; i++ {
				if nils[i] {
					m.nulls.SetNull(i)
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
		return &memColumn{
			col:   col[start:end],
			nulls: m.nulls.Slice(start, end),
		}
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) PrettyValueAt(colIdx uint16, colType types.T) string {
	if m.nulls.NullAt(colIdx) {
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
