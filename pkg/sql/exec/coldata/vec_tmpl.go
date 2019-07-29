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
	"github.com/cockroachdb/cockroach/pkg/sql/exec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// {{/*

// _TYPES_T is the template type variable for types.T. It will be replaced by
// types.Foo for each type Foo in the types.T type.
const _TYPES_T = types.Unhandled

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// */}}

// Use execgen package to remove unused import warning.
var _ interface{} = execgen.GET

func (m *memColumn) Append(args AppendArgs) {
	switch args.ColType {
	// {{range .}}
	case _TYPES_T:
		fromCol := args.Src._TemplateType()
		toCol := m._TemplateType()
		numToAppend := args.SrcEndIdx - args.SrcStartIdx
		if args.Sel == nil {
			execgen.APPENDSLICE(toCol, fromCol, int(args.DestIdx), int(args.SrcStartIdx), int(args.SrcEndIdx))
			m.nulls.Extend(args.Src.Nulls(), args.DestIdx, args.SrcStartIdx, numToAppend)
		} else {
			sel := args.Sel[args.SrcStartIdx:args.SrcEndIdx]
			// TODO(asubiotto): We could be more efficient for fixed width types by
			// preallocating a destination slice (not so for variable length types).
			// Improve this.
			toCol = execgen.SLICE(toCol, 0, int(args.DestIdx))
			for _, selIdx := range sel {
				val := execgen.GET(fromCol, int(selIdx))
				execgen.APPENDVAL(toCol, val)
			}
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

func (m *memColumn) Copy(args CopyArgs) {
	if args.Nils != nil && args.Sel64 == nil {
		panic("Nils set without Sel64")
	}

	m.Nulls().UnsetNullRange(args.DestIdx, args.DestIdx+(args.SrcEndIdx-args.SrcStartIdx))

	switch args.ColType {
	// {{range .}}
	case _TYPES_T:
		fromCol := args.Src._TemplateType()
		toCol := m._TemplateType()
		if args.Sel64 != nil {
			sel := args.Sel64
			// TODO(asubiotto): Template this and the uint16 case below.
			if args.Nils != nil {
				if args.Src.MaybeHasNulls() {
					nulls := args.Src.Nulls()
					n := execgen.LEN(toCol)
					toColSliced := execgen.SLICE(toCol, int(args.DestIdx), n)
					for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
						if args.Nils[i] || nulls.NullAt64(selIdx) {
							m.nulls.SetNull64(uint64(i) + args.DestIdx)
						} else {
							v := execgen.GET(fromCol, int(selIdx))
							execgen.SET(toColSliced, i, v)
						}
					}
					return
				}
				// Nils but no Nulls.
				n := execgen.LEN(toCol)
				toColSliced := execgen.SLICE(toCol, int(args.DestIdx), n)
				for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
					if args.Nils[i] {
						m.nulls.SetNull64(uint64(i) + args.DestIdx)
					} else {
						v := execgen.GET(fromCol, int(selIdx))
						execgen.SET(toColSliced, i, v)
					}
				}
				return
			}
			// No Nils.
			if args.Src.MaybeHasNulls() {
				nulls := args.Src.Nulls()
				n := execgen.LEN(toCol)
				toColSliced := execgen.SLICE(toCol, int(args.DestIdx), n)
				for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
					if nulls.NullAt64(selIdx) {
						m.nulls.SetNull64(uint64(i) + args.DestIdx)
					} else {
						v := execgen.GET(fromCol, int(selIdx))
						execgen.SET(toColSliced, i, v)
					}
				}
				return
			}
			// No Nils or Nulls.
			n := execgen.LEN(toCol)
			toColSliced := execgen.SLICE(toCol, int(args.DestIdx), n)
			for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
				v := execgen.GET(fromCol, int(selIdx))
				execgen.SET(toColSliced, i, v)
			}
			return
		} else if args.Sel != nil {
			sel := args.Sel
			if args.Src.MaybeHasNulls() {
				nulls := args.Src.Nulls()
				n := execgen.LEN(toCol)
				toColSliced := execgen.SLICE(toCol, int(args.DestIdx), n)
				for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
					if nulls.NullAt64(uint64(selIdx)) {
						m.nulls.SetNull64(uint64(i) + args.DestIdx)
					} else {
						v := execgen.GET(fromCol, int(selIdx))
						execgen.SET(toColSliced, i, v)
					}
				}
				return
			}
			// No Nulls.
			n := execgen.LEN(toCol)
			toColSliced := execgen.SLICE(toCol, int(args.DestIdx), n)
			for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
				v := execgen.GET(fromCol, int(selIdx))
				execgen.SET(toColSliced, i, v)
			}
			return
		}
		// No Sel or Sel64.
		execgen.COPYSLICE(toCol, fromCol, int(args.DestIdx), int(args.SrcStartIdx), int(args.SrcEndIdx))
		if args.Src.MaybeHasNulls() {
			// TODO(asubiotto): This should use Extend but Extend only takes uint16
			// arguments.
			srcNulls := args.Src.Nulls()
			for curDestIdx, curSrcIdx := args.DestIdx, args.SrcStartIdx; curSrcIdx < args.SrcEndIdx; curDestIdx, curSrcIdx = curDestIdx+1, curSrcIdx+1 {
				if srcNulls.NullAt64(curSrcIdx) {
					m.nulls.SetNull64(curDestIdx)
				}
			}
		}
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %s", args.ColType))
	}
}

func (m *memColumn) Slice(colType types.T, start uint64, end uint64) Vec {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		col := m._TemplateType()
		return &memColumn{
			col:   execgen.SLICE(col, int(start), int(end)),
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
		col := m._TemplateType()
		v := execgen.GET(col, int(colIdx))
		return fmt.Sprintf("%v", v)
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

// Helper to set the value in a Vec when the type is unknown.
func SetValueAt(v Vec, elem interface{}, rowIdx uint16, colType types.T) {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		target := v._TemplateType()
		newVal := elem.(_GOTYPE)
		execgen.SET(target, int(rowIdx), newVal)
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}
