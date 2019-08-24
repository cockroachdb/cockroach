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
// This file is the execgen template for vec.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package coldata

import (
	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/exec/execgen"
	// */}}
	// HACK: crlfmt removes the "*/}}" comment if it's the last line in the import
	// block. This was picked because it sorts after "pkg/sql/exec/execgen" and
	// has no deps.
	_ "github.com/cockroachdb/cockroach/pkg/util/bufalloc"
)

// {{/*

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// _GOTYPESLICE is a template Go type slice variable.
type _GOTYPESLICE interface{}

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// */}}

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
				val := execgen.UNSAFEGET(fromCol, int(selIdx))
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

// {{/*
func _COPY_WITH_SEL(
	m *memColumn, args CopyArgs, fromCol, toCol _GOTYPESLICE, sel interface{}, _SEL_ON_DEST bool,
) { // */}}
	// {{define "copyWithSel"}}
	if args.Src.MaybeHasNulls() {
		nulls := args.Src.Nulls()
		n := execgen.LEN(toCol)
		toColSliced := execgen.SLICE(toCol, int(args.DestIdx), n)
		for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
			if nulls.NullAt64(uint64(selIdx)) {
				m.nulls.SetNull64(uint64(i) + args.DestIdx)
			} else {
				v := execgen.UNSAFEGET(fromCol, int(selIdx))
				// {{if .SelOnDest}}
				execgen.SET(toColSliced, int(selIdx), v)
				// {{else}}
				execgen.SET(toColSliced, i, v)
				// {{end}}
			}
		}
		return
	}
	// No Nulls.
	n := execgen.LEN(toCol)
	toColSliced := execgen.SLICE(toCol, int(args.DestIdx), n)
	for i := range sel[args.SrcStartIdx:args.SrcEndIdx] {
		selIdx := sel[int(args.SrcStartIdx)+i]
		v := execgen.UNSAFEGET(fromCol, int(selIdx))
		// {{if .SelOnDest}}
		execgen.SET(toColSliced, int(selIdx), v)
		// {{else}}
		execgen.SET(toColSliced, i, v)
		// {{end}}
	}
	// {{end}}
	// {{/*
}

// */}}

func (m *memColumn) Copy(args CopyArgs) {
	m.Nulls().UnsetNullRange(args.DestIdx, args.DestIdx+(args.SrcEndIdx-args.SrcStartIdx))

	switch args.ColType {
	// {{range .}}
	case _TYPES_T:
		fromCol := args.Src._TemplateType()
		toCol := m._TemplateType()
		if args.Sel64 != nil {
			sel := args.Sel64
			if args.SelOnDest {
				_COPY_WITH_SEL(m, args, sel, toCol, fromCol, true)
			} else {
				_COPY_WITH_SEL(m, args, sel, toCol, fromCol, false)
			}
			return
		} else if args.Sel != nil {
			sel := args.Sel
			if args.SelOnDest {
				_COPY_WITH_SEL(m, args, sel, toCol, fromCol, true)
			} else {
				_COPY_WITH_SEL(m, args, sel, toCol, fromCol, false)
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

func (m *memColumn) Slice(colType coltypes.T, start uint64, end uint64) Vec {
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

func (m *memColumn) PrettyValueAt(colIdx uint16, colType coltypes.T) string {
	if m.nulls.NullAt(colIdx) {
		return "NULL"
	}
	switch colType {
	// {{range .}}
	case _TYPES_T:
		col := m._TemplateType()
		v := execgen.UNSAFEGET(col, int(colIdx))
		return fmt.Sprintf("%v", v)
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

// Helper to set the value in a Vec when the type is unknown.
func SetValueAt(v Vec, elem interface{}, rowIdx uint16, colType coltypes.T) {
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
