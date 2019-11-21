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
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
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

// Dummy import to pull in "time" package.
var _ time.Time

// */}}

func (m *memColumn) Append(args SliceArgs) {
	switch args.ColType {
	// {{range .}}
	case _TYPES_T:
		fromCol := args.Src._TemplateType()
		toCol := m._TemplateType()
		if args.Sel == nil {
			execgen.APPENDSLICE(toCol, fromCol, int(args.DestIdx), int(args.SrcStartIdx), int(args.SrcEndIdx))
		} else {
			sel := args.Sel[args.SrcStartIdx:args.SrcEndIdx]
			// TODO(asubiotto): We could be more efficient for fixed width types by
			// preallocating a destination slice (not so for variable length types).
			// Improve this.
			// {{if eq .LTyp.String "Bytes"}}
			// We need to truncate toCol before appending to it, so in case of Bytes,
			// we append an empty slice.
			execgen.APPENDSLICE(toCol, toCol, int(args.DestIdx), 0, 0)
			// {{else}}
			toCol = execgen.SLICE(toCol, 0, int(args.DestIdx))
			// {{end}}
			for _, selIdx := range sel {
				val := execgen.UNSAFEGET(fromCol, int(selIdx))
				execgen.APPENDVAL(toCol, val)
			}
		}
		m.nulls.set(args)
		m.col = toCol
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %s", args.ColType))
	}
}

// {{/*
func _COPY_WITH_SEL(
	m *memColumn, args CopySliceArgs, fromCol, toCol _GOTYPESLICE, sel interface{}, _SEL_ON_DEST bool,
) { // */}}
	// {{define "copyWithSel" -}}
	if args.Src.MaybeHasNulls() {
		nulls := args.Src.Nulls()
		for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
			if nulls.NullAt64(uint64(selIdx)) {
				// {{if .SelOnDest}}
				// Remove an unused warning in some cases.
				_ = i
				m.nulls.SetNull64(uint64(selIdx))
				// {{else}}
				m.nulls.SetNull64(uint64(i) + args.DestIdx)
				// {{end}}
			} else {
				v := execgen.UNSAFEGET(fromCol, int(selIdx))
				// {{if .SelOnDest}}
				m.nulls.UnsetNull64(uint64(selIdx))
				execgen.SET(toCol, int(selIdx), v)
				// {{else}}
				execgen.SET(toCol, i+int(args.DestIdx), v)
				// {{end}}
			}
		}
		return
	}
	// No Nulls.
	for i := range sel[args.SrcStartIdx:args.SrcEndIdx] {
		selIdx := sel[int(args.SrcStartIdx)+i]
		v := execgen.UNSAFEGET(fromCol, int(selIdx))
		// {{if .SelOnDest}}
		execgen.SET(toCol, int(selIdx), v)
		// {{else}}
		execgen.SET(toCol, i+int(args.DestIdx), v)
		// {{end}}
	}
	// {{end}}
	// {{/*
}

// */}}

func (m *memColumn) Copy(args CopySliceArgs) {
	if !args.SelOnDest {
		// We're about to overwrite this entire range, so unset all the nulls.
		m.Nulls().UnsetNullRange(args.DestIdx, args.DestIdx+(args.SrcEndIdx-args.SrcStartIdx))
	}
	// } else {
	// SelOnDest indicates that we're applying the input selection vector as a lens
	// into the output vector as well. We'll set the non-nulls by hand below.
	// }

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
		m.nulls.set(args.SliceArgs)
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %s", args.ColType))
	}
}

func (m *memColumn) Window(colType coltypes.T, start uint64, end uint64) Vec {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		col := m._TemplateType()
		return &memColumn{
			t:     colType,
			col:   execgen.WINDOW(col, int(start), int(end)),
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
