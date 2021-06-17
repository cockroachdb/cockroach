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

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = typeconv.DatumVecCanonicalTypeFamily
	_ apd.Context
	_ duration.Duration
	_ json.JSON
)

// {{/*

// _GOTYPESLICE is the template variable.
type _GOTYPESLICE interface{}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// */}}

func (m *memColumn) Append(args SliceArgs) {
	switch m.CanonicalTypeFamily() {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch m.t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			fromCol := args.Src.TemplateType()
			toCol := m.TemplateType()
			// NOTE: it is unfortunate that we always append whole slice without paying
			// attention to whether the values are NULL. However, if we do start paying
			// attention, the performance suffers dramatically, so we choose to copy
			// over "actual" as well as "garbage" values.
			if args.Sel == nil {
				execgen.APPENDSLICE(toCol, fromCol, args.DestIdx, args.SrcStartIdx, args.SrcEndIdx)
			} else {
				sel := args.Sel[args.SrcStartIdx:args.SrcEndIdx]
				// {{if .IsBytesLike }}
				// We need to truncate toCol before appending to it, so in case of
				// bytes-like columns, we append an empty slice.
				execgen.APPENDSLICE(toCol, toCol, args.DestIdx, 0, 0)
				// {{else}}
				// {{/* Here Window means slicing which allows us to use APPENDVAL below. */}}
				toCol = toCol.Window(0, args.DestIdx)
				// {{end}}
				for _, selIdx := range sel {
					val := fromCol.Get(selIdx)
					execgen.APPENDVAL(toCol, val)
				}
			}
			m.nulls.set(args)
			m.col = toCol
			// {{end}}
		}
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %s", m.t))
	}
}

// {{/*
func _COPY_WITH_SEL(
	m *memColumn, args CopySliceArgs, fromCol, toCol _GOTYPESLICE, sel interface{}, _SEL_ON_DEST bool,
) { // */}}
	// {{define "copyWithSel" -}}
	sel = sel[args.SrcStartIdx:args.SrcEndIdx]
	n := len(sel)
	// {{if and (.Sliceable) (not .SelOnDest)}}
	toCol = toCol[args.DestIdx:]
	_ = toCol[n-1]
	// {{end}}
	if args.Src.MaybeHasNulls() {
		nulls := args.Src.Nulls()
		for i := 0; i < n; i++ {
			//gcassert:bce
			selIdx := sel[i]
			if nulls.NullAt(selIdx) {
				// {{if .SelOnDest}}
				m.nulls.SetNull(selIdx)
				// {{else}}
				m.nulls.SetNull(i + args.DestIdx)
				// {{end}}
			} else {
				v := fromCol.Get(selIdx)
				// {{if .SelOnDest}}
				m.nulls.UnsetNull(selIdx)
				toCol.Set(selIdx, v)
				// {{else}}
				// {{if .Sliceable}}
				// {{/*
				//     For the sliceable types, we sliced toCol to start at
				//     args.DestIdx, so we use index i directly.
				// */}}
				//gcassert:bce
				toCol.Set(i, v)
				// {{else}}
				// {{/*
				//     For the non-sliceable types, toCol vector is the original
				//     one (i.e. without an adjustment), so we need to add
				//     args.DestIdx to set the element at the correct index.
				// */}}
				toCol.Set(i+args.DestIdx, v)
				// {{end}}
				// {{end}}
			}
		}
		return
	}
	// No Nulls.
	for i := 0; i < n; i++ {
		//gcassert:bce
		selIdx := sel[i]
		v := fromCol.Get(selIdx)
		// {{if .SelOnDest}}
		toCol.Set(selIdx, v)
		// {{else}}
		// {{if .Sliceable}}
		// {{/*
		//     For the sliceable types, we sliced toCol to start at
		//     args.DestIdx, so we use index i directly.
		// */}}
		//gcassert:bce
		toCol.Set(i, v)
		// {{else}}
		// {{/*
		//     For the non-sliceable types, toCol vector is the original one
		//     (i.e. without an adjustment), so we need to add args.DestIdx to
		//     set the element at the correct index.
		// */}}
		toCol.Set(i+args.DestIdx, v)
		// {{end}}
		// {{end}}
	}
	// {{end}}
	// {{/*
}

// */}}

func (m *memColumn) Copy(args CopySliceArgs) {
	if args.SrcStartIdx == args.SrcEndIdx {
		// Nothing to copy, so return early.
		return
	}
	if !args.SelOnDest {
		// We're about to overwrite this entire range, so unset all the nulls.
		m.Nulls().UnsetNullRange(args.DestIdx, args.DestIdx+(args.SrcEndIdx-args.SrcStartIdx))
	}
	// } else {
	// SelOnDest indicates that we're applying the input selection vector as a lens
	// into the output vector as well. We'll set the non-nulls by hand below.
	// }

	switch m.CanonicalTypeFamily() {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch m.t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			fromCol := args.Src.TemplateType()
			toCol := m.TemplateType()
			if args.Sel != nil {
				sel := args.Sel
				if args.SelOnDest {
					_COPY_WITH_SEL(m, args, sel, toCol, fromCol, true)
				} else {
					_COPY_WITH_SEL(m, args, sel, toCol, fromCol, false)
				}
				return
			}
			// No Sel.
			toCol.CopySlice(fromCol, args.DestIdx, args.SrcStartIdx, args.SrcEndIdx)
			m.nulls.set(args.SliceArgs)
			// {{end}}
		}
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %s", m.t))
	}
}

func (m *memColumn) Window(start int, end int) Vec {
	switch m.CanonicalTypeFamily() {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch m.t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			col := m.TemplateType()
			return &memColumn{
				t:                   m.t,
				canonicalTypeFamily: m.canonicalTypeFamily,
				col:                 col.Window(start, end),
				nulls:               m.nulls.Slice(start, end),
			}
			// {{end}}
		}
		// {{end}}
	}
	panic(fmt.Sprintf("unhandled type %s", m.t))
}

// SetValueAt is an inefficient helper to set the value in a Vec when the type
// is unknown.
func SetValueAt(v Vec, elem interface{}, rowIdx int) {
	switch t := v.Type(); v.CanonicalTypeFamily() {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			target := v.TemplateType()
			newVal := elem.(_GOTYPE)
			target.Set(rowIdx, newVal)
			// {{end}}
		}
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %s", t))
	}
}

// GetValueAt is an inefficient helper to get the value in a Vec when the type
// is unknown.
func GetValueAt(v Vec, rowIdx int) interface{} {
	if v.Nulls().NullAt(rowIdx) {
		return nil
	}
	t := v.Type()
	switch v.CanonicalTypeFamily() {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			target := v.TemplateType()
			return target.Get(rowIdx)
			// {{end}}
		}
		// {{end}}
	}
	panic(fmt.Sprintf("unhandled type %s", t))
}
