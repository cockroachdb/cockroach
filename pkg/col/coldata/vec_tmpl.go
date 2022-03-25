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
//go:build execgen_template
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

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = typeconv.DatumVecCanonicalTypeFamily
	_ apd.Context
	_ duration.Duration
	_ json.JSON
	_ = colexecerror.InternalError
	_ = errors.AssertionFailedf
)

// {{/*

// _GOTYPESLICE is the template variable.
type _GOTYPESLICE interface{}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// */}}

// TypedVecs represents a slice of Vecs that have been converted into the typed
// columns. The idea is that every Vec is stored both in Vecs slice as well as
// in the typed slice, in order. Components that know the type of the vector
// they are working with can then access the typed column directly, avoiding
// expensive type casts.
type TypedVecs struct {
	Vecs  []Vec
	Nulls []*Nulls

	// Fields below need to be accessed by an index mapped via ColsMap.
	// {{range .}}
	// {{range .WidthOverloads}}
	_TYPECols []_GOTYPESLICE
	// {{end}}
	// {{end}}
	// ColsMap contains the positions of the corresponding vectors in the slice
	// for the same types. For example, if we have a batch with
	//   types = [Int64, Int64, Bool, Bytes, Bool, Int64],
	// then ColsMap will be
	//                      [0, 1, 0, 0, 1, 2]
	//                       ^  ^  ^  ^  ^  ^
	//                       |  |  |  |  |  |
	//                       |  |  |  |  |  3rd among all Int64's
	//                       |  |  |  |  2nd among all Bool's
	//                       |  |  |  1st among all Bytes's
	//                       |  |  1st among all Bool's
	//                       |  2nd among all Int64's
	//                       1st among all Int64's
	ColsMap []int
}

// SetBatch updates TypedVecs to represent all vectors from batch.
func (v *TypedVecs) SetBatch(batch Batch) {
	v.Vecs = batch.ColVecs()
	if cap(v.Nulls) < len(v.Vecs) {
		v.Nulls = make([]*Nulls, len(v.Vecs))
		v.ColsMap = make([]int, len(v.Vecs))
	} else {
		v.Nulls = v.Nulls[:len(v.Vecs)]
		v.ColsMap = v.ColsMap[:len(v.Vecs)]
	}
	// {{range .}}
	// {{range .WidthOverloads}}
	v._TYPECols = v._TYPECols[:0]
	// {{end}}
	// {{end}}
	for i, vec := range v.Vecs {
		v.Nulls[i] = vec.Nulls()
		switch vec.CanonicalTypeFamily() {
		// {{range .}}
		case _CANONICAL_TYPE_FAMILY:
			switch vec.Type().Width() {
			// {{range .WidthOverloads}}
			case _TYPE_WIDTH:
				v.ColsMap[i] = len(v._TYPECols)
				v._TYPECols = append(v._TYPECols, vec.TemplateType())
				// {{end}}
			}
		// {{end}}
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", vec.Type()))
		}
	}
}

// Reset performs a deep reset of v while keeping the references to the slices.
func (v *TypedVecs) Reset() {
	v.Vecs = nil
	for i := range v.Nulls {
		v.Nulls[i] = nil
	}
	// {{range .}}
	// {{range .WidthOverloads}}
	for i := range v._TYPECols {
		v._TYPECols[i] = nil
	}
	// {{end}}
	// {{end}}
}

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
				toCol.appendSliceWithSel(fromCol, args.DestIdx, sel)
				// {{else}}
				// {{/* Here Window means slicing which allows us to use APPENDVAL below. */}}
				toCol = toCol.Window(0, args.DestIdx)
				for _, selIdx := range sel {
					val := fromCol.Get(selIdx)
					execgen.APPENDVAL(toCol, val)
				}
				// {{end}}
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

func (m *memColumn) Copy(args SliceArgs) {
	if args.SrcStartIdx == args.SrcEndIdx {
		// Nothing to copy, so return early.
		return
	}
	if m.Nulls().MaybeHasNulls() {
		// We're about to overwrite this entire range, so unset all the nulls.
		m.Nulls().UnsetNullRange(args.DestIdx, args.DestIdx+(args.SrcEndIdx-args.SrcStartIdx))
	}

	switch m.CanonicalTypeFamily() {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch m.t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			fromCol := args.Src.TemplateType()
			toCol := m.TemplateType()
			if args.Sel != nil {
				sel := args.Sel[args.SrcStartIdx:args.SrcEndIdx]
				n := len(sel)
				// {{if .Sliceable}}
				toCol = toCol[args.DestIdx:]
				_ = toCol[n-1]
				// {{end}}
				if args.Src.MaybeHasNulls() {
					nulls := args.Src.Nulls()
					for i := 0; i < n; i++ {
						//gcassert:bce
						selIdx := sel[i]
						if nulls.NullAt(selIdx) {
							m.nulls.SetNull(i + args.DestIdx)
						} else {
							// {{if .IsBytesLike}}
							toCol.Copy(fromCol, i+args.DestIdx, selIdx)
							// {{else}}
							v := fromCol.Get(selIdx)
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
					// {{if .IsBytesLike}}
					toCol.Copy(fromCol, i+args.DestIdx, selIdx)
					// {{else}}
					v := fromCol.Get(selIdx)
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
				return
			}
			// No Sel.
			toCol.CopySlice(fromCol, args.DestIdx, args.SrcStartIdx, args.SrcEndIdx)
			m.nulls.set(args)
			// {{end}}
		}
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %s", m.t))
	}
}

// {{/*
func _COPY_WITH_REORDERED_SOURCE(_SRC_HAS_NULLS bool) { // */}}
	// {{define "copyWithReorderedSource" -}}
	for i := 0; i < n; i++ {
		//gcassert:bce
		destIdx := sel[i]
		srcIdx := order[destIdx]
		// {{if .SrcHasNulls}}
		if nulls.NullAt(srcIdx) {
			m.nulls.SetNull(destIdx)
		} else
		// {{end}}
		{
			// {{if .Global.IsBytesLike}}
			toCol.Copy(fromCol, destIdx, srcIdx)
			// {{else}}
			v := fromCol.Get(srcIdx)
			toCol.Set(destIdx, v)
			// {{end}}
		}
	}
	// {{end}}
	// {{/*
}

// */}}

func (m *memColumn) CopyWithReorderedSource(src Vec, sel, order []int) {
	if len(sel) == 0 {
		return
	}
	if m.nulls.MaybeHasNulls() {
		m.nulls.UnsetNulls()
	}
	switch m.CanonicalTypeFamily() {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch m.t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			fromCol := src.TemplateType()
			toCol := m.TemplateType()
			n := len(sel)
			_ = sel[n-1]
			if src.MaybeHasNulls() {
				nulls := src.Nulls()
				_COPY_WITH_REORDERED_SOURCE(true)
			} else {
				_COPY_WITH_REORDERED_SOURCE(false)
			}
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
