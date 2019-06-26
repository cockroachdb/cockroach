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

func (m *memColumn) Copy(args CopyArgs) {
	if args.DestIdx != 0 && m.HasNulls() {
		panic("copying to non-zero dest index with nulls is not implemented yet (would overwrite nulls)")
	}
	if args.Nils != nil && args.Sel64 == nil {
		panic("Nils set without Sel64")
	}
	// TODO(asubiotto): This is extremely wrong (we might be overwriting nulls
	// past the end of where we are copying to that should be left alone).
	// Previous code did this though so we won't be introducing new problems. We
	// really have to fix and test this.
	m.Nulls().UnsetNulls()

	switch args.ColType {
	// {{range .}}
	case _TYPES_T:
		fromCol := args.Src._TemplateType()
		toCol := m._TemplateType()
		if args.Sel64 != nil {
			sel := args.Sel64
			// TODO(asubiotto): Template this and the uint16 case below.
			if args.Nils != nil {
				if args.Src.HasNulls() {
					nulls := args.Src.Nulls()
					toColSliced := toCol[args.DestIdx:]
					for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
						if args.Nils[i] || nulls.NullAt64(selIdx) {
							m.nulls.SetNull64(uint64(i) + args.DestIdx)
						} else {
							toColSliced[i] = fromCol[selIdx]
						}
					}
					return
				}
				// Nils but no Nulls.
				toColSliced := toCol[args.DestIdx:]
				for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
					if args.Nils[i] {
						m.nulls.SetNull64(uint64(i) + args.DestIdx)
					} else {
						toColSliced[i] = fromCol[selIdx]
					}
				}
				return
			}
			// No Nils.
			if args.Src.HasNulls() {
				nulls := args.Src.Nulls()
				toColSliced := toCol[args.DestIdx:]
				for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
					if nulls.NullAt64(selIdx) {
						m.nulls.SetNull64(uint64(i) + args.DestIdx)
					} else {
						toColSliced[i] = fromCol[selIdx]
					}
				}
				return
			}
			// No Nils or Nulls.
			toColSliced := toCol[args.DestIdx:]
			for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
				toColSliced[i] = fromCol[selIdx]
			}
			return
		} else if args.Sel != nil {
			sel := args.Sel
			if args.Src.HasNulls() {
				nulls := args.Src.Nulls()
				toColSliced := toCol[args.DestIdx:]
				for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
					if nulls.NullAt64(uint64(selIdx)) {
						m.nulls.SetNull64(uint64(i) + args.DestIdx)
					} else {
						toColSliced[i] = fromCol[selIdx]
					}
				}
				return
			}
			// No Nulls.
			toColSliced := toCol[args.DestIdx:]
			for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
				toColSliced[i] = fromCol[selIdx]
			}
			return
		}
		// No Sel or Sel64.
		copy(toCol[args.DestIdx:], fromCol[args.SrcStartIdx:args.SrcEndIdx])
		// We do not check for existence of nulls in m due to forcibly unsetting
		// the bitmap at the start.
		if args.Src.HasNulls() {
			m.nulls.hasNulls = true
			if args.DestIdx == 0 && args.SrcStartIdx == 0 {
				// We can copy this bitmap indiscriminately.
				copy(m.nulls.nulls, args.Src.Nulls().NullBitmap())
			} else {
				// TODO(asubiotto): This should use Extend but Extend only takes uint16
				// arguments.
				srcNulls := args.Src.Nulls()
				for curDestIdx, curSrcIdx := args.DestIdx, args.SrcStartIdx; curSrcIdx < args.SrcEndIdx; curDestIdx, curSrcIdx = curDestIdx+1, curSrcIdx+1 {
					if srcNulls.NullAt64(curSrcIdx) {
						m.nulls.SetNull64(curDestIdx)
					}
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
