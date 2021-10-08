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
// This file is the execgen template for rowstovec.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = typeconv.DatumVecCanonicalTypeFamily
	_ apd.Context
	_ duration.Duration
	_ encoding.Direction
	_ json.JSON
)

// {{/*

func _ROWS_TO_COL_VEC(
	rows rowenc.EncDatumRows, vec coldata.Vec, columnIdx int, alloc *rowenc.DatumAlloc,
) { // */}}
	// {{define "rowsToColVec" -}}
	col := vec.TemplateType()
	if len(rows) > 0 {
		_ = col.Get(len(rows) - 1)
		var v interface{}
		for i := range rows {
			row := rows[i]
			if row[columnIdx].Datum == nil {
				if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
					return
				}
			}
			datum := row[columnIdx].Datum
			if datum == tree.DNull {
				vec.Nulls().SetNull(i)
			} else {
				_PRELUDE(datum)
				v = _CONVERT(datum)
				castV := v.(_GOTYPE)
				// {{if .Sliceable}}
				// {{if not (eq .VecMethod "Decimal")}}
				// {{/*
				//     For some reason, decimal vectors - although sliceable -
				//     still have bounds checks.
				//     TODO(yuzefovich): figure it out.
				// */}}
				//gcassert:bce
				// {{end}}
				// {{end}}
				col.Set(i, castV)
			}
		}
	}
	// {{end}}
	// {{/*
}

// */}}

// EncDatumRowsToColVec converts one column from EncDatumRows to a column
// vector. columnIdx is the 0-based index of the column in the EncDatumRows.
func EncDatumRowsToColVec(
	allocator *colmem.Allocator,
	rows rowenc.EncDatumRows,
	vec coldata.Vec,
	columnIdx int,
	t *types.T,
	alloc *rowenc.DatumAlloc,
) error {
	var err error
	allocator.PerformOperation(
		[]coldata.Vec{vec},
		func() {
			switch t.Family() {
			// {{range .}}
			case _TYPE_FAMILY:
				switch t.Width() {
				// {{range .Widths}}
				case _TYPE_WIDTH:
					_ROWS_TO_COL_VEC(rows, vec, columnIdx, t, alloc)
					// {{end}}
				}
				// {{end}}
			}
		},
	)
	return err
}
