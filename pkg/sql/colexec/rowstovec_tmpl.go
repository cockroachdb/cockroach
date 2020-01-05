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
	"fmt"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	// */}}
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

// {{/*

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

const (
	_FAMILY = types.Family(0)
	_WIDTH  = int32(0)
)

type _GOTYPE interface{}

func _ROWS_TO_COL_VEC(
	rows sqlbase.EncDatumRows, vec coldata.Vec, columnIdx int, alloc *sqlbase.DatumAlloc,
) error { // */}}
	// {{define "rowsToColVec" -}}
	col := vec._TemplateType()
	datumToPhysicalFn := typeconv.GetDatumToPhysicalFn(columnType)
	for i := range rows {
		row := rows[i]
		if row[columnIdx].Datum == nil {
			if err = row[columnIdx].EnsureDecoded(columnType, alloc); err != nil {
				return
			}
		}
		datum := row[columnIdx].Datum
		if datum == tree.DNull {
			vec.Nulls().SetNull(uint16(i))
		} else {
			v, err := datumToPhysicalFn(datum)
			if err != nil {
				return
			}

			castV := v.(_GOTYPE)
			execgen.SET(col, i, castV)
		}
	}
	// {{end}}
	// {{/*
}

// */}}

// EncDatumRowsToColVec converts one column from EncDatumRows to a column
// vector. columnIdx is the 0-based index of the column in the EncDatumRows.
func EncDatumRowsToColVec(
	allocator *Allocator,
	rows sqlbase.EncDatumRows,
	vec coldata.Vec,
	columnIdx int,
	columnType *types.T,
	alloc *sqlbase.DatumAlloc,
) error {
	var err error
	allocator.PerformOperation(
		[]coldata.Vec{vec},
		func() {
			switch columnType.Family() {
			// {{range .}}
			case _FAMILY:
				// {{ if .Widths }}
				switch columnType.Width() {
				// {{range .Widths}}
				case _WIDTH:
					_ROWS_TO_COL_VEC(rows, vec, columnIdx, columnType, alloc)
				// {{end}}
				default:
					execerror.VectorizedInternalPanic(fmt.Sprintf("unsupported width %d for column type %s", columnType.Width(), columnType.String()))
				}
				// {{ else }}
				_ROWS_TO_COL_VEC(rows, vec, columnIdx, columnType, alloc)
				// {{end}}
			// {{end}}
			default:
				execerror.VectorizedInternalPanic(fmt.Sprintf("unsupported column type %s", columnType.String()))
			}
		},
	)
	return err
}
