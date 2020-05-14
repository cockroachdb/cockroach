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

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

// Remove unused warning.
var _ = execgen.UNSAFEGET

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
	datumToPhysicalFn := getDatumToPhysicalFn(typ)
	for i := range rows {
		row := rows[i]
		if row[columnIdx].Datum == nil {
			if err = row[columnIdx].EnsureDecoded(typ, alloc); err != nil {
				return
			}
		}
		datum := row[columnIdx].Datum
		if datum == tree.DNull {
			vec.Nulls().SetNull(i)
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
	allocator *colmem.Allocator,
	rows sqlbase.EncDatumRows,
	vec coldata.Vec,
	columnIdx int,
	typ *types.T,
	alloc *sqlbase.DatumAlloc,
) error {
	var err error
	allocator.PerformOperation(
		[]coldata.Vec{vec},
		func() {
			switch typ.Family() {
			// {{range .}}
			case _FAMILY:
				// {{ if .Widths }}
				switch typ.Width() {
				// {{range .Widths}}
				case _WIDTH:
					_ROWS_TO_COL_VEC(rows, vec, columnIdx, typ, alloc)
				// {{end}}
				default:
					colexecerror.InternalError(fmt.Sprintf("unsupported width %d for type %s", typ.Width(), typ.String()))
				}
				// {{ else }}
				_ROWS_TO_COL_VEC(rows, vec, columnIdx, typ, alloc)
				// {{end}}
			// {{end}}
			default:
				colexecerror.InternalError(fmt.Sprintf("unsupported type %s", typ.String()))
			}
		},
	)
	return err
}
