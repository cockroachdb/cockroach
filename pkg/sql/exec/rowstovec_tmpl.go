// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// {{/*
// +build execgen_template
//
// This file is the execgen template for rowstovec.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types/conv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
)

// {{/*

// Dummy import to pull in "apd" package.
var _ apd.Decimal

const (
	_FAMILY = semtypes.Family(0)
	_WIDTH  = int32(0)
)

type _GOTYPE interface{}

func _ROWS_TO_COL_VEC(
	rows sqlbase.EncDatumRows, vec coldata.Vec, columnIdx int, alloc *sqlbase.DatumAlloc,
) error { // */}}
	// {{define "rowsToColVec"}}
	col := vec._TemplateType()
	datumToPhysicalFn := conv.GetDatumToPhysicalFn(columnType)
	for i := range rows {
		row := rows[i]
		if row[columnIdx].Datum == nil {
			if err := row[columnIdx].EnsureDecoded(columnType, alloc); err != nil {
				return err
			}
		}
		datum := row[columnIdx].Datum
		if datum == tree.DNull {
			vec.Nulls().SetNull(uint16(i))
		} else {
			v, err := datumToPhysicalFn(datum)
			if err != nil {
				return err
			}
			col[i] = v.(_GOTYPE)
		}
	}
	// {{end}}
	// {{/*
	return nil
}

// */}}

// EncDatumRowsToColVec converts one column from EncDatumRows to a column
// vector. columnIdx is the 0-based index of the column in the EncDatumRows.
func EncDatumRowsToColVec(
	rows sqlbase.EncDatumRows,
	vec coldata.Vec,
	columnIdx int,
	columnType *semtypes.T,
	alloc *sqlbase.DatumAlloc,
) error {

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
			panic(fmt.Sprintf("unsupported width %d for column type %s", columnType.Width(), columnType.String()))
		}
		// {{ else }}
		_ROWS_TO_COL_VEC(rows, vec, columnIdx, columnType, alloc)
		// {{end}}
	// {{end}}
	default:
		panic(fmt.Sprintf("unsupported column type %s", columnType.String()))
	}
	return nil
}
