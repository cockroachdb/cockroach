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
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// {{/*

// Dummy import to pull in "apd" package.
var _ apd.Decimal

const (
	_SEMANTIC_TYPE = sqlbase.ColumnType_SemanticType(0)
	_WIDTH         = int32(0)
)

type _GOTYPE interface{}

func _ROWS_TO_COL_VEC(
	rows sqlbase.EncDatumRows, vec ColVec, columnIdx int, alloc *sqlbase.DatumAlloc,
) error { // */}}
	// {{define "rowsToColVec"}}
	nRows := uint16(len(rows))
	col := vec._TemplateType()
	datumToPhysicalFn := types.GetDatumToPhysicalFn(*columnType)
	for i := uint16(0); i < nRows; i++ {
		if rows[i][columnIdx].Datum == nil {
			if err := rows[i][columnIdx].EnsureDecoded(columnType, alloc); err != nil {
				return err
			}
		}
		datum := rows[i][columnIdx].Datum
		if datum == tree.DNull {
			vec.SetNull(i)
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
	vec ColVec,
	columnIdx int,
	columnType *sqlbase.ColumnType,
	alloc *sqlbase.DatumAlloc,
) error {

	switch columnType.SemanticType {
	// {{range .}}
	case _SEMANTIC_TYPE:
		// {{ if .Widths }}
		switch columnType.Width {
		// {{range .Widths}}
		case _WIDTH:
			_ROWS_TO_COL_VEC(rows, vec, columnIdx, columnType, alloc)
		// {{end}}
		default:
			panic(fmt.Sprintf("unsupported width %d for column type %s", columnType.Width, columnType.SQLString()))
		}
		// {{ else }}
		_ROWS_TO_COL_VEC(rows, vec, columnIdx, columnType, alloc)
		// {{end}}
	// {{end}}
	default:
		panic(fmt.Sprintf("unsupported column type %s", columnType.SQLString()))
	}
	return nil
}
