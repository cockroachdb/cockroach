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

package main

import (
	"io"
	"text/template"
)

const rowsToVecTemplate = `
package exec

import (
  "fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// EncDatumRowsToColVec converts one column from EncDatumRows to a column
// vector.
func EncDatumRowsToColVec(
	rows sqlbase.EncDatumRows,
	vec ColVec,
	columnIdx int,
	columnType *sqlbase.ColumnType,
	alloc *sqlbase.DatumAlloc,
) error {
	nRows := uint16(len(rows))
	{{range .}}
	if columnType.SemanticType == sqlbase.{{.SemanticType}} && columnType.Width == {{.Width}} {
		col := vec.{{.ExecType}}()
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
				{{if .HasSetMethod}}
				col.Set(i, {{.DatumToPhysicalFn}})
				{{else}}
				col[i] = {{.DatumToPhysicalFn}}
				{{end}}
			}
		}
		return nil
	}
	{{end}}
	panic(fmt.Sprintf("Unsupported column type and width: %s, %d", columnType.SQLString(), columnType.Width))
}
`

// columnConversion defines a conversion from a sqlbase.ColumnType to an
// exec.ColVec.
type columnConversion struct {
	// SemanticType is the semantic type of the ColumnType.
	SemanticType string
	// Width is the optional width of the ColumnType.
	Width int32
	// ExecType is the exec.T to which we're converting. It should correspond to
	// a method name on exec.ColVec.
	ExecType string
	// HasSetMethod is true if the ColVec is an interface with a Set method rather
	// than just a slice.
	HasSetMethod bool
	// DatumToPhysicalFn is a stringified function for converting a datum to the
	// physical type used in the column vector.
	DatumToPhysicalFn string
}

var columnConversions = []columnConversion{
	{
		SemanticType:      "ColumnType_BOOL",
		ExecType:          "Bool",
		HasSetMethod:      true,
		DatumToPhysicalFn: "bool(*datum.(*tree.DBool))",
	},
	{
		SemanticType:      "ColumnType_FLOAT",
		ExecType:          "Float64",
		DatumToPhysicalFn: "float64(*datum.(*tree.DFloat))",
	},
	{
		SemanticType:      "ColumnType_INT",
		Width:             8,
		ExecType:          "Int8",
		DatumToPhysicalFn: "int8(*datum.(*tree.DInt))",
	},
	{
		SemanticType:      "ColumnType_INT",
		Width:             16,
		ExecType:          "Int16",
		DatumToPhysicalFn: "int16(*datum.(*tree.DInt))",
	},
	{
		SemanticType:      "ColumnType_INT",
		Width:             32,
		ExecType:          "Int32",
		DatumToPhysicalFn: "int32(*datum.(*tree.DInt))",
	},
	{
		SemanticType:      "ColumnType_INT",
		Width:             64,
		ExecType:          "Int64",
		DatumToPhysicalFn: "int64(*datum.(*tree.DInt))",
	},
	{
		SemanticType:      "ColumnType_INT",
		Width:             0,
		ExecType:          "Int64",
		DatumToPhysicalFn: "int64(*datum.(*tree.DInt))",
	},
	{
		SemanticType:      "ColumnType_BYTES",
		ExecType:          "Bytes",
		HasSetMethod:      true,
		DatumToPhysicalFn: "encoding.UnsafeConvertStringToBytes(string(*datum.(*tree.DBytes)))",
	},
	{
		SemanticType:      "ColumnType_STRING",
		ExecType:          "Bytes",
		HasSetMethod:      true,
		DatumToPhysicalFn: "encoding.UnsafeConvertStringToBytes(string(*datum.(*tree.DString)))",
	},
}

func genRowsToVec(wr io.Writer) error {
	tmpl, err := template.New("rowsToVec").Parse(rowsToVecTemplate)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, columnConversions)
}

var _ generator = genRowsToVec
