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
	"fmt"
	"io"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
// vector. columnIdx is the 0-based index of the column in the EncDatumRows.
func EncDatumRowsToColVec(
	rows sqlbase.EncDatumRows,
	vec ColVec,
	columnIdx int,
	columnType *sqlbase.ColumnType,
	alloc *sqlbase.DatumAlloc,
) error {
	nRows := uint16(len(rows))
  // TODO(solon): Make this chain of conditionals more efficient: either a
  // switch statement or even better a lookup table on SemanticType. Also get
  // rid of the somewhat dubious assumption that Width is unset (0) for column
  // types where it does not apply.
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

func genRowsToVec(wr io.Writer) error {
	// Build the list of supported column conversions.
	var columnConversions []columnConversion
	for s, name := range sqlbase.ColumnType_SemanticType_name {
		semanticType := sqlbase.ColumnType_SemanticType(s)
		for _, width := range getWidths(semanticType) {
			ct := sqlbase.ColumnType{SemanticType: semanticType, Width: width}
			t := types.FromColumnType(ct)
			if t == types.Unhandled {
				continue
			}
			conversion := columnConversion{
				SemanticType: "ColumnType_" + name,
				Width:        width,
				ExecType:     t.String(),
				// TODO(solon): Determine the following fields via reflection.
				HasSetMethod:      t == types.Bool || t == types.Bytes,
				DatumToPhysicalFn: getDatumToPhysicalFn(ct),
			}
			columnConversions = append(columnConversions, conversion)
		}
	}

	tmpl, err := template.New("rowsToVec").Parse(rowsToVecTemplate)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, columnConversions)
}

func init() {
	registerGenerator(genRowsToVec, "rowstovec.og.go")
}

// getWidths returns allowable ColumnType.Width values for the specified
// SemanticType.
func getWidths(semanticType sqlbase.ColumnType_SemanticType) []int32 {
	if semanticType == sqlbase.ColumnType_INT {
		return []int32{0, 8, 16, 32, 64}
	}
	return []int32{0}
}

func getDatumToPhysicalFn(ct sqlbase.ColumnType) string {
	switch ct.SemanticType {
	case sqlbase.ColumnType_BOOL:
		return "bool(*datum.(*tree.DBool))"
	case sqlbase.ColumnType_BYTES:
		return "encoding.UnsafeConvertStringToBytes(string(*datum.(*tree.DBytes)))"
	case sqlbase.ColumnType_INT:
		switch ct.Width {
		case 8:
			return "int8(*datum.(*tree.DInt))"
		case 16:
			return "int16(*datum.(*tree.DInt))"
		case 32:
			return "int32(*datum.(*tree.DInt))"
		case 0, 64:
			return "int64(*datum.(*tree.DInt))"
		}
		panic(fmt.Sprintf("unhandled INT width %d", ct.Width))
	case sqlbase.ColumnType_FLOAT:
		return "float64(*datum.(*tree.DFloat))"
	case sqlbase.ColumnType_OID:
		return "int64(datum.(*tree.DOid).DInt)"
	case sqlbase.ColumnType_STRING, sqlbase.ColumnType_NAME:
		return "encoding.UnsafeConvertStringToBytes(string(*datum.(*tree.DString)))"
	}
	panic(fmt.Sprintf("unhandled ColumnType %s", ct.String()))
}
