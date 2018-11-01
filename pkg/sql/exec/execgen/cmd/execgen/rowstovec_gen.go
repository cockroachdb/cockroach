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

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

const rowsToVecTemplate = `
package exec

import (
  "fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
				col[i] = v.({{.GoType}})
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
	GoType   string
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
				GoType:       t.GoTypeName(),
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
