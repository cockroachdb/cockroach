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
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types/conv"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
)

// Width is used when a SemanticType has a width that has an associated distinct
// ExecType. One or more of these structs is used as a special case when
// multiple widths need to be associated to one SemanticType in a
// columnConversion struct.
type Width struct {
	Width    int32
	ExecType string
	GoType   string
}

// columnConversion defines a conversion from a types.ColumnType to an
// exec.ColVec.
type columnConversion struct {
	// SemanticType is the semantic type of the ColumnType.
	SemanticType string

	// Widths is set if this SemanticType has several widths to special-case. If
	// set, only the ExecType and GoType in the Widths is used.
	Widths []Width

	// ExecType is the exec.T to which we're converting. It should correspond to
	// a method name on exec.ColVec.
	ExecType string
	GoType   string
}

func genRowsToVec(wr io.Writer) error {
	f, err := ioutil.ReadFile("pkg/sql/exec/rowstovec_tmpl.go")
	if err != nil {
		return err
	}

	s := string(f)

	// Replace the template variables.
	s = strings.Replace(s, "_TemplateType", "{{.ExecType}}", -1)
	s = strings.Replace(s, "_GOTYPE", "{{.GoType}}", -1)
	s = strings.Replace(s, "_SEMANTIC_TYPE", "semtypes.{{.SemanticType}}", -1)
	s = strings.Replace(s, "_WIDTH", "{{.Width}}", -1)

	rowsToVecRe := makeFunctionRegex("_ROWS_TO_COL_VEC", 4)
	s = rowsToVecRe.ReplaceAllString(s, `{{ template "rowsToColVec" . }}`)

	// Build the list of supported column conversions.
	conversionsMap := make(map[semtypes.SemanticType]*columnConversion)
	for _, ct := range semtypes.OidToType {
		t := conv.FromColumnType(ct)
		if t == types.Unhandled {
			continue
		}

		var conversion *columnConversion
		var ok bool
		if conversion, ok = conversionsMap[ct.SemanticType()]; !ok {
			conversion = &columnConversion{
				SemanticType: ct.SemanticType().String(),
			}
			conversionsMap[ct.SemanticType()] = conversion
		}

		if ct.Width() != 0 {
			conversion.Widths = append(
				conversion.Widths, Width{Width: ct.Width(), ExecType: t.String(), GoType: t.GoTypeName()},
			)
		} else {
			conversion.ExecType = t.String()
			conversion.GoType = t.GoTypeName()
		}
	}

	tmpl, err := template.New("rowsToVec").Parse(s)
	if err != nil {
		return err
	}

	columnConversions := make([]columnConversion, 0, len(conversionsMap))
	for _, conversion := range conversionsMap {
		columnConversions = append(columnConversions, *conversion)
	}
	return tmpl.Execute(wr, columnConversions)
}

func init() {
	registerGenerator(genRowsToVec, "rowstovec.eg.go")
}
