// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// Width is used when a type family has a width that has an associated distinct
// ExecType. One or more of these structs is used as a special case when
// multiple widths need to be associated with one type family in a
// columnConversion struct.
type Width struct {
	Width    int32
	ExecType coltypes.T
}

// columnConversion defines a conversion from a coltypes.ColumnType to an
// exec.ColVec.
type columnConversion struct {
	// Family is the type family of the ColumnType.
	Family string

	// Widths is set if this type family has several widths to special-case. If
	// set, only the ExecType and GoType in the Widths is used.
	Widths []Width

	// ExecType is the exec.T to which we're converting. It should correspond to
	// a method name on exec.ColVec.
	ExecType coltypes.T
}

func genRowsToVec(wr io.Writer) error {
	f, err := ioutil.ReadFile("pkg/sql/colexec/rowstovec_tmpl.go")
	if err != nil {
		return err
	}

	s := string(f)

	// Replace the template variables.
	s = strings.Replace(s, "_TemplateType", "{{.ExecType.String}}", -1)
	s = strings.Replace(s, "_GOTYPE", "{{.ExecType.GoTypeName}}", -1)
	s = strings.Replace(s, "_FAMILY", "types.{{.Family}}", -1)
	s = strings.Replace(s, "_WIDTH", "{{.Width}}", -1)

	rowsToVecRe := makeFunctionRegex("_ROWS_TO_COL_VEC", 4)
	s = rowsToVecRe.ReplaceAllString(s, `{{ template "rowsToColVec" . }}`)

	s = replaceManipulationFuncs(".ExecType", s)

	// Build the list of supported column conversions.
	conversionsMap := make(map[types.Family]*columnConversion)
	for _, ct := range types.OidToType {
		t := typeconv.FromColumnType(ct)
		if t == coltypes.Unhandled {
			continue
		}

		var conversion *columnConversion
		var ok bool
		if conversion, ok = conversionsMap[ct.Family()]; !ok {
			conversion = &columnConversion{
				Family: ct.Family().String(),
			}
			conversionsMap[ct.Family()] = conversion
		}

		if ct.Width() != 0 {
			conversion.Widths = append(
				conversion.Widths, Width{Width: ct.Width(), ExecType: t},
			)
		} else {
			conversion.ExecType = t
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
