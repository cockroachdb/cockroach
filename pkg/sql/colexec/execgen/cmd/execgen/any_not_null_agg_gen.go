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
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const anyNotNullAggTmpl = "pkg/sql/colexec/colexecagg/any_not_null_agg_tmpl.go"

func genAnyNotNullAgg(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_GOTYPESLICE", "{{.GoTypeSliceName}}",
		"_GOTYPE", "{{.GoType}}",
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	findAnyNotNull := makeFunctionRegex("_FIND_ANY_NOT_NULL", 6)
	s = findAnyNotNull.ReplaceAllString(s, `{{template "findAnyNotNull" buildDict "Global" . "HasNulls" $5 "HasSel" $6}}`)

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("any_not_null_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}

func init() {
	registerAggGenerator(genAnyNotNullAgg, "any_not_null_agg.eg.go", anyNotNullAggTmpl)
}
