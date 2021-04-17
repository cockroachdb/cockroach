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

const minMaxAggTmpl = "pkg/sql/colexec/colexecagg/min_max_agg_tmpl.go"

func genMinMaxAgg(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_AGG_TITLE", "{{.AggTitle}}",
		"_AGG", "{{$agg}}",
		"_GOTYPESLICE", "{{.GoTypeSliceName}}",
		"_GOTYPE", "{{.GoType}}",
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignCmpRe := makeFunctionRegex("_ASSIGN_CMP", 6)
	s = assignCmpRe.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 6))

	accumulateMinMax := makeFunctionRegex("_ACCUMULATE_MINMAX", 5)
	s = accumulateMinMax.ReplaceAllString(s, `{{template "accumulateMinMax" buildDict "Global" . "HasNulls" $4 "HasSel" $5}}`)

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("min_max_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, []struct {
		Agg       string
		AggTitle  string
		Overloads []*oneArgOverload
	}{
		{
			Agg:       "min",
			AggTitle:  "Min",
			Overloads: sameTypeComparisonOpToOverloads[tree.LT],
		},
		{
			Agg:       "max",
			AggTitle:  "Max",
			Overloads: sameTypeComparisonOpToOverloads[tree.GT],
		},
	})
}

func init() {
	registerAggGenerator(genMinMaxAgg, "min_max_agg.eg.go", minMaxAggTmpl)
}
