// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"io"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
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
			Overloads: sameTypeComparisonOpToOverloads[treecmp.LT],
		},
		{
			Agg:       "max",
			AggTitle:  "Max",
			Overloads: sameTypeComparisonOpToOverloads[treecmp.GT],
		},
	})
}

func init() {
	registerAggGenerator(
		genMinMaxAgg, "min_max_agg.eg.go", /* filenameSuffix */
		minMaxAggTmpl, "minMax" /* aggName */, true, /* genWindowVariant */
	)
}
