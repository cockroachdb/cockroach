// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
)

const minMaxRemovableAggTmpl = "pkg/sql/colexec/colexecwindow/min_max_removable_agg_tmpl.go"

func genMinMaxRemovableAgg(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_AGG_TITLE", "{{.AggTitle}}",
		"_AGG", "{{$agg}}",
		"_GOTYPE", "{{.GoType}}",
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignCmpRe := makeFunctionRegex("_ASSIGN_CMP", 6)
	s = assignCmpRe.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 6))

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
	registerGenerator(genMinMaxRemovableAgg, "min_max_removable_agg.eg.go", minMaxRemovableAggTmpl)
}
