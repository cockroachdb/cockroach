// Copyright 2019 The Cockroach Authors.
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

const vecCmpTmpl = "pkg/sql/colexec/vec_comparators_tmpl.go"

func genVecComparators(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_GOTYPESLICE", "{{.GoTypeSliceName}}",
		"_TYPE", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	compareRe := makeFunctionRegex("_COMPARE", 5)
	s = compareRe.ReplaceAllString(s, makeTemplateFunctionCall("Compare", 5))

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("vec_comparators").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[treecmp.EQ])
}

func init() {
	registerGenerator(genVecComparators, "vec_comparators.eg.go", vecCmpTmpl)
}
