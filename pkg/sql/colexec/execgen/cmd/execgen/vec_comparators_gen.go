// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const vecCmpTmpl = "pkg/sql/colexec/vec_comparators_tmpl.go"

func genVecComparators(wr io.Writer) error {
	d, err := ioutil.ReadFile(vecCmpTmpl)
	if err != nil {
		return err
	}
	s := string(d)

	s = strings.ReplaceAll(s, "_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}")
	s = strings.ReplaceAll(s, "_TYPE_WIDTH", "{{.Width}}{{if eq .Width -1}}: default{{end}}")
	s = strings.ReplaceAll(s, "_GOTYPESLICE", "{{.GoTypeSliceName}}")
	s = strings.ReplaceAll(s, "_TYPE", "{{.VecMethod}}")

	compareRe := makeFunctionRegex("_COMPARE", 5)
	s = compareRe.ReplaceAllString(s, makeTemplateFunctionCall("Compare", 5))

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("vec_comparators").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}

func init() {
	registerGenerator(genVecComparators, "vec_comparators.eg.go", vecCmpTmpl)
}
