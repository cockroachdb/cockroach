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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const firstLastNthTmpl = "pkg/sql/colexec/colexecwindow/first_last_nth_value_tmpl.go"

func replacefirstLastNthTmplVariables(tmpl string) string {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_GOTYPESLICE", "{{.GoTypeSliceName}}",
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
		".IsFirstValue", `eq "_OP_NAME" "firstValue"`,
		".IsLastValue", `eq "_OP_NAME" "lastValue"`,
		".IsNthValue", `eq "_OP_NAME" "nthValue"`,
	)
	return r.Replace(tmpl)
}

func genFirstValueOp(inputFileContents string, wr io.Writer) error {
	s := replacefirstLastNthTmplVariables(inputFileContents)
	r := strings.NewReplacer(
		"_OP_NAME", "firstValue",
		"_UPPERCASE_NAME", "FirstValue",
	)
	s = r.Replace(s)

	// Now, generate the op, from the template.
	tmpl, err := template.New("first_value_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}

func genLastValueOp(inputFileContents string, wr io.Writer) error {
	s := replacefirstLastNthTmplVariables(inputFileContents)
	r := strings.NewReplacer(
		"_OP_NAME", "lastValue",
		"_UPPERCASE_NAME", "LastValue",
	)
	s = r.Replace(s)

	// Now, generate the op, from the template.
	tmpl, err := template.New("last_value_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}

func genNthValueOp(inputFileContents string, wr io.Writer) error {
	s := replacefirstLastNthTmplVariables(inputFileContents)
	r := strings.NewReplacer(
		"_OP_NAME", "nthValue",
		"_UPPERCASE_NAME", "NthValue",
	)
	s = r.Replace(s)

	// Now, generate the op, from the template.
	tmpl, err := template.New("nth_value_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}

func init() {
	registerGenerator(genFirstValueOp, "first_value.eg.go", firstLastNthTmpl)
	registerGenerator(genLastValueOp, "last_value.eg.go", firstLastNthTmpl)
	registerGenerator(genNthValueOp, "nth_value.eg.go", firstLastNthTmpl)
}
