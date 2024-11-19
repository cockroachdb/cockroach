// Copyright 2021 The Cockroach Authors.
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

const firstLastNthTmpl = "pkg/sql/colexec/colexecwindow/first_last_nth_value_tmpl.go"

func init() {
	firstLastNthValueOpsGenerator := func(lowerCaseName, upperCaseName string) generator {
		return func(inputFileContents string, wr io.Writer) error {
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
			s := r.Replace(inputFileContents)

			r = strings.NewReplacer(
				"_OP_NAME", lowerCaseName,
				"_UPPERCASE_NAME", upperCaseName,
			)
			s = r.Replace(s)

			// Now, generate the op, from the template.
			tmpl, err := template.New(lowerCaseName + "Op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
			if err != nil {
				return err
			}

			return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[treecmp.EQ])
		}
	}

	registerGenerator(
		firstLastNthValueOpsGenerator("firstValue", "FirstValue"),
		"first_value.eg.go", firstLastNthTmpl)
	registerGenerator(
		firstLastNthValueOpsGenerator("lastValue", "LastValue"),
		"last_value.eg.go", firstLastNthTmpl)
	registerGenerator(
		firstLastNthValueOpsGenerator("nthValue", "NthValue"),
		"nth_value.eg.go", firstLastNthTmpl)
}
