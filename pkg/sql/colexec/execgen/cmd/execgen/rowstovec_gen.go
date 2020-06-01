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

const rowsToVecTmpl = "pkg/sql/colexec/rowstovec_tmpl.go"

func genRowsToVec(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_GOTYPE", "{{.GoType}}",
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	rowsToVecRe := makeFunctionRegex("_ROWS_TO_COL_VEC", 4)
	s = rowsToVecRe.ReplaceAllString(s, `{{template "rowsToColVec" .}}`)

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("rowsToVec").Parse(s)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}

func init() {
	registerGenerator(genRowsToVec, "rowstovec.eg.go", rowsToVecTmpl)
}
