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
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
)

const mergeJoinBaseTmpl = "pkg/sql/colexec/colexecjoin/mergejoinbase_tmpl.go"

func genMergeJoinBase(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignEqRe := makeFunctionRegex("_ASSIGN_EQ", 6)
	s = assignEqRe.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 6))

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("mergejoinbase").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[treecmp.EQ])
}

func init() {
	registerGenerator(genMergeJoinBase, "mergejoinbase.eg.go", mergeJoinBaseTmpl)
}
