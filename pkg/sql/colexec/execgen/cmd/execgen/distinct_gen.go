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
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const distinctOpsTmpl = "pkg/sql/colexec/distinct_tmpl.go"

func genDistinctOps(wr io.Writer) error {
	d, err := ioutil.ReadFile(distinctOpsTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.ReplaceAll(s, "_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}")
	s = strings.ReplaceAll(s, "_TYPE_WIDTH", typeWidthReplacement)
	s = strings.ReplaceAll(s, "_GOTYPESLICE", "{{.GoTypeSliceName}}")
	s = strings.ReplaceAll(s, "_GOTYPE", "{{.GoType}}")
	s = strings.ReplaceAll(s, "_TYPE", "{{.VecMethod}}")
	s = strings.ReplaceAll(s, "TemplateType", "{{.VecMethod}}")

	assignNeRe := makeFunctionRegex("_ASSIGN_NE", 3)
	s = assignNeRe.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 3))

	innerLoopRe := makeFunctionRegex("_CHECK_DISTINCT", 5)
	s = innerLoopRe.ReplaceAllString(s, `{{template "checkDistinct" buildDict "Global" .}}`)

	innerLoopNullsRe := makeFunctionRegex("_CHECK_DISTINCT_WITH_NULLS", 7)
	s = innerLoopNullsRe.ReplaceAllString(s, `{{template "checkDistinctWithNulls" buildDict "Global" .}}`)
	s = replaceManipulationFuncs(s)

	// Now, generate the op, from the template.
	tmpl, err := template.New("distinct_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.NE])
}
func init() {
	registerGenerator(genDistinctOps, "distinct.eg.go", distinctOpsTmpl)
}
