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

func genSelectIn(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/colexec/select_in_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	assignEq := makeFunctionRegex("_ASSIGN_EQ", 3)
	s = assignEq.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 3))
	s = strings.Replace(s, "_GOTYPE", "{{.LGoType}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)

	s = replaceManipulationFuncs(".LTyp", s)

	tmpl, err := template.New("select_in").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}

func init() {
	registerGenerator(genSelectIn, "select_in.eg.go")
}
