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
)

const selectionOpsTmpl = "pkg/sql/colexec/selection_ops_tmpl.go"

func getSelectionOpsTmpl() (*template.Template, error) {
	t, err := ioutil.ReadFile(selectionOpsTmpl)
	if err != nil {
		return nil, err
	}

	s := string(t)

	s = strings.ReplaceAll(s, "_LEFT_CANONICAL_TYPE_FAMILY", "{{.LeftCanonicalFamilyStr}}")
	s = strings.ReplaceAll(s, "_LEFT_TYPE_WIDTH", typeWidthReplacement)
	s = strings.ReplaceAll(s, "_RIGHT_CANONICAL_TYPE_FAMILY", "{{.RightCanonicalFamilyStr}}")
	s = strings.ReplaceAll(s, "_RIGHT_TYPE_WIDTH", typeWidthReplacement)

	s = strings.ReplaceAll(s, "_OP_CONST_NAME", "sel{{.Name}}{{.Left.VecMethod}}{{.Right.VecMethod}}ConstOp")
	s = strings.ReplaceAll(s, "_OP_NAME", "sel{{.Name}}{{.Left.VecMethod}}{{.Right.VecMethod}}Op")
	s = strings.ReplaceAll(s, "_NAME", "{{.Name}}")
	s = strings.ReplaceAll(s, "_R_GO_TYPE", "{{.Right.GoType}}")
	s = strings.ReplaceAll(s, "_L_TYP", "{{.Left.VecMethod}}")
	s = strings.ReplaceAll(s, "_R_TYP", "{{.Right.VecMethod}}")

	assignCmpRe := makeFunctionRegex("_ASSIGN_CMP", 6)
	s = assignCmpRe.ReplaceAllString(s, makeTemplateFunctionCall("Right.Assign", 6))

	s = replaceManipulationFuncsAmbiguous(".Left", s)
	s = strings.ReplaceAll(s, "_R_UNSAFEGET", "execgen.UNSAFEGET")
	s = strings.ReplaceAll(s, "_R_SLICE", "execgen.SLICE")
	s = replaceManipulationFuncsAmbiguous(".Right", s)

	s = strings.ReplaceAll(s, "_HAS_NULLS", "$hasNulls")
	selConstLoop := makeFunctionRegex("_SEL_CONST_LOOP", 1)
	s = selConstLoop.ReplaceAllString(s, `{{template "selConstLoop" buildDict "Global" $ "HasNulls" $1 "Overload" .}}`)
	selLoop := makeFunctionRegex("_SEL_LOOP", 1)
	s = selLoop.ReplaceAllString(s, `{{template "selLoop" buildDict "Global" $ "HasNulls" $1 "Overload" .}}`)

	return template.New("selection_ops").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
}

func genSelectionOps(wr io.Writer) error {
	tmpl, err := getSelectionOpsTmpl()
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, twoArgsResolvedOverloadsInfo)
}

func init() {
	registerGenerator(genSelectionOps, "selection_ops.eg.go", selectionOpsTmpl)
}
