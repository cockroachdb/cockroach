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
)

const castTmpl = "pkg/sql/colexec/colexecbase/cast_tmpl.go"

func genCastOperators(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_LEFT_CANONICAL_TYPE_FAMILY", "{{.LeftCanonicalFamilyStr}}",
		"_LEFT_TYPE_WIDTH", typeWidthReplacement,
		"_RIGHT_CANONICAL_TYPE_FAMILY", "{{.RightCanonicalFamilyStr}}",
		"_RIGHT_TYPE_WIDTH", typeWidthReplacement,
		"_R_GO_TYPE", "{{.Right.GoType}}",
		"_L_TYP", "{{.Left.VecMethod}}",
		"_R_TYP", "{{.Right.VecMethod}}",
		"_NAME", "{{.Left.VecMethod}}{{.Right.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	setValues := makeFunctionRegex("_CAST_TUPLES", 2)
	s = setValues.ReplaceAllString(s, `{{template "castTuples" buildDict "Global" . "HasNulls" $1 "HasSel" $2}}`)

	castRe := makeFunctionRegex("_CAST", 4)
	s = castRe.ReplaceAllString(s, makeTemplateFunctionCall("Right.Cast", 4))

	s = strings.ReplaceAll(s, "_L_UNSAFEGET", "execgen.UNSAFEGET")
	s = replaceManipulationFuncsAmbiguous(".Left", s)

	s = strings.ReplaceAll(s, "_R_UNSAFEGET", "execgen.UNSAFEGET")
	s = replaceManipulationFuncsAmbiguous(".Right", s)

	tmpl, err := template.New("cast").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, twoArgsResolvedOverloadsInfo.CastOverloads)
}

func init() {
	registerGenerator(genCastOperators, "cast.eg.go", castTmpl)
}
