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
)

const castTmpl = "pkg/sql/colexec/cast_tmpl.go"

func genCastOperators(wr io.Writer) error {
	t, err := ioutil.ReadFile(castTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.ReplaceAll(s, "_LEFT_CANONICAL_TYPE_FAMILY", "{{.LeftCanonicalFamilyStr}}")
	s = strings.ReplaceAll(s, "_LEFT_TYPE_WIDTH", typeWidthReplacement)
	s = strings.ReplaceAll(s, "_RIGHT_CANONICAL_TYPE_FAMILY", "{{.RightCanonicalFamilyStr}}")
	s = strings.ReplaceAll(s, "_RIGHT_TYPE_WIDTH", typeWidthReplacement)
	s = strings.ReplaceAll(s, "_R_GO_TYPE", "{{.Right.GoType}}")
	s = strings.ReplaceAll(s, "_L_TYP", "{{.Left.VecMethod}}")
	s = strings.ReplaceAll(s, "_R_TYP", "{{.Right.VecMethod}}")

	castRe := makeFunctionRegex("_CAST", 2)
	s = castRe.ReplaceAllString(s, makeTemplateFunctionCall("Right.Cast", 2))

	s = strings.ReplaceAll(s, "_L_SLICE", "execgen.SLICE")
	s = strings.ReplaceAll(s, "_L_UNSAFEGET", "execgen.UNSAFEGET")
	s = replaceManipulationFuncsAmbiguous(".Left", s)

	s = strings.ReplaceAll(s, "_R_SET", "execgen.SET")
	s = replaceManipulationFuncsAmbiguous(".Right", s)

	tmpl, err := template.New("cast").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, twoArgsResolvedOverloadsInfo.CastOverloads)
}

func init() {
	registerGenerator(genCastOperators, "cast.eg.go", castTmpl)
}
