// Copyright 2020 The Cockroach Authors.
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

const datumToVecTmpl = "pkg/sql/colconv/datum_to_vec_tmpl.go"

func genDatumToVec(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_TYPE_FAMILY", "{{.TypeFamily}}",
		"_TYPE_WIDTH", typeWidthReplacement,
	)
	s := r.Replace(inputFileContents)

	preludeRe := makeFunctionRegex("_PRELUDE", 1)
	s = preludeRe.ReplaceAllString(s, makeTemplateFunctionCall("Prelude", 1))
	convertRe := makeFunctionRegex("_CONVERT", 1)
	s = convertRe.ReplaceAllString(s, makeTemplateFunctionCall("Convert", 1))

	tmpl, err := template.New("utils").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, getRowsToVecTmplInfos())
}

func init() {
	registerGenerator(genDatumToVec, "datum_to_vec.eg.go", datumToVecTmpl)
}
