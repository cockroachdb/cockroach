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

const hashUtilsTmpl = "pkg/sql/colexec/colexechash/hash_utils_tmpl.go"

func genHashUtils(inputFileContents string, wr io.Writer) error {

	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignHash := makeFunctionRegex("_ASSIGN_HASH", 4)
	s = assignHash.ReplaceAllString(s, makeTemplateFunctionCall("Global.UnaryAssign", 4))

	rehash := makeFunctionRegex("_REHASH_BODY", 7)
	s = rehash.ReplaceAllString(s, `{{template "rehashBody" buildDict "Global" . "HasSel" $6 "HasNulls" $7}}`)

	s = replaceManipulationFuncsAmbiguous(".Global", s)

	tmpl, err := template.New("hash_utils").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, hashOverloads)
}

func init() {
	registerGenerator(genHashUtils, "hash_utils.eg.go", hashUtilsTmpl)
}
