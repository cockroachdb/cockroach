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
	"io/ioutil"
	"strings"
	"text/template"
)

func genHashUtils(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/colexec/hash_utils_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)

	assignHash := makeFunctionRegex("_ASSIGN_HASH", 2)
	s = assignHash.ReplaceAllString(s, makeTemplateFunctionCall("Global.UnaryAssign", 2))

	rehash := makeFunctionRegex("_REHASH_BODY", 8)
	s = rehash.ReplaceAllString(s, `{{template "rehashBody" buildDict "Global" . "HasSel" $7 "HasNulls" $8}}`)

	s = replaceManipulationFuncs(".Global.LTyp", s)

	tmpl, err := template.New("hash_utils").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, hashOverloads)
}

func init() {
	registerGenerator(genHashUtils, "hash_utils.eg.go")
}
