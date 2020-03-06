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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const hashAggTmpl = "pkg/sql/colexec/hash_aggregator_tmpl.go"

func genHashAggregator(wr io.Writer) error {
	t, err := ioutil.ReadFile(hashAggTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.LTyp}}", -1)
	s = replaceManipulationFuncs(".Global.LTyp", s)

	assignCmpRe := makeFunctionRegex("_ASSIGN_NE", 3)
	s = assignCmpRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Assign", 3))

	matchLoop := makeFunctionRegex("_MATCH_LOOP", 8)
	s = matchLoop.ReplaceAllString(
		s, `{{template "matchLoop" buildDict "Global" . "LhsMaybeHasNulls" $7 "RhsMaybeHasNulls" $8}}`)

	tmpl, err := template.New("hash_aggregator").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.NE])
}

func init() {
	registerGenerator(genHashAggregator, "hash_aggregator.eg.go", hashAggTmpl)
}
