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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const hashAggTmpl = "pkg/sql/colexec/hash_aggregator_tmpl.go"

func genHashAggregator(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	s = replaceManipulationFuncsAmbiguous(".Global", s)

	assignCmpRe := makeFunctionRegex("_ASSIGN_NE", 6)
	s = assignCmpRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Assign", 6))

	populateSels := makeFunctionRegex("_POPULATE_SELS", 3)
	s = populateSels.ReplaceAllString(s, `{{template "populateSels" buildDict "Global" . "BatchHasSelection" $3}}`)

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
