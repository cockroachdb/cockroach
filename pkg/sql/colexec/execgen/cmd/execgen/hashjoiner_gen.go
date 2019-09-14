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

func genHashJoiner(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/colexec/hashjoiner_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_SEL_IND", "{{.SelInd}}", -1)
	s = strings.Replace(s, "_GOTYPESLICE", "{{.LTyp.GoTypeSliceName}}", -1)

	assignNeRe := makeFunctionRegex("_ASSIGN_NE", 3)
	s = assignNeRe.ReplaceAllString(s, `{{.Global.Assign "$1" "$2" "$3"}}`)

	assignHash := makeFunctionRegex("_ASSIGN_HASH", 2)
	s = assignHash.ReplaceAllString(s, `{{.Global.UnaryAssign "$1" "$2"}}`)

	rehash := makeFunctionRegex("_REHASH_BODY", 9)
	s = rehash.ReplaceAllString(s, `{{template "rehashBody" buildDict "Global" . "HasSel" $8 "HasNulls" $9}}`)

	checkCol := makeFunctionRegex("_CHECK_COL_WITH_NULLS", 7)
	s = checkCol.ReplaceAllString(s, `{{template "checkColWithNulls" buildDict "Global" . "UseSel" $7}}`)

	distinctCollectRightOuter := makeFunctionRegex("_DISTINCT_COLLECT_RIGHT_OUTER", 3)
	s = distinctCollectRightOuter.ReplaceAllString(s, `{{template "distinctCollectRightOuter" buildDict "Global" . "UseSel" $3}}`)

	distinctCollectNoOuter := makeFunctionRegex("_DISTINCT_COLLECT_NO_OUTER", 4)
	s = distinctCollectNoOuter.ReplaceAllString(s, `{{template "distinctCollectNoOuter" buildDict "Global" . "UseSel" $4}}`)

	collectRightOuter := makeFunctionRegex("_COLLECT_RIGHT_OUTER", 5)
	s = collectRightOuter.ReplaceAllString(s, `{{template "collectRightOuter" buildDict "Global" . "UseSel" $5}}`)

	collectNoOuter := makeFunctionRegex("_COLLECT_NO_OUTER", 5)
	s = collectNoOuter.ReplaceAllString(s, `{{template "collectNoOuter" buildDict "Global" . "UseSel" $5}}`)

	checkColBody := makeFunctionRegex("_CHECK_COL_BODY", 9)
	s = checkColBody.ReplaceAllString(
		s,
		`{{template "checkColBody" buildDict "Global" .Global "UseSel" .UseSel "ProbeHasNulls" $7 "BuildHasNulls" $8 "AllowNullEquality" $9}}`)

	s = replaceManipulationFuncs(".Global.LTyp", s)

	tmpl, err := template.New("hashjoiner_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	allOverloads := intersectOverloads(sameTypeComparisonOpToOverloads[tree.NE], hashOverloads)

	return tmpl.Execute(wr, struct {
		NETemplate   interface{}
		HashTemplate interface{}
	}{
		NETemplate:   allOverloads[0],
		HashTemplate: allOverloads[1],
	})
}

func init() {
	registerGenerator(genHashJoiner, "hashjoiner.eg.go")
}
