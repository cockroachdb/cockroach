// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func genHashJoiner(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/exec/hashjoiner_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_TYPES_T", "types.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_SEL_IND", "{{.SelInd}}", -1)

	assignNeRe := makeFunctionRegex("_ASSIGN_NE", 3)
	s = assignNeRe.ReplaceAllString(s, `{{.Global.Assign "$1" "$2" "$3"}}`)

	assignHash := makeFunctionRegex("_ASSIGN_HASH", 2)
	s = assignHash.ReplaceAllString(s, `{{.Global.UnaryAssign "$1" "$2"}}`)

	rehash := makeFunctionRegex("_REHASH_BODY", 4)
	s = rehash.ReplaceAllString(s, `{{template "rehashBody" buildDict "Global" . "SelInd" $4}}`)

	checkCol := makeFunctionRegex("_CHECK_COL_WITH_NULLS", 7)
	s = checkCol.ReplaceAllString(s, `{{template "checkColWithNulls" buildDict "Global" . "SelInd" $7}}`)

	distinctCollectRightOuter := makeFunctionRegex("_DISTINCT_COLLECT_RIGHT_OUTER", 3)
	s = distinctCollectRightOuter.ReplaceAllString(s, `{{template "distinctCollectRightOuter" buildDict "Global" . "SelInd" $3}}`)

	distinctCollectNoOuter := makeFunctionRegex("_DISTINCT_COLLECT_NO_OUTER", 4)
	s = distinctCollectNoOuter.ReplaceAllString(s, `{{template "distinctCollectNoOuter" buildDict "Global" . "SelInd" $4}}`)

	collectRightOuter := makeFunctionRegex("_COLLECT_RIGHT_OUTER", 5)
	s = collectRightOuter.ReplaceAllString(s, `{{template "collectRightOuter" buildDict "Global" . "SelInd" $5}}`)

	collectNoOuter := makeFunctionRegex("_COLLECT_NO_OUTER", 5)
	s = collectNoOuter.ReplaceAllString(s, `{{template "collectNoOuter" buildDict "Global" . "SelInd" $5}}`)

	checkColMain := makeFunctionRegex("_CHECK_COL_MAIN", 1)
	s = checkColMain.ReplaceAllString(s, `{{template "checkColMain" .}}`)

	checkColBody := makeFunctionRegex("_CHECK_COL_BODY", 8)
	s = checkColBody.ReplaceAllString(s, `{{template "checkColBody" buildDict "Global" .Global "SelInd" .SelInd "ProbeHasNulls" $7 "BuildHasNulls" $8}}`)

	tmpl, err := template.New("hashjoiner_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)

	if err != nil {
		return err
	}

	allOverloads := intersectOverloads(comparisonOpToOverloads[tree.NE], hashOverloads)

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
