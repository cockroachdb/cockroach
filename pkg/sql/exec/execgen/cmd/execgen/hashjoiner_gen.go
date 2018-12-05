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
	"regexp"
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
	s = strings.Replace(s, "_SEL_CLAUSE_BEGIN()", `
		{{range $sel := $sels}}
		{{.ClauseBegin}}
	`, -1)
	s = strings.Replace(s, "_SEL_CLAUSE_END()", `
		{{.ClauseEnd}}
		{{end}}
	`, -1)

	assignNeRe := regexp.MustCompile(`_ASSIGN_NE\((.*),(.*),(.*)\)`)
	s = assignNeRe.ReplaceAllString(s, "{{$$neType.Assign $1 $2 $3}}")

	assignHash := regexp.MustCompile(`_ASSIGN_HASH\((.*),(.*)\)`)
	s = assignHash.ReplaceAllString(s, "{{$$hashType.UnaryAssign $1 $2}}")

	wrapSel := regexp.MustCompile(`_WRAP_SEL\((.*?)\)`)
	s = wrapSel.ReplaceAllString(s, `{{call .WrapSel $1}}`)

	tmpl, err := template.New("hashjoiner_op").Parse(s)

	if err != nil {
		return err
	}

	return tmpl.Execute(wr, struct {
		NETemplate   interface{}
		HashTemplate interface{}
		SelTemplate  interface{}
	}{
		NETemplate:   comparisonOpToOverloads[tree.NE],
		HashTemplate: hashOverloads,
		SelTemplate:  selOverloads,
	})
}

func init() {
	registerGenerator(genHashJoiner, "hashjoiner.eg.go")
}
