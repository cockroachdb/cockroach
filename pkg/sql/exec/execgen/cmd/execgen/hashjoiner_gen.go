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
	"github.com/pkg/errors"
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

	assignNeRe := regexp.MustCompile(`_ASSIGN_NE\((.*),(.*),(.*)\)`)
	s = assignNeRe.ReplaceAllString(s, "{{.Global.Assign $1 $2 $3}}")

	assignHash := regexp.MustCompile(`_ASSIGN_HASH\((.*),(.*)\)`)
	s = assignHash.ReplaceAllString(s, "{{.Global.UnaryAssign $1 $2}}")

	rehash := regexp.MustCompile(`_REHASH_BODY\((.*)\)`)
	s = rehash.ReplaceAllString(s, `{{template "rehashBody" buildDict "Global" . "SelInd" "$1"}}`)

	checkCol := regexp.MustCompile(`_CHECK_COL_WITH_NULLS\((.*)\)`)
	s = checkCol.ReplaceAllString(s, `{{template "checkColWithNulls" buildDict "Global" . "SelInd" "$1"}}`)

	checkColMain := regexp.MustCompile(`_CHECK_COL_MAIN\((.*\))`)
	s = checkColMain.ReplaceAllString(s, `{{template "checkColMain" .}}`)

	checkColBody := regexp.MustCompile(`_CHECK_COL_BODY\((.*),(.*),(.*)\)`)
	s = checkColBody.ReplaceAllString(s, `{{template "checkColBody" buildDict "Global" .Global "SelInd" .SelInd "ProbeHasNulls" $2 "BuildHasNulls" $3}}`)

	tmpl, err := template.New("hashjoiner_op").Funcs(template.FuncMap{
		"buildDict": func(values ...interface{}) (map[string]interface{}, error) {
			if len(values)%2 != 0 {
				return nil, errors.New("invalid call to buildDict")
			}
			dict := make(map[string]interface{}, len(values)/2)
			for i := 0; i < len(values); i += 2 {
				key, ok := values[i].(string)
				if !ok {
					return nil, errors.New("buildDict keys must be strings")
				}
				dict[key] = values[i+1]
			}
			return dict, nil
		},
	}).Parse(s)

	if err != nil {
		return err
	}

	return tmpl.Execute(wr, struct {
		NETemplate   interface{}
		HashTemplate interface{}
	}{
		NETemplate:   comparisonOpToOverloads[tree.NE],
		HashTemplate: hashOverloads,
	})
}

func init() {
	registerGenerator(genHashJoiner, "hashjoiner.eg.go")
}
