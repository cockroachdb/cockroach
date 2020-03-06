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

const sumAggTmpl = "pkg/sql/colexec/sum_agg_tmpl.go"

func genSumAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile(sumAggTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_GOTYPE", "{{.LTyp.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)

	assignAddRe := makeFunctionRegex("_ASSIGN_ADD", 3)
	s = assignAddRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Assign", 3))

	accumulateSum := makeFunctionRegex("_ACCUMULATE_SUM", 4)
	s = accumulateSum.ReplaceAllString(s, `{{template "accumulateSum" buildDict "Global" . "HasNulls" $4}}`)

	tmpl, err := template.New("sum_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeBinaryOpToOverloads[tree.Plus])
}

func init() {
	registerGenerator(genSumAgg, "sum_agg.eg.go", sumAggTmpl)
}
