// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"io"
	"io/ioutil"
	"regexp"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func genSumAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/exec/sum_agg_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_GOTYPE", "{{.LTyp.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "types.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)

	assignAddRe := regexp.MustCompile(`_ASSIGN_ADD\((.*),(.*),(.*)\)`)
	s = assignAddRe.ReplaceAllString(s, "{{.Assign $1 $2 $3}}")

	tmpl, err := template.New("sum_agg").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, binaryOpToOverloads[tree.Plus])
}

func init() {
	registerGenerator(genSumAgg, "sum_agg.eg.go")
}
