// Copyright 2019 The Cockroach Authors.
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
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func genCountAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/colexec/count_agg_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	accumulateSum := makeFunctionRegex("_ACCUMULATE_COUNT", 4)
	s = accumulateSum.ReplaceAllString(s, `{{template "accumulateCount" buildDict "Global" . "ColWithNulls" $4}}`)

	tmpl, err := template.New("count_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeBinaryOpToOverloads[tree.Plus])
}

func init() {
	registerGenerator(genCountAgg, "count_agg.eg.go")
}
