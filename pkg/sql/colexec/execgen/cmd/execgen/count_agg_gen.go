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
	"strings"
	"text/template"
)

const countAggTmpl = "pkg/sql/colexec/count_agg_tmpl.go"

func genCountAgg(inputFileContents string, wr io.Writer) error {
	s := strings.ReplaceAll(inputFileContents, "_COUNTKIND", "{{.CountKind}}")

	accumulateSum := makeFunctionRegex("_ACCUMULATE_COUNT", 4)
	s = accumulateSum.ReplaceAllString(s, `{{template "accumulateCount" buildDict "Global" . "ColWithNulls" $4}}`)

	tmpl, err := template.New("count_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, []struct {
		CountKind string
	}{
		// "Rows" count aggregate performs COUNT(*) aggregation, which counts
		// every row in the result unconditionally.
		{CountKind: "Rows"},
		// "" ("pure") count aggregate performs COUNT(col) aggregation, which
		// counts every row in the result where the value of col is not null.
		{CountKind: ""},
	})
}

func init() {
	registerAggGenerator(genCountAgg, "count_agg.eg.go", countAggTmpl)
}
