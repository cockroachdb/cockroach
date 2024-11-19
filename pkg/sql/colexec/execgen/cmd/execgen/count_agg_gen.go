// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"io"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

const countAggTmpl = "pkg/sql/colexec/colexecagg/count_agg_tmpl.go"

func genCountAgg(inputFileContents string, wr io.Writer) error {
	s := strings.ReplaceAll(inputFileContents, "_COUNTKIND", "{{.CountKind}}")

	accumulateSum := makeFunctionRegex("_ACCUMULATE_COUNT", 5)
	s = accumulateSum.ReplaceAllString(s, `{{template "accumulateCount" buildDict "Global" . "ColWithNulls" $4 "HasSel" $5}}`)

	removeRow := makeFunctionRegex("_REMOVE_ROW", 4)
	s = removeRow.ReplaceAllString(s, `{{template "removeRow" buildDict "Global" . "ColWithNulls" $4}}`)

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("count_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, []struct {
		aggTmplInfoBase
		CountKind string
	}{
		// "Rows" count aggregate performs COUNT(*) aggregation, which counts
		// every row in the result unconditionally.
		{
			aggTmplInfoBase: aggTmplInfoBase{canonicalTypeFamily: types.IntFamily},
			CountKind:       "Rows",
		},
		// "" ("pure") count aggregate performs COUNT(col) aggregation, which
		// counts every row in the result where the value of col is not null.
		{
			aggTmplInfoBase: aggTmplInfoBase{canonicalTypeFamily: types.IntFamily},
			CountKind:       "",
		},
	})
}

func init() {
	registerAggGenerator(
		genCountAgg, "count_agg.eg.go", /* filenameSuffix */
		countAggTmpl, "count" /* aggName */, true, /* genWindowVariant */
	)
}
