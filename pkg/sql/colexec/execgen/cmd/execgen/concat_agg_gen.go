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
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

const concatAggTmpl = "pkg/sql/colexec/colexecagg/concat_agg_tmpl.go"

func genConcatAgg(inputFileContents string, wr io.Writer) error {
	accumulateConcatRe := makeFunctionRegex("_ACCUMULATE_CONCAT", 5)
	s := accumulateConcatRe.ReplaceAllString(inputFileContents, `{{template "accumulateConcat" buildDict "HasNulls" $4 "HasSel" $5}}`)

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("concat_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, aggTmplInfoBase{canonicalTypeFamily: types.BytesFamily})
}

func init() {
	registerAggGenerator(genConcatAgg, "concat_agg.eg.go", concatAggTmpl)
}
