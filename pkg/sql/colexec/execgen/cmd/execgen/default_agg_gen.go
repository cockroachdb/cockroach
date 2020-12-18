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
)

const defaultAggTmpl = "pkg/sql/colexec/colexecagg/default_agg_tmpl.go"

func genDefaultAgg(inputFileContents string, wr io.Writer) error {
	addTuple := makeFunctionRegex("_ADD_TUPLE", 5)
	s := addTuple.ReplaceAllString(inputFileContents, `{{template "addTuple" buildDict "HasSel" $5}}`)

	setResult := makeFunctionRegex("_SET_RESULT", 2)
	s = setResult.ReplaceAllString(s, `{{template "setResult"}}`)

	tmpl, err := template.New("default_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, struct{}{})

}

func init() {
	registerAggGenerator(genDefaultAgg, "default_agg.eg.go", defaultAggTmpl)
}
