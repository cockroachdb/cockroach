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
	"strings"
	"text/template"
)

const vecToDatumTmpl = "pkg/sql/colexec/vec_to_datum_tmpl.go"

func genVecToDatum(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_HAS_NULLS", "$.HasNulls",
		"_HAS_SEL", "$.HasSel",
	)
	s := r.Replace(inputFileContents)

	setTupleIdx := makeFunctionRegex("_SET_TUPLE_IDX", 4)
	s = setTupleIdx.ReplaceAllString(s, `{{template "setTupleIdx" buildDict "HasSel" $4}}`)
	typedSliceToDatum := makeFunctionRegex("_TYPED_SLICE_TO_DATUM", 6)
	s = typedSliceToDatum.ReplaceAllString(s, `{{template "typedSliceToDatum" buildDict "HasNulls" $5 "HasSel" $6}}`)
	vecToDatum := makeFunctionRegex("_VEC_TO_DATUM", 7)
	s = vecToDatum.ReplaceAllString(s, `{{template "vecToDatum" buildDict "HasNulls" $6 "HasSel" $7}}`)

	tmpl, err := template.New("vec_to_datum").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, nil)
}

func init() {
	registerGenerator(genVecToDatum, "vec_to_datum.eg.go", vecToDatumTmpl)
}
