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

type logicalOperation struct {
	Lower string
	Title string

	IsOr bool
}

const andOrProjTmpl = "pkg/sql/colexec/and_or_projection_tmpl.go"

func genAndOrProjectionOps(inputFileContents string, wr io.Writer) error {

	r := strings.NewReplacer(
		"_OP_LOWER", "{{.Lower}}",
		"_OP_TITLE", "{{.Title}}",
		"_IS_OR_OP", ".IsOr",
		"_L_HAS_NULLS", "$.lHasNulls",
		"_R_HAS_NULLS", "$.rHasNulls",
	)
	s := r.Replace(inputFileContents)

	addTupleForRight := makeFunctionRegex("_ADD_TUPLE_FOR_RIGHT", 1)
	s = addTupleForRight.ReplaceAllString(s, `{{template "addTupleForRight" buildDict "Global" $ "lHasNulls" $1}}`)
	setValues := makeFunctionRegex("_SET_VALUES", 3)
	s = setValues.ReplaceAllString(s, `{{template "setValues" buildDict "Global" $ "IsOr" $1 "lHasNulls" $2 "rHasNulls" $3}}`)
	setSingleValue := makeFunctionRegex("_SET_SINGLE_VALUE", 3)
	s = setSingleValue.ReplaceAllString(s, `{{template "setSingleValue" buildDict "Global" $ "IsOr" $1 "lHasNulls" $2 "rHasNulls" $3}}`)

	tmpl, err := template.New("and").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	operations := []logicalOperation{
		{
			Lower: "and",
			Title: "And",
			IsOr:  false,
		},
		{
			Lower: "or",
			Title: "Or",
			IsOr:  true,
		},
	}

	return tmpl.Execute(wr, operations)
}

func init() {
	registerGenerator(genAndOrProjectionOps, "and_or_projection.eg.go", andOrProjTmpl)
}
