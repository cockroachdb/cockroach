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
	"strings"
	"text/template"
)

type logicalOperation struct {
	Lower string
	Title string

	IsOr bool
}

func genAndOrProjectionOps(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/colexec/and_or_projection_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)
	s = strings.Replace(s, "_OP_LOWER", "{{.Lower}}", -1)
	s = strings.Replace(s, "_OP_TITLE", "{{.Title}}", -1)
	s = strings.Replace(s, "_IS_OR_OP", ".IsOr", -1)
	s = strings.Replace(s, "_L_HAS_NULLS", "$.lHasNulls", -1)
	s = strings.Replace(s, "_R_HAS_NULLS", "$.rHasNulls", -1)

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
	registerGenerator(genAndOrProjectionOps, "and_or_projection.eg.go")
}
