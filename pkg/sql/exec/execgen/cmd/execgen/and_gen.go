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

func genAndOp(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/exec/and_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)
	s = strings.Replace(s, "_L_HAS_NULLS", "$.lHasNulls", -1)
	s = strings.Replace(s, "_R_HAS_NULLS", "$.rHasNulls", -1)
	s = strings.Replace(s, "_USES_SEL", "$.usesSel", -1)

	setValues := makeFunctionRegex("_SET_VALUES", 2)
	s = setValues.ReplaceAllString(s, `{{template "setValues" buildDict "Global" $ "lHasNulls" $1 "rHasNulls" $2}}`)
	setSingleValue := makeFunctionRegex("_SET_SINGLE_VALUE", 3)
	s = setSingleValue.ReplaceAllString(s, `{{template "setSingleValue" buildDict "Global" $ "usesSel" $1 "lHasNulls" $2 "rHasNulls" $3}}`)

	tmpl, err := template.New("and").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, nil /* data */)
}

func init() {
	registerGenerator(genAndOp, "and.eg.go")
}
