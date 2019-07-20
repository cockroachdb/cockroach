// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

type scalarInfo struct {
	IsScalar bool
	String   string
}

var scalarInfos = []scalarInfo{
	{
		IsScalar: false,
		String:   "NonScalar",
	},
	{
		IsScalar: true,
		String:   "Scalar",
	},
}

func genAnyNotNullAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/exec/any_not_null_agg_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_GOTYPE", "{{$type.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "types.{{$type}}", -1)
	s = strings.Replace(s, "_TYPE", "{{$type}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{$type}}", -1)
	s = strings.Replace(s, "_SCALAR", "{{$scalarInfo.String}}", -1)

	findAnyNotNull := makeFunctionRegex("_FIND_ANY_NOT_NULL", 4)
	s = findAnyNotNull.ReplaceAllString(s, `{{template "findAnyNotNull" buildDict "Global" . "HasNulls" $4}}`)

	tmpl, err := template.New("any_not_null_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, struct {
		Types       interface{}
		ScalarInfos interface{}
	}{
		Types:       types.AllTypes,
		ScalarInfos: scalarInfos,
	})
}

func init() {
	registerGenerator(genAnyNotNullAgg, "any_not_null_agg.eg.go")
}
