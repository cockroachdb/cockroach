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
	"regexp"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

func genAnyNotNullAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/colexec/any_not_null_agg_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_GOTYPESLICE", "{{.GoTypeSliceName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.}}", -1)

	findAnyNotNull := makeFunctionRegex("_FIND_ANY_NOT_NULL", 4)
	s = findAnyNotNull.ReplaceAllString(s, `{{template "findAnyNotNull" buildDict "Global" . "HasNulls" $4}}`)

	s = replaceManipulationFuncs("", s)

	// We have to use explicit template language for some manipulation functions
	// but need to comment it out so that formatting checks don't fail. Remove the
	// comments here.
	s = regexp.MustCompile("// (v := {{ .Global.Get|{{ .Global.Set)").ReplaceAllString(s, "$1")

	tmpl, err := template.New("any_not_null_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, coltypes.AllTypes)
}

func init() {
	registerGenerator(genAnyNotNullAgg, "any_not_null_agg.eg.go")
}
