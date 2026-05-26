// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"io"
	"strings"
	"text/template"
)

const isNullOpsTmpl = "pkg/sql/colexec/is_null_ops_tmpl.go"

func genIsNullOps(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_IS_TUPLE", ".IsTuple",
		"_KIND", "{{.Kind}}",
	)
	s := r.Replace(inputFileContents)

	computeIsNullRe := makeFunctionRegex("_COMPUTE_IS_NULL", 5)
	s = computeIsNullRe.ReplaceAllString(s, `{{template "computeIsNull" buildDict "HasNulls" $4 "IsTuple" $5}}`)
	maybeSelectRe := makeFunctionRegex("_MAYBE_SELECT", 6)
	s = maybeSelectRe.ReplaceAllString(s, `{{template "maybeSelect" buildDict "IsTuple" $6}}`)

	tmpl, err := template.New("is_null_ops").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, []struct {
		IsTuple bool
		Kind    string
	}{
		{IsTuple: false, Kind: ""},
		{IsTuple: true, Kind: "Tuple"},
	})
}

func init() {
	registerGenerator(genIsNullOps, "is_null_ops.eg.go", isNullOpsTmpl)
}
