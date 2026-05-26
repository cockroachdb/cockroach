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

type defaultCmpExprTmplInfo struct {
	CalledOnNullInput bool
	FlippedArgs       bool
	Negate            bool
}

const defaultCmpExprTmpl = "pkg/sql/colexec/colexeccmp/default_cmp_expr_tmpl.go"

func genDefaultCmpExpr(inputFileContents string, wr io.Writer) error {
	s := strings.ReplaceAll(
		inputFileContents, "_EXPR_NAME", "cmp{{if .CalledOnNullInput}}Nullable{{end}}"+
			"{{if .FlippedArgs}}Flipped{{end}}{{if .Negate}}Negate{{end}}ExprAdapter",
	)

	tmpl, err := template.New("default_cmp_expr").Parse(s)
	if err != nil {
		return err
	}
	var info []defaultCmpExprTmplInfo
	for _, nullable := range []bool{false, true} {
		for _, flipped := range []bool{false, true} {
			for _, negate := range []bool{false, true} {
				info = append(info, defaultCmpExprTmplInfo{
					CalledOnNullInput: nullable,
					FlippedArgs:       flipped,
					Negate:            negate,
				})
			}
		}
	}
	return tmpl.Execute(wr, info)
}

func init() {
	registerGenerator(genDefaultCmpExpr, "default_cmp_expr.eg.go", defaultCmpExprTmpl)
}
