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

type defaultCmpExprTmplInfo struct {
	NullableArgs bool
	FlippedArgs  bool
	Negate       bool
}

const defaultCmpExprTmpl = "pkg/sql/colexec/colexeccmp/default_cmp_expr_tmpl.go"

func genDefaultCmpExpr(inputFileContents string, wr io.Writer) error {
	s := strings.ReplaceAll(
		inputFileContents, "_EXPR_NAME", "cmp{{if .NullableArgs}}Nullable{{end}}"+
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
					NullableArgs: nullable,
					FlippedArgs:  flipped,
					Negate:       negate,
				})
			}
		}
	}
	return tmpl.Execute(wr, info)
}

func init() {
	registerGenerator(genDefaultCmpExpr, "default_cmp_expr.eg.go", defaultCmpExprTmpl)
}
