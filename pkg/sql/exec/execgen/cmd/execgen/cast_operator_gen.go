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

func genCastOperators(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/exec/cast_operator_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	assignCast := makeFunctionRegex("_ASSIGN_CAST", 2)
	s = assignCast.ReplaceAllString(s, `{{.Assign "$1" "$2"}}`)
	s = strings.Replace(s, "_ALLTYPES", "{{$typ}}", -1)
	s = strings.Replace(s, "_OVERLOADTYPES", "{{.ToTyp}}", -1)
	s = strings.Replace(s, "_FROMTYPE", "{{.FromTyp}}", -1)
	s = strings.Replace(s, "_TOTYPE", "{{.ToTyp}}", -1)

	isCastFuncSet := func(ov castOverload) bool {
		return ov.AssignFunc != nil
	}

	tmpl, err := template.New("cast_operator").Funcs(template.FuncMap{"isCastFuncSet": isCastFuncSet}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, castOverloads)
}

func init() {
	registerGenerator(genCastOperators, "cast_operator.eg.go")
}
