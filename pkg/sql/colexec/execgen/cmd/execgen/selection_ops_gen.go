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

func getSelectionOpsTmpl() (*template.Template, error) {
	t, err := ioutil.ReadFile("pkg/sql/colexec/selection_ops_tmpl.go")
	if err != nil {
		return nil, err
	}

	s := string(t)
	s = strings.Replace(s, "_OP_CONST_NAME", "sel{{.Name}}{{.LTyp}}{{.RTyp}}ConstOp", -1)
	s = strings.Replace(s, "_OP_NAME", "sel{{.Name}}{{.LTyp}}{{.RTyp}}Op", -1)
	s = strings.Replace(s, "_R_GO_TYPE", "{{.RGoType}}", -1)
	s = strings.Replace(s, "_L_TYP_VAR", "{{$lTyp}}", -1)
	s = strings.Replace(s, "_R_TYP_VAR", "{{$rTyp}}", -1)
	s = strings.Replace(s, "_L_TYP", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_R_TYP", "{{.RTyp}}", -1)
	s = strings.Replace(s, "_NAME", "{{.Name}}", -1)

	assignCmpRe := regexp.MustCompile(`_ASSIGN_CMP\((.*),(.*),(.*)\)`)
	s = assignCmpRe.ReplaceAllString(s, "{{.Assign $1 $2 $3}}")

	s = replaceManipulationFuncs(".LTyp", s)
	s = strings.Replace(s, "_R_UNSAFEGET", "execgen.UNSAFEGET", -1)
	s = strings.Replace(s, "_R_SLICE", "execgen.SLICE", -1)
	s = replaceManipulationFuncs(".RTyp", s)

	s = strings.Replace(s, "_HAS_NULLS", "$hasNulls", -1)
	selConstLoop := makeFunctionRegex("_SEL_CONST_LOOP", 1)
	s = selConstLoop.ReplaceAllString(s, `{{template "selConstLoop" buildDict "Global" $ "HasNulls" $1 "Overload" .}}`)
	selLoop := makeFunctionRegex("_SEL_LOOP", 1)
	s = selLoop.ReplaceAllString(s, `{{template "selLoop" buildDict "Global" $ "HasNulls" $1 "Overload" .}}`)

	return template.New("selection_ops").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
}

func genSelectionOps(wr io.Writer) error {
	tmpl, err := getSelectionOpsTmpl()
	if err != nil {
		return err
	}
	lTypToRTypToOverloads := make(map[coltypes.T]map[coltypes.T][]*overload)
	for _, ov := range comparisonOpOverloads {
		lTyp := ov.LTyp
		rTyp := ov.RTyp
		rTypToOverloads := lTypToRTypToOverloads[lTyp]
		if rTypToOverloads == nil {
			rTypToOverloads = make(map[coltypes.T][]*overload)
			lTypToRTypToOverloads[lTyp] = rTypToOverloads
		}
		rTypToOverloads[rTyp] = append(rTypToOverloads[rTyp], ov)
	}
	return tmpl.Execute(wr, lTypToRTypToOverloads)
}

func init() {
	registerGenerator(genSelectionOps, "selection_ops.eg.go")
}
