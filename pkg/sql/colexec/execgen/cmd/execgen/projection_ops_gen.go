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

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

// getProjConstOpTmplString returns a "projConstOp" template with isConstLeft
// determining whether the constant is on the left or on the right.
func getProjConstOpTmplString(isConstLeft bool) (string, error) {
	t, err := ioutil.ReadFile("pkg/sql/colexec/proj_const_ops_tmpl.go")
	if err != nil {
		return "", err
	}

	s := string(t)
	s = replaceProjConstTmplVariables(s, isConstLeft)
	return s, nil
}

// replaceProjTmplVariables replaces template variables used in the templates
// for projection operators. It should only be used within this file.
// Note that not all template variables can be present in the template, and it
// is ok - such replacements will be noops.
func replaceProjTmplVariables(tmpl string) string {
	tmpl = strings.Replace(tmpl, "_L_UNSAFEGET", "execgen.UNSAFEGET", -1)
	tmpl = replaceManipulationFuncs(".LTyp", tmpl)
	tmpl = strings.Replace(tmpl, "_R_UNSAFEGET", "execgen.UNSAFEGET", -1)
	tmpl = replaceManipulationFuncs(".RTyp", tmpl)
	tmpl = strings.Replace(tmpl, "_RET_UNSAFEGET", "execgen.UNSAFEGET", -1)
	tmpl = replaceManipulationFuncs(".RetTyp", tmpl)

	// The order in which variables are replaced is important - since some
	// variable names are prefixes of others, we need to replace the longer names
	// first.
	tmpl = strings.Replace(tmpl, "_OP_NAME", "proj{{.Name}}{{.LTyp}}{{.RTyp}}Op", -1)
	tmpl = strings.Replace(tmpl, "_NAME", "{{.Name}}", -1)
	tmpl = strings.Replace(tmpl, "_L_GO_TYPE", "{{.LGoType}}", -1)
	tmpl = strings.Replace(tmpl, "_R_GO_TYPE", "{{.RGoType}}", -1)
	tmpl = strings.Replace(tmpl, "_L_TYP_VAR", "{{$lTyp}}", -1)
	tmpl = strings.Replace(tmpl, "_R_TYP_VAR", "{{$rTyp}}", -1)
	tmpl = strings.Replace(tmpl, "_L_TYP", "{{.LTyp}}", -1)
	tmpl = strings.Replace(tmpl, "_R_TYP", "{{.RTyp}}", -1)
	tmpl = strings.Replace(tmpl, "_RET_TYP", "{{.RetTyp}}", -1)

	assignRe := makeFunctionRegex("_ASSIGN", 3)
	tmpl = assignRe.ReplaceAllString(tmpl, makeTemplateFunctionCall("Assign", 3))

	tmpl = strings.Replace(tmpl, "_HAS_NULLS", "$hasNulls", -1)
	setProjectionRe := makeFunctionRegex("_SET_PROJECTION", 1)
	tmpl = setProjectionRe.ReplaceAllString(tmpl, `{{template "setProjection" buildDict "Global" $ "HasNulls" $1 "Overload" .}}`)
	setSingleTupleProjectionRe := makeFunctionRegex("_SET_SINGLE_TUPLE_PROJECTION", 1)
	tmpl = setSingleTupleProjectionRe.ReplaceAllString(tmpl, `{{template "setSingleTupleProjection" buildDict "Global" $ "HasNulls" $1 "Overload" .}}`)

	return tmpl
}

// replaceProjConstTmplVariables replaces template variables that are specific
// to projection operators with a constant argument. isConstLeft is true when
// the constant is on the left side. It should only be used within this file.
func replaceProjConstTmplVariables(tmpl string, isConstLeft bool) string {
	if isConstLeft {
		tmpl = strings.Replace(tmpl, "_CONST_SIDE", "L", -1)
		tmpl = strings.Replace(tmpl, "_IS_CONST_LEFT", "true", -1)
		tmpl = strings.Replace(tmpl, "_OP_CONST_NAME", "proj{{.Name}}{{.LTyp}}Const{{.RTyp}}Op", -1)
		tmpl = replaceManipulationFuncs(".RTyp", tmpl)
	} else {
		tmpl = strings.Replace(tmpl, "_CONST_SIDE", "R", -1)
		tmpl = strings.Replace(tmpl, "_IS_CONST_LEFT", "false", -1)
		tmpl = strings.Replace(tmpl, "_OP_CONST_NAME", "proj{{.Name}}{{.LTyp}}{{.RTyp}}ConstOp", -1)
		tmpl = replaceManipulationFuncs(".LTyp", tmpl)
	}
	return replaceProjTmplVariables(tmpl)
}

// genProjNonConstOps is the generator for projection operators on two vectors.
func genProjNonConstOps(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/colexec/proj_non_const_ops_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)
	s = replaceProjTmplVariables(s)

	tmpl, err := template.New("proj_non_const_ops").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, getLTypToRTypToOverloads())
}

func getLTypToRTypToOverloads() map[coltypes.T]map[coltypes.T][]*overload {
	var allOverloads []*overload
	allOverloads = append(allOverloads, binaryOpOverloads...)
	allOverloads = append(allOverloads, comparisonOpOverloads...)

	lTypToRTypToOverloads := make(map[coltypes.T]map[coltypes.T][]*overload)
	for _, ov := range allOverloads {
		lTyp := ov.LTyp
		rTyp := ov.RTyp
		rTypToOverloads := lTypToRTypToOverloads[lTyp]
		if rTypToOverloads == nil {
			rTypToOverloads = make(map[coltypes.T][]*overload)
			lTypToRTypToOverloads[lTyp] = rTypToOverloads
		}
		rTypToOverloads[rTyp] = append(rTypToOverloads[rTyp], ov)
	}
	return lTypToRTypToOverloads
}

func init() {
	projConstOpsGenerator := func(isConstLeft bool) generator {
		return func(wr io.Writer) error {
			tmplString, err := getProjConstOpTmplString(isConstLeft)
			if err != nil {
				return err
			}
			tmpl, err := template.New("proj_const_ops").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(tmplString)
			if err != nil {
				return err
			}
			return tmpl.Execute(wr, getLTypToRTypToOverloads())
		}
	}

	registerGenerator(projConstOpsGenerator(true /* isConstLeft */), "proj_const_left_ops.eg.go")
	registerGenerator(projConstOpsGenerator(false /* isConstLeft */), "proj_const_right_ops.eg.go")
	registerGenerator(genProjNonConstOps, "proj_non_const_ops.eg.go")
}
