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
	"strings"
	"text/template"
)

const projConstOpsTmpl = "pkg/sql/colexec/colexecproj/proj_const_ops_tmpl.go"

// replaceProjTmplVariables replaces template variables used in the templates
// for projection operators. It should only be used within this file.
// Note that not all template variables can be present in the template, and it
// is ok - such replacements will be noops.
func replaceProjTmplVariables(tmpl string) string {
	tmpl = strings.ReplaceAll(tmpl, "_L_UNSAFEGET", "execgen.UNSAFEGET")
	tmpl = replaceManipulationFuncsAmbiguous(".Left", tmpl)
	tmpl = strings.ReplaceAll(tmpl, "_R_UNSAFEGET", "execgen.UNSAFEGET")
	tmpl = replaceManipulationFuncsAmbiguous(".Right", tmpl)
	tmpl = strings.ReplaceAll(tmpl, "_RETURN_UNSAFEGET", "execgen.RETURNUNSAFEGET")
	tmpl = replaceManipulationFuncsAmbiguous(".Right", tmpl)

	r := strings.NewReplacer(
		"_LEFT_CANONICAL_TYPE_FAMILY", "{{.LeftCanonicalFamilyStr}}",
		"_LEFT_TYPE_WIDTH", typeWidthReplacement,
		"_RIGHT_CANONICAL_TYPE_FAMILY", "{{.RightCanonicalFamilyStr}}",
		"_RIGHT_TYPE_WIDTH", typeWidthReplacement,

		"_OP_NAME", "proj{{.Name}}{{.Left.VecMethod}}{{.Right.VecMethod}}Op",
		"_NAME", "{{.Name}}",
		"_L_GO_TYPE", "{{.Left.GoType}}",
		"_R_GO_TYPE", "{{.Right.GoType}}",
		"_L_TYP", "{{.Left.VecMethod}}",
		"_R_TYP", "{{.Right.VecMethod}}",
		"_RET_TYP", "{{.Right.RetVecMethod}}",
	)
	tmpl = r.Replace(tmpl)

	assignRe := makeFunctionRegex("_ASSIGN", 6)
	tmpl = assignRe.ReplaceAllString(tmpl, makeTemplateFunctionCall("Right.Assign", 6))

	tmpl = strings.ReplaceAll(tmpl, "_HAS_NULLS", "$hasNulls")
	tmpl = strings.ReplaceAll(tmpl, "_HAS_SEL", "$hasSel")
	setProjectionRe := makeFunctionRegex("_SET_PROJECTION", 1)
	tmpl = setProjectionRe.ReplaceAllString(tmpl, `{{template "setProjection" buildDict "Global" $ "HasNulls" $1 "Overload" .}}`)
	setSingleTupleProjectionRe := makeFunctionRegex("_SET_SINGLE_TUPLE_PROJECTION", 2)
	tmpl = setSingleTupleProjectionRe.ReplaceAllString(tmpl, `{{template "setSingleTupleProjection" buildDict "Global" $ "HasNulls" $1 "HasSel" $2 "Overload" .}}`)

	return tmpl
}

// replaceProjConstTmplVariables replaces template variables that are specific
// to projection operators with a constant argument. isConstLeft is true when
// the constant is on the left side. It should only be used within this file.
func replaceProjConstTmplVariables(tmpl string, isConstLeft bool) string {
	if isConstLeft {
		tmpl = strings.ReplaceAll(tmpl, "_CONST_SIDE", "L")
		tmpl = strings.ReplaceAll(tmpl, "_IS_CONST_LEFT", "true")
		tmpl = strings.ReplaceAll(tmpl, "_OP_CONST_NAME", "proj{{.Name}}{{.Left.VecMethod}}Const{{.Right.VecMethod}}Op")
		tmpl = strings.ReplaceAll(tmpl, "_NON_CONST_GOTYPESLICE", "{{.Right.GoTypeSliceName}}")
		tmpl = replaceManipulationFuncsAmbiguous(".Right", tmpl)
	} else {
		tmpl = strings.ReplaceAll(tmpl, "_CONST_SIDE", "R")
		tmpl = strings.ReplaceAll(tmpl, "_IS_CONST_LEFT", "false")
		tmpl = strings.ReplaceAll(tmpl, "_OP_CONST_NAME", "proj{{.Name}}{{.Left.VecMethod}}{{.Right.VecMethod}}ConstOp")
		tmpl = strings.ReplaceAll(tmpl, "_NON_CONST_GOTYPESLICE", "{{.Left.GoTypeSliceName}}")
		tmpl = replaceManipulationFuncsAmbiguous(".Left", tmpl)
	}
	return replaceProjTmplVariables(tmpl)
}

const projNonConstOpsTmpl = "pkg/sql/colexec/colexecproj/proj_non_const_ops_tmpl.go"

// genProjNonConstOps is the generator for projection operators on two vectors.
func genProjNonConstOps(inputFileContents string, wr io.Writer) error {
	s := replaceProjTmplVariables(inputFileContents)

	tmpl, err := template.New("proj_non_const_ops").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, twoArgsResolvedOverloadsInfo)
}

func init() {
	projConstOpsGenerator := func(isConstLeft bool) generator {
		return func(inputFileContents string, wr io.Writer) error {
			tmplString := replaceProjConstTmplVariables(inputFileContents, isConstLeft)
			tmpl, err := template.New("proj_const_ops").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(tmplString)
			if err != nil {
				return err
			}
			return tmpl.Execute(wr, twoArgsResolvedOverloadsInfo)
		}
	}

	registerGenerator(projConstOpsGenerator(true /* isConstLeft */), "proj_const_left_ops.eg.go", projConstOpsTmpl)
	registerGenerator(projConstOpsGenerator(false /* isConstLeft */), "proj_const_right_ops.eg.go", projConstOpsTmpl)
	registerGenerator(genProjNonConstOps, "proj_non_const_ops.eg.go", projNonConstOpsTmpl)
}
