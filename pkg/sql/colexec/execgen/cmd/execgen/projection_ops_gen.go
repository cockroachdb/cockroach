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
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

const projConstOpsTmpl = "pkg/sql/colexec/proj_const_ops_tmpl.go"

// getProjConstOpTmplString returns a "projConstOp" template with isConstLeft
// determining whether the constant is on the left or on the right.
func getProjConstOpTmplString(isConstLeft bool) (string, error) {
	t, err := ioutil.ReadFile(projConstOpsTmpl)
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
	tmpl = strings.ReplaceAll(tmpl, "_L_UNSAFEGET", "execgen.UNSAFEGET")
	tmpl = replaceManipulationFuncsAmbiguous(".Left", tmpl)
	tmpl = strings.ReplaceAll(tmpl, "_R_UNSAFEGET", "execgen.UNSAFEGET")
	tmpl = replaceManipulationFuncsAmbiguous(".Right", tmpl)
	tmpl = strings.ReplaceAll(tmpl, "_RETURN_UNSAFEGET", "execgen.RETURNUNSAFEGET")
	tmpl = replaceManipulationFuncsAmbiguous(".Right", tmpl)

	tmpl = strings.ReplaceAll(tmpl, "_LEFT_CANONICAL_TYPE_FAMILY", "{{.LeftFamilies}}")
	tmpl = strings.ReplaceAll(tmpl, "_LEFT_TYPE_WIDTH", typeWidthReplacement)
	tmpl = strings.ReplaceAll(tmpl, "_RIGHT_CANONICAL_TYPE_FAMILY", "{{.RightFamilies}}")
	tmpl = strings.ReplaceAll(tmpl, "_RIGHT_TYPE_WIDTH", typeWidthReplacement)

	tmpl = strings.ReplaceAll(tmpl, "_OP_NAME", "proj{{.Name}}{{.Left.VecMethod}}{{.Right.VecMethod}}Op")
	tmpl = strings.ReplaceAll(tmpl, "_NAME", "{{.Name}}")
	tmpl = strings.ReplaceAll(tmpl, "_L_GO_TYPE", "{{.Left.GoType}}")
	tmpl = strings.ReplaceAll(tmpl, "_R_GO_TYPE", "{{.Right.GoType}}")
	tmpl = strings.ReplaceAll(tmpl, "_L_TYP", "{{.Left.VecMethod}}")
	tmpl = strings.ReplaceAll(tmpl, "_R_TYP", "{{.Right.VecMethod}}")
	tmpl = strings.ReplaceAll(tmpl, "_RET_TYP", "{{.Right.RetVecMethod}}")

	assignRe := makeFunctionRegex("_ASSIGN", 3)
	tmpl = assignRe.ReplaceAllString(tmpl, makeTemplateFunctionCall("Right.Assign", 3))

	tmpl = strings.ReplaceAll(tmpl, "_HAS_NULLS", "$hasNulls")
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

const projNonConstOpsTmpl = "pkg/sql/colexec/proj_non_const_ops_tmpl.go"

// genProjNonConstOps is the generator for projection operators on two vectors.
func genProjNonConstOps(wr io.Writer) error {
	t, err := ioutil.ReadFile(projNonConstOpsTmpl)
	if err != nil {
		return err
	}

	s := string(t)
	s = replaceProjTmplVariables(s)

	tmpl, err := template.New("proj_non_const_ops").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, getProjTmplInfo())
}

type twoArgsResolved struct {
	*overloadBase
	Left  *argWidthOverload
	Right *lastArgWidthOverload
}

type projTmplInfoRightWidth struct {
	Width int32
	*twoArgsResolved
}

type projTmplInfoRightFamily struct {
	RightCanonicalFamily types.Family
	RightFamilies        string
	RightWidths          []*projTmplInfoRightWidth
}

type projTmplInfoLeftWidth struct {
	Width         int32
	RightFamilies []*projTmplInfoRightFamily
}

type projTmplInfoLeftFamily struct {
	LeftCanonicalFamily types.Family
	LeftFamilies        string
	LeftWidths          []*projTmplInfoLeftWidth
}

type projTmplInfoOp struct {
	*overloadBase
	LeftFamilies []*projTmplInfoLeftFamily
}

type projTmplInfo struct {
	BinOps            []*projTmplInfoOp
	CmpOps            []*projTmplInfoOp
	CastOverloads     *projTmplInfoOp
	ResolvedBinCmpOps []*twoArgsResolved
}

func getProjTmplInfo() projTmplInfo {
	var result projTmplInfo
	for _, overloads := range twoArgsOverloads {
		ov := overloads.Left[0].overloadBase
		var info *projTmplInfoOp
		if ov.IsBinOp {
			for _, existingInfo := range result.BinOps {
				if existingInfo.Name == ov.Name {
					info = existingInfo
					break
				}
			}
			if info == nil {
				info = &projTmplInfoOp{
					overloadBase: ov,
				}
				result.BinOps = append(result.BinOps, info)
			}
		} else if ov.IsCmpOp {
			for _, existingInfo := range result.CmpOps {
				if existingInfo.Name == ov.Name {
					info = existingInfo
					break
				}
			}
			if info == nil {
				info = &projTmplInfoOp{
					overloadBase: ov,
				}
				result.CmpOps = append(result.CmpOps, info)
			}
		} else if ov.IsCastOp {
			info = result.CastOverloads
			if info == nil {
				info = &projTmplInfoOp{
					overloadBase: ov,
				}
				result.CastOverloads = info
			}
		} else {
			colexecerror.InternalError(fmt.Sprintf("unexpectedly neither binary, comparison, nor cast overload: %s", ov))
		}
		for overloadIdx := range overloads.Left {
			leftTypeOverload := overloads.Left[overloadIdx]
			leftWidthOverload := leftTypeOverload.WidthOverload
			rightTypeOverload := overloads.Right[overloadIdx]
			rightWidthOverloads := rightTypeOverload.WidthOverloads
			var leftFamilies *projTmplInfoLeftFamily
			for _, lf := range info.LeftFamilies {
				if lf.LeftCanonicalFamily == leftTypeOverload.CanonicalTypeFamily {
					leftFamilies = lf
					break
				}
			}
			if leftFamilies == nil {
				leftFamilies = &projTmplInfoLeftFamily{
					LeftCanonicalFamily: leftTypeOverload.CanonicalTypeFamily,
					LeftFamilies:        leftTypeOverload.CanonicalTypeFamilyStr,
				}
				info.LeftFamilies = append(info.LeftFamilies, leftFamilies)
			}
			var leftWidths *projTmplInfoLeftWidth
			for _, lw := range leftFamilies.LeftWidths {
				if lw.Width == leftWidthOverload.Width {
					leftWidths = lw
					break
				}
			}
			if leftWidths == nil {
				leftWidths = &projTmplInfoLeftWidth{
					Width: leftWidthOverload.Width,
				}
				leftFamilies.LeftWidths = append(leftFamilies.LeftWidths, leftWidths)
			}
			var rightFamilies *projTmplInfoRightFamily
			for _, rf := range leftWidths.RightFamilies {
				if rf.RightCanonicalFamily == rightTypeOverload.CanonicalTypeFamily {
					rightFamilies = rf
					break
				}
			}
			if rightFamilies == nil {
				rightFamilies = &projTmplInfoRightFamily{
					RightCanonicalFamily: rightTypeOverload.CanonicalTypeFamily,
					RightFamilies:        rightTypeOverload.CanonicalTypeFamilyStr,
				}
				leftWidths.RightFamilies = append(leftWidths.RightFamilies, rightFamilies)
			}
			for _, rightWidthOverload := range rightWidthOverloads {
				tar := &twoArgsResolved{
					overloadBase: leftTypeOverload.overloadBase,
					Left:         leftWidthOverload,
					Right:        rightWidthOverload,
				}
				rightFamilies.RightWidths = append(rightFamilies.RightWidths,
					&projTmplInfoRightWidth{
						Width:           rightWidthOverload.Width,
						twoArgsResolved: tar,
					})
				if ov.IsBinOp || ov.IsCmpOp {
					result.ResolvedBinCmpOps = append(result.ResolvedBinCmpOps, tar)
				}
			}
		}
	}
	return result
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
			return tmpl.Execute(wr, getProjTmplInfo())
		}
	}

	registerGenerator(projConstOpsGenerator(true /* isConstLeft */), "proj_const_left_ops.eg.go", projConstOpsTmpl)
	registerGenerator(projConstOpsGenerator(false /* isConstLeft */), "proj_const_right_ops.eg.go", projConstOpsTmpl)
	registerGenerator(genProjNonConstOps, "proj_non_const_ops.eg.go", projNonConstOpsTmpl)
}
