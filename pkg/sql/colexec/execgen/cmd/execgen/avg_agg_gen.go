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
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type avgTmplInfo struct {
	aggTmplInfoBase
	InputVecMethod string
	RetGoType      string
	RetGoTypeSlice string
	RetVecMethod   string

	avgOverload assignFunc
}

func (a avgTmplInfo) AssignAdd(targetElem, leftElem, rightElem, _, _, _ string) string {
	// Note that we already have correctly resolved method for "Plus" overload,
	// and we simply need to create a skeleton of lastArgWidthOverload to
	// supply treebin.Plus as the binary operator in order for the correct code
	// to be returned.
	lawo := &lastArgWidthOverload{lastArgTypeOverload: &lastArgTypeOverload{
		overloadBase: newBinaryOverloadBase(treebin.Plus),
	}}
	return a.avgOverload(lawo, targetElem, leftElem, rightElem, "", "", "")
}

func (a avgTmplInfo) AssignSubtract(
	targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string,
) string {
	// Note that we need to create lastArgWidthOverload only in order to tell
	// the resolved overload to use Minus overload in particular, so all other
	// fields remain unset.
	lawo := &lastArgWidthOverload{lastArgTypeOverload: &lastArgTypeOverload{
		overloadBase: newBinaryOverloadBase(treebin.Minus),
	}}
	return a.avgOverload(lawo, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol)
}

func (a avgTmplInfo) AssignDivInt64(targetElem, leftElem, rightElem, _, _, _ string) string {
	switch a.RetVecMethod {
	case toVecMethod(types.DecimalFamily, anyWidth):
		// Note that the result of summation of integers is stored as a
		// decimal, so ints and decimals share the division code.
		return fmt.Sprintf(`
			%s.SetInt64(%s)
			if _, err := tree.DecimalCtx.Quo(&%s, &%s, &%s); err != nil {
				colexecerror.InternalError(err)
			}`,
			targetElem, rightElem, targetElem, leftElem, targetElem,
		)
	case toVecMethod(types.FloatFamily, anyWidth):
		return fmt.Sprintf("%s = %s / float64(%s)", targetElem, leftElem, rightElem)
	case toVecMethod(types.IntervalFamily, anyWidth):
		return fmt.Sprintf("%s = %s.Div(int64(%s))", targetElem, leftElem, rightElem)
	}
	colexecerror.InternalError(errors.AssertionFailedf("unsupported avg agg type"))
	// This code is unreachable, but the compiler cannot infer that.
	return ""
}

var (
	_ = avgTmplInfo{}.AssignAdd
	_ = avgTmplInfo{}.AssignDivInt64
	_ = avgTmplInfo{}.AssignSubtract
)

// avgAggTypeTmplInfo is similar to lastArgTypeOverload and provides a way to
// see the type family of the overload. This is the top level of data passed to
// the template.
type avgAggTypeTmplInfo struct {
	TypeFamily     string
	WidthOverloads []avgAggWidthTmplInfo
}

// avgAggWidthTmplInfo is similar to lastArgWidthOverload and provides a way to
// see the width of the type of the overload. This is the middle level of data
// passed to the template.
type avgAggWidthTmplInfo struct {
	Width int32
	// Overload field contains all the necessary information for the template.
	// It should be accessed via {{with .Overload}} template instruction so that
	// the template has all of its info in scope.
	Overload avgTmplInfo
}

const avgAggTmpl = "pkg/sql/colexec/colexecagg/avg_agg_tmpl.go"

func genAvgAgg(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_TYPE_FAMILY", "{{.TypeFamily}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_RET_GOTYPESLICE", `{{.RetGoTypeSlice}}`,
		"_RET_GOTYPE", `{{.RetGoType}}`,
		"_RET_TYPE", "{{.RetVecMethod}}",
		"_TYPE", "{{.InputVecMethod}}",
		"TemplateType", "{{.InputVecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignDivRe := makeFunctionRegex("_ASSIGN_DIV_INT64", 6)
	s = assignDivRe.ReplaceAllString(s, makeTemplateFunctionCall("AssignDivInt64", 6))
	assignAddRe := makeFunctionRegex("_ASSIGN_ADD", 6)
	s = assignAddRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.AssignAdd", 6))
	assignSubtractRe := makeFunctionRegex("_ASSIGN_SUBTRACT", 6)
	s = assignSubtractRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.AssignSubtract", 6))

	accumulateAvg := makeFunctionRegex("_ACCUMULATE_AVG", 5)
	s = accumulateAvg.ReplaceAllString(s, `{{template "accumulateAvg" buildDict "Global" . "HasNulls" $4 "HasSel" $5}}`)
	removeRow := makeFunctionRegex("_REMOVE_ROW", 4)
	s = removeRow.ReplaceAllString(s, `{{template "removeRow" buildDict "Global" . "HasNulls" $4}}`)

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("avg_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	var tmplInfos []avgAggTypeTmplInfo
	// Average is computed as SUM / COUNT. The counting is performed directly
	// by the aggregate function struct, the division is handled by
	// AssignDivInt64 defined above, and resolving SUM overload is performed by
	// the helper function.
	// Note that all types on which we support avg aggregate function are the
	// canonical representatives, so we can operate with their type family
	// directly.
	for _, inputTypeFamily := range []types.Family{types.IntFamily, types.DecimalFamily, types.FloatFamily, types.IntervalFamily} {
		tmplInfo := avgAggTypeTmplInfo{TypeFamily: toString(inputTypeFamily)}
		for _, inputTypeWidth := range supportedWidthsByCanonicalTypeFamily[inputTypeFamily] {
			// Note that we don't use execinfrapb.GetAggregateInfo because we don't
			// want to bring in a dependency on that package to reduce the burden
			// of regenerating execgen code when the protobufs get generated.
			retTypeFamily, retTypeWidth := inputTypeFamily, inputTypeWidth
			if inputTypeFamily == types.IntFamily {
				// Average of integers is a decimal.
				retTypeFamily, retTypeWidth = types.DecimalFamily, anyWidth
			}
			tmplInfo.WidthOverloads = append(tmplInfo.WidthOverloads, avgAggWidthTmplInfo{
				Width: inputTypeWidth,
				Overload: avgTmplInfo{
					aggTmplInfoBase: aggTmplInfoBase{
						canonicalTypeFamily: typeconv.TypeFamilyToCanonicalTypeFamily(retTypeFamily),
					},
					InputVecMethod: toVecMethod(inputTypeFamily, inputTypeWidth),
					RetGoType:      toPhysicalRepresentation(retTypeFamily, retTypeWidth),
					RetGoTypeSlice: goTypeSliceName(retTypeFamily, retTypeWidth),
					RetVecMethod:   toVecMethod(retTypeFamily, retTypeWidth),
					avgOverload:    getSumAddOverload(inputTypeFamily),
				}})
		}
		tmplInfos = append(tmplInfos, tmplInfo)
	}
	return tmpl.Execute(wr, tmplInfos)
}

func init() {
	registerAggGenerator(genAvgAgg, "avg_agg.eg.go", avgAggTmpl, true /* genWindowVariant */)
}
