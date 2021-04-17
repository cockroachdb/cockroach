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

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type sumAggTmplInfo struct {
	aggTmplInfoBase
	SumKind        string
	NeedsHelper    bool
	InputVecMethod string
	RetGoType      string
	RetVecMethod   string

	addOverload assignFunc
}

func (s sumAggTmplInfo) AssignAdd(
	targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string,
) string {
	// Note that we need to create lastArgWidthOverload only in order to tell
	// the resolved overload to use Plus overload in particular, so all other
	// fields remain unset.
	lawo := &lastArgWidthOverload{lastArgTypeOverload: &lastArgTypeOverload{
		overloadBase: newBinaryOverloadBase(tree.Plus),
	}}
	return s.addOverload(lawo, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol)
}

var _ = sumAggTmplInfo{}.AssignAdd

// avgAggTypeTmplInfo is similar to lastArgTypeOverload and provides a way to
// see the type family of the overload. This is the top level of data passed to
// the template.
type sumAggTypeTmplInfo struct {
	TypeFamily     string
	WidthOverloads []sumAggWidthTmplInfo
}

// avgAggWidthTmplInfo is similar to lastArgWidthOverload and provides a way to
// see the width of the type of the overload. This is the middle level of data
// passed to the template.
type sumAggWidthTmplInfo struct {
	Width int32
	// Overload field contains all the necessary information for the template.
	// It should be accessed via {{with .Overload}} template instruction so that
	// the template has all of its info in scope.
	Overload sumAggTmplInfo
}

// getSumAddOverload returns the resolved overload that can be used to
// accumulate a sum of values of inputType type. The resulting value's type is
// determined in the same way that 'sum' aggregate function is type-checked.
// For most types it is easy - we simply iterate over "Plus" overloads that
// take in the same type as both arguments. However, sum of integers returns a
// decimal result, so we need to pick the overload of appropriate width from
// "DECIMAL + INT" overload.
func getSumAddOverload(inputTypeFamily types.Family) assignFunc {
	if inputTypeFamily == types.IntFamily {
		var c decimalIntCustomizer
		return c.getBinOpAssignFunc()
	}
	var overload *oneArgOverload
	for _, o := range sameTypeBinaryOpToOverloads[tree.Plus] {
		if o.CanonicalTypeFamily == inputTypeFamily {
			overload = o
			break
		}
	}
	if overload == nil {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly didn't find plus binary overload for %s", inputTypeFamily))
	}
	if len(overload.WidthOverloads) != 1 {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly plus binary overload for %s doesn't contain a single overload", inputTypeFamily))
	}
	return overload.WidthOverloads[0].AssignFunc
}

const sumAggTmpl = "pkg/sql/colexec/colexecagg/sum_agg_tmpl.go"

func genSumAgg(inputFileContents string, wr io.Writer, isSumInt bool) error {
	r := strings.NewReplacer(
		"_TYPE_FAMILY", "{{.TypeFamily}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_SUMKIND", "{{.SumKind}}",
		"_RET_GOTYPE", `{{.RetGoType}}`,
		"_RET_TYPE", "{{.RetVecMethod}}",
		"_TYPE", "{{.InputVecMethod}}",
		"TemplateType", "{{.InputVecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignAddRe := makeFunctionRegex("_ASSIGN_ADD", 6)
	s = assignAddRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.AssignAdd", 6))

	accumulateSum := makeFunctionRegex("_ACCUMULATE_SUM", 5)
	s = accumulateSum.ReplaceAllString(s, `{{template "accumulateSum" buildDict "Global" . "HasNulls" $4 "HasSel" $5}}`)

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("sum_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	var supportedTypeFamilies []types.Family
	var sumKind string
	if isSumInt {
		supportedTypeFamilies = []types.Family{types.IntFamily}
		sumKind = "Int"
	} else {
		supportedTypeFamilies = []types.Family{types.IntFamily, types.DecimalFamily, types.FloatFamily, types.IntervalFamily}
	}
	getAddOverload := func(inputTypeFamily types.Family) assignFunc {
		if isSumInt {
			c := intCustomizer{width: anyWidth}
			return c.getBinOpAssignFunc()
		}
		return getSumAddOverload(inputTypeFamily)
	}

	var tmplInfos []sumAggTypeTmplInfo
	for _, inputTypeFamily := range supportedTypeFamilies {
		tmplInfo := sumAggTypeTmplInfo{
			TypeFamily: toString(inputTypeFamily),
		}
		for _, inputTypeWidth := range supportedWidthsByCanonicalTypeFamily[inputTypeFamily] {
			needsHelper := false
			// Note that we don't use execinfrapb.GetAggregateInfo because we don't
			// want to bring in a dependency on that package to reduce the burden
			// of regenerating execgen code when the protobufs get generated.
			retTypeFamily, retTypeWidth := inputTypeFamily, inputTypeWidth
			if inputTypeFamily == types.IntFamily {
				if isSumInt {
					retTypeFamily, retTypeWidth = types.IntFamily, anyWidth
				} else {
					// Non-integer summation of integers needs a helper because
					// the result is a decimal.
					needsHelper = true
					retTypeFamily, retTypeWidth = types.DecimalFamily, anyWidth
				}
			}
			tmplInfo.WidthOverloads = append(tmplInfo.WidthOverloads, sumAggWidthTmplInfo{
				Width: inputTypeWidth,
				Overload: sumAggTmplInfo{
					aggTmplInfoBase: aggTmplInfoBase{
						canonicalTypeFamily: typeconv.TypeFamilyToCanonicalTypeFamily(retTypeFamily),
					},
					SumKind:        sumKind,
					NeedsHelper:    needsHelper,
					InputVecMethod: toVecMethod(inputTypeFamily, inputTypeWidth),
					RetGoType:      toPhysicalRepresentation(retTypeFamily, retTypeWidth),
					RetVecMethod:   toVecMethod(retTypeFamily, retTypeWidth),
					addOverload:    getAddOverload(inputTypeFamily),
				}})
		}
		tmplInfos = append(tmplInfos, tmplInfo)
	}
	return tmpl.Execute(wr, struct {
		SumKind string
		Infos   []sumAggTypeTmplInfo
	}{
		SumKind: sumKind,
		Infos:   tmplInfos,
	})
}

func init() {
	sumAggGenerator := func(isSumInt bool) generator {
		return func(inputFileContents string, wr io.Writer) error {
			return genSumAgg(inputFileContents, wr, isSumInt)
		}
	}
	registerAggGenerator(sumAggGenerator(false /* isSumInt */), "sum_agg.eg.go", sumAggTmpl)
	registerAggGenerator(sumAggGenerator(true /* isSumInt */), "sum_int_agg.eg.go", sumAggTmpl)
}
