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

	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type sumTmplInfo struct {
	SumKind        string
	NeedsHelper    bool
	InputVecMethod string
	RetGoType      string
	RetVecMethod   string

	addOverload assignFunc
}

func (s sumTmplInfo) AssignAdd(
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

var _ = sumTmplInfo{}.AssignAdd

// getSumAddOverload returns the resolved overload that can be used to
// accumulate a sum of values of inputType type. The resulting value's type is
// determined in the same way that 'sum' aggregate function is type-checked.
// For most types it is easy - we simply iterate over "Plus" overloads that
// take in the same type as both arguments. However, sum of integers returns a
// decimal result, so we need to pick the overload of appropriate width from
// "DECIMAL + INT" overload.
func getSumAddOverload(inputType *types.T) assignFunc {
	if inputType.Family() == types.IntFamily {
		var c decimalIntCustomizer
		return c.getBinOpAssignFunc()
	}
	var overload *oneArgOverload
	for _, o := range sameTypeBinaryOpToOverloads[tree.Plus] {
		if o.CanonicalTypeFamily == inputType.Family() {
			overload = o
			break
		}
	}
	if overload == nil {
		colexecerror.InternalError(fmt.Sprintf("unexpectedly didn't find plus binary overload for %s", inputType.String()))
	}
	if len(overload.WidthOverloads) != 1 {
		colexecerror.InternalError(fmt.Sprintf("unexpectedly plus binary overload for %s doesn't contain a single overload", inputType.String()))
	}
	return overload.WidthOverloads[0].AssignFunc
}

const sumAggTmpl = "pkg/sql/colexec/sum_agg_tmpl.go"

func genSumAgg(inputFileContents string, wr io.Writer, isSumInt bool) error {
	r := strings.NewReplacer(
		"_SUMKIND", "{{.SumKind}}",
		"_RET_GOTYPE", `{{.RetGoType}}`,
		"_RET_TYPE", "{{.RetVecMethod}}",
		"_TYPE", "{{.InputVecMethod}}",
		"TemplateType", "{{.InputVecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignAddRe := makeFunctionRegex("_ASSIGN_ADD", 6)
	s = assignAddRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.AssignAdd", 6))

	accumulateSum := makeFunctionRegex("_ACCUMULATE_SUM", 4)
	s = accumulateSum.ReplaceAllString(s, `{{template "accumulateSum" buildDict "Global" . "HasNulls" $4}}`)

	tmpl, err := template.New("sum_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	var (
		supportedTypes []*types.T
		sumKind        string
	)
	if isSumInt {
		supportedTypes = []*types.T{types.Int2, types.Int4, types.Int}
		sumKind = "Int"
	} else {
		supportedTypes = []*types.T{types.Int2, types.Int4, types.Int, types.Decimal, types.Float, types.Interval}
	}
	getAddOverload := func(inputType *types.T) assignFunc {
		if isSumInt {
			c := intCustomizer{width: anyWidth}
			return c.getBinOpAssignFunc()
		}
		return getSumAddOverload(inputType)
	}

	var tmplInfos []sumTmplInfo
	for _, inputType := range supportedTypes {
		needsHelper := false
		// Note that we don't use execinfrapb.GetAggregateInfo because we don't
		// want to bring in a dependency on that package to reduce the burden
		// of regenerating execgen code when the protobufs get generated.
		retType := inputType
		if inputType.Family() == types.IntFamily {
			if isSumInt {
				retType = types.Int
			} else {
				// Non-integer summation of integers needs a helper because the
				// result is a decimal.
				needsHelper = true
				retType = types.Decimal
			}
		}
		tmplInfos = append(tmplInfos, sumTmplInfo{
			SumKind:        sumKind,
			NeedsHelper:    needsHelper,
			InputVecMethod: toVecMethod(inputType.Family(), inputType.Width()),
			RetGoType:      toPhysicalRepresentation(retType.Family(), retType.Width()),
			RetVecMethod:   toVecMethod(retType.Family(), retType.Width()),
			addOverload:    getAddOverload(inputType),
		})
	}
	return tmpl.Execute(wr, struct {
		SumKind string
		Infos   []sumTmplInfo
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
