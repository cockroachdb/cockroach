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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func (o lastArgWidthOverload) AssignAddForSumInt(
	targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string,
) string {
	switch o.lastArgTypeOverload.CanonicalTypeFamily {
	case types.IntFamily:
		var c intCustomizer
		// The result of "sum_int" computation is always INT8, regardless of
		// the argument type width, so we use special assignFunc that
		// "promotes" the result to be of types.Int equivalent.
		return c.getBinOpAssignFuncWithPromotedReturnType(types.Int)(
			&o, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol,
		)
	}
	colexecerror.InternalError("unsupported sum_int agg type")
	// This code is unreachable, but the compiler cannot infer that.
	return ""
}

var _ = lastArgWidthOverload{}.AssignAddForSumInt

const sumAggTmpl = "pkg/sql/colexec/sum_agg_tmpl.go"

func genSumAgg(inputFileContents string, wr io.Writer, isSumInt bool) error {
	kindReplacement := ""
	retGoTypeReplacement := ifIntegerType + `apd.Decimal{{else}}{{.GoType}}{{end}}`
	retTypeReplacement := ifIntegerType + `Decimal{{else}}{{.VecMethod}}{{end}}`
	if isSumInt {
		kindReplacement = "Int"
		retGoTypeReplacement = "int64"
		retTypeReplacement = "Int64"
	}
	r := strings.NewReplacer(
		"_KIND", kindReplacement,
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_IF_INTEGER_TYPE", ifIntegerType,
		"_RET_GOTYPE", retGoTypeReplacement,
		"_RET_TYPE", retTypeReplacement,
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignAddRe := makeFunctionRegex("_ASSIGN_ADD", 6)
	if isSumInt {
		// The result of computation of sum_int aggregation is INT8 regardless
		// of the argument type width, and we need to use the custom function
		// defined above that promotes the result type.
		s = assignAddRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.AssignAddForSumInt", 6))
	} else {
		s = assignAddRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Assign", 6))
	}

	accumulateSum := makeFunctionRegex("_ACCUMULATE_SUM", 4)
	s = accumulateSum.ReplaceAllString(s, `{{template "accumulateSum" buildDict "Global" . "HasNulls" $4}}`)

	tmpl, err := template.New("sum_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	var overloads []*oneArgOverload
	if isSumInt {
		// sum_int operates only on integers.
		for _, ov := range sameTypeBinaryOpToOverloads[tree.Plus] {
			if ov.CanonicalTypeFamily == typeconv.TypeFamilyToCanonicalTypeFamily(types.Int.Family()) {
				overloads = append(overloads, ov)
				break
			}
		}
	} else {
		// We support summation on ints, decimals, floats, and intervals. The
		// support of int summation requires special attention because it
		// returns a decimal result.
		overloads = append(overloads, getDecimalPlusIntAsOneArgOverload())
		for _, typ := range []*types.T{types.Decimal, types.Float, types.Interval} {
			for _, ov := range sameTypeBinaryOpToOverloads[tree.Plus] {
				if ov.CanonicalTypeFamily == typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
					overloads = append(overloads, ov)
					break
				}
			}
		}
	}
	return tmpl.Execute(wr, overloads)
}

func init() {
	sumAggGenerator := func(isSumInt bool) generator {
		return func(inputFileContents string, wr io.Writer) error {
			return genSumAgg(inputFileContents, wr, isSumInt)
		}
	}
	registerGenerator(sumAggGenerator(false /* isSumInt */), "sum_agg.eg.go", sumAggTmpl)
	registerGenerator(sumAggGenerator(true /* isSumInt */), "sum_int_agg.eg.go", sumAggTmpl)
}
