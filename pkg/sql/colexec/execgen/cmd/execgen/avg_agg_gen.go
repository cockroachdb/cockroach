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

func (o lastArgWidthOverload) AssignDivInt64(
	targetElem, leftElem, rightElem, _, _, _ string,
) string {
	switch o.lastArgTypeOverload.CanonicalTypeFamily {
	case types.IntFamily, types.DecimalFamily:
		// Note that the result of summation of integers is stored as a
		// decimal, so ints and decimals share the division code.
		return fmt.Sprintf(`
			%s.SetInt64(%s)
			if _, err := tree.DecimalCtx.Quo(&%s, &%s, &%s); err != nil {
				colexecerror.InternalError(err)
			}`,
			targetElem, rightElem, targetElem, leftElem, targetElem,
		)
	case types.FloatFamily:
		return fmt.Sprintf("%s = %s / float64(%s)", targetElem, leftElem, rightElem)
	case types.IntervalFamily:
		return fmt.Sprintf("%s = %s.Div(int64(%s))", targetElem, leftElem, rightElem)
	}
	colexecerror.InternalError("unsupported avg agg type")
	// This code is unreachable, but the compiler cannot infer that.
	return ""
}

var _ = lastArgWidthOverload{}.AssignDivInt64

const avgAggTmpl = "pkg/sql/colexec/avg_agg_tmpl.go"

func genAvgAgg(inputFileContents string, wr io.Writer) error {
	// Average is computed as SUM / COUNT. The counting is performed directly
	// by the aggregate function struct, and the summation is handled by the
	// resolved overload, so we need to take care of the the division, and it
	// is handled by AssignDivInt64 method defined above.
	//
	// However, average of integers returns decimal result, so we need to be
	// quite tricky with all integer types: namely, we need to
	// 1. choose the resolved overload as "DECIMAL + INT" so that the
	// intermediate results of summation didn't overflow
	// 2. override the return type of the average computation to be decimal
	// 3. overloadHelper struct from non-integer average aggregates (because
	// only integer ones will be actually using it).

	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_IF_INTEGER_TYPE", ifIntegerType,
		"_RET_GOTYPE", ifIntegerType+`apd.Decimal{{else}}{{.GoType}}{{end}}`,
		"_RET_TYPE", ifIntegerType+"Decimal{{else}}{{.VecMethod}}{{end}}",
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignDivRe := makeFunctionRegex("_ASSIGN_DIV_INT64", 6)
	s = assignDivRe.ReplaceAllString(s, makeTemplateFunctionCall("AssignDivInt64", 6))
	assignAddRe := makeFunctionRegex("_ASSIGN_ADD", 6)
	s = assignAddRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Assign", 6))

	accumulateAvg := makeFunctionRegex("_ACCUMULATE_AVG", 4)
	s = accumulateAvg.ReplaceAllString(s, `{{template "accumulateAvg" buildDict "Global" . "HasNulls" $4}}`)

	tmpl, err := template.New("avg_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	// We support average on Ints, Decimals, Floats, and Intervals. The support
	// of Ints requires special care (explained above). In order for "DECIMAL +
	// INT" overload be of the similar structure as oneArgOverload used for all
	// other types, we need to make the right argument as if it is the only
	// argument (in a sense, to "pull out" the right side - the second level of
	// family-width overload - onto the top level).
	var tmplInfos = []*oneArgOverload{getDecimalPlusIntAsOneArgOverload()}
	for _, typ := range []*types.T{types.Decimal, types.Float, types.Interval} {
		var overload *oneArgOverload
		for _, o := range sameTypeBinaryOpToOverloads[tree.Plus] {
			if o.CanonicalTypeFamily == typ.Family() {
				overload = o
				break
			}
		}
		if overload == nil {
			colexecerror.InternalError(fmt.Sprintf("unexpectedly didn't find plus binary overload for %s", typ.String()))
		}
		if len(overload.WidthOverloads) != 1 {
			colexecerror.InternalError(fmt.Sprintf("unexpectedly plus binary overload for %s doesn't contain a single overload", typ.String()))
		}
		tmplInfos = append(tmplInfos, overload)
	}

	return tmpl.Execute(wr, tmplInfos)
}

func init() {
	registerGenerator(genAvgAgg, "avg_agg.eg.go", avgAggTmpl)
}
