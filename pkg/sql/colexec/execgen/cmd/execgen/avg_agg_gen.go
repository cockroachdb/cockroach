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

type avgTmplInfo struct {
	NeedsHelper    bool
	InputVecMethod string
	RetGoType      string
	RetVecMethod   string

	addOverload assignFunc
}

func (a avgTmplInfo) AssignAdd(targetElem, leftElem, rightElem, _, _, _ string) string {
	// Note that we already have correctly resolved method for "Plus" overload,
	// and we simply need to create a skeleton of lastArgWidthOverload to
	// supply tree.Plus as the binary operator in order for the correct code to
	// be returned.
	lawo := &lastArgWidthOverload{lastArgTypeOverload: &lastArgTypeOverload{
		overloadBase: newBinaryOverloadBase(tree.Plus),
	}}
	return a.addOverload(lawo, targetElem, leftElem, rightElem, "", "", "")
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
	colexecerror.InternalError("unsupported avg agg type")
	// This code is unreachable, but the compiler cannot infer that.
	return ""
}

var (
	_ = avgTmplInfo{}.AssignAdd
	_ = avgTmplInfo{}.AssignDivInt64
)

const avgAggTmpl = "pkg/sql/colexec/avg_agg_tmpl.go"

func genAvgAgg(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
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

	accumulateAvg := makeFunctionRegex("_ACCUMULATE_AVG", 4)
	s = accumulateAvg.ReplaceAllString(s, `{{template "accumulateAvg" buildDict "Global" . "HasNulls" $4}}`)

	tmpl, err := template.New("avg_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	var tmplInfos []avgTmplInfo
	// Average is computed as SUM / COUNT. The counting is performed directly
	// by the aggregate function struct, the division is handled by
	// AssignDivInt64 defined above, and resolving SUM overload is performed by
	// the helper function.
	// Note that all types on which we support avg aggregate function are the
	// canonical representatives, so we can operate with their type family
	// directly.
	for _, inputType := range []*types.T{types.Int2, types.Int4, types.Int, types.Decimal, types.Float, types.Interval} {
		needsHelper := false
		// Note that we don't use execinfrapb.GetAggregateInfo because we don't
		// want to bring in a dependency on that package to reduce the burden
		// of regenerating execgen code when the protobufs get generated.
		retType := inputType
		if inputType.Family() == types.IntFamily {
			// Average of integers is a decimal.
			needsHelper = true
			retType = types.Decimal
		}
		tmplInfos = append(tmplInfos, avgTmplInfo{
			NeedsHelper:    needsHelper,
			InputVecMethod: toVecMethod(inputType.Family(), inputType.Width()),
			RetGoType:      toPhysicalRepresentation(retType.Family(), retType.Width()),
			RetVecMethod:   toVecMethod(retType.Family(), retType.Width()),
			addOverload:    getSumAddOverload(inputType),
		})
	}
	return tmpl.Execute(wr, tmplInfos)
}

func init() {
	registerAggGenerator(genAvgAgg, "avg_agg.eg.go", avgAggTmpl)
}
