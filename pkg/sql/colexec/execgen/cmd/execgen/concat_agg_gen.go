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

type concatTmplInfo struct {
	InputVecMethod string
	*lastArgWidthOverload
}

func (c concatTmplInfo) AggValMemoryUsage(aggVal string) string {
	switch c.RetGoType {
	case "[]byte":
		return fmt.Sprintf("int64(len(%s))", aggVal)
	default:
		colexecerror.InternalError(fmt.Sprintf("unsupported type for aggregate value:%s", aggVal))
	}
	//make compiler happy
	return ""
}

func getConcatOverload(inputType *types.T) *lastArgWidthOverload {
	var overload *oneArgOverload
	for _, o := range sameTypeBinaryOpToOverloads[tree.Concat] {
		if o.CanonicalTypeFamily == inputType.Family() {
			overload = o
			break
		}
	}
	if overload == nil {
		colexecerror.InternalError(fmt.Sprintf("unexpectedly didn't find concat binary overload for %s", inputType.String()))
	}
	if len(overload.WidthOverloads) != 1 {
		colexecerror.InternalError(fmt.Sprintf("unexpectedly concat binary overload for %s doesn't contain a single overload", inputType.String()))
	}
	return overload.WidthOverloads[0]
}

const concatAggTmpl = "pkg/sql/colexec/concat_agg_tmpl.go"

func genConcatAgg(inputFileContents string, wr io.Writer) error {
	var tmplInfos []concatTmplInfo
	supportedTypes := []*types.T{types.Bytes}
	for _, inputType := range supportedTypes {
		tmplInfos = append(tmplInfos, concatTmplInfo{
			InputVecMethod:       toVecMethod(inputType.Family(), inputType.Width()),
			lastArgWidthOverload: getConcatOverload(inputType),
		})
	}

	r := strings.NewReplacer(
		"_RET_TYPE", "{{.RetVecMethod}}",
		"_TYPE", "{{.InputVecMethod}}",
		"_RET_GOTYPESLICE", "{{.GoTypeSliceName}}",
		"_RET_GOTYPE", `{{.RetGoType}}`,
	)
	s := r.Replace(inputFileContents)
	s = replaceManipulationFuncs(s)

	assignConcatRe := makeFunctionRegex("_ASSIGN_CONCAT", 6)
	s = assignConcatRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Assign", 6))

	accumulateConcatRe := makeFunctionRegex("_ACCUMULATE_CONCAT", 4)
	s = accumulateConcatRe.ReplaceAllString(s, `{{template "accumulateConcat" buildDict "Global" . "HasNulls" $4}}`)

	aggValMemoryUsageRe := makeFunctionRegex("_AGG_VAL_MEMORY_USAGE", 1)
	s = aggValMemoryUsageRe.ReplaceAllString(s, makeTemplateFunctionCall("AggValMemoryUsage", 1))

	tmpl, err := template.New("concat_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, tmplInfos)
}

func init() {
	registerAggGenerator(genConcatAgg, "concat_agg.eg.go", concatAggTmpl)
}
