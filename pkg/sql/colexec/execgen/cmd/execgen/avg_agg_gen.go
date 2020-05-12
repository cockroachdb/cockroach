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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func (o lastArgWidthOverload) AssignAdd(target, l, r string) string {
	return o.WidthOverloads[0].Assign(target, l, r)
}

func (o lastArgWidthOverload) AssignDivInt64(target, l, r string) string {
	switch o.lastArgTypeOverload.CanonicalTypeFamily {
	case types.DecimalFamily:
		return fmt.Sprintf(`
			%s.SetInt64(%s)
			if _, err := tree.DecimalCtx.Quo(&%s, &%s, &%s); err != nil {
			colexecerror.InternalError(err)
		}`,
			target, r, target, l, target,
		)
	case types.FloatFamily:
		return fmt.Sprintf("%s = %s / float64(%s)", target, l, r)
	}
	colexecerror.InternalError("unsupported avg agg type")
	// This code is unreachable, but the compiler cannot infer that.
	return ""
}

var (
	_ = lastArgWidthOverload{}.AssignAdd
	_ = lastArgWidthOverload{}.AssignDivInt64
)

const avgAggTmpl = "pkg/sql/colexec/avg_agg_tmpl.go"

func genAvgAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile(avgAggTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.ReplaceAll(s, "_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}")
	s = strings.ReplaceAll(s, "_TYPE_WIDTH", typeWidthReplacement)
	s = strings.ReplaceAll(s, "_GOTYPESLICE", "{{.GoTypeSliceName}}")
	s = strings.ReplaceAll(s, "_GOTYPE", "{{.GoType}}")
	s = strings.ReplaceAll(s, "_TYPE", "{{.VecMethod}}")
	s = strings.ReplaceAll(s, "TemplateType", "{{.VecMethod}}")

	assignDivRe := makeFunctionRegex("_ASSIGN_DIV_INT64", 3)
	s = assignDivRe.ReplaceAllString(s, makeTemplateFunctionCall("AssignDivInt64", 3))
	assignAddRe := makeFunctionRegex("_ASSIGN_ADD", 3)
	s = assignAddRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.AssignAdd", 3))

	accumulateAvg := makeFunctionRegex("_ACCUMULATE_AVG", 4)
	s = accumulateAvg.ReplaceAllString(s, `{{template "accumulateAvg" buildDict "Global" . "HasNulls" $4}}`)

	tmpl, err := template.New("avg_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	// TODO(asubiotto): support more types.
	supportedTypes := []*types.T{types.Decimal, types.Float}
	tmplInfos := make([]*oneArgOverload, len(supportedTypes))
	for i, typ := range supportedTypes {
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
		tmplInfos[i] = overload
	}

	return tmpl.Execute(wr, tmplInfos)
}

func init() {
	registerGenerator(genAvgAgg, "avg_agg.eg.go", avgAggTmpl)
}
