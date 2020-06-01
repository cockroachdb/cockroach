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
)

const sumAggTmpl = "pkg/sql/colexec/sum_agg_tmpl.go"

func genSumAgg(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_GOTYPESLICE", "{{.GoTypeSliceName}}",
		"_GOTYPE", "{{.GoType}}",
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignAddRe := makeFunctionRegex("_ASSIGN_ADD", 6)
	s = assignAddRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Assign", 6))

	accumulateSum := makeFunctionRegex("_ACCUMULATE_SUM", 4)
	s = accumulateSum.ReplaceAllString(s, `{{template "accumulateSum" buildDict "Global" . "HasNulls" $4}}`)

	tmpl, err := template.New("sum_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	overloads := sameTypeBinaryOpToOverloads[tree.Plus]
	// We want to omit the overload that operates on datum-backed vectors.
	if overloads[len(overloads)-1].CanonicalTypeFamily != typeconv.DatumVecCanonicalTypeFamily {
		colexecerror.InternalError("unexpectedly plus overload on datum-backed types is not the last one")
	}
	overloads = overloads[:len(overloads)-1]
	return tmpl.Execute(wr, overloads)
}

func init() {
	registerGenerator(genSumAgg, "sum_agg.eg.go", sumAggTmpl)
}
