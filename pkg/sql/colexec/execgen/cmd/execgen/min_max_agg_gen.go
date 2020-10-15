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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type minMaxTmplInfo struct {
	// Note that we embed the corresponding comparison overload (either LT or
	// GT) into this struct (so that we could have access to methods like Set,
	// Range, etc.) but also customize the result variables.
	*lastArgWidthOverload
	Agg string
	// The following three fields have "Agg" prefix in order to not collide
	// with the fields in lastArgWidthOverload, just to be safe.
	AggRetGoTypeSlice string
	AggRetGoType      string
	AggRetVecMethod   string
}

// AggNameTitle returns the aggregation name in title case, e.g. "Min".
func (a minMaxTmplInfo) AggNameTitle() string {
	return strings.Title(a.Agg)
}

// Avoid unused warning for functions which are only used in templates.
var _ = minMaxTmplInfo{}.AggNameTitle()

const minMaxAggTmpl = "pkg/sql/colexec/min_max_agg_tmpl.go"

func genMinMaxAgg(inputFileContents string, wr io.Writer) error {
	// TODO(yuzefovich): clean up this file as per comments in #54080.
	r := strings.NewReplacer(
		"_AGG_TITLE", "{{.AggNameTitle}}",
		"_AGG", "{{.Agg}}",
		"_RET_GOTYPESLICE", "{{.AggRetGoTypeSlice}}",
		"_RET_GOTYPE", "{{.AggRetGoType}}",
		"_RET_TYPE", "{{.AggRetVecMethod}}",
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignCmpRe := makeFunctionRegex("_ASSIGN_CMP", 6)
	s = assignCmpRe.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 6))

	accumulateMinMax := makeFunctionRegex("_ACCUMULATE_MINMAX", 4)
	s = accumulateMinMax.ReplaceAllString(s, `{{template "accumulateMinMax" buildDict "Global" . "HasNulls" $4}}`)

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("min_max_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	var tmplInfos []minMaxTmplInfo
	for _, agg := range []string{"min", "max"} {
		cmpOp := tree.LT
		if agg == "max" {
			cmpOp = tree.GT
		}
		for _, ov := range sameTypeComparisonOpToOverloads[cmpOp] {
			for i := range ov.WidthOverloads {
				widthOv := ov.WidthOverloads[i]
				retGoTypeSlice := widthOv.GoTypeSliceName()
				retGoType := widthOv.GoType
				retVecMethod := widthOv.VecMethod
				tmplInfos = append(tmplInfos, minMaxTmplInfo{
					lastArgWidthOverload: widthOv,
					Agg:                  agg,
					AggRetGoTypeSlice:    retGoTypeSlice,
					AggRetGoType:         retGoType,
					AggRetVecMethod:      retVecMethod,
				})
			}
		}
	}
	return tmpl.Execute(wr, tmplInfos)
}

func init() {
	registerAggGenerator(genMinMaxAgg, "min_max_agg.eg.go", minMaxAggTmpl)
}
