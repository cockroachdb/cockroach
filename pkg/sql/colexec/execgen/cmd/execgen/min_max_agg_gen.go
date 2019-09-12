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
	"io/ioutil"
	"regexp"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type aggOverloads struct {
	Agg       execinfrapb.AggregatorSpec_Func
	Overloads []*overload
}

// AggNameLower returns the aggregation name in lower case, e.g. "min".
func (a aggOverloads) AggNameLower() string {
	return strings.ToLower(a.Agg.String())
}

// AggNameTitle returns the aggregation name in title case, e.g. "Min".
func (a aggOverloads) AggNameTitle() string {
	return strings.Title(a.AggNameLower())
}

// Avoid unused warning for functions which are only used in templates.
var _ = aggOverloads{}.AggNameLower()
var _ = aggOverloads{}.AggNameTitle()

func genMinMaxAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/colexec/min_max_agg_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)
	s = strings.Replace(s, "_AGG_TITLE", "{{.AggNameTitle}}", -1)
	s = strings.Replace(s, "_AGG", "{{$agg}}", -1)
	s = strings.Replace(s, "_GOTYPESLICE", "{{.LTyp.GoTypeSliceName}}", -1)
	s = strings.Replace(s, "_GOTYPE", "{{.LTyp.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)

	assignCmpRe := regexp.MustCompile(`_ASSIGN_CMP\((.*),(.*),(.*)\)`)
	s = assignCmpRe.ReplaceAllString(s, "{{.Global.Assign $1 $2 $3}}")

	accumulateMinMax := makeFunctionRegex("_ACCUMULATE_MINMAX", 4)
	s = accumulateMinMax.ReplaceAllString(s, `{{template "accumulateMinMax" buildDict "Global" . "LTyp" .LTyp "HasNulls" $4}}`)

	s = replaceManipulationFuncs(".LTyp", s)

	tmpl, err := template.New("min_max_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)

	if err != nil {
		return err
	}
	data := []aggOverloads{
		{
			Agg:       execinfrapb.AggregatorSpec_MIN,
			Overloads: sameTypeComparisonOpToOverloads[tree.LT],
		},
		{
			Agg:       execinfrapb.AggregatorSpec_MAX,
			Overloads: sameTypeComparisonOpToOverloads[tree.GT],
		},
	}
	return tmpl.Execute(wr, data)
}

func init() {
	registerGenerator(genMinMaxAgg, "min_max_agg.eg.go")
}
