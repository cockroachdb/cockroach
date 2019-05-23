// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"io"
	"io/ioutil"
	"regexp"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type aggOverloads struct {
	Agg       distsqlpb.AggregatorSpec_Func
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
	t, err := ioutil.ReadFile("pkg/sql/exec/min_max_agg_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)
	s = strings.Replace(s, "_AGG_TITLE", "{{.AggNameTitle}}", -1)
	s = strings.Replace(s, "_AGG", "{{$agg}}", -1)
	s = strings.Replace(s, "_GOTYPE", "{{.LTyp.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "types.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)

	assignCmpRe := regexp.MustCompile(`_ASSIGN_CMP\((.*),(.*),(.*)\)`)
	s = assignCmpRe.ReplaceAllString(s, "{{.Assign $1 $2 $3}}")

	tmpl, err := template.New("min_max_agg").Parse(s)
	if err != nil {
		return err
	}
	data := []aggOverloads{
		{
			Agg:       distsqlpb.AggregatorSpec_MIN,
			Overloads: comparisonOpToOverloads[tree.LT],
		},
		{
			Agg:       distsqlpb.AggregatorSpec_MAX,
			Overloads: comparisonOpToOverloads[tree.GT],
		},
	}
	return tmpl.Execute(wr, data)
}

func init() {
	registerGenerator(genMinMaxAgg, "min_max_agg.eg.go")
}
