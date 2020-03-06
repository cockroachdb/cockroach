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
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type sortOverload struct {
	*overload
	Dir       string
	DirString string
	Nulls     bool
}

type sortOverloads struct {
	LTyp      coltypes.T
	Overloads []sortOverload
}

// typesToSortOverloads maps types to whether nulls are handled to
// the overload representing the sort direction.
var typesToSortOverloads map[coltypes.T]map[bool]sortOverloads

const sortOpsTmpl = "pkg/sql/colexec/sort_tmpl.go"

func genSortOps(wr io.Writer) error {
	d, err := ioutil.ReadFile(sortOpsTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	// Replace the template variables.
	s = strings.Replace(s, "_GOTYPESLICE", "{{.LTyp.GoTypeSliceName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{$typ}}", -1)
	s = strings.Replace(s, "_TYPE", "{{$typ}}", -1)
	s = strings.Replace(s, "_DIR_ENUM", "{{.Dir}}", -1)
	s = strings.Replace(s, "_DIR", "{{.DirString}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_ISNULL", "{{$isNull}}", -1)
	s = strings.Replace(s, "_HANDLES_NULLS", "{{if .Nulls}}WithNulls{{else}}{{end}}", -1)

	assignLtRe := makeFunctionRegex("_ASSIGN_LT", 3)
	s = assignLtRe.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 3))

	s = replaceManipulationFuncs(".LTyp", s)

	// Now, generate the op, from the template.
	tmpl, err := template.New("sort_op").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, typesToSortOverloads)
}

const quickSortTmpl = "pkg/sql/colexec/quicksort_tmpl.go"

func genQuickSortOps(wr io.Writer) error {
	d, err := ioutil.ReadFile(quickSortTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	// Replace the template variables.
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_DIR", "{{.DirString}}", -1)
	s = strings.Replace(s, "_HANDLES_NULLS", "{{if .Nulls}}WithNulls{{else}}{{end}}", -1)

	// Now, generate the op, from the template.
	tmpl, err := template.New("quicksort").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, typesToSortOverloads)
}

func init() {
	registerGenerator(genSortOps, "sort.eg.go", sortOpsTmpl)
	registerGenerator(genQuickSortOps, "quicksort.eg.go", quickSortTmpl)
	typesToSortOverloads = make(map[coltypes.T]map[bool]sortOverloads)
	for _, o := range sameTypeComparisonOpToOverloads[tree.LT] {
		typesToSortOverloads[o.LTyp] = make(map[bool]sortOverloads)
		for _, b := range []bool{true, false} {
			typesToSortOverloads[o.LTyp][b] = sortOverloads{
				LTyp: o.LTyp,
				Overloads: []sortOverload{
					{overload: o, Dir: "execinfrapb.Ordering_Column_ASC", DirString: "Asc", Nulls: b},
					{}},
			}
		}
	}
	for _, o := range sameTypeComparisonOpToOverloads[tree.GT] {
		for _, b := range []bool{true, false} {
			typesToSortOverloads[o.LTyp][b].Overloads[1] = sortOverload{
				overload: o, Dir: "execinfrapb.Ordering_Column_DESC", DirString: "Desc", Nulls: b}
		}
	}
}
