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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type sortDirOverload struct {
	Dir             string
	DirString       string
	FamilyOverloads []*oneArgOverload
}

type sortDirNullsOverload struct {
	Nulls        bool
	DirOverloads []*sortDirOverload
}

var sortOverloads []*sortDirNullsOverload

const sortOpsTmpl = "pkg/sql/colexec/sort_tmpl.go"

func genSortOps(wr io.Writer) error {
	d, err := ioutil.ReadFile(sortOpsTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.ReplaceAll(s, "_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}")
	s = strings.ReplaceAll(s, "_TYPE_WIDTH", typeWidthReplacement)
	s = strings.ReplaceAll(s, "_GOTYPESLICE", "{{.GoTypeSliceName}}")
	s = strings.ReplaceAll(s, "_GOTYPE", "{{.GoType}}")
	s = strings.ReplaceAll(s, "_TYPE", "{{.VecMethod}}")
	s = strings.ReplaceAll(s, "TemplateType", "{{.VecMethod}}")

	s = strings.ReplaceAll(s, "_DIR_ENUM", "{{.Dir}}")
	s = strings.ReplaceAll(s, "_DIR", "{{$dir}}")
	s = strings.ReplaceAll(s, "_ISNULL", "{{$nulls}}")
	s = strings.ReplaceAll(s, "_HANDLES_NULLS", "{{if $nulls}}WithNulls{{else}}{{end}}")

	assignLtRe := makeFunctionRegex("_ASSIGN_LT", 6)
	s = assignLtRe.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 6))

	s = replaceManipulationFuncs(s)

	// Now, generate the op, from the template.
	tmpl, err := template.New("sort_op").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sortOverloads)
}

const quickSortTmpl = "pkg/sql/colexec/quicksort_tmpl.go"

func genQuickSortOps(wr io.Writer) error {
	d, err := ioutil.ReadFile(quickSortTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.ReplaceAll(s, "_TYPE", "{{.VecMethod}}")
	s = strings.ReplaceAll(s, "_DIR", "{{$dir}}")
	s = strings.ReplaceAll(s, "_HANDLES_NULLS", "{{if $nulls}}WithNulls{{else}}{{end}}")

	// Now, generate the op, from the template.
	tmpl, err := template.New("quicksort").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sortOverloads)
}

func init() {
	registerGenerator(genSortOps, "sort.eg.go", sortOpsTmpl)
	registerGenerator(genQuickSortOps, "quicksort.eg.go", quickSortTmpl)
	for _, nulls := range []bool{true, false} {
		nullsOverload := &sortDirNullsOverload{
			Nulls: nulls,
			DirOverloads: []*sortDirOverload{
				{
					Dir:             "execinfrapb.Ordering_Column_ASC",
					DirString:       "Asc",
					FamilyOverloads: sameTypeComparisonOpToOverloads[tree.LT],
				},
				{
					Dir:             "execinfrapb.Ordering_Column_DESC",
					DirString:       "Desc",
					FamilyOverloads: sameTypeComparisonOpToOverloads[tree.GT],
				},
			},
		}
		sortOverloads = append(sortOverloads, nullsOverload)
	}
}
