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

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type sortOverload struct {
	*overload
	Dir       string
	DirString string
}

type sortOverloads struct {
	LTyp      types.T
	Overloads []sortOverload
}

// sortOverloads maps type to distsqlpb.Ordering_Column_Direction to overload.
var typesToSortOverloads map[types.T]sortOverloads

func genSortOps(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/exec/sort_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)

	// Replace the template variables.
	s = strings.Replace(s, "_GOTYPE", "{{.LTyp.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "types.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_DIR_ENUM", "{{.Dir}}", -1)
	s = strings.Replace(s, "_DIR", "{{.DirString}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)

	assignLtRe := regexp.MustCompile(`_ASSIGN_LT\((.*),(.*),(.*)\)`)
	s = assignLtRe.ReplaceAllString(s, "{{.Assign $1 $2 $3}}")

	// Now, generate the op, from the template.
	tmpl, err := template.New("sort_op").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, typesToSortOverloads)
}

func genQuickSortOps(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/exec/quicksort_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)

	// Replace the template variables.
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_DIR", "{{.DirString}}", -1)

	// Now, generate the op, from the template.
	tmpl, err := template.New("quicksort").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, typesToSortOverloads)
}

func init() {
	registerGenerator(genSortOps, "sort.eg.go")
	registerGenerator(genQuickSortOps, "quicksort.eg.go")
	typesToSortOverloads = make(map[types.T]sortOverloads)
	for _, o := range comparisonOpToOverloads[tree.LT] {
		typesToSortOverloads[o.LTyp] = sortOverloads{
			LTyp: o.LTyp,
			Overloads: []sortOverload{
				{overload: o, Dir: "distsqlpb.Ordering_Column_ASC", DirString: "Asc"},
				{}},
		}
	}
	for _, o := range comparisonOpToOverloads[tree.GT] {
		typesToSortOverloads[o.LTyp].Overloads[1] = sortOverload{
			overload: o, Dir: "distsqlpb.Ordering_Column_DESC", DirString: "Desc"}
	}
}
