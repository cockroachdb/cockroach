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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func genDistinctOps(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/exec/distinct_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)

	// Replace the template variables.
	s = strings.Replace(s, "_GOTYPE", "{{.LTyp.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "types.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)

	assignNeRe := regexp.MustCompile(`_ASSIGN_NE\((.*),(.*),(.*)\)`)
	s = assignNeRe.ReplaceAllString(s, `{{.Assign "$1" "$2" "$3"}}`)

	innerLoopRe := regexp.MustCompile(`_CHECK_DISTINCT\(.*\)`)
	s = innerLoopRe.ReplaceAllString(s, `{{template "checkDistinct" .}}`)

	// Now, generate the op, from the template.
	tmpl, err := template.New("distinct_op").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, comparisonOpToOverloads[tree.NE])
}
func init() {
	registerGenerator(genDistinctOps, "distinct.eg.go")
}
