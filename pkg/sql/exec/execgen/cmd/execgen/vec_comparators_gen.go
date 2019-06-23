// Copyright 2019 The Cockroach Authors.
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

func genVecComparators(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/exec/vec_comparators_tmpl.go")
	if err != nil {
		return err
	}
	s := string(d)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_GOTYPE", "{{.LGoType}}", -1)
	compareRe := regexp.MustCompile(`_COMPARE\((.*),(.*),(.*)\)`)
	s = compareRe.ReplaceAllString(s, "{{.Compare $1 $2 $3}}")

	tmpl, err := template.New("vec_comparators").Parse(s)
	if err != nil {
		return err
	}

	ltOverloads := make([]*overload, 0)
	for _, overload := range comparisonOpOverloads {
		if overload.CmpOp == tree.LT {
			ltOverloads = append(ltOverloads, overload)
		}
	}
	return tmpl.Execute(wr, ltOverloads)
}

func init() {
	registerGenerator(genVecComparators, "vec_comparators.eg.go")
}
