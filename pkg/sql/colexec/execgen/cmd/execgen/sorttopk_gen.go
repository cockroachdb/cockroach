// Copyright 2021 The Cockroach Authors.
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

const sortTopKTmpl = "pkg/sql/colexec/sorttopk_tmpl.go"

func genSortTopK(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(

		"_PARTIAL_ORDER", "{{.PartialOrder}}",
	)
	s := r.Replace(inputFileContents)

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("sorttopk").Parse(s)
	if err != nil {
		return err
	}

	// It doesn't matter that we're passing in all overloads of Equality
	// comparison operator - we simply need to iterate over all supported
	// types.
	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}

func init() {
	/*
			topKSortOp := `
		{{template "topKSortOpConstructor" .}}
		`
	*/
	registerGenerator(genSortTopK, "sorttopk.eg.go", sortTopKTmpl)
}
