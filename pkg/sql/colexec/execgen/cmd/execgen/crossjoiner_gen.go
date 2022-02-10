// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
)

const crossJoinerTmpl = "pkg/sql/colexec/colexecjoin/crossjoiner_tmpl.go"

func genCrossJoiner(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("crossjoiner").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[treecmp.EQ])
}

func init() {
	registerGenerator(genCrossJoiner, "crossjoiner.eg.go", crossJoinerTmpl)
}
