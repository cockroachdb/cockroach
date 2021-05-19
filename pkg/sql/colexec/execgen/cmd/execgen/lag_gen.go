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

const lagTmpl = "pkg/sql/colexec/colexecwindow/lead_lag_tmpl.go"

func genLagOp(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_GOTYPESLICE", "{{.GoTypeSliceName}}",
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
		"_OP_NAME", "lag",
		"_UPPERCASE_NAME", "Lag",
	)
	s := r.Replace(inputFileContents)

	processBatch := makeFunctionRegex("_PROCESS_BATCH", 2)
	s = processBatch.ReplaceAllString(s, `{{template "processBatchTmpl" buildDict "Global" . "IsBytesLike" .IsBytesLike "OffsetHasNulls" $1 "DefaultHasNulls" $2}}`)

	s = replaceManipulationFuncs(s)

	// Now, generate the op, from the template.
	tmpl, err := template.New("lag_ops").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}

func init() {
	registerGenerator(genLagOp, "lag.eg.go", lagTmpl)
}
