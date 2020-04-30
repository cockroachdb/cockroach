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

const anyNotNullAggTmpl = "pkg/sql/colexec/any_not_null_agg_tmpl.go"

func genAnyNotNullAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile(anyNotNullAggTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.ReplaceAll(s, "_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}")
	s = strings.ReplaceAll(s, "_TYPE_WIDTH", typeWidthReplacement)
	s = strings.ReplaceAll(s, "_GOTYPESLICE", "{{.GoTypeSliceName}}")
	s = strings.ReplaceAll(s, "_GOTYPE", "{{.GoType}}")
	s = strings.ReplaceAll(s, "_TYPE", "{{.VecMethod}}")
	s = strings.ReplaceAll(s, "TemplateType", "{{.VecMethod}}")

	findAnyNotNull := makeFunctionRegex("_FIND_ANY_NOT_NULL", 4)
	s = findAnyNotNull.ReplaceAllString(s, `{{template "findAnyNotNull" buildDict "Global" . "HasNulls" $4}}`)

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("any_not_null_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}

func init() {
	registerGenerator(genAnyNotNullAgg, "any_not_null_agg.eg.go", anyNotNullAggTmpl)
}
