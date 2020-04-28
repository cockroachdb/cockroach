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

const vecTmpl = "pkg/col/coldata/vec_tmpl.go"

func genVec(wr io.Writer) error {
	d, err := ioutil.ReadFile(vecTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.ReplaceAll(s, "_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}")
	s = strings.ReplaceAll(s, "_TYPE_WIDTH", typeWidthReplacement)
	s = strings.ReplaceAll(s, "_GOTYPE", "{{.GoType}}")
	s = strings.ReplaceAll(s, "TemplateType", "{{.VecMethod}}")

	copyWithSel := makeFunctionRegex("_COPY_WITH_SEL", 6)
	s = copyWithSel.ReplaceAllString(s, `{{template "copyWithSel" buildDict "Global" . "SelOnDest" $6}}`)

	s = replaceManipulationFuncs(s)

	// Now, generate the op, from the template.
	tmpl, err := template.New("vec_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	// It doesn't matter that we're passing in all overloads of Equality
	// comparison operator - we simply need to iterate over all supported
	// types.
	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}
func init() {
	registerGenerator(genVec, "vec.eg.go", vecTmpl)
}
