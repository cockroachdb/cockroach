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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"io"
	"strings"
	"text/template"
)

const selectInTmpl = "pkg/sql/colexec/select_in_tmpl.go"

func genSelectIn(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_GOTYPESLICE", "{{.GoTypeSliceName}}",
		"_GOTYPE", "{{.GoType}}",
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
	)
	s := r.Replace(inputFileContents)

	assignEq := makeFunctionRegex("_ASSIGN_CMP", 6)
	s = assignEq.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 6))

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("select_in").Parse(s)
	if err != nil {
		return err
	}

	tmplInfos := populateTwoArgsOverloads(
		&overloadBase{
			kind:  comparisonOverload,
			Name:  execgen.ComparisonOpName[tree.EQ],
			CmpOp: tree.EQ,
			OpStr: comparisonOpInfix[tree.EQ],
		},
		cmpOpOutputTypes,
		func(lawo *lastArgWidthOverload, customizer typeCustomizer) {
			if b, ok := customizer.(cmpOpTypeCustomizer); ok {
				lawo.AssignFunc = func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string {
					cmp := b.getCmpOpCompareFunc()(targetElem, leftElem, rightElem, leftCol, rightCol)
					if cmp == "" {
						return ""
					}
					args := map[string]string{"Cmp": cmp}
					buf := strings.Builder{}
					t := template.Must(template.New("").Parse(`
											{{.Cmp}}
									`))
					if err := t.Execute(&buf, args); err != nil {
						colexecerror.InternalError(err)
					}
					return buf.String()
				}
				lawo.CompareFunc = b.getCmpOpCompareFunc()
			}
		},
		typeCustomizers,
	)

	return tmpl.Execute(wr, tmplInfos)
}

func init() {
	registerGenerator(genSelectIn, "select_in.eg.go", selectInTmpl)
}
