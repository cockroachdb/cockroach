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
	"fmt"
	"io"
	"io/ioutil"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// likeTemplate depends on the selConstOp template from selection_ops_gen. We
// handle LIKE operators separately from the other selection operators because
// there are several different implementations which may be chosen depending on
// the complexity of the LIKE pattern.
const likeTemplate = `
package colexec

import (
	"bytes"
	"context"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
)

{{range .}}
{{template "selConstOp" .}}
{{template "projConstOp" .}}
{{end}}
`

func genLikeOps(inputFileContents string, wr io.Writer) error {
	tmpl, err := getSelectionOpsTmpl(inputFileContents)
	if err != nil {
		return err
	}
	projConstFile, err := ioutil.ReadFile(projConstOpsTmpl)
	if err != nil {
		return err
	}
	projTemplate := replaceProjConstTmplVariables(string(projConstFile), false /* isConstLeft */)
	tmpl, err = tmpl.Funcs(template.FuncMap{"buildDict": buildDict}).Parse(projTemplate)
	if err != nil {
		return err
	}
	tmpl, err = tmpl.Parse(likeTemplate)
	if err != nil {
		return err
	}
	bytesRepresentation := toPhysicalRepresentation(types.BytesFamily, anyWidth)
	makeOverload := func(name string, rightGoType string, assignFunc func(targetElem, leftElem, rightElem string) string) *twoArgsResolvedOverload {
		base := &overloadBase{
			Name: name,
		}
		leftTypeOverload := &argTypeOverload{
			overloadBase:        base,
			argTypeOverloadBase: newArgTypeOverloadBase(types.BytesFamily),
		}
		leftWidthOverload := &argWidthOverload{
			argTypeOverload: leftTypeOverload,
			argWidthOverloadBase: &argWidthOverloadBase{
				argTypeOverloadBase: leftTypeOverload.argTypeOverloadBase,
				Width:               anyWidth,
				VecMethod:           toVecMethod(types.BytesFamily, anyWidth),
				GoType:              bytesRepresentation,
			},
		}
		rightTypeOverload := &lastArgTypeOverload{
			overloadBase:        base,
			argTypeOverloadBase: leftTypeOverload.argTypeOverloadBase,
			WidthOverloads:      make([]*lastArgWidthOverload, 1),
		}
		rightWidthOverload := &lastArgWidthOverload{
			lastArgTypeOverload: rightTypeOverload,
			argWidthOverloadBase: &argWidthOverloadBase{
				argTypeOverloadBase: rightTypeOverload.argTypeOverloadBase,
				Width:               anyWidth,
				VecMethod:           toVecMethod(types.BytesFamily, anyWidth),
				GoType:              rightGoType,
			},
			RetType:      types.Bool,
			RetVecMethod: toVecMethod(types.BoolFamily, anyWidth),
			RetGoType:    toPhysicalRepresentation(types.BoolFamily, anyWidth),
			AssignFunc: func(_ *lastArgWidthOverload, targetElem, leftElem, rightElem, _, _, _ string) string {
				return assignFunc(targetElem, leftElem, rightElem)
			},
		}
		rightTypeOverload.WidthOverloads[0] = rightWidthOverload
		return &twoArgsResolvedOverload{
			overloadBase: base,
			Left:         leftWidthOverload,
			Right:        rightWidthOverload,
		}
	}
	overloads := []*twoArgsResolvedOverload{
		makeOverload("Prefix", bytesRepresentation, func(targetElem, leftElem, rightElem string) string {
			return fmt.Sprintf("%s = bytes.HasPrefix(%s, %s)", targetElem, leftElem, rightElem)
		}),
		makeOverload("Suffix", bytesRepresentation, func(targetElem, leftElem, rightElem string) string {
			return fmt.Sprintf("%s = bytes.HasSuffix(%s, %s)", targetElem, leftElem, rightElem)
		}),
		makeOverload("Regexp", "*regexp.Regexp", func(targetElem, leftElem, rightElem string) string {
			return fmt.Sprintf("%s = %s.Match(%s)", targetElem, rightElem, leftElem)
		}),
		makeOverload("NotPrefix", bytesRepresentation, func(targetElem, leftElem, rightElem string) string {
			return fmt.Sprintf("%s = !bytes.HasPrefix(%s, %s)", targetElem, leftElem, rightElem)
		}),
		makeOverload("NotSuffix", bytesRepresentation, func(targetElem, leftElem, rightElem string) string {
			return fmt.Sprintf("%s = !bytes.HasSuffix(%s, %s)", targetElem, leftElem, rightElem)
		}),
		makeOverload("NotRegexp", "*regexp.Regexp", func(targetElem, leftElem, rightElem string) string {
			return fmt.Sprintf("%s = !%s.Match(%s)", targetElem, rightElem, leftElem)
		}),
	}
	return tmpl.Execute(wr, overloads)
}

func init() {
	registerGenerator(genLikeOps, "like_ops.eg.go", selectionOpsTmpl)
}
