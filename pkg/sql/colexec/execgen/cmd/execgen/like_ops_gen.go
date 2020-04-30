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

func genLikeOps(wr io.Writer) error {
	tmpl, err := getSelectionOpsTmpl()
	if err != nil {
		return err
	}
	projTemplate, err := getProjConstOpTmplString(false /* isConstLeft */)
	if err != nil {
		return err
	}
	tmpl, err = tmpl.Funcs(template.FuncMap{"buildDict": buildDict}).Parse(projTemplate)
	if err != nil {
		return err
	}
	tmpl, err = tmpl.Parse(likeTemplate)
	if err != nil {
		return err
	}
	bytesRepresentation := toPhysicalRepresentation(types.BytesFamily, anyWidth)
	makeOverload := func(name string, rightGoType string, assignFunc func(target, l, r string) string) *twoArgsResolvedOverload {
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
			AssignFunc: func(_ *lastArgWidthOverload, target, l, r string) string {
				return assignFunc(target, l, r)
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
		makeOverload("Prefix", bytesRepresentation, func(target, l, r string) string {
			return fmt.Sprintf("%s = bytes.HasPrefix(%s, %s)", target, l, r)
		}),
		makeOverload("Suffix", bytesRepresentation, func(target, l, r string) string {
			return fmt.Sprintf("%s = bytes.HasSuffix(%s, %s)", target, l, r)
		}),
		makeOverload("Regexp", "*regexp.Regexp", func(target, l, r string) string {
			return fmt.Sprintf("%s = %s.Match(%s)", target, r, l)
		}),
		makeOverload("NotPrefix", bytesRepresentation, func(target, l, r string) string {
			return fmt.Sprintf("%s = !bytes.HasPrefix(%s, %s)", target, l, r)
		}),
		makeOverload("NotSuffix", bytesRepresentation, func(target, l, r string) string {
			return fmt.Sprintf("%s = !bytes.HasSuffix(%s, %s)", target, l, r)
		}),
		makeOverload("NotRegexp", "*regexp.Regexp", func(target, l, r string) string {
			return fmt.Sprintf("%s = !%s.Match(%s)", target, r, l)
		}),
	}
	return tmpl.Execute(wr, overloads)
}

func init() {
	registerGenerator(genLikeOps, "like_ops.eg.go", selectionOpsTmpl)
}
