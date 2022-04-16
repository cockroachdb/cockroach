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

// likeTemplate depends either on the selConstOp template from selection_ops_gen
// or projConstOp template from projection_ops_gen. We handle LIKE operators
// separately from the other selection and projection operators because there
// are several different implementations which may be chosen depending on the
// complexity of the LIKE pattern.
//
// likeTemplate needs to be used as a format string expecting exactly two %s
// arguments:
// [1]: is the package suffix (either "sel" or "projconst")
// [2]: is the type of the operator (either "sel" or "proj").
const likeTemplate = `
// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec%[1]s

import (
	"bytes"
	"context"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
)

{{range .}}
{{template "%[2]sConstOp" .}}
{{end}}
`

func genLikeOps(
	tmplGetter func(inputFileContents string) (*template.Template, error),
	// pkgSuffix is the suffix of the package for the operator to be generated
	// in (either "sel" or "projconst").
	pkgSuffix string,
	// opType is the type of the operator to be generated (either "sel" or
	// "proj").
	opType string,
) func(string, io.Writer) error {
	return func(inputFileContents string, wr io.Writer) error {
		tmpl, err := tmplGetter(inputFileContents)
		if err != nil {
			return err
		}
		tmpl, err = tmpl.Parse(fmt.Sprintf(likeTemplate, pkgSuffix, opType))
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
		// makeSkeletonAssignFunc returns a string that assigns 'targetElem' to
		// the result of evaluation 'leftElem' (LIKE | NOT LIKE) pattern where
		// pattern is of the form '%word1%word2%...%' where "words" come from
		// 'rightElem' (which is [][]byte).
		//
		// The logic for evaluating such expression is that for each word we
		// find its first occurrence in the unprocessed part of 'leftElem'. If
		// it is not found, then 'leftElem' doesn't match the pattern, if it is
		// found, then we advance 'leftElem' right past that first occurrence.
		makeSkeletonAssignFunc := func(targetElem, leftElem, rightElem, comparison string) string {
			return fmt.Sprintf(`
				{
					var idx, skeletonIdx int
					for skeletonIdx < len(%[3]s) {
						idx = bytes.Index(%[2]s, %[3]s[skeletonIdx])
						if idx < 0 {
							break
						}
						%[2]s = %[2]s[idx+len(%[3]s[skeletonIdx]):]
						skeletonIdx++
					}
					%[1]s = skeletonIdx %[4]s len(%[3]s)
				}`, targetElem, leftElem, rightElem, comparison)
		}
		overloads := []*twoArgsResolvedOverload{
			makeOverload("Prefix", bytesRepresentation, func(targetElem, leftElem, rightElem string) string {
				return fmt.Sprintf("%s = bytes.HasPrefix(%s, %s)", targetElem, leftElem, rightElem)
			}),
			makeOverload("Suffix", bytesRepresentation, func(targetElem, leftElem, rightElem string) string {
				return fmt.Sprintf("%s = bytes.HasSuffix(%s, %s)", targetElem, leftElem, rightElem)
			}),
			makeOverload("Contains", bytesRepresentation, func(targetElem, leftElem, rightElem string) string {
				return fmt.Sprintf("%s = bytes.Contains(%s, %s)", targetElem, leftElem, rightElem)
			}),
			makeOverload("Skeleton", "[][]byte", func(targetElem, leftElem, rightElem string) string {
				return makeSkeletonAssignFunc(targetElem, leftElem, rightElem, "==")
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
			makeOverload("NotContains", bytesRepresentation, func(targetElem, leftElem, rightElem string) string {
				return fmt.Sprintf("%s = !bytes.Contains(%s, %s)", targetElem, leftElem, rightElem)
			}),
			makeOverload("NotSkeleton", "[][]byte", func(targetElem, leftElem, rightElem string) string {
				return makeSkeletonAssignFunc(targetElem, leftElem, rightElem, "!=")
			}),
			makeOverload("NotRegexp", "*regexp.Regexp", func(targetElem, leftElem, rightElem string) string {
				return fmt.Sprintf("%s = !%s.Match(%s)", targetElem, rightElem, leftElem)
			}),
		}
		return tmpl.Execute(wr, overloads)
	}
}

func init() {
	getProjectionOpsTmpl := func(inputFileContents string) (*template.Template, error) {
		projTemplate := replaceProjConstTmplVariables(inputFileContents, false /* isConstLeft */)
		return template.New("proj_like_ops").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(projTemplate)
	}
	registerGenerator(genLikeOps(getProjectionOpsTmpl, "projconst", "proj"), "proj_like_ops.eg.go", projConstOpsTmpl)
	registerGenerator(genLikeOps(getSelectionOpsTmpl, "sel", "sel"), "sel_like_ops.eg.go", selectionOpsTmpl)
}
