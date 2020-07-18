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
	"strings"
	"text/template"
)

type andOrTmplInfo struct {
	Lower string
	Title string

	IsOr              bool
	LeftIsNullVector  bool
	RightIsNullVector bool
}

const andOrProjTmpl = "pkg/sql/colexec/and_or_projection_tmpl.go"

func genAndOrProjectionOps(inputFileContents string, wr io.Writer) error {

	r := strings.NewReplacer(
		"_OPERATION", "{{$Operation}}",
		"_OP_LOWER", "{{.Lower}}",
		"_OP_TITLE", "{{.Title}}",
		"_IS_OR_OP", ".IsOr",
		"_L_IS_NULL_VECTOR", ".LeftIsNullVector",
		"_R_IS_NULL_VECTOR", ".RightIsNullVector",
		"_L_HAS_NULLS", "$.lHasNulls",
		"_R_HAS_NULLS", "$.rHasNulls",
	)
	s := r.Replace(inputFileContents)

	addTupleForRight := makeFunctionRegex("_ADD_TUPLE_FOR_RIGHT", 1)
	s = addTupleForRight.ReplaceAllString(s, `{{template "addTupleForRight" buildDict "Global" $ "lHasNulls" $1}}`)
	setValues := makeFunctionRegex("_SET_VALUES", 5)
	s = setValues.ReplaceAllString(s, `{{template "setValues" buildDict "Global" $ "IsOr" $1 "LeftIsNullVector" $2 "RightIsNullVector" $3 "lHasNulls" $4 "rHasNulls" $5}}`)
	setSingleValue := makeFunctionRegex("_SET_SINGLE_VALUE", 5)
	s = setSingleValue.ReplaceAllString(s, `{{template "setSingleValue" buildDict "Global" $ "IsOr" $1 "LeftIsNullVector" $2 "RightIsNullVector" $3 "lHasNulls" $4 "rHasNulls" $5}}`)

	tmpl, err := template.New("and").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	var tmplInfos []andOrTmplInfo
	for _, opTitle := range []string{"And", "Or"} {
		for _, leftIsNullVector := range []bool{false, true} {
			for _, rightIsNullVector := range []bool{false, true} {
				if leftIsNullVector && rightIsNullVector {
					// Such case should be handled by the optimizer.
					continue
				}
				suffix := ""
				if leftIsNullVector {
					suffix += "LeftNull"
				}
				if rightIsNullVector {
					suffix += "RightNull"
				}
				tmplInfos = append(tmplInfos, andOrTmplInfo{
					Lower:             strings.ToLower(opTitle) + suffix,
					Title:             opTitle + suffix,
					IsOr:              strings.ToLower(opTitle) == "or",
					LeftIsNullVector:  leftIsNullVector,
					RightIsNullVector: rightIsNullVector,
				})
			}
		}
	}
	return tmpl.Execute(wr, struct {
		Operations []string
		Overloads  []andOrTmplInfo
	}{
		Operations: []string{"And", "Or"},
		Overloads:  tmplInfos,
	})
}

func init() {
	registerGenerator(genAndOrProjectionOps, "and_or_projection.eg.go", andOrProjTmpl)
}
