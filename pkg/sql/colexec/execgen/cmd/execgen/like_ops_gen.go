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

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
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
	overloads := []overload{
		{
			Name:    "Prefix",
			LTyp:    coltypes.Bytes,
			RTyp:    coltypes.Bytes,
			RGoType: "[]byte",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = bytes.HasPrefix(%s, %s)", target, l, r)
			},
		},
		{
			Name:    "Suffix",
			LTyp:    coltypes.Bytes,
			RTyp:    coltypes.Bytes,
			RGoType: "[]byte",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = bytes.HasSuffix(%s, %s)", target, l, r)
			},
		},
		{
			Name:    "Regexp",
			LTyp:    coltypes.Bytes,
			RTyp:    coltypes.Bytes,
			RGoType: "*regexp.Regexp",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = %s.Match(%s)", target, r, l)
			},
		},
		{
			Name:    "NotPrefix",
			LTyp:    coltypes.Bytes,
			RTyp:    coltypes.Bytes,
			RGoType: "[]byte",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = !bytes.HasPrefix(%s, %s)", target, l, r)
			},
		},
		{
			Name:    "NotSuffix",
			LTyp:    coltypes.Bytes,
			RTyp:    coltypes.Bytes,
			RGoType: "[]byte",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = !bytes.HasSuffix(%s, %s)", target, l, r)
			},
		},
		{
			Name:    "NotRegexp",
			LTyp:    coltypes.Bytes,
			RTyp:    coltypes.Bytes,
			RGoType: "*regexp.Regexp",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = !%s.Match(%s)", target, r, l)
			},
		},
	}
	return tmpl.Execute(wr, overloads)
}

func init() {
	registerGenerator(genLikeOps, "like_ops.eg.go")
}
