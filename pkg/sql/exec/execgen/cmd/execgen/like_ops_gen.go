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

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// likeTemplate depends on the selConstOp template from selection_ops_gen. We
// handle LIKE operators separately from the other selection operators because
// there are several different implementations which may be chosen depending on
// the complexity of the LIKE pattern.
const likeTemplate = `
package exec

import (
	"bytes"
  "context"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

{{range .}}
{{template "selConstOp" .}}
{{template "projRConstOp" .}}
{{end}}
`

func genLikeOps(wr io.Writer) error {
	tmpl := template.New("like_sel_ops").Funcs(template.FuncMap{"buildDict": buildDict})
	var err error
	tmpl, err = tmpl.Parse(selTemplate)
	if err != nil {
		return err
	}
	tmpl, err = tmpl.Parse(projTemplate)
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
			LTyp:    types.Bytes,
			RTyp:    types.Bytes,
			RGoType: "[]byte",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = bytes.HasPrefix(%s, %s)", target, l, r)
			},
		},
		{
			Name:    "Suffix",
			LTyp:    types.Bytes,
			RTyp:    types.Bytes,
			RGoType: "[]byte",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = bytes.HasSuffix(%s, %s)", target, l, r)
			},
		},
		{
			Name:    "Regexp",
			LTyp:    types.Bytes,
			RTyp:    types.Bytes,
			RGoType: "*regexp.Regexp",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = %s.Match(%s)", target, r, l)
			},
		},
		{
			Name:    "NotPrefix",
			LTyp:    types.Bytes,
			RTyp:    types.Bytes,
			RGoType: "[]byte",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = !bytes.HasPrefix(%s, %s)", target, l, r)
			},
		},
		{
			Name:    "NotSuffix",
			LTyp:    types.Bytes,
			RTyp:    types.Bytes,
			RGoType: "[]byte",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = !bytes.HasSuffix(%s, %s)", target, l, r)
			},
		},
		{
			Name:    "NotRegexp",
			LTyp:    types.Bytes,
			RTyp:    types.Bytes,
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
