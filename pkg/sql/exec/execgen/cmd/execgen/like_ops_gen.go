// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
)

{{range .}}
{{template "selConstOp" .}}
{{end}}
`

func genLikeOps(wr io.Writer) error {
	tmpl := template.New("like_ops").Funcs(template.FuncMap{"buildDict": buildDict})
	var err error
	tmpl, err = tmpl.Parse(selTemplate)
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
