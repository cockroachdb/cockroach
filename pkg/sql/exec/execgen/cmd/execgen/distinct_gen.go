// Copyright 2018 The Cockroach Authors.
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
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

func genDistinctOps(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/exec/distinct_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.Replace(s, "_GOTYPE", "{{.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "types.{{.}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.}}", -1)
	s = strings.Replace(s, "_EQUALITY_FN", "{{.EqualityFunction}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.}}", -1)

	// Now, generate the op, from the template.
	tmpl, err := template.New("distinct_op").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, []types.T{types.Bool, types.Bytes,
		types.Int8, types.Int16, types.Int32, types.Int64, types.Float32, types.Float64})
}
func init() {
	registerGenerator(genDistinctOps, "distinct.og.go")
}
