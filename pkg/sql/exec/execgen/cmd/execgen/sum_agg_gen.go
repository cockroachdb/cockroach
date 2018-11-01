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

func genSumAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/exec/sum_agg_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_GOTYPE", "{{.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "types.{{.}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.}}", -1)

	tmpl, err := template.New("sum_agg").Parse(s)
	if err != nil {
		return err
	}

	// TODO(asubiotto): Only execute on types that have an addition.
	// TODO(asubiotto): Support decimal.
	return tmpl.Execute(
		wr,
		[]types.T{
			types.Int8, types.Int16, types.Int32, types.Int64, types.Float32, types.Float64,
		},
	)
}

func init() {
	registerGenerator(genSumAgg, "sum_agg.og.go")
}
