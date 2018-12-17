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

func genAnyNotNullAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/exec/any_not_null_agg_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_GOTYPE", "{{.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "types.{{.}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.}}", -1)

	tmpl, err := template.New("any_not_null_agg").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, types.AllTypes)
}

func init() {
	registerGenerator(genAnyNotNullAgg, "any_not_null_agg.eg.go")
}
