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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func genColvec(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/exec/colvec_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)

	// Replace the template variables.
	s = strings.Replace(s, "_GOTYPE", "{{.LTyp.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "types.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)

	// Now, generate the op, from the template.
	tmpl, err := template.New("colvec_op").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, comparisonOpToOverloads[tree.NE])
}
func init() {
	registerGenerator(genColvec, "colvec.eg.go")
}
