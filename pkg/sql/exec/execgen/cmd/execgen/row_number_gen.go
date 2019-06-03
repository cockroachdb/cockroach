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
	"io"
	"io/ioutil"
	"text/template"
)

func genRowNumberOp(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/exec/vecbuiltins/row_number_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)

	nextRowNumber := makeFunctionRegex("_NEXT_ROW_NUMBER_", 1)
	s = nextRowNumber.ReplaceAllString(s, `{{template "nextRowNumber" buildDict "Global" $ "HasPartition" $1 }}`)

	// Now, generate the op, from the template.
	tmpl, err := template.New("row_number_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, struct{}{})
}

func init() {
	registerGenerator(genRowNumberOp, "row_number.eg.go")
}
