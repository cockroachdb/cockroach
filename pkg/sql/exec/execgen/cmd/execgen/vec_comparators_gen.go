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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func genVecComparators(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/exec/vec_comparators.template")
	if err != nil {
		return err
	}
	tmpl, err := template.New("vec_comparators").Parse(string(d))
	if err != nil {
		return err
	}
	ltOverloads := make([]*overload, 0)
	for _, overload := range comparisonOpOverloads {
		if overload.CmpOp == tree.LT {
			ltOverloads = append(ltOverloads, overload)
		}
	}
	return tmpl.Execute(wr, ltOverloads)
}

func init() {
	registerGenerator(genVecComparators, "vec_comparators.eg.go")
}
