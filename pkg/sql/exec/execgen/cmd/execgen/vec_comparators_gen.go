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
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const vecComparatorTemplate = `
package exec

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

{{range .}}
type {{.LTyp}}VecComparator struct {
	vecs  [][]{{.LGoType}}
	nulls []*coldata.Nulls
}

func (c *{{.LTyp}}VecComparator) compare(vecIdx1, vecIdx2 int, rowIdx1, rowIdx2 uint16) int {
	n1 := c.nulls[vecIdx1].HasNulls() && c.nulls[vecIdx1].NullAt(rowIdx1)
	n2 := c.nulls[vecIdx2].HasNulls() && c.nulls[vecIdx2].NullAt(rowIdx2)
	if n1 && n2 {
		return 0
	} else if n1 {
		return -1
	} else if n2 {
		return 1
	}
	left := c.vecs[vecIdx1][rowIdx1]
	right := c.vecs[vecIdx2][rowIdx2]
	var cmp int
	{{.Compare "cmp" "left" "right"}}
	return cmp
}

func (c *{{.LTyp}}VecComparator) setVec(idx int, vec coldata.Vec) {
	c.vecs[idx] = vec.{{.LTyp}}()
	c.nulls[idx] = vec.Nulls()
}
{{end}}

func GetVecComparator(t types.T, numVecs int) vecComparator {
	switch t {
	{{range . }}
	case types.{{.LTyp}}:
		return &{{.LTyp}}VecComparator{
			vecs:  make([][]{{.LGoType}}, numVecs),
			nulls: make([]*coldata.Nulls, numVecs),
		}
	{{end}}
	}
	panic(fmt.Sprintf("unhandled type %v", t))
}
`

func genVecComparators(wr io.Writer) error {
	ltOverloads := make([]*overload, 0)
	for _, overload := range comparisonOpOverloads {
		if overload.CmpOp == tree.LT {
			ltOverloads = append(ltOverloads, overload)
		}
	}
	tmpl := template.New("vec_comparators")
	var err error
	tmpl, err = tmpl.Parse(vecComparatorTemplate)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, ltOverloads)
}

func init() {
	registerGenerator(genVecComparators, "vec_comparators.eg.go")
}
