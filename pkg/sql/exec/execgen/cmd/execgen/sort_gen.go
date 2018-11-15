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
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

const sorterTemplate = `
package exec

import (
	"bytes"
	"fmt"
	
  "github.com/cockroachdb/cockroach/pkg/sql/exec/types"
  "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (s *sorter) Less(i, j int) bool {
  switch s.spec.inputTypes[s.spec.sortCol] {
  {{range .}}
    case types.{{.ExecType}}:
      {{if eq .ExecType "Bool"}}
        return (s.values[s.spec.sortCol].{{.ExecType}}()[i] == false) &&
               (s.values[s.spec.sortCol].{{.ExecType}}()[j] == true)
      {{else if eq .ExecType "Bytes"}}
        return bytes.Compare(s.values[s.spec.sortCol].{{.ExecType}}()[i],
                       s.values[s.spec.sortCol].{{.ExecType}}()[j],
                      ) > 0
      {{else if eq .ExecType "Decimal"}}
		    return tree.CompareDecimals(&s.values[s.spec.sortCol].{{.ExecType}}()[i],
			    &s.values[s.spec.sortCol].{{.ExecType}}()[j]) > 0
      {{else}}
	    return s.values[s.spec.sortCol].{{.ExecType}}()[i] < s.values[s.spec.sortCol].{{.ExecType}}()[j] 
      {{end}}
  {{end}}
    default:
      panic(fmt.Sprintf("unhandled type %d", s.spec.inputTypes[s.spec.sortCol]))
  }
}

func (s *sorter) Swap(i, j int) {
	// Swap needs to swap the values in the column being sorted, as otherwise
	// subsequent calls to Less would be incorrect.
	// We also store the swap order in s.order to swap all the other columns.
  switch s.spec.inputTypes[s.spec.sortCol] {
  {{range .}}
    case types.{{.ExecType}}:
	s.order[i], s.order[j] = s.order[j], s.order[i]
	s.values[s.spec.sortCol].{{.ExecType}}()[i],
s.values[s.spec.sortCol].{{.ExecType}}()[j] =
	s.values[s.spec.sortCol].{{.ExecType}}()[j],
s.values[s.spec.sortCol].{{.ExecType}}()[i]


  {{end}}
    default:
      panic(fmt.Sprintf("unhandled type %d", s.spec.inputTypes[s.spec.sortCol]))
  }
}

func (s *sorter) fill(outVec, inVec ColVec, orderVec []int, start, end int) {
  switch s.spec.inputTypes[s.spec.sortCol] {
  {{range .}}
  case types.{{.ExecType}}:
	  outputVec := outVec.{{.ExecType}}()
	  inputVec := inVec.{{.ExecType}}()
	  for i := start; i < end; i++ {
		  outputVec[i] = inputVec[orderVec[i]]
	  }
  {{end}}
    default:
      panic(fmt.Sprintf("unhandled type %d", s.spec.inputTypes[s.spec.sortCol]))
  }
}

`

type sorterGen struct {
	ExecType string
	GoType   string
}

func genSorterLess(wr io.Writer) error {
	// Build list of all exec types.
	var gens []sorterGen
	for _, t := range types.AllTypes {
		gen := sorterGen{
			ExecType: t.String(),
			GoType:   t.GoTypeName(),
		}
		gens = append(gens, gen)
	}

	tmpl, err := template.New("sorterTemplate").Parse(sorterTemplate)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, gens)
}

func init() {
	registerGenerator(genSorterLess, "sorter.eg.go")
}
