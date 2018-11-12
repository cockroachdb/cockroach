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

const selTemplate = `
package exec

import (
	"bytes"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

{{define "opConstName"}}sel{{.Name}}{{.LTyp}}{{.RTyp}}ConstOp{{end}}
{{define "opName"}}sel{{.Name}}{{.LTyp}}{{.RTyp}}Op{{end}}

{{/* The outer range is a types.T, and the inner is the overloads associated
     with that type. */}}
{{range .}}
{{range .}}

type {{template "opConstName" .}} struct {
	input Operator

	colIdx   int
	constArg {{.RGoType}}
}

func (p *{{template "opConstName" .}}) Next() ColBatch {
	for {
		batch := p.input.Next()
		if batch.Length() == 0 {
			return batch
		}

		col := batch.ColVec(p.colIdx).{{.LTyp}}()[:ColBatchSize]
		var idx uint16
		n := batch.Length()
		if sel := batch.Selection(); sel != nil {
			sel := sel[:n]
			for _, i := range sel {
				var cmp bool
				{{(.Assign "cmp" "col[i]" "p.constArg")}}
				if cmp {
					sel[idx] = i
					idx++
				}
			}
		} else {
			batch.SetSelection(true)
			sel := batch.Selection()
			for i := uint16(0); i < n; i++ {
				var cmp bool
				{{(.Assign "cmp" "col[i]" "p.constArg")}}
				if cmp {
					sel[idx] = i
					idx++
				}
			}
		}
		if idx > 0 {
			batch.SetLength(idx)
			return batch
		}
	}
}

func (p {{template "opConstName" .}}) Init() {
	p.input.Init()
}

type {{template "opName" .}} struct {
	input Operator

	col1Idx int
	col2Idx int
}

func (p *{{template "opName" .}}) Next() ColBatch {
	for {
		batch := p.input.Next()
		if batch.Length() == 0 {
			return batch
		}

		col1 := batch.ColVec(p.col1Idx).{{.LTyp}}()[:ColBatchSize]
		col2 := batch.ColVec(p.col2Idx).{{.RTyp}}()[:ColBatchSize]
		n := batch.Length()

		var idx uint16
		if sel := batch.Selection(); sel != nil {
			sel := sel[:n]
			for _, i := range sel {
				var cmp bool
				{{(.Assign "cmp" "col1[i]" "col2[i]")}}
				if cmp {
					sel[idx] = i
					idx++
				}
			}
		} else {
			batch.SetSelection(true)
			sel := batch.Selection()
			for i := uint16(0); i < n; i++ {
				var cmp bool
				{{(.Assign "cmp" "col1[i]" "col2[i]")}}
				if cmp {
					sel[idx] = i
					idx++
				}
			}
		}
		if idx > 0 {
			batch.SetLength(idx)
			return batch
		}
	}
}

func (p {{template "opName" .}}) Init() {
	p.input.Init()
}

{{end}}
{{end}}

// GetSelectionConstOperator returns the appropriate constant selection operator
// for the given column type and comparison.
func GetSelectionConstOperator(
	ct sqlbase.ColumnType,
	cmpOp tree.ComparisonOperator,
	input Operator,
	colIdx int,
	constArg tree.Datum,
) (Operator, error) {
	c, err := types.GetDatumToPhysicalFn(ct)(constArg)
	if err != nil {
		return nil, err
	}
	switch t := types.FromColumnType(ct); t {
	{{range $typ, $overloads := .}}
	case types.{{$typ}}:
		switch cmpOp {
		{{range $overloads}}
		case tree.{{.Name}}:
			return &{{template "opConstName" .}}{
				input:    input,
				colIdx:   colIdx,
				constArg: c.({{.RGoType}}),
			}, nil
		{{end}}
		default:
			return nil, errors.Errorf("unhandled comparison operator: %s", cmpOp)
		}
	{{end}}
	default:
		return nil, errors.Errorf("unhandled type: %s", t)
	}
}

// GetSelectionOperator returns the appropriate two column selection operator
// for the given column type and comparison.
func GetSelectionOperator(
	ct sqlbase.ColumnType,
	cmpOp tree.ComparisonOperator,
	input Operator,
	col1Idx int,
	col2Idx int,
) (Operator, error) {
	switch t := types.FromColumnType(ct); t {
	{{range $typ, $overloads := .}}
	case types.{{$typ}}:
		switch cmpOp {
		{{range $overloads}}
		case tree.{{.Name}}:
			return &{{template "opName" .}}{
				input:   input,
				col1Idx: col1Idx,
				col2Idx: col2Idx,
			}, nil
		{{end}}
		default:
			return nil, errors.Errorf("unhandled comparison operator: %s", cmpOp)
		}
	{{end}}
	default:
		return nil, errors.Errorf("unhandled type: %s", t)
	}
}
`

func genSelectionOps(wr io.Writer) error {
	typToOverloads := make(map[types.T][]*overload)
	for _, overload := range comparisonOpOverloads {
		typ := overload.LTyp
		typToOverloads[typ] = append(typToOverloads[typ], overload)
	}
	tmpl, err := template.New("selection_ops").Parse(selTemplate)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, typToOverloads)
}

func init() {
	registerGenerator(genSelectionOps, "selection_ops.og.go")
}
