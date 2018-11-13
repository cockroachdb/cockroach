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

const projTemplate = `
package exec

import (
	"bytes"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

{{define "opConstName"}}proj{{.Name}}{{.LTyp}}{{.RTyp}}ConstOp{{end}}
{{define "opName"}}proj{{.Name}}{{.LTyp}}{{.RTyp}}Op{{end}}

{{/* The outer range is a types.T, and the inner is the overloads associated
     with that type. */}}
{{range .}}
{{range .}}

type {{template "opConstName" .}} struct {
	input Operator

	colIdx   int
	constArg {{.RGoType}}

	outputIdx int
}

func (p *{{template "opConstName" .}}) Next() ColBatch {
	batch := p.input.Next()
	if p.outputIdx == len(batch.ColVecs()) {
		batch.AppendCol(types.{{.RetTyp}})
	}
	projCol := batch.ColVec(p.outputIdx).{{.RetTyp}}()[:ColBatchSize]
	col := batch.ColVec(p.colIdx).{{.LTyp}}()[:ColBatchSize]
	n := batch.Length()
	if sel := batch.Selection(); sel != nil {
		for _, i := range sel {
			{{(.Assign "projCol[i]" "col[i]" "p.constArg")}}
		}
	} else {
		col = col[:n]
		for i := range col {
			{{(.Assign "projCol[i]" "col[i]" "p.constArg")}}
		}
	}
	return batch
}

func (p {{template "opConstName" .}}) Init() {
	p.input.Init()
}

type {{template "opName" .}} struct {
	input Operator

	col1Idx int
	col2Idx int

	outputIdx int
}

func (p *{{template "opName" .}}) Next() ColBatch {
	batch := p.input.Next()
	if p.outputIdx == len(batch.ColVecs()) {
		batch.AppendCol(types.{{.RetTyp}})
	}
	projCol := batch.ColVec(p.outputIdx).{{.RetTyp}}()[:ColBatchSize]
	col1 := batch.ColVec(p.col1Idx).{{.LTyp}}()[:ColBatchSize]
	col2 := batch.ColVec(p.col2Idx).{{.RTyp}}()[:ColBatchSize]
	n := batch.Length()
	if sel := batch.Selection(); sel != nil {
		for _, i := range sel {
			{{(.Assign "projCol[i]" "col1[i]" "col2[i]")}}
		}
	} else {
		col1 = col1[:n]
		for i := range col1 {
			{{(.Assign "projCol[i]" "col1[i]" "col2[i]")}}
		}
	}
	return batch
}

func (p {{template "opName" .}}) Init() {
	p.input.Init()
}

{{end}}
{{end}}

// GetProjectionConstOperator returns the appropriate constant projection
// operator for the given column type and comparison.
func GetProjectionConstOperator(
	ct sqlbase.ColumnType,
	op tree.Operator,
	input Operator,
	colIdx int,
	constArg tree.Datum,
  outputIdx int,
) (Operator, error) {
	c, err := types.GetDatumToPhysicalFn(ct)(constArg)
	if err != nil {
		return nil, err
	}
	switch t := types.FromColumnType(ct); t {
	{{range $typ, $overloads := .}}
	case types.{{$typ}}:
		switch op.(type) {
		case tree.BinaryOperator:
			switch op {
			{{range $overloads}}
			{{if .IsBinOp}}
			case tree.{{.Name}}:
				return &{{template "opConstName" .}}{
					input:    input,
					colIdx:   colIdx,
					constArg: c.({{.RGoType}}),
					outputIdx: outputIdx,
				}, nil
			{{end}}
			{{end}}
			default:
				return nil, errors.Errorf("unhandled binary operator: %s", op)
			}
		case tree.ComparisonOperator:
			switch op {
			{{range $overloads}}
			{{if .IsCmpOp}}
			case tree.{{.Name}}:
				return &{{template "opConstName" .}}{
					input:    input,
					colIdx:   colIdx,
					constArg: c.({{.RGoType}}),
					outputIdx: outputIdx,
				}, nil
			{{end}}
			{{end}}
			default:
				return nil, errors.Errorf("unhandled comparison operator: %s", op)
			}
		default:
			return nil, errors.New("unhandled operator type")
		}
	{{end}}
	default:
		return nil, errors.Errorf("unhandled type: %s", t)
	}
}

// GetProjectionOperator returns the appropriate projection operator for the
// given column type and comparison.
func GetProjectionOperator(
	ct sqlbase.ColumnType,
	op tree.Operator,
	input Operator,
	col1Idx int,
	col2Idx int,
  outputIdx int,
) (Operator, error) {
	switch t := types.FromColumnType(ct); t {
	{{range $typ, $overloads := .}}
	case types.{{$typ}}:
		switch op.(type) {
		case tree.BinaryOperator:
			switch op {
			{{range $overloads}}
			{{if .IsBinOp}}
			case tree.{{.Name}}:
				return &{{template "opName" .}}{
					input:    input,
					col1Idx:   col1Idx,
					col2Idx:   col2Idx,
					outputIdx: outputIdx,
				}, nil
			{{end}}
			{{end}}
			default:
				return nil, errors.Errorf("unhandled binary operator: %s", op)
			}
		case tree.ComparisonOperator:
			switch op {
			{{range $overloads}}
			{{if .IsCmpOp}}
			case tree.{{.Name}}:
				return &{{template "opName" .}}{
					input:    input,
					col1Idx:   col1Idx,
					col2Idx:   col2Idx,
					outputIdx: outputIdx,
				}, nil
			{{end}}
			{{end}}
			default:
				return nil, errors.Errorf("unhandled comparison operator: %s", op)
			}
		default:
			return nil, errors.New("unhandled operator type")
		}
	{{end}}
	default:
		return nil, errors.Errorf("unhandled type: %s", t)
	}
}
`

func genProjectionOps(wr io.Writer) error {
	tmpl, err := template.New("projection_ops").Parse(projTemplate)
	if err != nil {
		return err
	}

	var allOverloads []*overload
	allOverloads = append(allOverloads, binaryOpOverloads...)
	allOverloads = append(allOverloads, comparisonOpOverloads...)

	typToOverloads := make(map[types.T][]*overload)
	for _, overload := range allOverloads {
		typ := overload.LTyp
		typToOverloads[typ] = append(typToOverloads[typ], overload)
	}
	return tmpl.Execute(wr, typToOverloads)
}

func init() {
	registerGenerator(genProjectionOps, "projection_ops.eg.go")
}
