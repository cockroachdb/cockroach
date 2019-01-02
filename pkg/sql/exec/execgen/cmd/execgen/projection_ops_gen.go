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

{{define "opRConstName"}}proj{{.Name}}{{.LTyp}}{{.RTyp}}ConstOp{{end}}
{{define "opLConstName"}}proj{{.Name}}{{.LTyp}}Const{{.RTyp}}Op{{end}}
{{define "opName"}}proj{{.Name}}{{.LTyp}}{{.RTyp}}Op{{end}}

{{/* The outer range is a types.T, and the inner is the overloads associated
     with that type. */}}
{{range .TypToOverloads}}
{{range .}}

type {{template "opRConstName" .}} struct {
	input Operator

	colIdx   int
	constArg {{.RGoType}}

	outputIdx int
}

func (p *{{template "opRConstName" .}}) Next() ColBatch {
	batch := p.input.Next()
	if p.outputIdx == batch.Width() {
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

func (p {{template "opRConstName" .}}) Init() {
	p.input.Init()
}

type {{template "opLConstName" .}} struct {
	input Operator

	colIdx   int
	constArg {{.LGoType}}

	outputIdx int
}

func (p *{{template "opLConstName" .}}) Next() ColBatch {
	batch := p.input.Next()
	if p.outputIdx == batch.Width() {
		batch.AppendCol(types.{{.RetTyp}})
	}
	projCol := batch.ColVec(p.outputIdx).{{.RetTyp}}()[:ColBatchSize]
	col := batch.ColVec(p.colIdx).{{.RTyp}}()[:ColBatchSize]
	n := batch.Length()
	if sel := batch.Selection(); sel != nil {
		for _, i := range sel {
			{{(.Assign "projCol[i]" "p.constArg" "col[i]")}}
		}
	} else {
		col = col[:n]
		for i := range col {
			{{(.Assign "projCol[i]" "p.constArg" "col[i]")}}
		}
	}
	return batch
}

func (p {{template "opLConstName" .}}) Init() {
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
	if p.outputIdx == batch.Width() {
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

{{/* Range over true and false. $left will be true when outputting a left-const
     operator, and false when outputting a right-const operator. */}}
{{range $left := .ConstSides}}
// GetProjectionConstOperator returns the appropriate constant projection
// operator for the given column type and comparison.
func GetProjection{{if $left}}L{{else}}R{{end}}ConstOperator(
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
	{{range $typ, $overloads := $.TypToOverloads}}
	case types.{{$typ}}:
		switch op.(type) {
		case tree.BinaryOperator:
			switch op {
			{{range $overloads}}
			{{if .IsBinOp}}
			case tree.{{.Name}}:
				return &{{if $left}}{{template "opLConstName" .}}{{else}}{{template "opRConstName" .}}{{end}}{
					input:    input,
					colIdx:   colIdx,
					constArg: c.({{if $left}}{{.LGoType}}{{else}}{{.RGoType}}{{end}}),
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
				return &{{if $left}}{{template "opLConstName" .}}{{else}}{{template "opRConstName" .}}{{end}}{
					input:    input,
					colIdx:   colIdx,
					constArg: c.({{if $left}}{{.LGoType}}{{else}}{{.RGoType}}{{end}}),
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
{{end}}

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
	{{range $typ, $overloads := .TypToOverloads}}
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

type genInput struct {
	TypToOverloads map[types.T][]*overload
	// ConstSides is a boolean array that contains two elements, true and false.
	// It's used by the template to generate both variants of the const projection
	// op - once where the left is const, and one where the right is const.
	ConstSides []bool
}

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
	return tmpl.Execute(wr, genInput{typToOverloads, []bool{false, true}})
}

func init() {
	registerGenerator(genProjectionOps, "projection_ops.eg.go")
}
