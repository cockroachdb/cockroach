// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
  "context"
  "math"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types/conv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

{{define "opRConstName"}}proj{{.Name}}{{.LTyp}}{{.RTyp}}ConstOp{{end}}
{{define "opLConstName"}}proj{{.Name}}{{.LTyp}}Const{{.RTyp}}Op{{end}}
{{define "opName"}}proj{{.Name}}{{.LTyp}}{{.RTyp}}Op{{end}}

{{define "projRConstOp"}}
type {{template "opRConstName" .}} struct {
	OneInputNode

	colIdx   int
	constArg {{.RGoType}}

	outputIdx int
}

func (p {{template "opRConstName" .}}) EstimateStaticMemoryUsage() int {
	return EstimateBatchSizeBytes([]types.T{types.{{.RetTyp}}}, coldata.BatchSize)
}

func (p {{template "opRConstName" .}}) Next(ctx context.Context) coldata.Batch {
	batch := p.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return batch
	}
	if p.outputIdx == batch.Width() {
		batch.AppendCol(types.{{.RetTyp}})
	}
	vec := batch.ColVec(p.colIdx)
	col := vec.{{.LTyp}}()
	projVec := batch.ColVec(p.outputIdx)
	projCol := projVec.{{.RetTyp}}()
	if sel := batch.Selection(); sel != nil {
		for _, i := range sel {
			{{(.Assign "projCol[i]" "col[i]" "p.constArg")}}
		}
	} else {
		col = col[:n]
		_ = projCol[len(col)-1]
		for i := range col {
			{{(.Assign "projCol[i]" "col[i]" "p.constArg")}}
		}
	}
	if vec.Nulls().MaybeHasNulls() {
		nulls := vec.Nulls().Copy()
		projVec.SetNulls(&nulls)
	}
	return batch
}

func (p {{template "opRConstName" .}}) Init() {
	p.input.Init()
}
{{end}}

{{define "projLConstOp"}}
type {{template "opLConstName" .}} struct {
	OneInputNode

	colIdx   int
	constArg {{.LGoType}}

	outputIdx int
}

func (p {{template "opLConstName" .}}) EstimateStaticMemoryUsage() int {
	return EstimateBatchSizeBytes([]types.T{types.{{.RetTyp}}}, coldata.BatchSize)
}

func (p {{template "opLConstName" .}}) Next(ctx context.Context) coldata.Batch {
	batch := p.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return batch
	}
	if p.outputIdx == batch.Width() {
		batch.AppendCol(types.{{.RetTyp}})
	}
	vec := batch.ColVec(p.colIdx)
	col := vec.{{.RTyp}}()
	projVec := batch.ColVec(p.outputIdx)
	projCol := projVec.{{.RetTyp}}()
	if sel := batch.Selection(); sel != nil {
		for _, i := range sel {
			{{(.Assign "projCol[i]" "p.constArg" "col[i]")}}
		}
	} else {
		col = col[:n]
		_ = projCol[len(col)-1]
		for i := range col {
			{{(.Assign "projCol[i]" "p.constArg" "col[i]")}}
		}
	}
	if vec.Nulls().MaybeHasNulls() {
		nulls := vec.Nulls().Copy()
		projVec.SetNulls(&nulls)
	}
	return batch
}

func (p {{template "opLConstName" .}}) Init() {
	p.input.Init()
}
{{end}}

{{define "projOp"}}
type {{template "opName" .}} struct {
	OneInputNode

	col1Idx int
	col2Idx int

	outputIdx int
}

func (p {{template "opName" .}}) EstimateStaticMemoryUsage() int {
	return EstimateBatchSizeBytes([]types.T{types.{{.RetTyp}}}, coldata.BatchSize)
}

func (p {{template "opName" .}}) Next(ctx context.Context) coldata.Batch {
	batch := p.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return batch
	}
	if p.outputIdx == batch.Width() {
		batch.AppendCol(types.{{.RetTyp}})
	}
	projVec := batch.ColVec(p.outputIdx)
	projCol := projVec.{{.RetTyp}}()
	vec1 := batch.ColVec(p.col1Idx)
	vec2 := batch.ColVec(p.col2Idx)
	col1 := vec1.{{.LTyp}}()
	col2 := vec2.{{.RTyp}}()
	if sel := batch.Selection(); sel != nil {
		for _, i := range sel {
			{{(.Assign "projCol[i]" "col1[i]" "col2[i]")}}
		}
	} else {
		col1 = col1[:n]
		_ = projCol[len(col1)-1]
		_ = col2[len(col1)-1]
		for i := range col1 {
			{{(.Assign "projCol[i]" "col1[i]" "col2[i]")}}
		}
	}
	if vec1.Nulls().MaybeHasNulls() || vec2.Nulls().MaybeHasNulls() {
		projVec.SetNulls(vec1.Nulls().Or(vec2.Nulls()))
	}
	return batch
}

func (p {{template "opName" .}}) Init() {
	p.input.Init()
}
{{end}}

{{/* The outer range is a types.T, and the inner is the overloads associated
     with that type. */}}
{{range .TypToOverloads}}
{{range .}}
{{template "projRConstOp" .}}
{{template "projLConstOp" .}}
{{template "projOp" .}}
{{end}}
{{end}}

{{/* Range over true and false. $left will be true when outputting a left-const
     operator, and false when outputting a right-const operator. */}}
{{range $left := .ConstSides}}
// GetProjectionConstOperator returns the appropriate constant projection
// operator for the given column type and comparison.
func GetProjection{{if $left}}L{{else}}R{{end}}ConstOperator(
	ct *semtypes.T,
	op tree.Operator,
	input Operator,
	colIdx int,
	constArg tree.Datum,
  outputIdx int,
) (Operator, error) {
	c, err := conv.GetDatumToPhysicalFn(ct)(constArg)
	if err != nil {
		return nil, err
	}
	switch t := conv.FromColumnType(ct); t {
	{{range $typ, $overloads := $.TypToOverloads}}
	case types.{{$typ}}:
		switch op.(type) {
		case tree.BinaryOperator:
			switch op {
			{{range $overloads}}
			{{if .IsBinOp}}
			case tree.{{.Name}}:
				return &{{if $left}}{{template "opLConstName" .}}{{else}}{{template "opRConstName" .}}{{end}}{
					OneInputNode: NewOneInputNode(input),
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
					OneInputNode: NewOneInputNode(input),
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
	ct *semtypes.T,
	op tree.Operator,
	input Operator,
	col1Idx int,
	col2Idx int,
  outputIdx int,
) (Operator, error) {
	switch t := conv.FromColumnType(ct); t {
	{{range $typ, $overloads := .TypToOverloads}}
	case types.{{$typ}}:
		switch op.(type) {
		case tree.BinaryOperator:
			switch op {
			{{range $overloads}}
			{{if .IsBinOp}}
			case tree.{{.Name}}:
				return &{{template "opName" .}}{
					OneInputNode: NewOneInputNode(input),
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
					OneInputNode: NewOneInputNode(input),
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
