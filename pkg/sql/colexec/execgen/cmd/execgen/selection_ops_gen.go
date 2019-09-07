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

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

const selTemplate = `
package colexec

import (
	"bytes"
  "context"
  "math"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

{{define "opConstName"}}sel{{.Name}}{{.LTyp}}{{.RTyp}}ConstOp{{end}}
{{define "opName"}}sel{{.Name}}{{.LTyp}}{{.RTyp}}Op{{end}}

{{define "selConstLoop"}}
if sel := batch.Selection(); sel != nil {
	sel = sel[:n]
	for _, i := range sel {
		var cmp bool
		arg := {{.Global.LTyp.Get "col" "int(i)"}}
		{{(.Global.Assign "cmp" "arg" "p.constArg")}}
		if cmp {{if .HasNulls}}&& !nulls.NullAt(i) {{end}}{
			sel[idx] = i
			idx++
		}
	}
} else {
	batch.SetSelection(true)
	sel := batch.Selection()
	col = {{.Global.LTyp.Slice "col" "0" "int(n)"}}
	for {{.Global.LTyp.Range "i" "col"}} {
		var cmp bool
		arg := {{.Global.LTyp.Get "col" "i"}}
		{{(.Global.Assign "cmp" "arg" "p.constArg")}}
		if cmp {{if .HasNulls}}&& !nulls.NullAt(uint16(i)) {{end}}{
			sel[idx] = uint16(i)
			idx++
		}
	}
}
{{end}}

{{define "selLoop"}}
if sel := batch.Selection(); sel != nil {
	sel = sel[:n]
	for _, i := range sel {
		var cmp bool
		arg1 := {{.Global.LTyp.Get "col1" "int(i)"}}
		arg2 := {{.Global.RTyp.Get "col2" "int(i)"}}
		{{(.Global.Assign "cmp" "arg1" "arg2")}}
		if cmp {{if .HasNulls}}&& !nulls.NullAt(i) {{end}}{
			sel[idx] = i
			idx++
		}
	}
} else {
	batch.SetSelection(true)
	sel := batch.Selection()
	col1 = {{.Global.LTyp.Slice "col1" "0" "int(n)"}}
	col1Len := {{.Global.LTyp.Len "col1"}}
	col2 = {{.Global.RTyp.Slice "col2" "0" "col1Len"}}
	for {{.Global.LTyp.Range "i" "col1"}} {
		var cmp bool
		arg1 := {{.Global.LTyp.Get "col1" "i"}}
		arg2 := {{.Global.RTyp.Get "col2" "i"}}
		{{(.Global.Assign "cmp" "arg1" "arg2")}}
		if cmp {{if .HasNulls}}&& !nulls.NullAt(uint16(i)) {{end}}{
			sel[idx] = uint16(i)
			idx++
		}
	}
}
{{end}}

{{define "selConstOp"}}
type {{template "opConstName" .}} struct {
  selConstOpBase
	constArg {{.RGoType}}
}

func (p *{{template "opConstName" .}}) Next(ctx context.Context) coldata.Batch {
	for {
		batch := p.input.Next(ctx)
		if batch.Length() == 0 {
			return batch
		}

		vec := batch.ColVec(p.colIdx)
		col := vec.{{.LTyp}}()
		var idx uint16
		n := batch.Length()
		if vec.MaybeHasNulls() {
			nulls := vec.Nulls()
			{{template "selConstLoop" buildDict "Global" . "HasNulls" true }}
		} else {
			{{template "selConstLoop" buildDict "Global" . "HasNulls" false }}
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
{{end}}

{{define "selOp"}}
type {{template "opName" .}} struct {
  selOpBase
}

func (p *{{template "opName" .}}) Next(ctx context.Context) coldata.Batch {
	for {
		batch := p.input.Next(ctx)
		if batch.Length() == 0 {
			return batch
		}

		vec1 := batch.ColVec(p.col1Idx)
		vec2 := batch.ColVec(p.col2Idx)
		col1 := vec1.{{.LTyp}}()
		col2 := vec2.{{.RTyp}}()
		n := batch.Length()

		var idx uint16
		if vec1.MaybeHasNulls() || vec2.MaybeHasNulls() {
			nulls := vec1.Nulls().Or(vec2.Nulls())
			{{template "selLoop" buildDict "Global" . "HasNulls" true }}
		} else {
			{{template "selLoop" buildDict "Global" . "HasNulls" false }}
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

{{/* The outer range is a coltypes.T (the left type). The middle range is also a
     coltypes.T (the right type). The inner is the overloads associated with
     those two types. */}}
{{range .}}
{{range .}}
{{range .}}
{{template "selConstOp" .}}
{{template "selOp" .}}
{{end}}
{{end}}
{{end}}

// GetSelectionConstOperator returns the appropriate constant selection operator
// for the given left and right column types and comparison.
func GetSelectionConstOperator(
  leftColType *types.T,
  constColType *types.T,
	cmpOp tree.ComparisonOperator,
	input Operator,
	colIdx int,
	constArg tree.Datum,
) (Operator, error) {
	c, err := typeconv.GetDatumToPhysicalFn(constColType)(constArg)
	if err != nil {
		return nil, err
	}
  selConstOpBase := selConstOpBase {
		OneInputNode: NewOneInputNode(input),
		colIdx: colIdx,
  }
	switch leftType := typeconv.FromColumnType(leftColType); leftType {
	{{range $lTyp, $rTypToOverloads := . -}}
	case coltypes.{{$lTyp}}:
		switch rightType := typeconv.FromColumnType(constColType); rightType {
		{{range $rTyp, $overloads := $rTypToOverloads}}
		case coltypes.{{$rTyp}}:
			switch cmpOp {
			{{range $overloads -}}
			case tree.{{.Name}}:
				return &{{template "opConstName" .}}{selConstOpBase: selConstOpBase, constArg: c.({{.RGoType}})}, nil
			{{end -}}
			default:
				return nil, errors.Errorf("unhandled comparison operator: %s", cmpOp)
			}
		{{end -}}
		default:
			return nil, errors.Errorf("unhandled right type: %s", rightType)
		}
	{{end -}}
	default:
		return nil, errors.Errorf("unhandled left type: %s", leftType)
	}
}

// GetSelectionOperator returns the appropriate two column selection operator
// for the given left and right column types and comparison.
func GetSelectionOperator(
  leftColType *types.T,
  rightColType *types.T,
	cmpOp tree.ComparisonOperator,
	input Operator,
	col1Idx int,
	col2Idx int,
) (Operator, error) {
  selOpBase := selOpBase {
		OneInputNode: NewOneInputNode(input),
		col1Idx: col1Idx,
		col2Idx: col2Idx,
  }
	switch leftType := typeconv.FromColumnType(leftColType); leftType {
	{{range $lTyp, $rTypToOverloads := . -}}
	case coltypes.{{$lTyp}}:
		switch rightType := typeconv.FromColumnType(rightColType); rightType {
		{{range $rTyp, $overloads := $rTypToOverloads}}
		case coltypes.{{$rTyp}}:
			switch cmpOp {
			{{range $overloads -}}
			case tree.{{.Name}}:
				return &{{template "opName" .}}{selOpBase: selOpBase}, nil
			{{end -}}
			default:
				return nil, errors.Errorf("unhandled comparison operator: %s", cmpOp)
			}
		{{end -}}
		default:
			return nil, errors.Errorf("unhandled right type: %s", rightType)
		}
	{{end -}}
	default:
		return nil, errors.Errorf("unhandled left type: %s", leftType)
	}
}
`

func genSelectionOps(wr io.Writer) error {
	lTypToRTypToOverloads := make(map[coltypes.T]map[coltypes.T][]*overload)
	for _, ov := range comparisonOpOverloads {
		lTyp := ov.LTyp
		rTyp := ov.RTyp
		rTypToOverloads := lTypToRTypToOverloads[lTyp]
		if rTypToOverloads == nil {
			rTypToOverloads = make(map[coltypes.T][]*overload)
			lTypToRTypToOverloads[lTyp] = rTypToOverloads
		}
		rTypToOverloads[rTyp] = append(rTypToOverloads[rTyp], ov)
	}
	tmpl := template.New("selection_ops").Funcs(template.FuncMap{"buildDict": buildDict})
	var err error
	tmpl, err = tmpl.Parse(selTemplate)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, lTypToRTypToOverloads)
}

func init() {
	registerGenerator(genSelectionOps, "selection_ops.eg.go")
}
