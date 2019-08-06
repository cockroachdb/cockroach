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

const selTemplate = `
package exec

import (
	"bytes"
  "context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types/conv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
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
	OneInputNode

	colIdx   int
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
	OneInputNode

	col1Idx int
	col2Idx int
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

{{/* The outer range is a types.T, and the inner is the overloads associated
     with that type. */}}
{{range .}}
{{range .}}
{{template "selConstOp" .}}
{{template "selOp" .}}
{{end}}
{{end}}

// GetSelectionConstOperator returns the appropriate constant selection operator
// for the given column type and comparison.
func GetSelectionConstOperator(
	ct *semtypes.T,
	cmpOp tree.ComparisonOperator,
	input Operator,
	colIdx int,
	constArg tree.Datum,
) (Operator, error) {
	c, err := conv.GetDatumToPhysicalFn(ct)(constArg)
	if err != nil {
		return nil, err
	}
	switch t := conv.FromColumnType(ct); t {
	{{range $typ, $overloads := .}}
	case types.{{$typ}}:
		switch cmpOp {
		{{range $overloads}}
		case tree.{{.Name}}:
			return &{{template "opConstName" .}}{
				OneInputNode: NewOneInputNode(input),
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
	ct *semtypes.T,
	cmpOp tree.ComparisonOperator,
	input Operator,
	col1Idx int,
	col2Idx int,
) (Operator, error) {
	switch t := conv.FromColumnType(ct); t {
	{{range $typ, $overloads := .}}
	case types.{{$typ}}:
		switch cmpOp {
		{{range $overloads}}
		case tree.{{.Name}}:
			return &{{template "opName" .}}{
				OneInputNode: NewOneInputNode(input),
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
		if overload.LTyp != overload.RTyp {
			continue
		}
		typ := overload.LTyp
		typToOverloads[typ] = append(typToOverloads[typ], overload)
	}
	tmpl := template.New("selection_ops").Funcs(template.FuncMap{"buildDict": buildDict})
	var err error
	tmpl, err = tmpl.Parse(selTemplate)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, typToOverloads)
}

func init() {
	registerGenerator(genSelectionOps, "selection_ops.eg.go")
}
