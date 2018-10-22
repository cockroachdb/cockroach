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
)

const selTemplate = `
package exec

{{define "opConstName"}}sel{{.Name}}{{.LTyp}}{{.RTyp}}ConstOp{{end}}
{{define "opName"}}sel{{.Name}}{{.LTyp}}{{.RTyp}}Op{{end}}

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
				if col[i] {{.OpStr}} p.constArg {
					sel[idx] = i
					idx++
				}
			}
		} else {
			batch.SetSelection(true)
			sel := batch.Selection()
			for i := uint16(0); i < n; i++ {
				if col[i] {{.OpStr}} p.constArg {
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
				if col1[i] {{.OpStr}} col2[i] {
					sel[idx] = i
					idx++
				}
			}
		} else {
			batch.SetSelection(true)
			sel := batch.Selection()
			for i := uint16(0); i < n; i++ {
				if col1[i] {{.OpStr}} col2[i] {
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
`

func genSelectionOps(wr io.Writer) error {
	tmpl, err := template.New("selection_ops").Parse(selTemplate)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, comparisonOpOverloads)
}

func init() {
	registerGenerator(genSelectionOps, "selection_ops.og.go")
}
