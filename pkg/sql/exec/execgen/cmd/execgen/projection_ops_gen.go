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

const projTemplate = `
package exec

{{define "opConstName"}}proj{{.Name}}{{.LTyp}}{{.RTyp}}ConstOp{{end}}
{{define "opName"}}proj{{.Name}}{{.LTyp}}{{.RTyp}}Op{{end}}

{{range .}}

type {{template "opConstName" .}} struct {
	input Operator

	colIdx   int
	constArg {{.RGoType}}

	outputIdx int
}

func (p *{{template "opConstName" .}}) Next() ColBatch {
	batch := p.input.Next()
	projCol := batch.ColVec(p.outputIdx).{{.RetTyp}}()[:ColBatchSize]
	col := batch.ColVec(p.colIdx).{{.LTyp}}()[:ColBatchSize]
	n := batch.Length()
	if sel := batch.Selection(); sel != nil {
		for _, i := range sel {
			projCol[i] = col[i] {{.OpStr}} p.constArg
		}
	} else {
		col = col[:n]
		for i, v := range col {
			projCol[i] = v {{.OpStr}} p.constArg
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
	projCol := batch.ColVec(p.outputIdx).{{.RetTyp}}()[:ColBatchSize]
	col1 := batch.ColVec(p.col1Idx).{{.LTyp}}()[:ColBatchSize]
	col2 := batch.ColVec(p.col2Idx).{{.RTyp}}()[:ColBatchSize]
	n := batch.Length()
	if sel := batch.Selection(); sel != nil {
		for _, i := range sel {
			projCol[i] = col1[i] {{.OpStr}} col2[i]
		}
	} else {
		col1 = col1[:n]
		for i, v := range col1 {
			projCol[i] = v {{.OpStr}} col2[i]
		}
	}
	return batch
}

func (p {{template "opName" .}}) Init() {
	p.input.Init()
}

{{end}}
`

func genProjectionOps(wr io.Writer) error {
	tmpl, err := template.New("projection_ops").Parse(projTemplate)
	if err != nil {
		return err
	}
	var allOverloads []overload
	allOverloads = append(allOverloads, binaryOpOverloads...)
	allOverloads = append(allOverloads, comparisonOpOverloads...)
	return tmpl.Execute(wr, allOverloads)
}

func init() {
	registerGenerator(genProjectionOps, "projection_ops.og.go")
}
