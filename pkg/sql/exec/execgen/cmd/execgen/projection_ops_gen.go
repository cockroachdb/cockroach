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
{{range .}}

type {{template "opConstName" .}} struct {
	input Operator

	colIdx   int
	constArg {{.RTyp.GoTypeName}}

	outputIdx int
}

func (p *{{template "opConstName" .}}) Next() ColBatch {
	batch := p.input.Next()

	projCol := batch.ColVec(p.outputIdx).{{.RetTyp}}()[:ColBatchSize]
	col := batch.ColVec(p.colIdx).{{.LTyp}}()[:ColBatchSize]
	n := batch.Length()
	if batch.Selection() != nil {
		for s := uint16(0); s < n; s++ {
			i := batch.Selection()[s]
			projCol[i] = col[i] {{.OpStr}} p.constArg
		}
	} else {
		for i := uint16(0); i < n; i++ {
			projCol[i] = col[i] {{.OpStr}} p.constArg
		}
	}
	return batch
}

func (p {{template "opConstName" .}}) Init() {}

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
	if batch.Selection() != nil {
		for s := uint16(0); s < n; s++ {
			i := batch.Selection()[s]
			projCol[i] = col1[i] {{.OpStr}} col2[i]
		}
	} else {
		for i := uint16(0); i < n; i++ {
			projCol[i] = col1[i] {{.OpStr}} col2[i]
		}
	}
	return batch
}

func (p {{template "opName" .}}) Init() {}

{{end}}
{{end}}
`

func genProjectionOps(wr io.Writer) error {
	tmpl, err := template.New("projection_ops").Parse(projTemplate)
	if err != nil {
		return err
	}
	return tmpl.Execute(wr, opMap)
}

func init() {
	registerGenerator(genProjectionOps, "projection_ops.og.go")
}
