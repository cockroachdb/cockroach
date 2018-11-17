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

const colVecTemplate = `
package exec

import (
  "fmt"
	
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

func (m *memColumn) Append(
	vec ColVec, colType types.T, toLength uint64, fromLength uint16,
) {
	switch colType {
		{{range .}}
		case types.{{.ExecType}}:
			m.col = append(m.{{.ExecType}}()[:toLength], vec.{{.ExecType}}()[:fromLength]...)
		{{end}}
		default:
			panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) AppendWithSel(
	vec ColVec, sel []uint16, batchSize uint16, colType types.T, toLength uint64,
) {
	switch colType {
	{{range .}}
	case types.{{.ExecType}}:
		toCol := append(m.{{.ExecType}}()[:toLength], make([]{{.GoType}}, batchSize)...)
		fromCol := vec.{{.ExecType}}()

		for i := uint16(0); i < batchSize; i++ {
      toCol[uint64(i) + toLength] = fromCol[sel[i]]
		}

		m.col = toCol
	{{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) Copy(src ColVec, srcStartIdx, srcEndIdx int, typ types.T) {
	switch typ {
	{{range .}}
	case types.{{.ExecType}}:
		copy(m.{{.ExecType}}(), src.{{.ExecType}}()[srcStartIdx:srcEndIdx])
	{{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", typ))
	}
}

func (m *memColumn) CopyWithSelInt64(
	vec ColVec, sel []uint64, nSel uint16, colType types.T,
) {
	// todo (changangela): handle the case when nSel > ColBatchSize
	switch colType {
	{{range .}}
	case types.{{.ExecType}}:
		toCol := m.{{.ExecType}}()
		fromCol := vec.{{.ExecType}}()
		for i := uint16(0); i < nSel; i++ {
			toCol[i] = fromCol[sel[i]]
		}
	{{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) CopyWithSelInt16(vec ColVec, sel []uint16, nSel uint16, colType types.T) {
	switch colType {
	{{range .}}
	case types.{{.ExecType}}:
		toCol := m.{{.ExecType}}()
		fromCol := vec.{{.ExecType}}()
		for i := uint16(0); i < nSel; i++ {
			toCol[i] = fromCol[sel[i]]
		}
	{{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}
`

type colVecGen struct {
	ExecType string
	GoType   string
}

func genColVec(wr io.Writer) error {
	// Build list of all exec types.
	var gens []colVecGen
	for _, t := range types.AllTypes {
		gen := colVecGen{
			ExecType: t.String(),
			GoType:   t.GoTypeName(),
		}
		gens = append(gens, gen)
	}

	tmpl, err := template.New("colVecTemplate").Parse(colVecTemplate)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, gens)
}

func init() {
	registerGenerator(genColVec, "colvec.eg.go")
}
