// Copyright 2019 The Cockroach Authors.
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

// {{/*
// +build execgen_template
//
// This file is the execgen template for row_number.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package vecbuiltins

import (
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// {{/*
func _NEXT_ROW_NUMBER_(hasPartition bool) { // */}}
	// {{define "nextRowNumber"}}

	// {{ if $.HasPartition }}
	if r.partitionColIdx == batch.Width() {
		batch.AppendCol(types.Bool)
	} else if r.partitionColIdx > batch.Width() {
		panic("unexpected: column partitionColIdx is neither present nor the next to be appended")
	}
	partitionCol := batch.ColVec(r.partitionColIdx).Bool()
	// {{ end }}

	if r.outputColIdx == batch.Width() {
		batch.AppendCol(types.Int64)
	} else if r.outputColIdx > batch.Width() {
		panic("unexpected: column outputColIdx is neither present nor the next to be appended")
	}
	rowNumberCol := batch.ColVec(r.outputColIdx).Int64()
	sel := batch.Selection()
	if sel != nil {
		for i := uint16(0); i < batch.Length(); i++ {
			// {{ if $.HasPartition }}
			if partitionCol[sel[i]] {
				r.rowNumber = 1
			}
			// {{ end }}
			rowNumberCol[sel[i]] = r.rowNumber
			r.rowNumber++
		}
	} else {
		for i := uint16(0); i < batch.Length(); i++ {
			// {{ if $.HasPartition }}
			if partitionCol[i] {
				r.rowNumber = 1
			}
			// {{ end }}
			rowNumberCol[i] = r.rowNumber
			r.rowNumber++
		}
	}
	// {{end}}
	// {{/*
}

// */}}

func (r *rowNumberOp) nextBodyWithPartition(batch coldata.Batch) {
	_NEXT_ROW_NUMBER_(true)
}

func (r *rowNumberOp) nextBodyNoPartition(batch coldata.Batch) {
	_NEXT_ROW_NUMBER_(false)
}
