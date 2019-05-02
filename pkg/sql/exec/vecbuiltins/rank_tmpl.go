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
// This file is the execgen template for rank.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package vecbuiltins

import "github.com/cockroachdb/cockroach/pkg/sql/exec/types"

// {{/*
func _NEXT_RANK(hasPartition bool) { // */}}
	// {{define "nextRank"}}

	// {{ if $.HasPartition }}
	if r.partitionColIdx == r.batch.Width() {
		r.batch.AppendCol(types.Bool)
	} else if r.partitionColIdx > r.batch.Width() {
		panic("unexpected: column partitionColIdx is neither present nor the next to be appended")
	}
	partitionCol := r.batch.ColVec(r.partitionColIdx).Bool()
	// {{ end }}

	if r.outputColIdx == r.batch.Width() {
		r.batch.AppendCol(types.Int64)
	} else if r.outputColIdx > r.batch.Width() {
		panic("unexpected: column outputColIdx is neither present nor the next to be appended")
	}
	rankCol := r.batch.ColVec(r.outputColIdx).Int64()
	sel := r.batch.Selection()
	if sel != nil {
		for i := uint16(0); i < r.batch.Length(); i++ {
			// {{ if $.HasPartition }}
			if partitionCol[sel[i]] {
				r.rank = 1
				r.rankIncrement = 1
				rankCol[i] = 1
				continue
			}
			// {{end}}
			if r.distinctCol[sel[i]] {
				// TODO(yuzefovich): template this part out to generate two different
				// rank operators.
				if r.dense {
					r.rank++
				} else {
					r.rank += r.rankIncrement
					r.rankIncrement = 1
				}
				rankCol[sel[i]] = r.rank
			} else {
				rankCol[sel[i]] = r.rank
				if !r.dense {
					r.rankIncrement++
				}
			}
		}
	} else {
		for i := uint16(0); i < r.batch.Length(); i++ {
			// {{ if $.HasPartition }}
			if partitionCol[i] {
				r.rank = 1
				r.rankIncrement = 1
				rankCol[i] = 1
				continue
			}
			// {{end}}
			if r.distinctCol[i] {
				// TODO(yuzefovich): template this part out to generate two different
				// rank operators.
				if r.dense {
					r.rank++
				} else {
					r.rank += r.rankIncrement
					r.rankIncrement = 1
				}
				rankCol[i] = r.rank
			} else {
				rankCol[i] = r.rank
				if !r.dense {
					r.rankIncrement++
				}
			}
		}
	}
	// {{end}}
	// {{/*
}

// */}}

func (r *rankOp) nextBodyWithPartition() {
	_NEXT_RANK(true)
}

func (r *rankOp) nextBodyNoPartition() {
	_NEXT_RANK(false)
}
