// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for rank.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
)

// {{/*

// _UPDATE_RANK_ is the template function for updating the state of rank
// operators.
func _UPDATE_RANK_() {
	execerror.VectorizedInternalPanic("")
}

// _UPDATE_RANK_INCREMENT is the template function for updating the state of
// rank operators.
func _UPDATE_RANK_INCREMENT() {
	execerror.VectorizedInternalPanic("")
}

// */}}

type rankInitFields struct {
	OneInputNode
	// distinctCol is the output column of the chain of ordered distinct
	// operators in which true will indicate that a new rank needs to be assigned
	// to the corresponding tuple.
	distinctCol     []bool
	outputColIdx    int
	partitionColIdx int
}

// {{range .}}

type _RANK_STRINGOp struct {
	rankInitFields

	// rank indicates which rank should be assigned to the next tuple.
	rank int64
	// rankIncrement indicates by how much rank should be incremented when a
	// tuple distinct from the previous one on the ordering columns is seen. It
	// is used only in case of a regular rank function (i.e. not dense).
	rankIncrement int64
}

var _ Operator = &_RANK_STRINGOp{}

func (r *_RANK_STRINGOp) Init() {
	r.Input().Init()
	// RANK and DENSE_RANK start counting from 1. Before we assign the rank to a
	// tuple in the batch, we first increment r.rank, so setting this
	// rankIncrement to 1 will update r.rank to 1 on the very first tuple (as
	// desired).
	r.rankIncrement = 1
}

func (r *_RANK_STRINGOp) Next(ctx context.Context) coldata.Batch {
	batch := r.Input().Next(ctx)
	// {{ if .HasPartition }}
	if r.partitionColIdx == batch.Width() {
		batch.AppendCol(coltypes.Bool)
	} else if r.partitionColIdx > batch.Width() {
		execerror.VectorizedInternalPanic("unexpected: column partitionColIdx is neither present nor the next to be appended")
	}
	partitionCol := batch.ColVec(r.partitionColIdx).Bool()
	// {{ end }}

	if r.outputColIdx == batch.Width() {
		batch.AppendCol(coltypes.Int64)
	} else if r.outputColIdx > batch.Width() {
		execerror.VectorizedInternalPanic("unexpected: column outputColIdx is neither present nor the next to be appended")
	}

	if batch.Length() == 0 {
		return batch
	}

	rankCol := batch.ColVec(r.outputColIdx).Int64()
	sel := batch.Selection()
	// TODO(yuzefovich): template out sel vs non-sel cases.
	if sel != nil {
		for i := uint16(0); i < batch.Length(); i++ {
			// {{ if .HasPartition }}
			if partitionCol[sel[i]] {
				r.rank = 1
				r.rankIncrement = 1
				rankCol[sel[i]] = 1
				continue
			}
			// {{end}}
			if r.distinctCol[sel[i]] {
				_UPDATE_RANK_()
				rankCol[sel[i]] = r.rank
			} else {
				rankCol[sel[i]] = r.rank
				_UPDATE_RANK_INCREMENT()
			}
		}
	} else {
		for i := uint16(0); i < batch.Length(); i++ {
			// {{ if .HasPartition }}
			if partitionCol[i] {
				r.rank = 1
				r.rankIncrement = 1
				rankCol[i] = 1
				continue
			}
			// {{end}}
			if r.distinctCol[i] {
				_UPDATE_RANK_()
				rankCol[i] = r.rank
			} else {
				rankCol[i] = r.rank
				_UPDATE_RANK_INCREMENT()
			}
		}
	}
	return batch
}

// {{end}}
