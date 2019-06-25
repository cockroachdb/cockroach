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

package vecbuiltins

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// {{/*

// _UPDATE_RANK_ is the template function for updating the state of rank
// operators.
func _UPDATE_RANK_() {
	panic("")
}

// _UPDATE_RANK_INCREMENT is the template function for updating the state of
// rank operators.
func _UPDATE_RANK_INCREMENT() {
	panic("")
}

// */}}

type rankInitFields struct {
	input exec.Operator
	// distinctCol is the output column of the chain of ordered distinct
	// operators in which true will indicate that a new rank needs to be assigned
	// to the corresponding tuple.
	distinctCol     []bool
	outputColIdx    int
	partitionColIdx int
}

// {{range .}}

type rankDense__DENSE_HasPartition__PARTITION_Op struct {
	rankInitFields

	// rank indicates which rank should be assigned to the next tuple.
	rank int64
	// rankIncrement indicates by how much rank should be incremented when a
	// tuple distinct from the previous one on the ordering columns is seen. It
	// is used only in case of a regular rank function (i.e. not dense).
	rankIncrement int64
}

var _ exec.Operator = &rankDense__DENSE_HasPartition__PARTITION_Op{}

func (r *rankDense__DENSE_HasPartition__PARTITION_Op) Init() {
	r.input.Init()
	// RANK and DENSE_RANK start counting from 1. Before we assign the rank to a
	// tuple in the batch, we first increment r.rank, so setting this
	// rankIncrement to 1 will update r.rank to 1 on the very first tuple (as
	// desired).
	r.rankIncrement = 1
}

func (r *rankDense__DENSE_HasPartition__PARTITION_Op) Next(ctx context.Context) coldata.Batch {
	batch := r.input.Next(ctx)
	if batch.Length() == 0 {
		return batch
	}

	// {{ if .HasPartition }}
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
