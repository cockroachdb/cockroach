// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
)

// SerialUnorderedSynchronizer is an Operator that combines multiple Operator
// streams into one. It reads its inputs one by one until each one is exhausted,
// at which point it moves to the next input.
type SerialUnorderedSynchronizer struct {
	inputs []Operator
	// curSerialInputIdx is used if parallel is false. It indicates the index of
	// the current input being consumed.
	curSerialInputIdx int
	zeroBatch         coldata.Batch
}

// ChildCount implements the execinfra.OpNode interface.
func (s *SerialUnorderedSynchronizer) ChildCount() int {
	return len(s.inputs)
}

// Child implements the execinfra.OpNode interface.
func (s *SerialUnorderedSynchronizer) Child(nth int) execinfra.OpNode {
	return s.inputs[nth]
}

// NewSerialUnorderedSynchronizer creates a new SerialUnorderedSynchronizer.
func NewSerialUnorderedSynchronizer(
	inputs []Operator, typs []coltypes.T,
) *SerialUnorderedSynchronizer {
	return &SerialUnorderedSynchronizer{
		inputs:            inputs,
		curSerialInputIdx: 0,
		zeroBatch:         coldata.NewMemBatchWithSize(typs, 0),
	}
}

// Init is part of the Operator interface.
func (s *SerialUnorderedSynchronizer) Init() {
	for _, input := range s.inputs {
		input.Init()
	}
}

// Next is part of the Operator interface.
func (s *SerialUnorderedSynchronizer) Next(ctx context.Context) coldata.Batch {
	if ctx.Err() != nil {
		return s.zeroBatch
	}
	for {
		if s.curSerialInputIdx == len(s.inputs) {
			return s.zeroBatch
		}
		b := s.inputs[s.curSerialInputIdx].Next(ctx)
		if b.Length() == 0 {
			s.curSerialInputIdx++
		} else {
			return b
		}
	}
}
