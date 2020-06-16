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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// SerialUnorderedSynchronizer is an Operator that combines multiple Operator
// streams into one. It reads its inputs one by one until each one is exhausted,
// at which point it moves to the next input. See ParallelUnorderedSynchronizer
// for a parallel implementation. The serial one is used when concurrency is
// undesirable - for example when the whole query is planned on the gateway and
// we want to run it in the RootTxn.
type SerialUnorderedSynchronizer struct {
	inputs []SynchronizerInput
	// curSerialInputIdx indicates the index of the current input being consumed.
	curSerialInputIdx int
}

var _ colexecbase.Operator = &SerialUnorderedSynchronizer{}
var _ execinfra.OpNode = &SerialUnorderedSynchronizer{}

// ChildCount implements the execinfra.OpNode interface.
func (s *SerialUnorderedSynchronizer) ChildCount(verbose bool) int {
	return len(s.inputs)
}

// Child implements the execinfra.OpNode interface.
func (s *SerialUnorderedSynchronizer) Child(nth int, verbose bool) execinfra.OpNode {
	return s.inputs[nth].Op
}

// NewSerialUnorderedSynchronizer creates a new SerialUnorderedSynchronizer.
func NewSerialUnorderedSynchronizer(inputs []SynchronizerInput) *SerialUnorderedSynchronizer {
	return &SerialUnorderedSynchronizer{
		inputs:            inputs,
		curSerialInputIdx: 0,
	}
}

// Init is part of the Operator interface.
func (s *SerialUnorderedSynchronizer) Init() {
	for _, input := range s.inputs {
		input.Op.Init()
	}
}

// Next is part of the Operator interface.
func (s *SerialUnorderedSynchronizer) Next(ctx context.Context) coldata.Batch {
	for {
		if s.curSerialInputIdx == len(s.inputs) {
			return coldata.ZeroBatch
		}
		b := s.inputs[s.curSerialInputIdx].Op.Next(ctx)
		if b.Length() == 0 {
			s.curSerialInputIdx++
		} else {
			return b
		}
	}
}

// DrainMeta is part of the MetadataSource interface.
func (s *SerialUnorderedSynchronizer) DrainMeta(
	ctx context.Context,
) []execinfrapb.ProducerMetadata {
	var bufferedMeta []execinfrapb.ProducerMetadata
	for _, input := range s.inputs {
		bufferedMeta = append(bufferedMeta, input.MetadataSources.DrainMeta(ctx)...)
	}
	return bufferedMeta
}
