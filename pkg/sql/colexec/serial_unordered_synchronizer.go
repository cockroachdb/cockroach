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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// SerialUnorderedSynchronizer is an Operator that combines multiple Operator
// streams into one. It reads its inputs one by one until each one is exhausted,
// at which point it moves to the next input. See ParallelUnorderedSynchronizer
// for a parallel implementation. The serial one is used when concurrency is
// undesirable - for example when the whole query is planned on the gateway and
// we want to run it in the RootTxn.
type SerialUnorderedSynchronizer struct {
	colexecop.InitHelper
	span *tracing.Span

	inputs []colexecargs.OpWithMetaInfo
	// curSerialInputIdx indicates the index of the current input being consumed.
	curSerialInputIdx int

	// serialInputIdxExclusiveUpperBound indicates the InputIdx to error out on
	// should execution fail to halt prior to this input. This is only valid if
	// non-zero.
	serialInputIdxExclusiveUpperBound uint32

	// exceedsInputIdxExclusiveUpperBoundError is the error to return when
	// curSerialInputIdx has incremented to match
	// serialInputIdxExclusiveUpperBound.
	exceedsInputIdxExclusiveUpperBoundError error
}

var (
	_ colexecop.Operator = &SerialUnorderedSynchronizer{}
	_ execopnode.OpNode  = &SerialUnorderedSynchronizer{}
	_ colexecop.Closer   = &SerialUnorderedSynchronizer{}
)

// ChildCount implements the execopnode.OpNode interface.
func (s *SerialUnorderedSynchronizer) ChildCount(verbose bool) int {
	return len(s.inputs)
}

// Child implements the execopnode.OpNode interface.
func (s *SerialUnorderedSynchronizer) Child(nth int, verbose bool) execopnode.OpNode {
	return s.inputs[nth].Root
}

// NewSerialUnorderedSynchronizer creates a new SerialUnorderedSynchronizer.
func NewSerialUnorderedSynchronizer(
	inputs []colexecargs.OpWithMetaInfo,
	serialInputIdxExclusiveUpperBound uint32,
	exceedsInputIdxExclusiveUpperBoundError error,
) *SerialUnorderedSynchronizer {
	return &SerialUnorderedSynchronizer{
		inputs:                                  inputs,
		serialInputIdxExclusiveUpperBound:       serialInputIdxExclusiveUpperBound,
		exceedsInputIdxExclusiveUpperBoundError: exceedsInputIdxExclusiveUpperBoundError,
	}
}

// Init is part of the colexecop.Operator interface.
func (s *SerialUnorderedSynchronizer) Init(ctx context.Context) {
	if !s.InitHelper.Init(ctx) {
		return
	}
	s.Ctx, s.span = execinfra.ProcessorSpan(s.Ctx, "serial unordered sync")
	for _, input := range s.inputs {
		input.Root.Init(s.Ctx)
	}
}

// Next is part of the colexecop.Operator interface.
func (s *SerialUnorderedSynchronizer) Next() coldata.Batch {
	for {
		if s.curSerialInputIdx == len(s.inputs) {
			return coldata.ZeroBatch
		}
		b := s.inputs[s.curSerialInputIdx].Root.Next()
		if b.Length() == 0 {
			s.curSerialInputIdx++
			if s.serialInputIdxExclusiveUpperBound > 0 && s.curSerialInputIdx >= int(s.serialInputIdxExclusiveUpperBound) {
				colexecerror.ExpectedError(s.exceedsInputIdxExclusiveUpperBoundError)
			}
		} else {
			return b
		}
	}
}

// DrainMeta is part of the colexecop.MetadataSource interface.
func (s *SerialUnorderedSynchronizer) DrainMeta() []execinfrapb.ProducerMetadata {
	var bufferedMeta []execinfrapb.ProducerMetadata
	if s.span != nil {
		for i := range s.inputs {
			for _, stats := range s.inputs[i].StatsCollectors {
				s.span.RecordStructured(stats.GetStats())
			}
		}
		if meta := execinfra.GetTraceDataAsMetadata(s.span); meta != nil {
			bufferedMeta = append(bufferedMeta, *meta)
		}
	}
	for _, input := range s.inputs {
		bufferedMeta = append(bufferedMeta, input.MetadataSources.DrainMeta()...)
	}
	return bufferedMeta
}

// Close is part of the colexecop.ClosableOperator interface.
func (s *SerialUnorderedSynchronizer) Close(context.Context) error {
	if s.span != nil {
		s.span.Finish()
		s.span = nil
	}
	return nil
}
