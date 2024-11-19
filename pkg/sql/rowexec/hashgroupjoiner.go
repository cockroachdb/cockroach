// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
)

// hashGroupJoiner is an unoptimized implementation of hash group-join operation
// that uses the hash joiner and the hash aggregator directly.
type hashGroupJoiner struct {
	// Delegate the meat of the implementation to the hash aggregator that will
	// read from the hash joiner.
	*hashAggregator
	hj *hashJoiner

	hjStats, haStats func() *execinfrapb.ComponentStats
}

var _ execinfra.Processor = &hashGroupJoiner{}
var _ execinfra.RowSource = &hashGroupJoiner{}
var _ execopnode.OpNode = &hashGroupJoiner{}

func newHashGroupJoiner(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.HashGroupJoinerSpec,
	leftSource execinfra.RowSource,
	rightSource execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
) (*hashGroupJoiner, error) {
	hjPost := execinfrapb.PostProcessSpec{}
	if len(spec.JoinOutputColumns) > 0 {
		hjPost.Projection = true
		hjPost.OutputColumns = spec.JoinOutputColumns
	}
	hj, err := newHashJoiner(
		ctx,
		flowCtx,
		processorID,
		&spec.HashJoinerSpec,
		leftSource,
		rightSource,
		&hjPost,
	)
	if err != nil {
		return nil, err
	}

	ha, err := newHashAggregator(
		ctx,
		flowCtx,
		processorID,
		&spec.AggregatorSpec,
		hj,
		post,
	)
	if err != nil {
		return nil, err
	}
	hgj := &hashGroupJoiner{
		hashAggregator: ha,
		hj:             hj,
	}
	// If the trace is recording, both the joiner and the aggregator were
	// instrumented to collect stats and will produce two sets of statistics,
	// but we want to produce only a single combined stat.
	if execstats.ShouldCollectStats(ctx, flowCtx.CollectStats) {
		hgj.hjStats = hj.ExecStatsForTrace
		hgj.haStats = ha.ExecStatsForTrace
		hj.ExecStatsForTrace = nil
		ha.ExecStatsForTrace = nil
		hgj.ExecStatsForTrace = hgj.execStatsForTrace
	}
	return hgj, nil
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (h *hashGroupJoiner) execStatsForTrace() *execinfrapb.ComponentStats {
	hjStats, haStats := h.hjStats(), h.haStats()
	// Keep the input stats from the joiner, the output from the aggregator, and
	// combine the memory usage of both.
	res := hjStats
	res.Output = haStats.Output
	res.Exec.MaxAllocatedMem.MaybeAdd(haStats.Exec.MaxAllocatedMem)
	return res
}

// ChildCount is part of the execopnode.OpNode interface.
func (h *hashGroupJoiner) ChildCount(verbose bool) int {
	return h.hj.ChildCount(verbose)
}

// Child is part of the execopnode.OpNode interface.
func (h *hashGroupJoiner) Child(nth int, verbose bool) execopnode.OpNode {
	return h.hj.Child(nth, verbose)
}
