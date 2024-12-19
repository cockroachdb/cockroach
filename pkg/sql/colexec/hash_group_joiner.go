// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
)

// hashGroupJoiner currently is the naive implementation of hash group-join
// operation which simply uses the hash joiner and the hash aggregator directly.
// TODO(yuzefovich): add optimized implementation.
type hashGroupJoiner struct {
	colexecop.TwoInputInitHelper

	hjLeftSource *copyingOperator
	hj           colexecop.BufferingInMemoryOperator
	ha           *hashAggregator
}

var _ colexecop.BufferingInMemoryOperator = &hashGroupJoiner{}
var _ colexecop.Closer = &hashGroupJoiner{}
var _ colexecop.ResettableOperator = &hashGroupJoiner{}

// NewHashGroupJoiner creates a new hash group-join operator.
func NewHashGroupJoiner(
	ctx context.Context,
	leftSource, rightSource colexecop.Operator,
	// Hide the complexity of creating the hash joiner behind a constructor
	// function in order to not make colexec package depend on colexecjoin.
	hjConstructor func(leftSource colexecop.Operator) colexecop.BufferingInMemoryOperator,
	numJoinOutputCols int,
	hjProjection []uint32,
	haArgs *colexecagg.NewHashAggregatorArgs,
	leftSourceSQArgs *colexecutils.NewSpillingQueueArgs,
) colexecop.BufferingInMemoryOperator {
	// We wrap the left input with a copying operator so that we could spill to
	// disk in case the memory limit is exceeded during the aggregation.
	// Exporting buffered tuples from the right input is handled by the hash
	// joiner, so no need to do this for the right input.
	// TODO(yuzefovich): remove this copying operator if
	// HashAggregationDiskSpillingEnabled is false.
	hjLeftSource := newCopyingOperator(leftSource, leftSourceSQArgs)
	hj := hjConstructor(hjLeftSource)

	aggInput := hj.(colexecop.Operator)
	if len(hjProjection) > 0 {
		aggInput = colexecbase.NewSimpleProjectOp(hj, numJoinOutputCols, hjProjection)
	}

	haArgs.Input = aggInput
	ha := NewHashAggregator(
		ctx,
		haArgs,
		// We don't need the aggregator to track input tuples, so we pass nil
		// here.
		nil, /* newSpillingQueueArgs */
	)

	return &hashGroupJoiner{
		TwoInputInitHelper: colexecop.MakeTwoInputInitHelper(leftSource, rightSource),
		hjLeftSource:       hjLeftSource,
		hj:                 hj,
		ha:                 ha.(*hashAggregator),
	}
}

// Init implements the colexecop.Operator interface.
func (h *hashGroupJoiner) Init(ctx context.Context) {
	if !h.TwoInputInitHelper.Init(ctx) {
		return
	}
	h.hj.Init(ctx)
	h.ha.Init(ctx)
}

// Next implements the colexecop.Operator interface.
func (h *hashGroupJoiner) Next() coldata.Batch {
	return h.ha.Next()
}

// ExportBuffered implements the colexecop.BufferingInMemoryOperator interface.
//
// The memory limit can be reached either during the join phase or the
// aggregation phase. If it's the former, then it must be while building the
// hash table based on the right input without having read anything from the
// left input. In this case, we only need to export tuples that are buffered in
// the hash joiner. If it's the latter, then it must be while handling the new
// bucket. In this case, in addition to tuples buffered in the hash joiner we
// also need to export all tuples we have read from the left input (due to not
// being able to spill the intermediate aggregation state). Thus, we currently
// always instantiate a copyingOperator around the left input which allows us to
// perform the export.
func (h *hashGroupJoiner) ExportBuffered(input colexecop.Operator) coldata.Batch {
	if h.InputTwo == input {
		// When exporting from the right input, simply delegate to the hash
		// joiner.
		return h.hj.ExportBuffered(input)
	}
	if h.hjLeftSource.sq == nil {
		// All tuples have been exported.
		return coldata.ZeroBatch
	}
	if !h.hjLeftSource.zeroBatchEnqueued {
		h.hjLeftSource.sq.Enqueue(h.Ctx, coldata.ZeroBatch)
		h.hjLeftSource.zeroBatchEnqueued = true
	}
	b, err := h.hjLeftSource.sq.Dequeue(h.Ctx)
	if err != nil {
		colexecerror.InternalError(err)
	}
	return b
}

// ReleaseBeforeExport implements the colexecop.BufferingInMemoryOperator
// interface.
func (h *hashGroupJoiner) ReleaseBeforeExport() {
	h.ha.ReleaseBeforeExport()
}

// ReleaseAfterExport implements the colexecop.BufferingInMemoryOperator
// interface.
func (h *hashGroupJoiner) ReleaseAfterExport(input colexecop.Operator) {
	if h.InputTwo == input {
		// The right input handling is delegated to the hash joiner.
		h.hj.ReleaseAfterExport(input)
		return
	}
	if h.hjLeftSource.sq == nil {
		// Resources have already been released.
		return
	}
	if err := h.hjLeftSource.sq.Close(h.Ctx); err != nil {
		colexecerror.InternalError(err)
	}
	h.hjLeftSource.sq = nil
}

// Reset implements the colexecop.Resetter interface.
func (h *hashGroupJoiner) Reset(ctx context.Context) {
	h.TwoInputInitHelper.Reset(ctx)
	h.hj.(colexecop.ResettableOperator).Reset(ctx)
	h.ha.Reset(ctx)
}

// Close implements the colexecop.Closer interface.
func (h *hashGroupJoiner) Close(ctx context.Context) error {
	lastErr := h.ha.Close(ctx)
	if err := h.hjLeftSource.Close(ctx); err != nil {
		lastErr = err
	}
	return lastErr
}

// copyingOperator is a utility operator that copies all the batches from the
// input into the spilling queue first before propagating the batch further.
type copyingOperator struct {
	colexecop.OneInputHelper
	colexecop.NonExplainable

	// sq will be nil once the queue has been closed.
	sq                *colexecutils.SpillingQueue
	zeroBatchEnqueued bool
}

var _ colexecop.ClosableOperator = &copyingOperator{}
var _ colexecop.ResettableOperator = &copyingOperator{}

func newCopyingOperator(
	input colexecop.Operator, args *colexecutils.NewSpillingQueueArgs,
) *copyingOperator {
	return &copyingOperator{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		sq:             colexecutils.NewSpillingQueue(args),
	}
}

// Next implements the colexecop.Operator interface.
func (c *copyingOperator) Next() coldata.Batch {
	b := c.Input.Next()
	c.sq.Enqueue(c.Ctx, b)
	c.zeroBatchEnqueued = b.Length() == 0
	return b
}

// Reset implements the colexecop.Resetter interface.
func (c *copyingOperator) Reset(ctx context.Context) {
	if r, ok := c.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	c.sq.Reset(ctx)
	c.zeroBatchEnqueued = false
}

// Close implements the colexecop.Closer interface.
func (c *copyingOperator) Close(ctx context.Context) error {
	if c.sq == nil {
		return nil
	}
	err := c.sq.Close(ctx)
	c.sq = nil
	return err
}
