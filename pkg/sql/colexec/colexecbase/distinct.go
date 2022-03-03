// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// OrderedDistinctColsToOperators is a utility function that given an input and
// a slice of columns, creates a chain of distinct operators and returns the
// last distinct operator in that chain as well as its output column.
func OrderedDistinctColsToOperators(
	input colexecop.Operator, distinctCols []uint32, typs []*types.T, nullsAreDistinct bool,
) (colexecop.ResettableOperator, []bool) {
	distinctCol := make([]bool, coldata.BatchSize())
	// zero the boolean column on every iteration.
	input = &fnOp{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		fn:             func() { copy(distinctCol, colexecutils.ZeroBoolColumn) },
	}
	for i := range distinctCols {
		input = newSingleDistinct(input, int(distinctCols[i]), distinctCol, typs[distinctCols[i]], nullsAreDistinct)
	}
	r, ok := input.(colexecop.ResettableOperator)
	if !ok {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly an ordered distinct is not a Resetter"))
	}
	distinctChain := &distinctChainOps{
		ResettableOperator: r,
	}
	return distinctChain, distinctCol
}

type distinctChainOps struct {
	colexecop.ResettableOperator
	colexecop.NonExplainable
}

var _ colexecop.ResettableOperator = &distinctChainOps{}

// NewOrderedDistinct creates a new ordered distinct operator on the given
// input columns with the given types.
func NewOrderedDistinct(
	input colexecop.Operator,
	distinctCols []uint32,
	typs []*types.T,
	nullsAreDistinct bool,
	errorOnDup string,
) colexecop.ResettableOperator {
	od := &orderedDistinct{
		OneInputNode:         colexecop.NewOneInputNode(input),
		UpsertDistinctHelper: UpsertDistinctHelper{ErrorOnDup: errorOnDup},
	}
	op, outputCol := OrderedDistinctColsToOperators(&od.distinctChainInput, distinctCols, typs, nullsAreDistinct)
	op = &colexecutils.BoolVecToSelOp{
		OneInputHelper: colexecop.MakeOneInputHelper(op),
		OutputCol:      outputCol,
		// orderedDistinct is responsible for feeding the distinct chain with
		// input batches (this is needed to support emitting errors on
		// duplicates), so we want BoolVecToSelOp to emit a batch for each of
		// its input batches, even if no rows are selected (i.e. a zero-length
		// output batch).
		ProcessOnlyOneBatch: true,
	}
	od.distinctChain = op
	return od
}

type orderedDistinct struct {
	colexecop.InitHelper
	colexecop.OneInputNode
	UpsertDistinctHelper

	distinctChain      colexecop.ResettableOperator
	distinctChainInput colexecop.FeedOperator
}

var _ colexecop.ResettableOperator = &orderedDistinct{}

// Init implements the colexecop.Operator interface.
func (d *orderedDistinct) Init(ctx context.Context) {
	if !d.InitHelper.Init(ctx) {
		return
	}
	d.Input.Init(ctx)
	d.distinctChain.Init(ctx)
}

// Next implements the colexecop.Operator interface.
func (d *orderedDistinct) Next() coldata.Batch {
	for {
		b := d.Input.Next()
		origLen := b.Length()
		if origLen == 0 {
			return coldata.ZeroBatch
		}
		d.distinctChainInput.SetBatch(b)
		b = d.distinctChain.Next()
		updatedLength := b.Length()
		d.MaybeEmitErrorOnDup(origLen, updatedLength)
		if updatedLength > 0 {
			return b
		}
	}
}

// Reset implements the colexecop.Resetter interface.
func (d *orderedDistinct) Reset(ctx context.Context) {
	if r, ok := d.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	d.distinctChain.Reset(ctx)
}

// UpsertDistinctHelper is a utility that helps distinct operators emit errors
// when they observe duplicate tuples. This behavior is needed by UPSERT
// operations.
type UpsertDistinctHelper struct {
	ErrorOnDup string
}

// MaybeEmitErrorOnDup might emit an error when it detects duplicates. It takes
// in two arguments - one representing the original length of the batch (before
// performing distinct operation on it) and another for the updated length
// (after the distinct operation).
// TODO(yuzefovich): consider templating out the distinct operators to have and
// to omit this helper based on the distinct spec.
func (h *UpsertDistinctHelper) MaybeEmitErrorOnDup(origLen, updatedLen int) {
	if h.ErrorOnDup != "" && origLen > updatedLen {
		// At least one duplicate row was removed from the batch, so we raise an
		// error.
		// TODO(yuzefovich): ErrorOnDup could be passed via redact.Safe() if there
		// was a guarantee that it does not contain PII.
		colexecerror.ExpectedError(pgerror.Newf(pgcode.CardinalityViolation, "%s", h.ErrorOnDup))
	}
}
