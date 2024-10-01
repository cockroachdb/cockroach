// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	var inputToClose colexecop.Closer
	if c, ok := input.(colexecop.Closer); ok {
		inputToClose = c
	}
	distinctCol := make([]bool, coldata.BatchSize())
	// zero the boolean column on every iteration.
	input = &fnOp{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		fn:             func() { copy(distinctCol, colexecutils.ZeroBoolColumn) },
	}
	if len(distinctCols) > 0 {
		for i := range distinctCols {
			input = newSingleDistinct(input, int(distinctCols[i]), distinctCol, typs[distinctCols[i]], nullsAreDistinct)
		}
	} else {
		// If there are no distinct columns, we have to mark the very first
		// tuple as distinct ourselves.
		firstTuple := true
		input.(*fnOp).fn = func() {
			copy(distinctCol, colexecutils.ZeroBoolColumn)
			distinctCol[0] = firstTuple
			firstTuple = false
		}
	}
	r, ok := input.(colexecop.ResettableOperator)
	if !ok {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly an ordered distinct is not a Resetter"))
	}
	distinctChain := &distinctChainOps{
		ResettableOperator: r,
		inputToClose:       inputToClose,
	}
	return distinctChain, distinctCol
}

type distinctChainOps struct {
	colexecop.ResettableOperator
	colexecop.NonExplainable
	inputToClose colexecop.Closer
}

var _ colexecop.ResettableOperator = &distinctChainOps{}
var _ colexecop.ClosableOperator = &distinctChainOps{}

func (d *distinctChainOps) Close(ctx context.Context) error {
	if d.inputToClose != nil {
		return d.inputToClose.Close(ctx)
	}
	return nil
}

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
var _ colexecop.ClosableOperator = &orderedDistinct{}

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

// Close implements the colexecop.Closer interface.
func (d *orderedDistinct) Close(ctx context.Context) error {
	if c, ok := d.Input.(colexecop.Closer); ok {
		return c.Close(ctx)
	}
	return nil
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
