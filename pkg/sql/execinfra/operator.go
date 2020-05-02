// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
)

// Operator is a column vector operator that produces a Batch as output.
type Operator interface {
	// Init initializes this operator. Will be called once at operator setup
	// time. If an operator has an input operator, it's responsible for calling
	// Init on that input operator as well.
	// TODO(yuzefovich): we might need to clarify whether it is ok to call
	// Init() multiple times before the first call to Next(). It is possible to
	// hit the memory limit during Init(), and a disk-backed operator needs to
	// make sure that the input has been initialized. We could also in case that
	// Init() doesn't succeed for bufferingInMemoryOperator - which should only
	// happen when 'workmem' setting is too low - just bail, even if we have
	// disk spilling for that operator.
	Init()

	// Next returns the next Batch from this operator. Once the operator is
	// finished, it will return a Batch with length 0. Subsequent calls to
	// Next at that point will always return a Batch with length 0.
	//
	// Calling Next may invalidate the contents of the last Batch returned by
	// Next.
	// Canceling the provided context results in forceful termination of
	// execution.
	Next(context.Context) coldata.Batch

	OpNode
}

// OpNode is an interface to operator-like structures with children.
type OpNode interface {
	// ChildCount returns the number of children (inputs) of the operator.
	ChildCount(verbose bool) int

	// Child returns the nth child (input) of the operator.
	Child(nth int, verbose bool) OpNode
}

// ZeroInputNode is an OpNode with no inputs.
type ZeroInputNode struct{}

// ChildCount implements OpNode interface.
func (ZeroInputNode) ChildCount(verbose bool) int {
	return 0
}

// Child implements the OpNode interface.
func (ZeroInputNode) Child(nth int, verbose bool) OpNode {
	colexecerror.InternalError(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// NonExplainable is a marker interface which identifies an Operator that
// should be omitted from the output of EXPLAIN (VEC). Note that VERBOSE
// explain option will override the omitting behavior.
type NonExplainable interface {
	// nonExplainableMarker is just a marker method. It should never be called.
	nonExplainableMarker()
}

// IdempotentCloser is an object that releases resource on the first call to
// IdempotentClose but does nothing for any subsequent call.
type IdempotentCloser interface {
	IdempotentClose(ctx context.Context) error
}
