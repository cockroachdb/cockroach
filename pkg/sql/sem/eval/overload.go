// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// AggregateOverload is the concrete type for the tree.Overload.Aggregate field.
type AggregateOverload func([]*types.T, *Context, tree.Datums) AggregateFunc

// Aggregate is a marker to indicate that this is a tree.AggregateOverload.
func (ao AggregateOverload) Aggregate() {}

// AggregateFunc accumulates the result of a function of a Datum.
type AggregateFunc interface {
	// Add accumulates the passed datums into the AggregateFunc.
	// Most implementations require one and only one firstArg argument.
	// If an aggregate function requires more than one argument,
	// all additional arguments (after firstArg) are passed in as a
	// variadic collection, otherArgs.
	// This interface (as opposed to `args ...Datum`) avoids unnecessary
	// allocation of otherArgs in the majority of cases.
	Add(_ context.Context, firstArg tree.Datum, otherArgs ...tree.Datum) error

	// Result returns the current value of the accumulation. This value
	// will be a deep copy of any AggregateFunc internal state, so that
	// it will not be mutated by additional calls to Add.
	Result() (tree.Datum, error)

	// Reset resets the aggregate function which allows for reusing the same
	// instance for computation without the need to create a new instance.
	// Any memory is kept, if possible.
	Reset(context.Context)

	// Close closes out the AggregateFunc and allows it to release any memory it
	// requested during aggregation, and must be called upon completion of the
	// aggregation.
	Close(context.Context)

	// Size returns the size of the AggregateFunc implementation in bytes. It
	// does *not* account for additional memory used during accumulation.
	Size() int64
}

// FnOverload is a function generally defined as a builtin. It doesn't have
// a concrete type with a marker method only because it's onerous to add.
type FnOverload = func(context.Context, *Context, tree.Datums) (tree.Datum, error)

// FnWithExprsOverload is the concrete type for the tree.Overload.FnWithExprs
// field.
type FnWithExprsOverload func(context.Context, *Context, tree.Exprs) (tree.Datum, error)

// FnWithExprs is a marker to indicate that this is a
// tree.FnWithExprsOverload.
func (fo FnWithExprsOverload) FnWithExprs() {}

// SQLFnOverload is the concrete type for the tree.Overload.SQLFn field.
type SQLFnOverload func(context.Context, *Context, tree.Datums) (string, error)

// SQLFn is a marker to indicate that this is a tree.SQLFnOverload.
func (so SQLFnOverload) SQLFn() {}

// GeneratorOverload is the type of constructor functions for
// ValueGenerator objects.
type GeneratorOverload func(ctx context.Context, evalCtx *Context, args tree.Datums) (ValueGenerator, error)

// Generator is a marker to indicate that this is a tree.GeneratorOverload.
func (geo GeneratorOverload) Generator() {}

// GeneratorWithExprsOverload is an alternative constructor function type for
// ValueGenerators that gives implementations the ability to see the builtin's
// arguments before evaluation, as Exprs.
type GeneratorWithExprsOverload func(ctx context.Context, evalCtx *Context, args tree.Exprs) (ValueGenerator, error)

// GeneratorWithExprs is a marker to indicate that this is a
// tree.GeneratorWithExprsOverload.
func (geo GeneratorWithExprsOverload) GeneratorWithExprs() {}

// WindowOverload is the type of constructor functions for
// WindowFunc objects.
type WindowOverload func([]*types.T, *Context) WindowFunc

// Window is a marker to indicate that this is a tree.WindowOverload.
func (wo WindowOverload) Window() {}
