// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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

// Sanity check all builtin overloads.
func init() {
	builtinsregistry.AddSubscription(func(name string, _ *tree.FunctionProperties, overloads []tree.Overload) {
		for i := range overloads {
			fn := &overloads[i]
			// Set the routine type for each overload.
			fn.Type = tree.BuiltinRoutine
			var numSet int
			if fn.AggregateFunc != nil {
				numSet++
			}
			if fn.WindowFunc != nil {
				numSet++
			}
			if fn.Fn != nil {
				numSet++
				// Verify that Fn has the correct signature. All other functions
				// are checked via the marker interfaces, but Fn isn't checked
				// at compile time.
				if _, ok := fn.Fn.(FnOverload); !ok {
					panic(errors.AssertionFailedf("%s: Fn is not FnOverload: %v", name, fn))
				}
			}
			if fn.FnWithExprs != nil {
				numSet++
			}
			if fn.Generator != nil {
				numSet++
			}
			if fn.GeneratorWithExprs != nil {
				numSet++
			}
			if fn.SQLFn != nil {
				numSet++
			}
			if fn.Body != "" {
				numSet++
			}
			var numSetExpected int
			switch fn.Class {
			case tree.NormalClass:
				if fn.Fn == nil && fn.FnWithExprs == nil && fn.Body == "" {
					panic(errors.AssertionFailedf(
						"%s: normal builtins should have Fn, FnWithExprs, or Body set: %v", name, fn,
					))
				}
				numSetExpected = 1

			case tree.AggregateClass:
				if fn.AggregateFunc == nil || fn.WindowFunc == nil {
					panic(errors.AssertionFailedf(
						"%s: aggregate builtins should have AggregateFunc "+
							"and WindowFunc set: %v", name, fn,
					))
				}
				numSetExpected = 2

			case tree.WindowClass:
				if fn.WindowFunc == nil {
					panic(errors.AssertionFailedf(
						"%s: window builtins should have WindowFunc set: %v", name, fn,
					))
				}
				numSetExpected = 1

			case tree.GeneratorClass:
				if fn.Generator == nil && fn.GeneratorWithExprs == nil {
					panic(errors.AssertionFailedf(
						"%s: generator builtins should have Generator or "+
							"GeneratorWithExprs set: %v", name, fn,
					))
				}
				// Generator builtins have Fn and FnWithExprs set to
				// implementations that return assertion errors.
				numSetExpected = 3

			case tree.SQLClass:
				if fn.Fn == nil || fn.SQLFn == nil {
					panic(errors.AssertionFailedf(
						"%s: SQL builtins should have Fn and SQLFn set: %v", name, fn,
					))
				}
				numSetExpected = 2

			default:
				panic(errors.AssertionFailedf("%s: unexpected class %d: %v", name, fn.Class, fn))
			}

			if numSet != numSetExpected {
				panic(errors.AssertionFailedf(
					"%s: overload specified %d functions, %d expected: %v", name, numSet, numSetExpected, fn,
				))
			}
		}
	})
}
