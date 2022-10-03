// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// GetGenerator is used to construct a ValueGenerator from a FuncExpr.
func GetGenerator(
	ctx context.Context, evalCtx *Context, expr *tree.FuncExpr,
) (ValueGenerator, error) {
	if !expr.IsGeneratorClass() {
		return nil, errors.AssertionFailedf(
			"cannot call EvalArgsAndGetGenerator() on non-aggregate function: %q",
			tree.ErrString(expr),
		)
	}
	ol := expr.ResolvedOverload()
	if ol.GeneratorWithExprs != nil {
		return ol.GeneratorWithExprs.(GeneratorWithExprsOverload)(ctx, evalCtx, expr.Exprs)
	}
	nullArg, args, err := (*evaluator)(evalCtx).evalFuncArgs(ctx, expr)
	if err != nil || nullArg {
		return nil, err
	}
	return ol.Generator.(GeneratorOverload)(ctx, evalCtx, args)
}

// Table generators, also called "set-generating functions", are
// special functions that return an entire table.
//
// Overview of the concepts:
//
// - ValueGenerator is an interface that offers a
//   Start/Next/Values/Stop API similar to sql.planNode.
//
// - because generators are regular functions, it is possible to use
//   them in any expression context. This is useful to e.g
//   pass an entire table as argument to the ARRAY( ) conversion
//   function.
//
// - the data source mechanism in the sql package has a special case
//   for generators appearing in FROM contexts and knows how to
//   construct a special row source from them.

// ValueGenerator is the interface provided by the value generator
// functions for SQL SRfs. Objects that implement this interface are
// able to produce rows of values in a streaming fashion (like Go
// iterators or generators in Python).
type ValueGenerator interface {
	// ResolvedType returns the type signature of this value generator.
	ResolvedType() *types.T

	// Start initializes the generator. Must be called once before
	// Next() and Values(). It can be called again to restart
	// the generator after Next() has returned false.
	//
	// txn represents the txn that the generator will run inside of. The generator
	// is expected to hold on to this txn and use it in Next() calls.
	Start(ctx context.Context, txn *kv.Txn) error

	// Next determines whether there is a row of data available.
	Next(context.Context) (bool, error)

	// Values retrieves the current row of data.
	Values() (tree.Datums, error)

	// Close must be called after Start() before disposing of the
	// ValueGenerator. It does not need to be called if Start() has not
	// been called yet. It must not be called in-between restarts.
	Close(ctx context.Context)
}

// AliasAwareValueGenerator is a value generator that can inspect the alias with
// which it was invoked. SetAlias will always be run before Start.
type AliasAwareValueGenerator interface {
	SetAlias(types []*types.T, labels []string) error
}

// CallbackValueGenerator is a ValueGenerator that calls a supplied callback for
// producing the values. To be used with
// eval.TestingKnobs.CallbackGenerators.
type CallbackValueGenerator struct {
	// cb is the callback to be called for producing values. It gets passed in 0
	// as prev initially, and the value it previously returned for subsequent
	// invocations. Once it returns -1 or an error, it will not be invoked any
	// more.
	cb  func(ctx context.Context, prev int, txn *kv.Txn) (int, error)
	val int
	txn *kv.Txn
}

var _ ValueGenerator = &CallbackValueGenerator{}

// NewCallbackValueGenerator creates a new CallbackValueGenerator.
func NewCallbackValueGenerator(
	cb func(ctx context.Context, prev int, txn *kv.Txn) (int, error),
) *CallbackValueGenerator {
	return &CallbackValueGenerator{
		cb: cb,
	}
}

// ResolvedType is part of the ValueGenerator interface.
func (c *CallbackValueGenerator) ResolvedType() *types.T {
	return types.Int
}

// Start is part of the ValueGenerator interface.
func (c *CallbackValueGenerator) Start(_ context.Context, txn *kv.Txn) error {
	c.txn = txn
	return nil
}

// Next is part of the ValueGenerator interface.
func (c *CallbackValueGenerator) Next(ctx context.Context) (bool, error) {
	var err error
	c.val, err = c.cb(ctx, c.val, c.txn)
	if err != nil {
		return false, err
	}
	if c.val == -1 {
		return false, nil
	}
	return true, nil
}

// Values is part of the ValueGenerator interface.
func (c *CallbackValueGenerator) Values() (tree.Datums, error) {
	return tree.Datums{tree.NewDInt(tree.DInt(c.val))}, nil
}

// Close is part of the ValueGenerator interface.
func (c *CallbackValueGenerator) Close(_ context.Context) {}
