// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package generator

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// CallbackValueGenerator is a ValueGenerator that calls a supplied callback for
// producing the values. To be used with
// EvalContextTestingKnobs.CallbackGenerators.
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
func (c *CallbackValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
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
