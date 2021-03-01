// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

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
	Values() (Datums, error)

	// Close must be called after Start() before disposing of the
	// ValueGenerator. It does not need to be called if Start() has not
	// been called yet. It must not be called in-between restarts.
	Close(ctx context.Context)
}

// GeneratorFactory is the type of constructor functions for
// ValueGenerator objects.
type GeneratorFactory func(ctx *EvalContext, args Datums) (ValueGenerator, error)
