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

import "context"

// AggregateFunc accumulates the result of a function of a Datum.
type AggregateFunc interface {
	// Add accumulates the passed datums into the AggregateFunc.
	// Most implementations require one and only one firstArg argument.
	// If an aggregate function requires more than one argument,
	// all additional arguments (after firstArg) are passed in as a
	// variadic collection, otherArgs.
	// This interface (as opposed to `args ...Datum`) avoids unnecessary
	// allocation of otherArgs in the majority of cases.
	Add(_ context.Context, firstArg Datum, otherArgs ...Datum) error

	// Result returns the current value of the accumulation. This value
	// will be a deep copy of any AggregateFunc internal state, so that
	// it will not be mutated by additional calls to Add.
	Result() (Datum, error)

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
