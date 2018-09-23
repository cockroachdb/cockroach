// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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

	// Close closes out the AggregateFunc and allows it to release any memory it
	// requested during aggregation, and must be called upon completion of the
	// aggregation.
	Close(context.Context)

	// Size returns the size of the AggregateFunc implementation in bytes. It does
	// not account for additional memory used during accumulation.
	Size() int64
}
