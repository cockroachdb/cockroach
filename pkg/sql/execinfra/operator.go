// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import "time"

// OpNode is an interface to operator-like structures with children.
type OpNode interface {
	// ChildCount returns the number of children (inputs) of the operator.
	ChildCount(verbose bool) int

	// Child returns the nth child (input) of the operator.
	Child(nth int, verbose bool) OpNode
}

// KVReader is an operator that performs KV reads.
type KVReader interface {
	// GetBytesRead returns the number of bytes read from KV by this operator.
	GetBytesRead() int64
	// GetRowsRead returns the number of rows read from KV by this operator.
	GetRowsRead() int64
	// GetCumulativeContentionTime returns the amount of time KV reads spent
	// contending.
	GetCumulativeContentionTime() time.Duration
}
