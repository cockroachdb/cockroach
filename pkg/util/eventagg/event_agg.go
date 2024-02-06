// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eventagg

import "context"

// Mergeable represents an aggregate type T that has definitions for how to:
//   - Merge its values with another instance of the same type.
//   - Derive an aggregation Key for use in aggregations.
type Mergeable[T any] interface {
	// Merge merges the values of other into this instance of T.
	Merge(other T)
	// Key returns the aggregation key, derived from T, that should be
	// used in aggregations.
	Key() string
}

// aggregator represents the core interface for the eventagg pkg. An aggregator
// operates on Mergeable events, which can be pushed via PushEvent.
type aggregator[T Mergeable[T]] interface {
	// PushEvent feeds a Mergeable event to the aggregator for processing.
	PushEvent(ctx context.Context, e T)
}
