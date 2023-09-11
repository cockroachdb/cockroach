// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing

// TracingAggregatorEvent describes an event that can be aggregated and stored by the
// bulk.TracingAggregator.
//
// This lives here rather than in bulk to break a dependency cycle.
type TracingAggregatorEvent interface {
	// Identity returns a TracingAggregatorEvent that when combined with another
	// event returns the other TracingAggregatorEvent unchanged.
	Identity() TracingAggregatorEvent
	// Combine combines two TracingAggregatorEvents together.
	Combine(other TracingAggregatorEvent)
	// ProtoName returns the fully qualified name of the underlying proto that is
	// a TracingAggregatorEvent.
	ProtoName() string
	// String returns the string representation of the TracingAggregatorEvent.
	String() string
}
