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

import "github.com/cockroachdb/cockroach/pkg/util/envutil"

// envEnableStructuredEvents determines whether the eventagg package is enabled. The features within
// this package are currently experimental, and must be explicitly enabled via this envvar.
var envEnableStructuredEvents = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_STRUCTURED_EVENTS", false)

// Mergeable represents an event type that has definitions for how to merge
// its values into an aggregate representation, and derive an aggregation key
// used to determine bucketing in aggregations.
//
//   - K is the type of the derived key.
//   - Agg is the aggregate representation of this type.
type Mergeable[K comparable, V any] interface {
	// MergeInto merges this Mergeable into the aggregate representation Agg.
	MergeInto(aggregate V)
	// GroupingKey returns the aggregation key, derived from this event, that should be
	// used in aggregations.
	GroupingKey() K
}
