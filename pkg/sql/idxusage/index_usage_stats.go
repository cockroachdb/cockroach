// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// idxusage is a subsystem that is responsible for collecting index usage
// statistics.

package idxusage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Writer provides interface to record index usage statistics.
type Writer interface {
	// RecordRead records a read operation on the specified index.
	RecordRead(ctx context.Context, key roachpb.IndexUsageKey)

	// TODO(azhng): as we introduce more plumbing throughout the codebase,
	//  we should introduce additional interfaces here to record other index usage
	//  type.
}

// IteratorOptions provides knobs to change the iterating behavior when
// calling ForEach.
type IteratorOptions struct {
	SortedTableID bool
	SortedIndexID bool
	Max           *uint64
}

// StatsVisitor is the callback invoked when calling ForEach.
type StatsVisitor func(key *roachpb.IndexUsageKey, value *roachpb.IndexUsageStatistics) error

// Reader provides interfaces to retrieve index usage statistics from the
// subsystem.
type Reader interface {
	// Get returns the index usage statistics for a given key.
	Get(key roachpb.IndexUsageKey) roachpb.IndexUsageStatistics

	// ForEach iterates through all stored index usage statistics
	// based on the options specified in IteratorOptions. If an error is
	// encountered when calling StatsVisitor, the iteration is aborted.
	ForEach(options IteratorOptions, visitor StatsVisitor) error
}
