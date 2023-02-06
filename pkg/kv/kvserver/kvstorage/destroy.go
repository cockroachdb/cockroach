// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstorage

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// ClearRangeDataOptions specify which parts of a Replica are to be destroyed.
type ClearRangeDataOptions struct {
	// ClearReplicatedByRangeID indicates that replicated RangeID-based keys
	// (abort span, etc) should be removed.
	ClearReplicatedByRangeID bool
	// ClearUnreplicatedByRangeID indicates that unreplicated RangeID-based keys
	// (logstore state incl. HardState, etc) should be removed.
	ClearUnreplicatedByRangeID bool
	// ClearReplicatedBySpan causes the state machine data (i.e. the replicated state
	// for the given RSpan) that is key-addressable (i.e. range descriptor, user keys,
	// locks) to be removed. No data is removed if this is the zero span.
	ClearReplicatedBySpan roachpb.RSpan

	// If MustUseClearRange is true, a Pebble range tombstone will always be used
	// to clear the key spans (unless empty). This is typically used when we need
	// to write additional keys to an SST after this clear, e.g. a replica
	// tombstone, since keys must be written in order. When this is false, a
	// heuristic will be used instead.
	MustUseClearRange bool
}
