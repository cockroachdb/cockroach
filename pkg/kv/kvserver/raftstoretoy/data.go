// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftstoretoy/rscodec"

type RaftIndex uint64 // simplification: RaftIndex == LeaseAppliedIndex

type Replica struct {
	// ID is the FullLogID under which the Replica operates.
	ID rscodec.FullLogID
	// WAGIndex tracks the set of WAG operations already applied to the replica.
	WAGIndex WAGIndex
	// RaftIndex is the log position which is materialized in the Replica state.
	RaftIndex RaftIndex
}
