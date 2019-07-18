// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fail

// Event is an identifier for a particular fail point.
//
//go:generate stringer -type=Event
type Event int

// The enumeration of all fail point events.
// Keep events in this list grouped by operation.
const (
	_ Event = iota

	///////////////////////////////////////////////////////////////////////////
	//                              Raft events                              //
	///////////////////////////////////////////////////////////////////////////

	// EventRaftApplyEntry occurs when a Raft entry is applied to a replica's
	// replicated state machine after being committed to the Raft log.
	EventRaftApplyEntry
	// EventRaftSnapshotBeforeIngestSST occurs after a snapshot has applied
	// its metadata write batch but before it has ingested its data SSTs.
	EventRaftSnapshotBeforeIngestSST
	// EventRaftSnapshotBeforeProgressKeyCleared occurs after a snapshot has
	// ingested its data SSTs but before it has cleared its in-progress key.
	EventRaftSnapshotBeforeProgressKeyCleared
)
