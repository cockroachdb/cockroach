// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// WAGOperation represents operations that can appear in the log.
type WAGOperation interface {
	Read(sm SMEngine) error
	Apply(eng LogEngine) error
}

// LogEntry represents a structured entry in the log. A LogEntry can be
// understood as a WAG node that representing the effect of the command on the
// state machine. It has a dependency on the preceding log entry (and
// transitively, all previous log entries).
//
// A LogEntry containing a Split or Merge WAG node has additional dependencies,
// see Split and Merge for details.
type LogEntry struct {
	RaftIndex uint64

	Write [][2]string // simulates WriteBatch
	Split *Split      // optional
	Merge *Merge      // optional
}

type Split struct{}

// Merge represents a range merge operation in the log.
type Merge struct {
	LeftRangeID  roachpb.RangeID
	RightRangeID roachpb.RangeID
}

func (Merge) Apply() {}

// Create represents a request to initialize a replica via snapshot.
type Create struct {
	RangeID   roachpb.RangeID
	ReplicaID roachpb.ReplicaID
	// Additional snapshot metadata would go here
}

// Destroy represents a range destruction operation.
type Destroy struct{}

func (Destroy) Apply() {}
