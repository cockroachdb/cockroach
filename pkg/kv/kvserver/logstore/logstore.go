// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package logstore implements the Raft log storage.
package logstore

import (
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Ready contains the log entries and state to be saved to stable storage. This
// is a subset of raft.Ready relevant to log storage. All fields are read-only.
type Ready struct {
	// The current state of a replica to be saved to stable storage. Empty if
	// there is no update.
	raftpb.HardState

	// Entries specifies entries to be saved to stable storage. Empty if there is
	// no update.
	Entries []raftpb.Entry

	// MustSync indicates whether the HardState and Entries must be synchronously
	// written to disk, or if an asynchronous write is permissible.
	MustSync bool
}

// MakeReady constructs a Ready struct from raft.Ready.
func MakeReady(from raft.Ready) Ready {
	return Ready{HardState: from.HardState, Entries: from.Entries, MustSync: from.MustSync}
}
