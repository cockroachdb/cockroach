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
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

// RaftState stores information about the last entry and the size of the log.
type RaftState struct {
	LastIndex uint64
	LastTerm  uint64
	ByteSize  int64
}

// AppendStats describes a completed log storage append operation.
type AppendStats struct {
	Begin time.Time
	End   time.Time

	RegularEntries    int
	RegularBytes      int64
	SideloadedEntries int
	SideloadedBytes   int64

	PebbleBegin time.Time
	PebbleEnd   time.Time
	PebbleBytes int64

	Sync bool
}

// LogAppend adds the given entries to the raft log. Takes the previous log
// state, and returns the updated state. It's the caller's responsibility to
// maintain exclusive access to the raft log for the duration of the method
// call.
//
// LogAppend is intentionally oblivious to the existence of sideloaded
// proposals. They are managed by the caller, including cleaning up obsolete
// on-disk payloads in case the log tail is replaced.
func LogAppend(
	ctx context.Context,
	raftLogPrefix roachpb.Key,
	rw storage.ReadWriter,
	prev RaftState,
	entries []raftpb.Entry,
) (RaftState, error) {
	if len(entries) == 0 {
		return prev, nil
	}
	var diff enginepb.MVCCStats
	var value roachpb.Value
	for i := range entries {
		ent := &entries[i]
		key := keys.RaftLogKeyFromPrefix(raftLogPrefix, ent.Index)

		if err := value.SetProto(ent); err != nil {
			return RaftState{}, err
		}
		value.InitChecksum(key)
		var err error
		if ent.Index > prev.LastIndex {
			err = storage.MVCCBlindPut(ctx, rw, &diff, key, hlc.Timestamp{}, hlc.ClockTimestamp{}, value, nil /* txn */)
		} else {
			err = storage.MVCCPut(ctx, rw, &diff, key, hlc.Timestamp{}, hlc.ClockTimestamp{}, value, nil /* txn */)
		}
		if err != nil {
			return RaftState{}, err
		}
	}

	newLastIndex := entries[len(entries)-1].Index
	// Delete any previously appended log entries which never committed.
	if prev.LastIndex > 0 {
		for i := newLastIndex + 1; i <= prev.LastIndex; i++ {
			// Note that the caller is in charge of deleting any sideloaded payloads
			// (which they must only do *after* the batch has committed).
			_, err := storage.MVCCDelete(ctx, rw, &diff, keys.RaftLogKeyFromPrefix(raftLogPrefix, i),
				hlc.Timestamp{}, hlc.ClockTimestamp{}, nil)
			if err != nil {
				return RaftState{}, err
			}
		}
	}
	return RaftState{
		LastIndex: newLastIndex,
		LastTerm:  entries[len(entries)-1].Term,
		ByteSize:  prev.ByteSize + diff.SysBytes,
	}, nil
}
