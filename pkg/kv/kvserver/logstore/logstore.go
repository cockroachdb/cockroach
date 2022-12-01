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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var disableSyncRaftLog = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"kv.raft_log.disable_synchronization_unsafe",
	"set to true to disable synchronization on Raft log writes to persistent storage. "+
		"Setting to true risks data loss or data corruption on server crashes. "+
		"The setting is meant for internal testing only and SHOULD NOT be used in production.",
	false,
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

// Metrics contains metrics specific to the log storage.
type Metrics struct {
	RaftLogCommitLatency *metric.Histogram
}

// LogStore is a stub of a separated Raft log storage.
type LogStore struct {
	RangeID     roachpb.RangeID
	Engine      storage.Engine
	Sideload    SideloadStorage
	StateLoader StateLoader
	EntryCache  *raftentry.Cache
	Settings    *cluster.Settings
	Metrics     Metrics
}

// StoreEntries persists newly appended Raft log Entries to the log storage.
// Accepts the state of the log before the operation, returns the state after.
// Persists HardState atomically with, or strictly after Entries.
func (s *LogStore) StoreEntries(
	ctx context.Context, state RaftState, rd Ready, stats *AppendStats,
) (RaftState, error) {
	// TODO(pavelkalinnikov): Doesn't this comment contradict the code?
	// Use a more efficient write-only batch because we don't need to do any
	// reads from the batch. Any reads are performed on the underlying DB.
	batch := s.Engine.NewUnindexedBatch(false /* writeOnly */)
	defer batch.Close()

	prevLastIndex := state.LastIndex
	if len(rd.Entries) > 0 {
		stats.Begin = timeutil.Now()
		// All of the entries are appended to distinct keys, returning a new
		// last index.
		thinEntries, numSideloaded, sideLoadedEntriesSize, otherEntriesSize, err := MaybeSideloadEntries(ctx, rd.Entries, s.Sideload)
		if err != nil {
			const expl = "during sideloading"
			return RaftState{}, errors.Wrap(err, expl)
		}
		state.ByteSize += sideLoadedEntriesSize
		if state, err = logAppend(
			ctx, s.StateLoader.RaftLogPrefix(), batch, state, thinEntries,
		); err != nil {
			const expl = "during append"
			return RaftState{}, errors.Wrap(err, expl)
		}
		stats.RegularEntries += len(thinEntries) - numSideloaded
		stats.RegularBytes += otherEntriesSize
		stats.SideloadedEntries += numSideloaded
		stats.SideloadedBytes += sideLoadedEntriesSize
		stats.End = timeutil.Now()
	}

	if !raft.IsEmptyHardState(rd.HardState) {
		// NB: Note that without additional safeguards, it's incorrect to write
		// the HardState before appending rd.Entries. When catching up, a follower
		// will receive Entries that are immediately Committed in the same
		// Ready. If we persist the HardState but happen to lose the Entries,
		// assertions can be tripped.
		//
		// We have both in the same batch, so there's no problem. If that ever
		// changes, we must write and sync the Entries before the HardState.
		if err := s.StateLoader.SetHardState(ctx, batch, rd.HardState); err != nil {
			const expl = "during setHardState"
			return RaftState{}, errors.Wrap(err, expl)
		}
	}
	// Synchronously commit the batch with the Raft log entries and Raft hard
	// state as we're promising not to lose this data.
	//
	// Note that the data is visible to other goroutines before it is synced to
	// disk. This is fine. The important constraints are that these syncs happen
	// before Raft messages are sent and before the call to RawNode.Advance. Our
	// regular locking is sufficient for this and if other goroutines can see the
	// data early, that's fine. In particular, snapshots are not a problem (I
	// think they're the only thing that might access log entries or HardState
	// from other goroutines). Snapshots do not include either the HardState or
	// uncommitted log entries, and even if they did include log entries that
	// were not persisted to disk, it wouldn't be a problem because raft does not
	// infer the that entries are persisted on the node that sends a snapshot.
	//
	// TODO(pavelkalinnikov): revisit the comment above (written in 82cbb49). It
	// communicates an important invariant, but is hard to grok now and can be
	// outdated. Raft invariants are in the responsibility of the layer above
	// (Replica), so this comment might need to move.
	stats.PebbleBegin = timeutil.Now()
	stats.PebbleBytes = int64(batch.Len())
	sync := rd.MustSync && !disableSyncRaftLog.Get(&s.Settings.SV)
	if err := batch.Commit(sync); err != nil {
		const expl = "while committing batch"
		return RaftState{}, errors.Wrap(err, expl)
	}
	stats.Sync = sync
	stats.PebbleEnd = timeutil.Now()
	if rd.MustSync {
		s.Metrics.RaftLogCommitLatency.RecordValue(stats.PebbleEnd.Sub(stats.PebbleBegin).Nanoseconds())
	}

	if len(rd.Entries) > 0 {
		// We may have just overwritten parts of the log which contain
		// sideloaded SSTables from a previous term (and perhaps discarded some
		// entries that we didn't overwrite). Remove any such leftover on-disk
		// payloads (we can do that now because we've committed the deletion
		// just above).
		firstPurge := rd.Entries[0].Index // first new entry written
		purgeTerm := rd.Entries[0].Term - 1
		lastPurge := prevLastIndex // old end of the log, include in deletion
		purgedSize, err := maybePurgeSideloaded(ctx, s.Sideload, firstPurge, lastPurge, purgeTerm)
		if err != nil {
			const expl = "while purging sideloaded storage"
			return RaftState{}, errors.Wrap(err, expl)
		}
		state.ByteSize -= purgedSize
		if state.ByteSize < 0 {
			// Might have gone negative if node was recently restarted.
			state.ByteSize = 0
		}
	}

	// Update raft log entry cache. We clear any older, uncommitted log entries
	// and cache the latest ones.
	s.EntryCache.Add(s.RangeID, rd.Entries, true /* truncate */)

	return state, nil
}

// logAppend adds the given entries to the raft log. Takes the previous log
// state, and returns the updated state. It's the caller's responsibility to
// maintain exclusive access to the raft log for the duration of the method
// call.
//
// logAppend is intentionally oblivious to the existence of sideloaded
// proposals. They are managed by the caller, including cleaning up obsolete
// on-disk payloads in case the log tail is replaced.
func logAppend(
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
