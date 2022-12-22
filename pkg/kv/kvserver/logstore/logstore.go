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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var disableSyncRaftLog = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"kv.raft_log.disable_synchronization_unsafe",
	"set to true to disable synchronization on Raft log writes to persistent storage. "+
		"Setting to true risks data loss or data corruption on server crashes. "+
		"The setting is meant for internal testing only and SHOULD NOT be used in production.",
	envutil.EnvOrDefaultBool("COCKROACH_DISABLE_RAFT_LOG_SYNCHRONIZATION_UNSAFE", false),
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

func newStoreEntriesBatch(eng storage.Engine) storage.Batch {
	// Use an unindexed batch because we don't need to read our writes, and
	// it is more efficient.
	return eng.NewUnindexedBatch(false /* writeOnly */)
}

// StoreEntries persists newly appended Raft log Entries to the log storage.
// Accepts the state of the log before the operation, returns the state after.
// Persists HardState atomically with, or strictly after Entries.
func (s *LogStore) StoreEntries(
	ctx context.Context, state RaftState, rd Ready, stats *AppendStats,
) (RaftState, error) {
	batch := newStoreEntriesBatch(s.Engine)
	defer batch.Close()
	return s.storeEntriesAndCommitBatch(ctx, state, rd, stats, batch)
}

func (s *LogStore) storeEntriesAndCommitBatch(
	ctx context.Context, state RaftState, rd Ready, stats *AppendStats, batch storage.Batch,
) (RaftState, error) {
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

var valPool = sync.Pool{
	New: func() interface{} { return &roachpb.Value{} },
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
	value := valPool.Get().(*roachpb.Value)
	value.RawBytes = value.RawBytes[:0]
	defer valPool.Put(value)
	for i := range entries {
		ent := &entries[i]
		key := keys.RaftLogKeyFromPrefix(raftLogPrefix, ent.Index)

		if err := value.SetProto(ent); err != nil {
			return RaftState{}, err
		}
		value.InitChecksum(key)
		var err error
		if ent.Index > prev.LastIndex {
			err = storage.MVCCBlindPut(ctx, rw, &diff, key, hlc.Timestamp{}, hlc.ClockTimestamp{}, *value, nil /* txn */)
		} else {
			err = storage.MVCCPut(ctx, rw, &diff, key, hlc.Timestamp{}, hlc.ClockTimestamp{}, *value, nil /* txn */)
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

// LoadTerm returns the term of the entry at the given index for the specified
// range. The result is loaded from the storage engine if it's not in the cache.
func LoadTerm(
	ctx context.Context,
	rsl StateLoader,
	eng storage.Engine,
	rangeID roachpb.RangeID,
	eCache *raftentry.Cache,
	index uint64,
) (uint64, error) {
	entry, found := eCache.Get(rangeID, index)
	if found {
		return entry.Term, nil
	}

	reader := eng.NewReadOnly(storage.StandardDurability)
	defer reader.Close()

	if err := raftlog.Visit(reader, rangeID, index, index+1, func(ent raftpb.Entry) error {
		if found {
			return errors.Errorf("found more than one entry in [%d,%d)", index, index+1)
		}
		found = true
		entry = ent
		return nil
	}); err != nil {
		return 0, err
	}

	if found {
		// Found an entry. Double-check that it has a correct index.
		if got, want := entry.Index, index; got != want {
			return 0, errors.Errorf("there is a gap at index %d, found entry #%d", want, got)
		}
		// Cache the entry except if it is sideloaded. We don't load/inline the
		// sideloaded entries here to keep the term fetching cheap.
		// TODO(pavelkalinnikov): consider not caching here, after measuring if it
		// makes any difference.
		typ, err := raftlog.EncodingOf(entry)
		if err != nil {
			return 0, err
		}
		if typ != raftlog.EntryEncodingSideloaded {
			eCache.Add(rangeID, []raftpb.Entry{entry}, false /* truncate */)
		}
		return entry.Term, nil
	}
	// Otherwise, the entry at the given index is not found. This can happen if
	// the index is ahead of lastIndex, or it has been compacted away.

	lastIndex, err := rsl.LoadLastIndex(ctx, reader)
	if err != nil {
		return 0, err
	}
	if index > lastIndex {
		return 0, raft.ErrUnavailable
	}

	ts, err := rsl.LoadRaftTruncatedState(ctx, reader)
	if err != nil {
		return 0, err
	}
	if index == ts.Index {
		return ts.Term, nil
	}
	if index > ts.Index {
		return 0, errors.Errorf("there is a gap at index %d", index)
	}
	return 0, raft.ErrCompacted
}

// LoadEntries retrieves entries from the engine. It inlines the sideloaded
// entries, and caches all the loaded entries. The size of the returned entries
// does not exceed maxSize, unless only one entry is returned.
//
// TODO(pavelkalinnikov): return all entries we've read, consider maxSize a
// target size. Currently we may read one extra entry and drop it.
func LoadEntries(
	ctx context.Context,
	rsl StateLoader,
	eng storage.Engine,
	rangeID roachpb.RangeID,
	eCache *raftentry.Cache,
	sideloaded SideloadStorage,
	lo, hi, maxBytes uint64,
) ([]raftpb.Entry, error) {
	if lo > hi {
		return nil, errors.Errorf("lo:%d is greater than hi:%d", lo, hi)
	}

	n := hi - lo
	if n > 100 {
		n = 100
	}
	ents := make([]raftpb.Entry, 0, n)

	ents, size, hitIndex, exceededMaxBytes := eCache.Scan(ents, rangeID, lo, hi, maxBytes)

	// Return results if the correct number of results came back or if
	// we ran into the max bytes limit.
	if uint64(len(ents)) == hi-lo || exceededMaxBytes {
		return ents, nil
	}

	// Scan over the log to find the requested entries in the range [lo, hi),
	// stopping once we have enough.
	expectedIndex := hitIndex

	scanFunc := func(ent raftpb.Entry) error {
		// Exit early if we have any gaps or it has been compacted.
		if ent.Index != expectedIndex {
			return iterutil.StopIteration()
		}
		expectedIndex++

		typ, err := raftlog.EncodingOf(ent)
		if err != nil {
			return err
		}
		if typ == raftlog.EntryEncodingSideloaded {
			newEnt, err := MaybeInlineSideloadedRaftCommand(
				ctx, rangeID, ent, sideloaded, eCache,
			)
			if err != nil {
				return err
			}
			if newEnt != nil {
				ent = *newEnt
			}
		}

		// Note that we track the size of proposals with payloads inlined.
		size += uint64(ent.Size())
		if size > maxBytes {
			exceededMaxBytes = true
			if len(ents) == 0 { // make sure to return at least one entry
				ents = append(ents, ent)
			}
			return iterutil.StopIteration()
		}

		ents = append(ents, ent)
		return nil
	}

	reader := eng.NewReadOnly(storage.StandardDurability)
	defer reader.Close()
	if err := raftlog.Visit(reader, rangeID, expectedIndex, hi, scanFunc); err != nil {
		return nil, err
	}
	eCache.Add(rangeID, ents, false /* truncate */)

	// Did the correct number of results come back? If so, we're all good.
	if uint64(len(ents)) == hi-lo {
		return ents, nil
	}

	// Did we hit the size limit? If so, return what we have.
	if exceededMaxBytes {
		return ents, nil
	}

	// Did we get any results at all? Because something went wrong.
	if len(ents) > 0 {
		// Was the missing index after the last index?
		lastIndex, err := rsl.LoadLastIndex(ctx, reader)
		if err != nil {
			return nil, err
		}
		if lastIndex <= expectedIndex {
			return nil, raft.ErrUnavailable
		}

		// We have a gap in the record, if so, return a nasty error.
		return nil, errors.Errorf("there is a gap in the index record between lo:%d and hi:%d at index:%d", lo, hi, expectedIndex)
	}

	// No results, was it due to unavailability or truncation?
	ts, err := rsl.LoadRaftTruncatedState(ctx, reader)
	if err != nil {
		return nil, err
	}
	if ts.Index >= lo {
		// The requested lo index has already been truncated.
		return nil, raft.ErrCompacted
	}
	// The requested lo index does not yet exist.
	return nil, raft.ErrUnavailable
}
