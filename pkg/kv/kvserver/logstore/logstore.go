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
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// DisableSyncRaftLog disables raft log synchronization and can cause data loss.
var DisableSyncRaftLog = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.raft_log.disable_synchronization_unsafe",
	"disables synchronization of Raft log writes to persistent storage. "+
		"Setting to true risks data loss or data corruption on process or OS crashes. "+
		"This not only disables fsync, but also disables flushing writes to the OS buffer. "+
		"The setting is meant for internal testing only and SHOULD NOT be used in production.",
	envutil.EnvOrDefaultBool("COCKROACH_DISABLE_RAFT_LOG_SYNCHRONIZATION_UNSAFE", false),
	settings.WithName("kv.raft_log.synchronization.unsafe.disabled"),
	settings.WithUnsafe,
)

var enableNonBlockingRaftLogSync = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.raft_log.non_blocking_synchronization.enabled",
	"set to true to enable non-blocking synchronization on Raft log writes to "+
		"persistent storage. Setting to true does not risk data loss or data corruption "+
		"on server crashes, but can reduce write latency.",
	envutil.EnvOrDefaultBool("COCKROACH_ENABLE_RAFT_LOG_NON_BLOCKING_SYNCHRONIZATION", true),
)

// MsgStorageAppend is a raftpb.Message with type MsgStorageAppend.
type MsgStorageAppend raftpb.Message

// MakeMsgStorageAppend constructs a MsgStorageAppend from a raftpb.Message.
func MakeMsgStorageAppend(m raftpb.Message) MsgStorageAppend {
	if m.Type != raftpb.MsgStorageAppend {
		panic(fmt.Sprintf("unexpected message type %s", m.Type))
	}
	return MsgStorageAppend(m)
}

// RaftState stores information about the last entry and the size of the log.
type RaftState struct {
	LastIndex kvpb.RaftIndex
	LastTerm  kvpb.RaftTerm
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
	// Only set when !NonBlocking, which means almost never, since
	// kv.raft_log.non_blocking_synchronization.enabled defaults to true.
	PebbleCommitStats storage.BatchCommitStats

	Sync bool
	// If true, PebbleEnd-PebbleBegin does not include the sync time.
	NonBlocking bool
}

// Metrics contains metrics specific to the log storage.
type Metrics struct {
	RaftLogCommitLatency metric.IHistogram
}

// LogStore is a stub of a separated Raft log storage.
type LogStore struct {
	RangeID     roachpb.RangeID
	Engine      storage.Engine
	Sideload    SideloadStorage
	StateLoader StateLoader
	SyncWaiter  *SyncWaiterLoop
	EntryCache  *raftentry.Cache
	Settings    *cluster.Settings
	Metrics     Metrics

	DisableSyncLogWriteToss bool // for testing only
}

// SyncCallback is a callback that is notified when a raft log write has been
// durably committed to disk. The function is handed the response messages that
// are associated with the MsgStorageAppend that triggered the fsync.
// commitStats is populated iff this was a non-blocking sync.
type SyncCallback interface {
	OnLogSync(context.Context, []raftpb.Message, storage.BatchCommitStats)
}

func newStoreEntriesBatch(eng storage.Engine) storage.Batch {
	// Use an unindexed batch because we don't need to read our writes, and
	// it is more efficient.
	return eng.NewUnindexedBatch()
}

// StoreEntries persists newly appended Raft log Entries to the log storage,
// then calls the provided callback with the input's response messages (if any)
// once the entries are durable. The durable log write may or may not be
// blocking (and therefore the callback may or may not be called synchronously),
// depending on the kv.raft_log.non_blocking_synchronization.enabled cluster
// setting. Either way, the effects of the log append will be immediately
// visible to readers of the Engine.
//
// Accepts the state of the log before the operation, returns the state after.
// Persists HardState atomically with, or strictly after Entries.
func (s *LogStore) StoreEntries(
	ctx context.Context, state RaftState, m MsgStorageAppend, cb SyncCallback, stats *AppendStats,
) (RaftState, error) {
	batch := newStoreEntriesBatch(s.Engine)
	return s.storeEntriesAndCommitBatch(ctx, state, m, cb, stats, batch)
}

// storeEntriesAndCommitBatch is like StoreEntries, but it accepts a
// storage.Batch, which it takes responsibility for committing and closing.
func (s *LogStore) storeEntriesAndCommitBatch(
	ctx context.Context,
	state RaftState,
	m MsgStorageAppend,
	cb SyncCallback,
	stats *AppendStats,
	batch storage.Batch,
) (RaftState, error) {
	// Before returning, Close the batch if we haven't handed ownership of it to a
	// SyncWaiterLoop. If batch == nil, SyncWaiterLoop is responsible for closing
	// it once the in-progress disk writes complete.
	defer func() {
		if batch != nil {
			batch.Close()
		}
	}()

	prevLastIndex := state.LastIndex
	overwriting := false
	if len(m.Entries) > 0 {
		firstPurge := kvpb.RaftIndex(m.Entries[0].Index) // first new entry written
		overwriting = firstPurge <= prevLastIndex
		stats.Begin = timeutil.Now()
		// All of the entries are appended to distinct keys, returning a new
		// last index.
		thinEntries, numSideloaded, sideLoadedEntriesSize, otherEntriesSize, err := MaybeSideloadEntries(ctx, m.Entries, s.Sideload)
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

	hs := raftpb.HardState{
		Term:   m.Term,
		Vote:   m.Vote,
		Commit: m.Commit,
	}
	if !raft.IsEmptyHardState(hs) {
		// NB: Note that without additional safeguards, it's incorrect to write
		// the HardState before appending m.Entries. When catching up, a follower
		// will receive Entries that are immediately Committed in the same
		// Ready. If we persist the HardState but happen to lose the Entries,
		// assertions can be tripped.
		//
		// We have both in the same batch, so there's no problem. If that ever
		// changes, we must write and sync the Entries before the HardState.
		if err := s.StateLoader.SetHardState(ctx, batch, hs); err != nil {
			const expl = "during setHardState"
			return RaftState{}, errors.Wrap(err, expl)
		}
	}
	// Synchronously commit the batch with the Raft log entries and Raft hard
	// state as we're promising not to lose this data.
	//
	// Note that the data is visible to other goroutines before it is synced to
	// disk. This is fine. The important constraints are that these syncs happen
	// before the MsgStorageAppend's responses are delivered back to the RawNode.
	// Our regular locking is sufficient for this and if other goroutines can see
	// the data early, that's fine. In particular, snapshots are not a problem (I
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
	wantsSync := len(m.Responses) > 0
	willSync := wantsSync && !DisableSyncRaftLog.Get(&s.Settings.SV)
	// Use the non-blocking log sync path if we are performing a log sync ...
	nonBlockingSync := willSync &&
		// and the cluster setting is enabled ...
		enableNonBlockingRaftLogSync.Get(&s.Settings.SV) &&
		// and we are not overwriting any previous log entries. If we are
		// overwriting, we may need to purge the sideloaded SSTables associated with
		// overwritten entries. This must be performed after the corresponding
		// entries are durably replaced and it's easier to do ensure proper ordering
		// using a blocking log sync. This is a rare case, so it's not worth
		// optimizing for.
		!overwriting &&
		// Also, randomly disable non-blocking sync in test builds to exercise the
		// interleaved blocking and non-blocking syncs (unless the testing knobs
		// disable this randomization explicitly).
		!(buildutil.CrdbTestBuild && !s.DisableSyncLogWriteToss && rand.Intn(2) == 0)
	if nonBlockingSync {
		// If non-blocking synchronization is enabled, apply the batched updates to
		// the engine and initiate a synchronous disk write, but don't wait for the
		// write to complete.
		if err := batch.CommitNoSyncWait(); err != nil {
			const expl = "while committing batch without sync wait"
			return RaftState{}, errors.Wrap(err, expl)
		}
		stats.PebbleEnd = timeutil.Now()
		// Instead, enqueue that waiting on the SyncWaiterLoop, who will signal the
		// callback when the write completes.
		waiterCallback := nonBlockingSyncWaiterCallbackPool.Get().(*nonBlockingSyncWaiterCallback)
		*waiterCallback = nonBlockingSyncWaiterCallback{
			ctx:            ctx,
			cb:             cb,
			msgs:           m.Responses,
			batch:          batch,
			metrics:        s.Metrics,
			logCommitBegin: stats.PebbleBegin,
		}
		s.SyncWaiter.enqueue(ctx, batch, waiterCallback)
		// Do not Close batch on return. Will be Closed by SyncWaiterLoop.
		batch = nil
	} else {
		if err := batch.Commit(willSync); err != nil {
			const expl = "while committing batch"
			return RaftState{}, errors.Wrap(err, expl)
		}
		stats.PebbleEnd = timeutil.Now()
		stats.PebbleCommitStats = batch.CommitStats()
		if wantsSync {
			logCommitEnd := stats.PebbleEnd
			s.Metrics.RaftLogCommitLatency.RecordValue(logCommitEnd.Sub(stats.PebbleBegin).Nanoseconds())
			cb.OnLogSync(ctx, m.Responses, storage.BatchCommitStats{})
		}
	}
	stats.Sync = wantsSync
	stats.NonBlocking = nonBlockingSync

	if overwriting {
		// We may have just overwritten parts of the log which contain
		// sideloaded SSTables from a previous term (and perhaps discarded some
		// entries that we didn't overwrite). Remove any such leftover on-disk
		// payloads (we can do that now because we've committed the deletion
		// just above).
		firstPurge := kvpb.RaftIndex(m.Entries[0].Index) // first new entry written
		purgeTerm := kvpb.RaftTerm(m.Entries[0].Term - 1)
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
	//
	// In the blocking log sync case, these entries are already durable. In the
	// non-blocking case, these entries have been written to the pebble engine (so
	// reads of the engine will see them), but they are not yet be durable. This
	// means that the entry cache can lead the durable log. This is allowed by
	// etcd/raft, which maintains its own tracking of entry durability by
	// splitting its log into an unstable portion for entries that are not known
	// to be durable and a stable portion for entries that are known to be
	// durable.
	s.EntryCache.Add(s.RangeID, m.Entries, true /* truncate */)

	return state, nil
}

// nonBlockingSyncWaiterCallback packages up the callback that is handed to the
// SyncWaiterLoop during a non-blocking Raft log sync. Structuring the callback
// as a struct with a method instead of an anonymous function avoids individual
// fields escaping to the heap. It also provides the opportunity to pool the
// callback.
type nonBlockingSyncWaiterCallback struct {
	// Used to run SyncCallback.
	ctx  context.Context
	cb   SyncCallback
	msgs []raftpb.Message
	// Used to extract stats. This is the batch that has been synced.
	batch storage.WriteBatch
	// Used to record Metrics.
	metrics        Metrics
	logCommitBegin time.Time
}

// run is the callback's logic. It is executed on the SyncWaiterLoop goroutine.
func (cb *nonBlockingSyncWaiterCallback) run() {
	dur := timeutil.Since(cb.logCommitBegin).Nanoseconds()
	cb.metrics.RaftLogCommitLatency.RecordValue(dur)
	commitStats := cb.batch.CommitStats()
	cb.cb.OnLogSync(cb.ctx, cb.msgs, commitStats)
	cb.release()
}

func (cb *nonBlockingSyncWaiterCallback) release() {
	*cb = nonBlockingSyncWaiterCallback{}
	nonBlockingSyncWaiterCallbackPool.Put(cb)
}

var nonBlockingSyncWaiterCallbackPool = sync.Pool{
	New: func() interface{} { return new(nonBlockingSyncWaiterCallback) },
}

var logAppendPool = sync.Pool{
	New: func() interface{} {
		return new(struct {
			roachpb.Value
			enginepb.MVCCStats
		})
	},
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

	// NB: the Value and MVCCStats lifetime is this function, so we coalesce their
	// allocation into the same pool.
	// TODO(pavelkalinnikov): figure out why they escape into the heap, and find a
	// way to avoid the pool.
	v := logAppendPool.Get().(*struct {
		roachpb.Value
		enginepb.MVCCStats
	})
	defer logAppendPool.Put(v)
	value, diff := &v.Value, &v.MVCCStats
	value.RawBytes = value.RawBytes[:0]
	diff.Reset()

	opts := storage.MVCCWriteOptions{Stats: diff, Category: fs.ReplicationReadCategory}
	for i := range entries {
		ent := &entries[i]
		key := keys.RaftLogKeyFromPrefix(raftLogPrefix, kvpb.RaftIndex(ent.Index))

		if err := value.SetProto(ent); err != nil {
			return RaftState{}, err
		}
		value.InitChecksum(key)
		var err error
		if kvpb.RaftIndex(ent.Index) > prev.LastIndex {
			_, err = storage.MVCCBlindPut(ctx, rw, key, hlc.Timestamp{}, *value, opts)
		} else {
			_, err = storage.MVCCPut(ctx, rw, key, hlc.Timestamp{}, *value, opts)
		}
		if err != nil {
			return RaftState{}, err
		}
	}

	newLastIndex := kvpb.RaftIndex(entries[len(entries)-1].Index)
	// Delete any previously appended log entries which never committed.
	if prev.LastIndex > 0 {
		for i := newLastIndex + 1; i <= prev.LastIndex; i++ {
			// Note that the caller is in charge of deleting any sideloaded payloads
			// (which they must only do *after* the batch has committed).
			_, _, err := storage.MVCCDelete(ctx, rw, keys.RaftLogKeyFromPrefix(raftLogPrefix, i),
				hlc.Timestamp{}, opts)
			if err != nil {
				return RaftState{}, err
			}
		}
	}
	return RaftState{
		LastIndex: newLastIndex,
		LastTerm:  kvpb.RaftTerm(entries[len(entries)-1].Term),
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
	index kvpb.RaftIndex,
) (kvpb.RaftTerm, error) {
	entry, found := eCache.Get(rangeID, index)
	if found {
		return kvpb.RaftTerm(entry.Term), nil
	}

	reader := eng.NewReader(storage.StandardDurability)
	defer reader.Close()

	if err := raftlog.Visit(ctx, reader, rangeID, index, index+1, func(ent raftpb.Entry) error {
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
		if got, want := kvpb.RaftIndex(entry.Index), index; got != want {
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
		if !typ.IsSideloaded() {
			eCache.Add(rangeID, []raftpb.Entry{entry}, false /* truncate */)
		}
		return kvpb.RaftTerm(entry.Term), nil
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
	lo, hi kvpb.RaftIndex,
	maxBytes uint64,
) (_ []raftpb.Entry, _cachedSize uint64, _loadedSize uint64, _ error) {
	if lo > hi {
		return nil, 0, 0, errors.Errorf("lo:%d is greater than hi:%d", lo, hi)
	}

	n := hi - lo
	if n > 100 {
		n = 100
	}
	ents := make([]raftpb.Entry, 0, n)

	ents, cachedSize, hitIndex, exceededMaxBytes := eCache.Scan(ents, rangeID, lo, hi, maxBytes)

	// Return results if the correct number of results came back or if
	// we ran into the max bytes limit.
	if kvpb.RaftIndex(len(ents)) == hi-lo || exceededMaxBytes {
		return ents, cachedSize, 0, nil
	}

	combinedSize := cachedSize // size tracks total size of ents.

	// Scan over the log to find the requested entries in the range [lo, hi),
	// stopping once we have enough.
	expectedIndex := hitIndex

	scanFunc := func(ent raftpb.Entry) error {
		// Exit early if we have any gaps or it has been compacted.
		if kvpb.RaftIndex(ent.Index) != expectedIndex {
			return iterutil.StopIteration()
		}
		expectedIndex++

		typ, err := raftlog.EncodingOf(ent)
		if err != nil {
			return err
		}
		if typ.IsSideloaded() {
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
		combinedSize += uint64(ent.Size())
		if combinedSize > maxBytes {
			exceededMaxBytes = true
			if len(ents) == 0 { // make sure to return at least one entry
				ents = append(ents, ent)
			}
			return iterutil.StopIteration()
		}

		ents = append(ents, ent)
		return nil
	}

	reader := eng.NewReader(storage.StandardDurability)
	defer reader.Close()
	if err := raftlog.Visit(ctx, reader, rangeID, expectedIndex, hi, scanFunc); err != nil {
		return nil, 0, 0, err
	}
	eCache.Add(rangeID, ents, false /* truncate */)

	// Did the correct number of results come back? If so, we're all good.
	if kvpb.RaftIndex(len(ents)) == hi-lo {
		return ents, cachedSize, combinedSize - cachedSize, nil
	}

	// Did we hit the size limit? If so, return what we have.
	if exceededMaxBytes {
		return ents, cachedSize, combinedSize - cachedSize, nil
	}

	// Did we get any results at all? Because something went wrong.
	if len(ents) > 0 {
		// Was the missing index after the last index?
		lastIndex, err := rsl.LoadLastIndex(ctx, reader)
		if err != nil {
			return nil, 0, 0, err
		}
		if lastIndex <= expectedIndex {
			return nil, 0, 0, raft.ErrUnavailable
		}

		// We have a gap in the record, if so, return a nasty error.
		return nil, 0, 0, errors.Errorf("there is a gap in the index record between lo:%d and hi:%d at index:%d", lo, hi, expectedIndex)
	}

	// No results, was it due to unavailability or truncation?
	ts, err := rsl.LoadRaftTruncatedState(ctx, reader)
	if err != nil {
		return nil, 0, 0, err
	}
	if ts.Index >= lo {
		// The requested lo index has already been truncated.
		return nil, 0, 0, raft.ErrCompacted
	}
	// The requested lo index does not yet exist.
	return nil, 0, 0, raft.ErrUnavailable
}
