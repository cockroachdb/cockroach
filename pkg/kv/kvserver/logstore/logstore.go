// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package logstore implements the Raft log storage.
package logstore

import (
	"context"
	"math"
	"math/rand"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/crlib/crtime"
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

// raftLogSingleDeleteMode selects how UseRaftLogSingleDelete decides whether
// to use Pebble's SingleDelete for raft log entry deletes or regular point
// deletes.
type raftLogSingleDeleteMode int

const (
	// raftLogSingleDeleteDefault defers the choice to engine separation.
	raftLogSingleDeleteDefault raftLogSingleDeleteMode = iota
	// raftLogSingleDeleteEnabled forces SingleDelete on. Only for tests.
	raftLogSingleDeleteEnabled
	// raftLogSingleDeleteDisabled forces SingleDelete off. Only for tests.
	raftLogSingleDeleteDisabled
)

// raftLogSingleDelete controls UseRaftLogSingleDelete. It is intentionally
// unexported and not env-driven as it's dangerous to enable it without a proper
// migration process.
var raftLogSingleDelete = raftLogSingleDeleteDefault

// UseRaftLogSingleDelete reports whether to use Pebble's SingleDelete instead
// of regular point Delete when clearing individual raft log entries.
//
// SingleDelete is enabled unconditionally when engines are separated. This
// takes advantage of the fact that all clusters with separated engines (newly
// created or migrated) start from a fresh LogEngine in which the SingleDelete
// pre-condition invariant holds (no stacked puts).
// TODO(ibrahim): Remove this function once single deletions are the default for
// raft log.
func UseRaftLogSingleDelete(separated bool) bool {
	switch raftLogSingleDelete {
	case raftLogSingleDeleteEnabled:
		return true
	case raftLogSingleDeleteDisabled:
		return false
	default:
		return separated
	}
}

// TestingSetRaftLogSingleDelete forces UseRaftLogSingleDelete to return the
// given value regardless of engine separation. It returns a cleanup function
// that restores the previous setting; callers should defer it.
func TestingSetRaftLogSingleDelete(enabled bool) func() {
	prev := raftLogSingleDelete
	if enabled {
		raftLogSingleDelete = raftLogSingleDeleteEnabled
	} else {
		raftLogSingleDelete = raftLogSingleDeleteDisabled
	}
	return func() { raftLogSingleDelete = prev }
}

var enableNonBlockingRaftLogSync = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.raft_log.non_blocking_synchronization.enabled",
	"set to true to enable non-blocking synchronization on Raft log writes to "+
		"persistent storage. Setting to true does not risk data loss or data corruption "+
		"on server crashes, but can reduce write latency.",
	envutil.EnvOrDefaultBool("COCKROACH_ENABLE_RAFT_LOG_NON_BLOCKING_SYNCHRONIZATION", true),
)

// raftLogTruncationClearRangeThreshold is the number of entries at which Raft
// log truncation uses a Pebble range tombstone rather than point deletes. It is
// set high enough to avoid writing too many range tombstones to Pebble, but low
// enough that we don't do too many point deletes either (in particular, we
// don't want to overflow the Pebble write batch).
//
// In the steady state, Raft log truncation occurs when RaftLogQueueStaleSize
// (64 KB) or RaftLogQueueStaleThreshold (100 entries) is exceeded, so
// truncations are generally small. If followers are lagging, we let the log
// grow to RaftLogTruncationThreshold (16 MB) before truncating.
//
// 100k was chosen because it is unlikely to be hit in most common cases,
// keeping the number of range tombstones low, but will trigger when Raft logs
// have grown abnormally large. RaftLogTruncationThreshold will typically not
// trigger it, unless the average log entry is <= 160 bytes. The key size is ~16
// bytes, so Pebble point deletion batches will be bounded at ~1.6MB.
var raftLogTruncationClearRangeThreshold = kvpb.RaftIndex(metamorphic.ConstantWithTestRange(
	"raft-log-truncation-clearrange-threshold", 100000 /* default */, 1 /* min */, 1e6 /* max */))

// RaftState stores information about the last entry and the size of the log.
type RaftState struct {
	LastIndex kvpb.RaftIndex
	LastTerm  kvpb.RaftTerm
	ByteSize  int64
}

// EntryStats contains stats about the appended log slice.
type EntryStats struct {
	RegularEntries    int
	RegularBytes      int64
	SideloadedEntries int
	SideloadedBytes   int64
}

// Add increments the stats with the given delta.
func (e *EntryStats) Add(delta EntryStats) {
	e.RegularEntries += delta.RegularEntries
	e.RegularBytes += delta.RegularBytes
	e.SideloadedEntries += delta.SideloadedEntries
	e.SideloadedBytes += delta.SideloadedBytes
}

// AppendStats describes a completed log storage append operation.
type AppendStats struct {
	Begin crtime.Mono
	End   crtime.Mono

	EntryStats

	PebbleBegin crtime.Mono
	PebbleEnd   crtime.Mono
	PebbleBytes int64
	// Only set when !NonBlocking, which means almost never, since
	// kv.raft_log.non_blocking_synchronization.enabled defaults to true.
	PebbleCommitStats storage.BatchCommitStats

	Sync bool
	// If true, PebbleEnd-PebbleBegin does not include the sync time.
	NonBlocking bool
}

// WriteStats contains stats about a write to raft storage.
type WriteStats struct {
	CommitDur time.Duration
	storage.BatchCommitStats
}

// LogStore is a stub of a separated Raft log storage.
type LogStore struct {
	RangeID roachpb.RangeID
	Engine  storage.Engine
	// Separated is true iff the raft log engine is separated from the state
	// machine engine.
	Separated   bool
	Sideload    SideloadStorage
	StateLoader StateLoader // used only for writes under raftMu
	SyncWaiter  *SyncWaiterLoop
	Settings    *cluster.Settings

	DisableSyncLogWriteToss bool // for testing only
}

// SyncCallback is a callback that is notified when a raft log write has been
// durably committed to disk.
//
// The function is handed the struct containing messages that are associated
// with the MsgStorageAppend that triggered the fsync.
//
// commitStats is populated iff this was a non-blocking sync.
type SyncCallback interface {
	OnLogSync(context.Context, raft.StorageAppendAck, WriteStats)
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
	ctx context.Context, state RaftState, app raft.StorageAppend, cb SyncCallback, stats *AppendStats,
) (RaftState, error) {
	batch := newStoreEntriesBatch(s.Engine)
	return s.storeEntriesAndCommitBatch(ctx, state, app, cb, stats, batch)
}

// storeEntriesAndCommitBatch is like StoreEntries, but it accepts a
// storage.Batch, which it takes responsibility for committing and closing.
func (s *LogStore) storeEntriesAndCommitBatch(
	ctx context.Context,
	state RaftState,
	m raft.StorageAppend,
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
		stats.Begin = crtime.NowMono()
		// All of the entries are appended to distinct keys, returning a new
		// last index.
		thinEntries, entryStats, err := MaybeSideloadEntries(ctx, m.Entries, s.Sideload)
		if err != nil {
			const expl = "during sideloading"
			return RaftState{}, errors.Wrap(err, expl)
		}
		stats.EntryStats.Add(entryStats) // TODO(pav-kv): just return the stats.
		state.ByteSize += entryStats.SideloadedBytes
		if state, err = logAppend(
			ctx, s.StateLoader.RangeIDPrefixBuf, s.Engine, batch, state, thinEntries,
			UseRaftLogSingleDelete(s.Separated),
		); err != nil {
			const expl = "during append"
			return RaftState{}, errors.Wrap(err, expl)
		}
		stats.End = crtime.NowMono()
	}

	if err := storeHardState(ctx, batch, s.StateLoader, m.HardState); err != nil {
		return RaftState{}, err
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
	stats.PebbleBegin = crtime.NowMono()
	stats.PebbleBytes = int64(batch.Len())
	// We want a timely sync in two cases:
	//	1. Raft has requested one, with MustSync(). This usually means there are
	// messages to send to the proposer (MsgVoteResp/MsgAppResp, etc.) conditional
	// on this write being durable. These messages are on the critical path for
	// raft to make progress, e.g. elect/fortify leader or commit entries.
	//	2. The log append overwrites a suffix of the log. There can be sideloaded
	// entry files to remove as a result, so we sync Pebble before doing that. The
	// sync is blocking for convenience, but we could instead wait for sync
	// elsewhere, and remove the files asynchronously. There are some limitations
	// today preventing this, see #136416.
	wantsSync := m.MustSync() || overwriting
	willSync := wantsSync && !DisableSyncRaftLog.Get(&s.Settings.SV)
	// Use the non-blocking log sync path if we are performing a log sync ...
	nonBlockingSync := willSync &&
		// and the cluster setting is enabled ...
		enableNonBlockingRaftLogSync.Get(&s.Settings.SV) &&
		// and we are not overwriting any previous log entries. If we are
		// overwriting, we may need to purge the sideloaded SSTables associated with
		// overwritten entries. This must be performed after the corresponding
		// entries are durably replaced and it's easier to ensure proper ordering
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
		stats.PebbleEnd = crtime.NowMono()
		// Instead, enqueue that waiting on the SyncWaiterLoop, who will signal the
		// callback when the write completes.
		waiterCallback := nonBlockingSyncWaiterCallbackPool.Get().(*nonBlockingSyncWaiterCallback)
		*waiterCallback = nonBlockingSyncWaiterCallback{
			ctx:            ctx,
			cb:             cb,
			onDone:         m.Ack(),
			batch:          batch,
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
		stats.PebbleEnd = crtime.NowMono()
		stats.PebbleCommitStats = batch.CommitStats()
		if wantsSync {
			commitDur := stats.PebbleEnd.Sub(stats.PebbleBegin)
			cb.OnLogSync(ctx, m.Ack(), WriteStats{CommitDur: commitDur})
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

	return state, nil
}

// nonBlockingSyncWaiterCallback packages up the callback that is handed to the
// SyncWaiterLoop during a non-blocking Raft log sync. Structuring the callback
// as a struct with a method instead of an anonymous function avoids individual
// fields escaping to the heap. It also provides the opportunity to pool the
// callback.
type nonBlockingSyncWaiterCallback struct {
	// Used to run SyncCallback.
	ctx    context.Context
	cb     SyncCallback
	onDone raft.StorageAppendAck
	// Used to extract stats. This is the batch that has been synced.
	batch storage.WriteBatch
	// Used to measure raft storage write/sync latency.
	logCommitBegin crtime.Mono
}

// run is the callback's logic. It is executed on the SyncWaiterLoop goroutine.
func (cb *nonBlockingSyncWaiterCallback) run() {
	cb.cb.OnLogSync(cb.ctx, cb.onDone, WriteStats{
		CommitDur:        cb.logCommitBegin.Elapsed(),
		BatchCommitStats: cb.batch.CommitStats(),
	})
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

func storeHardState(
	ctx context.Context, w storage.Writer, sl StateLoader, hs raftpb.HardState,
) error {
	if raft.IsEmptyHardState(hs) {
		return nil
	}
	// NB: Note that without additional safeguards, it's incorrect to write the
	// HardState before appending m.Entries. When catching up, a follower will
	// receive Entries that are immediately Committed in the same Ready. If we
	// persist the HardState but happen to lose the Entries, assertions can be
	// tripped.
	//
	// We have both in the same batch, so there's no problem. If that ever
	// changes, we must write and sync the Entries before the HardState.
	if err := sl.SetHardState(ctx, w, hs); err != nil {
		return errors.Wrap(err, "during SetHardState")
	}
	return nil
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
	prefixBuf keys.RangeIDPrefixBuf,
	eng storage.Engine, // only used to create a read-only batch if needed
	w storage.Writer,
	prev RaftState,
	entries []raftpb.Entry,
	enginesSeparated bool,
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

	newFirst := kvpb.RaftIndex(entries[0].Index)
	if newFirst == 0 {
		// No raft entry should have index 0.
		return RaftState{}, errors.AssertionFailedf("raft entry index must be >= 1")
	}
	newLast := kvpb.RaftIndex(entries[len(entries)-1].Index)
	useSingleDelete := UseRaftLogSingleDelete(enginesSeparated)
	overlapping := newFirst <= prev.LastIndex

	if util.RaceEnabled {
		// In race builds, assert that there are no unexpected pre-existing
		// raft log entries. This is important as we use single-delete for
		// log truncation, and it relies on not having multiple PUTs for
		// one key not interleaved by deletes.
		r := eng.NewReader(storage.StandardDurability)
		defer r.Close()
		if hi, err := EmptyLogRange(ctx, r, prefixBuf, prev.LastIndex /* lo */, math.MaxUint64 /* hi */); err != nil {
			return RaftState{}, err
		} else if uint64(hi) != math.MaxUint64 {
			return RaftState{}, errors.AssertionFailedf("unexpected raft log entry at index: %d when appending", hi+1)
		}
	}

	// In the common case, the new entries don't overlap the existing log and are
	// appended to its end. If there is an overlap, the [newFirst, prev.LastIndex]
	// span is overwritten by the [newFirst, newLast] suffix.
	//
	// TODO(ibrahim): both ClearRangeSizeKnown calls below pass MaxInt to force
	// point deletes (no Pebble range tombstones). Worth investigating whether a
	// lower threshold is appropriate for large overwrites/tail-drops.
	if overlapping {
		// Subtract [newFirst, prev.LastIndex] from the log size stats.
		startKey, endKey := raftLogBounds(prefixBuf, newFirst-1, prev.LastIndex)
		reader := eng.NewReader(storage.StandardDurability)
		defer reader.Close()
		ms, err := storage.ComputeStats(
			ctx, reader, fs.ReplicationReadCategory, startKey, endKey, 0, /* nowNanos */
		)
		if err != nil {
			return RaftState{}, err
		}
		// Inline raft log entries only contribute to Sys{Bytes,Count}.
		diff.SysBytes -= ms.SysBytes
		diff.SysCount -= ms.SysCount

		// When using SingleDelete, explicitly delete the old suffix, because
		// SingleDelete requires there to be no double PUT of the same key. If not
		// using SingleDelete, the MVCCBlindPut below will overwrite those entries,
		// and another ClearRangeSizeKnown will remove the (newLast, prev.LastIndex]
		// remainder, if any.
		if useSingleDelete {
			if err := ClearRangeSizeKnown(
				w, prefixBuf, newFirst-1, prev.LastIndex, math.MaxInt, true, /* maybeUseSingleDel */
			); err != nil {
				return RaftState{}, err
			}
		}
	}

	opts := storage.MVCCWriteOptions{Stats: diff, Category: fs.ReplicationReadCategory}
	// NB: each iteration consumes its key before the next RaftLogKeyFromPrefix
	// call rewrites the bytes after raftLogPrefix.
	raftLogPrefix := prefixBuf.RaftLogPrefix()
	for i := range entries {
		ent := &entries[i]
		key := keys.RaftLogKeyFromPrefix(raftLogPrefix, kvpb.RaftIndex(ent.Index))
		if err := value.SetProto(ent); err != nil {
			return RaftState{}, err
		}
		value.InitChecksum(key)
		if _, err := storage.MVCCBlindPut(ctx, w, key, hlc.Timestamp{}, *value, opts); err != nil {
			return RaftState{}, err
		}
	}

	if overlapping && !useSingleDelete {
		// Truncate the log suffix not covered by the new entries.
		// No-op if newLast >= prev.LastIndex.
		if err := ClearRangeSizeKnown(
			w, prefixBuf, newLast, prev.LastIndex, math.MaxInt, false, /* maybeUseSingleDel */
		); err != nil {
			return RaftState{}, err
		}
	}

	return RaftState{
		LastIndex: newLast,
		LastTerm:  kvpb.RaftTerm(entries[len(entries)-1].Term),
		ByteSize:  prev.ByteSize + diff.SysBytes,
	}, nil
}

// Compact prepares a write that removes entries (prev.Index, next.Index] from
// the raft log, and updates the truncated state to the next one. Does nothing
// if the interval is empty.
//
// TODO(#136109): make this a method of a write-through LogStore data structure,
// which is aware of the current log state.
func Compact(
	ctx context.Context,
	prev kvserverpb.RaftTruncatedState,
	next kvserverpb.RaftTruncatedState,
	loader StateLoader,
	writer storage.Writer,
	enginesSeparated bool,
) error {
	if next.Index <= prev.Index {
		// TODO(pav-kv): return an assertion failure error.
		return nil
	}
	// Truncate the Raft log from the entry after the previous truncation index to
	// the new truncation index. This is performed atomically with updating the
	// RaftTruncatedState so that the state of the log is consistent.
	if err := ClearRangeSizeKnown(
		writer, loader.RangeIDPrefixBuf,
		prev.Index, next.Index, int(raftLogTruncationClearRangeThreshold),
		UseRaftLogSingleDelete(enginesSeparated),
	); err != nil {
		return errors.Wrapf(err,
			"unable to clear truncated Raft entries for %+v after index %d",
			next, prev.Index)
	}

	key := loader.RaftTruncatedStateKey()
	var value roachpb.Value
	if _, err := next.MarshalToSizedBuffer(value.AllocBytes(next.Size())); err != nil {
		return err
	}
	value.InitChecksum(key)

	if _, err := storage.MVCCBlindPut(
		ctx, writer, key, hlc.Timestamp{}, value, storage.MVCCWriteOptions{},
	); err != nil {
		return errors.Wrap(err, "unable to write RaftTruncatedState")
	}
	return nil
}

// ClearRange clears raft log entries in the range (lo, hi]. It calls
// storage.ClearRangeWithHeuristic() which scans up to pointKeyThreshold keys to
// choose between point deletes and a single Pebble range tombstone.
// No-op if lo >= hi. Uses point deletes when number of entries < pointKeyThreshold,
// or a range deletion otherwise.
func ClearRange(
	ctx context.Context,
	r storage.Reader,
	w storage.Writer,
	prefixBuf keys.RangeIDPrefixBuf,
	lo, hi kvpb.RaftIndex,
	pointKeyThreshold int,
) error {
	if lo >= hi {
		return nil
	}
	start, end := raftLogBounds(prefixBuf, lo, hi)
	return storage.ClearRangeWithHeuristic(ctx, r, w, start, end, pointKeyThreshold)
}

// ClearRangeSizeKnown clears raft log entries in range (lo, hi] when the caller
// already knows how many entries the range contains. Uses point deletes
// when hi-lo < pointKeyThreshold, or a range deletion otherwise.
// No-op if lo >= hi.
//
// If maybeUseSingleDel is true, it uses SingleDelete instead of regular Delete
// if the size is <= pointKeyThreshold.
func ClearRangeSizeKnown(
	w storage.Writer,
	prefixBuf keys.RangeIDPrefixBuf,
	lo, hi kvpb.RaftIndex,
	pointKeyThreshold int,
	maybeUseSingleDel bool,
) error {
	if lo >= hi {
		return nil
	}
	if hi-lo >= kvpb.RaftIndex(pointKeyThreshold) {
		start, end := raftLogBounds(prefixBuf, lo, hi)
		return w.ClearRawRange(start, end, true /* pointKeys */, false /* rangeKeys */)
	}
	raftLogPrefix := prefixBuf.RaftLogPrefix()
	// NB: each iteration consumes a key before the next call mutates the bytes
	// after raftLogPrefix.
	// NB: avoid overflow in the unlikely case hi == MaxUint64.
	for idx := lo; idx < hi; idx++ {
		key := keys.RaftLogKeyFromPrefix(raftLogPrefix, idx+1)
		var err error
		if maybeUseSingleDel {
			err = w.SingleClearUnversioned(key)
		} else {
			err = w.ClearUnversioned(key, storage.ClearOptions{})
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// raftLogBounds converts (lo, hi] to their roachpb keys. The sentinels lo == 0
// and hi == math.MaxUint64 expand to raftLogPrefix and raftLogPrefix.PrefixEnd
// respectively.
//
// The returned keys are independent of prefixBuf's underlying buffer:
// keys.RaftLogKeyFromPrefix returns a slice that aliases the bytes immediately
// after the raft log prefix, so building two bounds from the same prefix
// without cloning would have the second call clobber the first.
func raftLogBounds(
	prefixBuf keys.RangeIDPrefixBuf, lo, hi kvpb.RaftIndex,
) (start, end roachpb.Key) {
	raftLogPrefix := prefixBuf.RaftLogPrefix()
	if lo == 0 {
		start = raftLogPrefix.Clone()
	} else {
		start = keys.RaftLogKeyFromPrefix(raftLogPrefix, lo+1).Clone()
	}
	if hi == math.MaxUint64 {
		end = raftLogPrefix.PrefixEnd()
	} else {
		end = keys.RaftLogKeyFromPrefix(raftLogPrefix, hi+1).Clone()
	}
	return start, end
}

// EmptyLogRange returns the index preceding the first existing raft log entry
// between (lo, hi]. Returns hi if the entire span is empty.
func EmptyLogRange(
	ctx context.Context,
	r storage.Reader,
	prefixBuf keys.RangeIDPrefixBuf,
	lo kvpb.RaftIndex,
	hi kvpb.RaftIndex,
) (kvpb.RaftIndex, error) {
	if lo >= hi {
		return hi, nil // no-op
	}
	pref := prefixBuf.RaftLogPrefix()
	end := keys.RaftLogKeyFromPrefix(pref, hi).Next().Clone()
	start := keys.RaftLogKeyFromPrefix(pref, lo).Next()
	iter, err := r.NewEngineIterator(ctx, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	ok, err := iter.SeekEngineKeyGE(storage.EngineKey{Key: start})
	if err != nil || !ok {
		return hi, err // error or not found
	}
	key, err := iter.UnsafeEngineKey()
	if err != nil {
		return 0, err
	}
	firstIndex, err := keys.DecodeRaftLogKeyFromSuffix(key.Key[len(pref):])
	if err != nil {
		return 0, err
	}
	if firstIndex <= lo || firstIndex > hi {
		return 0, errors.AssertionFailedf("firstIndex %d not in (%d,%d]", firstIndex, lo, hi)
	}
	return firstIndex - 1, nil
}

// ComputeSize computes the size (in bytes) of the raft log from the storage
// engine. This will iterate over the raft log and sideloaded files, so
// depending on the size of these it can be mildly to extremely expensive and
// thus should not be called frequently.
//
// TODO(#136358): we should be able to maintain this size incrementally, and not
// need scanning the log to re-compute it.
func (s *LogStore) ComputeSize(ctx context.Context) (int64, error) {
	prefix := keys.RaftLogPrefix(s.RangeID)
	prefixEnd := prefix.PrefixEnd()
	ms, err := storage.ComputeStats(ctx, s.Engine, fs.ReplicationReadCategory,
		prefix, prefixEnd, 0 /* nowNanos */)
	if err != nil {
		return 0, err
	}
	_, totalSideloaded, err := s.Sideload.Stats(ctx, kvpb.RaftSpan{Last: math.MaxUint64})
	if err != nil {
		return 0, err
	}
	return ms.SysBytes + totalSideloaded, nil
}

// LoadEntry loads the entry at the given index for the specified range. If the
// entry is sideloaded, it is not expanded. The valid range for the index is
// (Compacted, LastIndex].
//
// An error returned means that either the entry is not found, or it could not
// be parsed. The caller is expected to check the log bounds before this call,
// to exclude the valid "not found" cases.
func LoadEntry(
	ctx context.Context, eng storage.Engine, rangeID roachpb.RangeID, index kvpb.RaftIndex,
) (raftpb.Entry, error) {
	reader := eng.NewReader(storage.StandardDurability)
	defer reader.Close()

	entry, found := raftpb.Entry{}, false
	if err := raftlog.Visit(ctx, reader, rangeID, index, index+1, func(ent raftpb.Entry) error {
		if found {
			return errors.Errorf("found more than one entry in [%d,%d)", index, index+1)
		}
		entry, found = ent, true
		return nil
	}); err != nil {
		return raftpb.Entry{}, err
	} else if !found {
		return raftpb.Entry{}, errors.Errorf("entry #%d not found", index)
	} else if got, want := kvpb.RaftIndex(entry.Index), index; got != want {
		return raftpb.Entry{}, errors.Errorf("there is a gap at index %d, found entry #%d", want, got)
	}
	return entry, nil
}

// LoadEntries loads a slice of consecutive log entries in [lo, hi), starting
// from lo. It inlines the sideloaded entries, and caches all the loaded
// entries. The size of the returned entries does not exceed maxSize, unless the
// first entry exceeds the limit (in which case it is returned regardless).
//
// The valid range for lo/hi is: Compacted < lo <= hi <= LastIndex+1. The caller
// should check the bounds before making this call.
//
// An error returned means that either an entry in the indices span is not
// found, or it could not be parsed. Since the caller checks the boundaries, an
// error is generally unexpected and means something bad.
//
// The bytesAccount is used to account for and limit the loaded bytes. It can be
// nil when the accounting / limiting is not needed.
//
// TODO(pavelkalinnikov): return all entries we've read, consider maxSize a
// target size. Currently we may read one extra entry and drop it.
func LoadEntries(
	ctx context.Context,
	eng storage.Engine,
	rangeID roachpb.RangeID,
	eCache *raftentry.Cache, // TODO(#145562): this should be the caller's concern
	sideloaded SideloadStorage,
	lo, hi kvpb.RaftIndex,
	maxBytes uint64,
	account *BytesAccount,
) (_ []raftpb.Entry, _cachedSize uint64, _loadedSize uint64, _ error) {
	if lo > hi {
		return nil, 0, 0, errors.Errorf("lo:%d is greater than hi:%d", lo, hi)
	}

	ents := make([]raftpb.Entry, 0, min(hi-lo, 100))
	ents, _, hitIndex, _ := eCache.Scan(ents, rangeID, lo, hi, maxBytes)

	// TODO(pav-kv): pass the sizeHelper to eCache.Scan above, to avoid scanning
	// the same entries twice, and computing their sizes.
	sh := sizeHelper{maxBytes: maxBytes, account: account}
	for i, entry := range ents {
		if sh.done || !sh.add(uint64(entry.Size())) {
			// Remove the remaining entries, and dereference the memory they hold.
			ents = slices.Delete(ents, i, len(ents))
			break
		}
	}
	// NB: if we couldn't get quota for all cached entries, return only a prefix
	// for which we got it. Even though all the cached entries are already in
	// memory, returning all of them would increase their lifetime, incur size
	// amplification when processing them, and risk reaching out-of-memory state.
	cachedSize := sh.bytes
	// Return results if the correct number of results came back, or we ran into
	// the max bytes limit, or reached the memory budget limit.
	if len(ents) == int(hi-lo) || sh.done {
		return ents, cachedSize, 0, nil
	}

	// Scan over the log to find the requested entries in the range [lo, hi),
	// stopping once we have enough.
	expectedIndex := hitIndex

	scanFunc := func(ent raftpb.Entry) error {
		// Exit early if we have any gaps or it has been compacted.
		if kvpb.RaftIndex(ent.Index) != expectedIndex {
			return iterutil.StopIteration()
		}
		expectedIndex++

		if typ, _, err := raftlog.EncodingOf(ent); err != nil {
			return err
		} else if typ.IsSideloaded() {
			if ent, err = MaybeInlineSideloadedRaftCommand(
				ctx, rangeID, ent, sideloaded, eCache,
			); err != nil {
				return err
			}
		}

		if sh.add(uint64(ent.Size())) {
			ents = append(ents, ent)
		}
		if sh.done {
			return iterutil.StopIteration()
		}
		return nil
	}

	reader := eng.NewReader(storage.StandardDurability)
	defer reader.Close()
	if err := raftlog.Visit(ctx, reader, rangeID, expectedIndex, hi, scanFunc); err != nil {
		return nil, 0, 0, err
	}
	eCache.Add(rangeID, ents, false /* truncate */)

	// Did the correct number of results come back? If so, we're all good.
	// Did we hit the size limits? If so, return what we have.
	if len(ents) == int(hi-lo) || sh.done {
		return ents, cachedSize, sh.bytes - cachedSize, nil
	}

	// Something went wrong, and we could not load enough entries. We either have
	// a gap in the log, or hi > LastIndex+1. Let the caller distinguish if they
	// need to.
	return nil, 0, 0, raft.ErrUnavailable
}
