// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package logstore implements the Raft log storage.
package logstore

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"

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
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
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

// MsgStorageAppend is a raftpb.Message with type MsgStorageAppend.
type MsgStorageAppend raftpb.Message

// MakeMsgStorageAppend constructs a MsgStorageAppend from a raftpb.Message.
func MakeMsgStorageAppend(m raftpb.Message) MsgStorageAppend {
	if m.Type != raftpb.MsgStorageAppend {
		panic(fmt.Sprintf("unexpected message type %s", m.Type))
	}
	return MsgStorageAppend(m)
}

// HardState returns the hard state assembled from the message.
func (m *MsgStorageAppend) HardState() raftpb.HardState {
	return raftpb.HardState{
		Term:      m.Term,
		Vote:      m.Vote,
		Commit:    m.Commit,
		Lead:      m.Lead,
		LeadEpoch: m.LeadEpoch,
	}
}

// MustSync returns true if this storage write must be synced.
func (m *MsgStorageAppend) MustSync() bool {
	return len(m.Responses) != 0
}

// OnDone returns the storage write post-processing information.
func (m *MsgStorageAppend) OnDone() MsgStorageAppendDone { return m.Responses }

// MsgStorageAppendDone encapsulates the actions to do after MsgStorageAppend is
// done, such as sending messages back to raft node and its peers.
type MsgStorageAppendDone []raftpb.Message

// Responses returns the messages to send after the write/sync is completed.
func (m MsgStorageAppendDone) Responses() []raftpb.Message { return m }

// Mark returns the LogMark of the raft log in storage after the write/sync is
// completed. Returns zero value if the write does not update the log mark.
func (m MsgStorageAppendDone) Mark() raft.LogMark {
	if len(m) == 0 {
		return raft.LogMark{}
	}
	// Optimization: the MsgStorageAppendResp message, if any, is always the last
	// one in the list.
	// TODO(pav-kv): this is an undocumented API quirk. Refactor the raft write
	// API to be more digestible outside the package.
	if buildutil.CrdbTestBuild {
		for _, msg := range m[:len(m)-1] {
			if msg.Type == raftpb.MsgStorageAppendResp {
				panic("unexpected MsgStorageAppendResp not in last position")
			}
		}
	}
	if msg := m[len(m)-1]; msg.Type != raftpb.MsgStorageAppendResp {
		return raft.LogMark{}
	} else if msg.Index != 0 {
		return raft.LogMark{Term: msg.LogTerm, Index: msg.Index}
	} else if msg.Snapshot != nil {
		return raft.LogMark{Term: msg.LogTerm, Index: msg.Snapshot.Metadata.Index}
	}
	return raft.LogMark{}
}

// RaftState stores information about the last entry and the size of the log.
type RaftState struct {
	LastIndex kvpb.RaftIndex
	LastTerm  kvpb.RaftTerm
	ByteSize  int64
}

// AppendStats describes a completed log storage append operation.
type AppendStats struct {
	Begin crtime.Mono
	End   crtime.Mono

	RegularEntries    int
	RegularBytes      int64
	SideloadedEntries int
	SideloadedBytes   int64

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

// Metrics contains metrics specific to the log storage.
type Metrics struct {
	RaftLogCommitLatency metric.IHistogram
}

// LogStore is a stub of a separated Raft log storage.
type LogStore struct {
	RangeID     roachpb.RangeID
	Engine      storage.Engine
	Sideload    SideloadStorage
	StateLoader StateLoader // used only for writes under raftMu
	SyncWaiter  *SyncWaiterLoop
	EntryCache  *raftentry.Cache
	Settings    *cluster.Settings
	Metrics     Metrics

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
	OnLogSync(context.Context, MsgStorageAppendDone, storage.BatchCommitStats)
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
		stats.Begin = crtime.NowMono()
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
		stats.End = crtime.NowMono()
	}

	if hs := m.HardState(); !raft.IsEmptyHardState(hs) {
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
	stats.PebbleBegin = crtime.NowMono()
	stats.PebbleBytes = int64(batch.Len())
	wantsSync := m.MustSync()
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
		stats.PebbleEnd = crtime.NowMono()
		// Instead, enqueue that waiting on the SyncWaiterLoop, who will signal the
		// callback when the write completes.
		waiterCallback := nonBlockingSyncWaiterCallbackPool.Get().(*nonBlockingSyncWaiterCallback)
		*waiterCallback = nonBlockingSyncWaiterCallback{
			ctx:            ctx,
			cb:             cb,
			onDone:         m.OnDone(),
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
		stats.PebbleEnd = crtime.NowMono()
		stats.PebbleCommitStats = batch.CommitStats()
		if wantsSync {
			logCommitEnd := stats.PebbleEnd
			s.Metrics.RaftLogCommitLatency.RecordValue(logCommitEnd.Sub(stats.PebbleBegin).Nanoseconds())
			cb.OnLogSync(ctx, m.OnDone(), storage.BatchCommitStats{})
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
	ctx    context.Context
	cb     SyncCallback
	onDone MsgStorageAppendDone
	// Used to extract stats. This is the batch that has been synced.
	batch storage.WriteBatch
	// Used to record Metrics.
	metrics        Metrics
	logCommitBegin crtime.Mono
}

// run is the callback's logic. It is executed on the SyncWaiterLoop goroutine.
func (cb *nonBlockingSyncWaiterCallback) run() {
	dur := cb.logCommitBegin.Elapsed().Nanoseconds()
	cb.metrics.RaftLogCommitLatency.RecordValue(dur)
	commitStats := cb.batch.CommitStats()
	cb.cb.OnLogSync(cb.ctx, cb.onDone, commitStats)
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

// Compact constructs a write that compacts the raft log from
// currentTruncatedState up to suggestedTruncatedState. Returns (true, nil) iff
// the compaction can proceed, and the write has been constructed.
//
// TODO(#136109): make this a method of a write-through LogStore data structure,
// which is aware of the current log state.
func Compact(
	ctx context.Context,
	currentTruncatedState kvserverpb.RaftTruncatedState,
	suggestedTruncatedState *kvserverpb.RaftTruncatedState,
	loader StateLoader,
	readWriter storage.ReadWriter,
) (apply bool, _ error) {
	if suggestedTruncatedState.Index <= currentTruncatedState.Index {
		// The suggested truncated state moves us backwards; instruct the caller to
		// not update the in-memory state.
		return false, nil
	}

	// Truncate the Raft log from the entry after the previous truncation index to
	// the new truncation index. This is performed atomically with the raft
	// command application so that the TruncatedState index is always consistent
	// with the state of the Raft log itself.
	prefixBuf := &loader.RangeIDPrefixBuf
	numTruncatedEntries := suggestedTruncatedState.Index - currentTruncatedState.Index
	if numTruncatedEntries >= raftLogTruncationClearRangeThreshold {
		start := prefixBuf.RaftLogKey(currentTruncatedState.Index + 1).Clone()
		end := prefixBuf.RaftLogKey(suggestedTruncatedState.Index + 1).Clone() // end is exclusive
		if err := readWriter.ClearRawRange(start, end, true, false); err != nil {
			return false, errors.Wrapf(err,
				"unable to clear truncated Raft entries for %+v between indexes %d-%d",
				suggestedTruncatedState, currentTruncatedState.Index+1, suggestedTruncatedState.Index+1)
		}
	} else {
		// NB: RangeIDPrefixBufs have sufficient capacity (32 bytes) to avoid
		// allocating when constructing Raft log keys (16 bytes).
		prefix := prefixBuf.RaftLogPrefix()
		for idx := currentTruncatedState.Index + 1; idx <= suggestedTruncatedState.Index; idx++ {
			if err := readWriter.ClearUnversioned(
				keys.RaftLogKeyFromPrefix(prefix, idx),
				storage.ClearOptions{},
			); err != nil {
				return false, errors.Wrapf(err, "unable to clear truncated Raft entries for %+v at index %d",
					suggestedTruncatedState, idx)
			}
		}
	}

	// The suggested truncated state moves us forward; apply it and tell the
	// caller as much.
	if err := storage.MVCCPutProto(
		ctx,
		readWriter,
		prefixBuf.RaftTruncatedStateKey(),
		hlc.Timestamp{},
		suggestedTruncatedState,
		storage.MVCCWriteOptions{Category: fs.ReplicationReadCategory},
	); err != nil {
		return false, errors.Wrap(err, "unable to write RaftTruncatedState")
	}

	return true, nil
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
	ms, err := storage.ComputeStats(ctx, s.Engine, prefix, prefixEnd, 0 /* nowNanos */)
	if err != nil {
		return 0, err
	}
	// The remaining bytes if one were to truncate [0, 0) gives us the total
	// number of bytes in sideloaded files.
	_, totalSideloaded, err := s.Sideload.BytesIfTruncatedFromTo(ctx, 0, 0)
	if err != nil {
		return 0, err
	}
	return ms.SysBytes + totalSideloaded, nil
}

// LoadTerm returns the term of the entry at the given index for the specified
// range. The result is loaded from the storage engine if it's not in the cache.
// The valid range for index is [FirstIndex-1, LastIndex].
//
// There are 3 cases for when the term is not found: (1) the index has been
// compacted away, (2) index > LastIndex, or (3) there is a gap in the log. In
// the first case, we return ErrCompacted, and ErrUnavailable otherwise. Most
// callers never try to read indices above LastIndex, so an error means (3)
// which is a serious issue. But if the caller is unsure, they can check the
// LastIndex to distinguish.
//
// TODO(#132114): eliminate both ErrCompacted and ErrUnavailable.
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
		typ, _, err := raftlog.EncodingOf(entry)
		if err != nil {
			return 0, err
		}
		if !typ.IsSideloaded() {
			eCache.Add(rangeID, []raftpb.Entry{entry}, false /* truncate */)
		}
		return kvpb.RaftTerm(entry.Term), nil
	}

	// Otherwise, the entry at the given index is not found. See the function
	// comment for how this case is handled.
	ts, err := rsl.LoadRaftTruncatedState(ctx, reader)
	if err != nil {
		return 0, err
	} else if index == ts.Index {
		return ts.Term, nil
	} else if index < ts.Index {
		return 0, raft.ErrCompacted
	}
	return 0, raft.ErrUnavailable
}

// LoadEntries loads a slice of consecutive log entries in [lo, hi), starting
// from lo. It inlines the sideloaded entries, and caches all the loaded
// entries. The size of the returned entries does not exceed maxSize, unless the
// first entry exceeds the limit (in which case it is returned regardless).
//
// The valid range for lo/hi is: FirstIndex <= lo <= hi <= LastIndex+1.
//
// There are 3 cases for when an entry is not found: (1) the lo index has been
// compacted away, (2) hi > LastIndex+1, or (3) there is a gap in the log. In
// the first case, we return ErrCompacted, and ErrUnavailable otherwise. Most
// callers never try to read indices above LastIndex, so an error means (3)
// which is a serious issue. But if the caller is unsure, they can check the
// LastIndex to distinguish.
//
// TODO(#132114): eliminate both ErrCompacted and ErrUnavailable.
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
	account *BytesAccount,
) (_ []raftpb.Entry, _cachedSize uint64, _loadedSize uint64, _ error) {
	if lo > hi {
		return nil, 0, 0, errors.Errorf("lo:%d is greater than hi:%d", lo, hi)
	}

	n := hi - lo
	if n > 100 {
		n = 100
	}
	ents := make([]raftpb.Entry, 0, n)
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

		typ, _, err := raftlog.EncodingOf(ent)
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

	// Something went wrong, and we could not load enough entries.
	ts, err := rsl.LoadRaftTruncatedState(ctx, reader)
	if err != nil {
		return nil, 0, 0, err
	} else if lo <= ts.Index {
		// The requested lo index has already been truncated.
		return nil, 0, 0, raft.ErrCompacted
	}
	// We either have a gap in the log, or hi > LastIndex. Let the caller
	// distinguish if they need to.
	return nil, 0, 0, raft.ErrUnavailable
}
