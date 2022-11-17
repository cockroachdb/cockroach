// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logstore

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// RaftLogStore implements the log storage part of raft.Storage interface.
type RaftLogStore struct {
}

// All calls to raft.RawNode require that both Replica.raftMu and
// Replica.mu are held. All of the functions exposed via the
// raft.Storage interface will in turn be called from RawNode, so none
// of these methods may acquire either lock, but they may require
// their caller to hold one or both locks (even though they do not
// follow our "Locked" naming convention). Specific locking
// requirements (e.g. whether r.mu must be held for reading or writing)
// are noted in each method's comments.
//
// Many of the methods defined in this file are wrappers around static
// functions. This is done to facilitate their use from
// Replica.Snapshot(), where it is important that all the data that
// goes into the snapshot comes from a consistent view of the
// database, and not the replica's in-memory state or via a reference
// to Replica.store.Engine().

// InitialState implements the raft.Storage interface.
// InitialState requires that r.mu is held for writing because it requires
// exclusive access to r.mu.stateLoader.
func (r *RaftLogStore) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	ctx := r.AnnotateCtx(context.TODO())
	hs, err := r.mu.stateLoader.LoadHardState(ctx, r.store.Engine())
	// For uninitialized ranges, membership is unknown at this point.
	if raft.IsEmptyHardState(hs) || err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}
	cs := r.mu.state.Desc.Replicas().ConfState()
	return hs, cs, nil
}

// Entries implements the raft.Storage interface. Note that maxBytes is advisory
// and this method will always return at least one entry even if it exceeds
// maxBytes. Sideloaded proposals count towards maxBytes with their payloads inlined.
// Entries requires that r.mu is held for writing because it requires exclusive
// access to r.mu.stateLoader.
func (r *RaftLogStore) Entries(lo, hi, maxBytes uint64) ([]raftpb.Entry, error) {
	readonly := r.store.Engine().NewReadOnly(storage.StandardDurability)
	defer readonly.Close()
	ctx := r.AnnotateCtx(context.TODO())
	if r.raftMu.sideloaded == nil {
		return nil, errors.New("sideloaded storage is uninitialized")
	}
	return entries(ctx, r.mu.stateLoader, readonly, r.RangeID, r.store.raftEntryCache,
		r.raftMu.sideloaded, lo, hi, maxBytes)
}

// entries retrieves entries from the engine. To accommodate loading the term,
// `sideloaded` can be supplied as nil, in which case sideloaded entries will
// not be inlined, the raft entry cache will not be populated with *any* of the
// loaded entries, and maxBytes will not be applied to the payloads.
func entries(
	ctx context.Context,
	rsl stateloader.StateLoader,
	reader storage.Reader,
	rangeID roachpb.RangeID,
	eCache *raftentry.Cache,
	sideloaded logstore.SideloadStorage,
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

	// Whether we can populate the Raft entries cache. False if we found a
	// sideloaded proposal, but the caller didn't give us a sideloaded storage.
	canCache := true

	scanFunc := func(ent raftpb.Entry) error {
		// Exit early if we have any gaps or it has been compacted.
		if ent.Index != expectedIndex {
			return iterutil.StopIteration()
		}
		expectedIndex++

		if logstore.SniffSideloadedRaftCommand(ent.Data) {
			canCache = canCache && sideloaded != nil
			if sideloaded != nil {
				newEnt, err := logstore.MaybeInlineSideloadedRaftCommand(
					ctx, rangeID, ent, sideloaded, eCache,
				)
				if err != nil {
					return err
				}
				if newEnt != nil {
					ent = *newEnt
				}
			}
		}

		// Note that we track the size of proposals with payloads inlined.
		size += uint64(ent.Size())
		if size > maxBytes {
			exceededMaxBytes = true
			if len(ents) > 0 {
				if exceededMaxBytes {
					return iterutil.StopIteration()
				}
				return nil
			}
		}
		ents = append(ents, ent)
		if exceededMaxBytes {
			return iterutil.StopIteration()
		}
		return nil
	}

	if err := iterateEntries(ctx, reader, rangeID, expectedIndex, hi, scanFunc); err != nil {
		return nil, err
	}
	// Cache the fetched entries, if we may.
	if canCache {
		eCache.Add(rangeID, ents, false /* truncate */)
	}

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
		// Was the lo already truncated?
		if ents[0].Index > lo {
			return nil, raft.ErrCompacted
		}

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

// iterateEntries iterates over each of the Raft log entries in the range
// [lo,hi). At each step of the iteration, f() is invoked with the current log
// entry.
//
// The function does not accept a maximum number of entries or bytes. Instead,
// callers should enforce any limits by returning iterutil.StopIteration from
// the iteration function to terminate iteration early, if necessary.
func iterateEntries(
	ctx context.Context,
	reader storage.Reader,
	rangeID roachpb.RangeID,
	lo, hi uint64,
	f func(raftpb.Entry) error,
) error {
	key := keys.RaftLogKey(rangeID, lo)
	endKey := keys.RaftLogKey(rangeID, hi)
	iter := reader.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		UpperBound: endKey,
	})
	defer iter.Close()

	var meta enginepb.MVCCMetadata
	var ent raftpb.Entry

	iter.SeekGE(storage.MakeMVCCMetadataKey(key))
	for ; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil || !ok {
			return err
		}

		if err := protoutil.Unmarshal(iter.UnsafeValue(), &meta); err != nil {
			return errors.Wrap(err, "unable to decode MVCCMetadata")
		}
		if err := storage.MakeValue(meta).GetProto(&ent); err != nil {
			return errors.Wrap(err, "unable to unmarshal raft Entry")
		}
		if err := f(ent); err != nil {
			return iterutil.Map(err)
		}
	}
}

// invalidLastTerm is an out-of-band value for r.mu.lastTerm that
// invalidates lastTerm caching and forces retrieval of Term(lastTerm)
// from the raftEntryCache/RocksDB.
const invalidLastTerm = 0

// Term implements the raft.Storage interface.
// Term requires that r.mu is held for writing because it requires exclusive
// access to r.mu.stateLoader.
func (r *RaftLogStore) Term(i uint64) (uint64, error) {
	// TODO(nvanbenschoten): should we set r.mu.lastTerm when
	//   r.mu.lastIndex == i && r.mu.lastTerm == invalidLastTerm?
	if r.mu.lastIndex == i && r.mu.lastTerm != invalidLastTerm {
		return r.mu.lastTerm, nil
	}
	// Try to retrieve the term for the desired entry from the entry cache.
	if e, ok := r.store.raftEntryCache.Get(r.RangeID, i); ok {
		return e.Term, nil
	}
	readonly := r.store.Engine().NewReadOnly(storage.StandardDurability)
	defer readonly.Close()
	ctx := r.AnnotateCtx(context.TODO())
	return term(ctx, r.mu.stateLoader, readonly, r.RangeID, r.store.raftEntryCache, i)
}

func term(
	ctx context.Context,
	rsl stateloader.StateLoader,
	reader storage.Reader,
	rangeID roachpb.RangeID,
	eCache *raftentry.Cache,
	i uint64,
) (uint64, error) {
	// entries() accepts a `nil` sideloaded storage and will skip inlining of
	// sideloaded entries. We only need the term, so this is what we do.
	ents, err := entries(ctx, rsl, reader, rangeID, eCache, nil /* sideloaded */, i, i+1, math.MaxUint64 /* maxBytes */)
	if errors.Is(err, raft.ErrCompacted) {
		ts, err := rsl.LoadRaftTruncatedState(ctx, reader)
		if err != nil {
			return 0, err
		}
		if i == ts.Index {
			return ts.Term, nil
		}
		return 0, raft.ErrCompacted
	} else if err != nil {
		return 0, err
	}
	if len(ents) == 0 {
		return 0, nil
	}
	return ents[0].Term, nil
}

// LastIndex implements the raft.Storage interface.
// LastIndex requires that r.mu is held for reading.
func (r *RaftLogStore) LastIndex() (uint64, error) {
	return r.mu.lastIndex, nil
}

// FirstIndex implements the raft.Storage interface.
// FirstIndex requires that r.mu is held for reading.
func (r *RaftLogStore) FirstIndex() (uint64, error) {
	return r.mu.state.TruncatedState.Index + 1
}

// raftLogState stores information about the last entry and the size of the log.
type raftLogState struct {
	lastIndex uint64
	lastTerm  uint64
	byteSize  int64
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
	prev raftLogState,
	entries []raftpb.Entry,
) (raftLogState, error) {
	if len(entries) == 0 {
		return prev, nil
	}
	var diff enginepb.MVCCStats
	var value roachpb.Value
	for i := range entries {
		ent := &entries[i]
		key := keys.RaftLogKeyFromPrefix(raftLogPrefix, ent.Index)

		if err := value.SetProto(ent); err != nil {
			return raftLogState{}, err
		}
		value.InitChecksum(key)
		var err error
		if ent.Index > prev.lastIndex {
			err = storage.MVCCBlindPut(ctx, rw, &diff, key, hlc.Timestamp{}, hlc.ClockTimestamp{}, value, nil /* txn */)
		} else {
			err = storage.MVCCPut(ctx, rw, &diff, key, hlc.Timestamp{}, hlc.ClockTimestamp{}, value, nil /* txn */)
		}
		if err != nil {
			return raftLogState{}, err
		}
	}

	newLastIndex := entries[len(entries)-1].Index
	// Delete any previously appended log entries which never committed.
	if prev.lastIndex > 0 {
		for i := newLastIndex + 1; i <= prev.lastIndex; i++ {
			// Note that the caller is in charge of deleting any sideloaded payloads
			// (which they must only do *after* the batch has committed).
			_, err := storage.MVCCDelete(ctx, rw, &diff, keys.RaftLogKeyFromPrefix(raftLogPrefix, i),
				hlc.Timestamp{}, hlc.ClockTimestamp{}, nil)
			if err != nil {
				return raftLogState{}, err
			}
		}
	}
	return raftLogState{
		lastIndex: newLastIndex,
		lastTerm:  entries[len(entries)-1].Term,
		byteSize:  prev.byteSize + diff.SysBytes,
	}, nil
}

// TODO(pavelkalinnikov): we probably need a log-only version of clearRangeData
// here too.
