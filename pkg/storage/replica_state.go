// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package storage

import (
	"github.com/coreos/etcd/raft/raftpb"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// replicaStateLoader contains the precomputed range-local replicated and
// unreplicated prefixes. These prefixes are used to avoid an allocation on
// every call to the commonly used range keys (e.g. RaftAppliedIndexKey).
//
// Note that this struct is not safe for concurrent use. It is usually
// protected by Replica.raftMu or a goroutine local variable is used.
type replicaStateLoader struct {
	keys.RangeIDPrefixBuf
}

func makeReplicaStateLoader(rangeID roachpb.RangeID) replicaStateLoader {
	return replicaStateLoader{
		RangeIDPrefixBuf: keys.MakeRangeIDPrefixBuf(rangeID),
	}
}

// loadState loads a ReplicaState from disk. The exception is the Desc field,
// which is updated transactionally, and is populated from the supplied
// RangeDescriptor under the convention that that is the latest committed
// version.
func (rsl replicaStateLoader) load(
	ctx context.Context, reader engine.Reader, desc *roachpb.RangeDescriptor,
) (storagebase.ReplicaState, error) {
	var s storagebase.ReplicaState
	// TODO(tschottdorf): figure out whether this is always synchronous with
	// on-disk state (likely iffy during Split/ChangeReplica triggers).
	s.Desc = protoutil.Clone(desc).(*roachpb.RangeDescriptor)
	// Read the range lease.
	lease, err := rsl.loadLease(ctx, reader)
	if err != nil {
		return storagebase.ReplicaState{}, err
	}
	s.Lease = &lease

	if s.GCThreshold, err = rsl.loadGCThreshold(ctx, reader); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.TxnSpanGCThreshold, err = rsl.loadTxnSpanGCThreshold(ctx, reader); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.RaftAppliedIndex, s.LeaseAppliedIndex, err = rsl.loadAppliedIndex(ctx, reader); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.Stats, err = rsl.loadMVCCStats(ctx, reader); err != nil {
		return storagebase.ReplicaState{}, err
	}

	// The truncated state should not be optional (i.e. the pointer is
	// pointless), but it is and the migration is not worth it.
	truncState, err := rsl.loadTruncatedState(ctx, reader)
	if err != nil {
		return storagebase.ReplicaState{}, err
	}
	s.TruncatedState = &truncState

	return s, nil
}

// save persists the given ReplicaState to disk. It assumes that the contained
// Stats are up-to-date and returns the stats which result from writing the
// updated State.
//
// As an exception to the rule, the Desc field (whose on-disk state is special
// in that it's a full MVCC value and updated transactionally) is only used for
// its RangeID.
//
// TODO(tschottdorf): test and assert that none of the optional values are
// missing when- ever saveState is called. Optional values should be reserved
// strictly for use in EvalResult. Do before merge.
func (rsl replicaStateLoader) save(
	ctx context.Context, eng engine.ReadWriter, state storagebase.ReplicaState,
) (enginepb.MVCCStats, error) {
	ms := &state.Stats
	if err := rsl.setLease(ctx, eng, ms, state.Lease); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.setAppliedIndex(
		ctx, eng, ms, state.RaftAppliedIndex, state.LeaseAppliedIndex,
	); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.setGCThreshold(ctx, eng, ms, &state.GCThreshold); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.setTxnSpanGCThreshold(ctx, eng, ms, &state.TxnSpanGCThreshold); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.setTruncatedState(ctx, eng, ms, state.TruncatedState); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.setMVCCStats(ctx, eng, &state.Stats); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return state.Stats, nil
}

func (rsl replicaStateLoader) loadLease(
	ctx context.Context, reader engine.Reader,
) (roachpb.Lease, error) {
	var lease roachpb.Lease
	_, err := engine.MVCCGetProto(ctx, reader, rsl.RangeLeaseKey(),
		hlc.Timestamp{}, true, nil, &lease)
	return lease, err
}

func (rsl replicaStateLoader) setLease(
	ctx context.Context, eng engine.ReadWriter, ms *enginepb.MVCCStats, lease *roachpb.Lease,
) error {
	if lease == nil {
		return errors.New("cannot persist nil Lease")
	}
	return engine.MVCCPutProto(ctx, eng, ms, rsl.RangeLeaseKey(),
		hlc.Timestamp{}, nil, lease)
}

func loadAppliedIndex(
	ctx context.Context, reader engine.Reader, rangeID roachpb.RangeID,
) (uint64, uint64, error) {
	rsl := makeReplicaStateLoader(rangeID)
	return rsl.loadAppliedIndex(ctx, reader)
}

// loadAppliedIndex returns the Raft applied index and the lease applied index.
func (rsl replicaStateLoader) loadAppliedIndex(
	ctx context.Context, reader engine.Reader,
) (uint64, uint64, error) {
	var appliedIndex uint64
	v, _, err := engine.MVCCGet(ctx, reader, rsl.RaftAppliedIndexKey(),
		hlc.Timestamp{}, true, nil)
	if err != nil {
		return 0, 0, err
	}
	if v != nil {
		int64AppliedIndex, err := v.GetInt()
		if err != nil {
			return 0, 0, err
		}
		appliedIndex = uint64(int64AppliedIndex)
	}
	// TODO(tschottdorf): code duplication.
	var leaseAppliedIndex uint64
	v, _, err = engine.MVCCGet(ctx, reader, rsl.LeaseAppliedIndexKey(),
		hlc.Timestamp{}, true, nil)
	if err != nil {
		return 0, 0, err
	}
	if v != nil {
		int64LeaseAppliedIndex, err := v.GetInt()
		if err != nil {
			return 0, 0, err
		}
		leaseAppliedIndex = uint64(int64LeaseAppliedIndex)
	}

	return appliedIndex, leaseAppliedIndex, nil
}

// setAppliedIndex sets the {raft,lease} applied index values, properly
// accounting for existing keys in the returned stats.
func (rsl replicaStateLoader) setAppliedIndex(
	ctx context.Context,
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	appliedIndex,
	leaseAppliedIndex uint64,
) error {
	var value roachpb.Value
	value.SetInt(int64(appliedIndex))
	if err := engine.MVCCPut(ctx, eng, ms,
		rsl.RaftAppliedIndexKey(),
		hlc.Timestamp{},
		value,
		nil /* txn */); err != nil {
		return err
	}
	value.SetInt(int64(leaseAppliedIndex))
	return engine.MVCCPut(ctx, eng, ms,
		rsl.LeaseAppliedIndexKey(),
		hlc.Timestamp{},
		value,
		nil /* txn */)
}

// setAppliedIndexBlind sets the {raft,lease} applied index values using a
// "blind" put which ignores any existing keys. This is identical to
// setAppliedIndex but is used to optimize the writing of the applied index
// values during write operations where we definitively know the size of the
// previous values.
func (rsl replicaStateLoader) setAppliedIndexBlind(
	ctx context.Context,
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	appliedIndex,
	leaseAppliedIndex uint64,
) error {
	var value roachpb.Value
	value.SetInt(int64(appliedIndex))
	if err := engine.MVCCBlindPut(ctx, eng, ms,
		rsl.RaftAppliedIndexKey(),
		hlc.Timestamp{},
		value,
		nil /* txn */); err != nil {
		return err
	}
	value.SetInt(int64(leaseAppliedIndex))
	return engine.MVCCBlindPut(ctx, eng, ms,
		rsl.LeaseAppliedIndexKey(),
		hlc.Timestamp{},
		value,
		nil /* txn */)
}

func inlineValueIntEncodedSize(v int64) int {
	var value roachpb.Value
	value.SetInt(v)
	meta := enginepb.MVCCMetadata{RawBytes: value.RawBytes}
	return meta.Size()
}

// Calculate the size (MVCCStats.SysBytes) of the {raft,lease} applied index
// keys/values.
func (rsl replicaStateLoader) calcAppliedIndexSysBytes(
	appliedIndex, leaseAppliedIndex uint64,
) int64 {
	return int64(engine.MakeMVCCMetadataKey(rsl.RaftAppliedIndexKey()).EncodedSize() +
		engine.MakeMVCCMetadataKey(rsl.LeaseAppliedIndexKey()).EncodedSize() +
		inlineValueIntEncodedSize(int64(appliedIndex)) +
		inlineValueIntEncodedSize(int64(leaseAppliedIndex)))
}

func loadTruncatedState(
	ctx context.Context, reader engine.Reader, rangeID roachpb.RangeID,
) (roachpb.RaftTruncatedState, error) {
	rsl := makeReplicaStateLoader(rangeID)
	return rsl.loadTruncatedState(ctx, reader)
}

func (rsl replicaStateLoader) loadTruncatedState(
	ctx context.Context, reader engine.Reader,
) (roachpb.RaftTruncatedState, error) {
	var truncState roachpb.RaftTruncatedState
	if _, err := engine.MVCCGetProto(ctx, reader,
		rsl.RaftTruncatedStateKey(), hlc.Timestamp{}, true,
		nil, &truncState); err != nil {
		return roachpb.RaftTruncatedState{}, err
	}
	return truncState, nil
}

func (rsl replicaStateLoader) setTruncatedState(
	ctx context.Context,
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	truncState *roachpb.RaftTruncatedState,
) error {
	if (*truncState == roachpb.RaftTruncatedState{}) {
		return errors.New("cannot persist empty RaftTruncatedState")
	}
	return engine.MVCCPutProto(ctx, eng, ms,
		rsl.RaftTruncatedStateKey(), hlc.Timestamp{}, nil, truncState)
}

func (rsl replicaStateLoader) loadGCThreshold(
	ctx context.Context, reader engine.Reader,
) (hlc.Timestamp, error) {
	var t hlc.Timestamp
	_, err := engine.MVCCGetProto(ctx, reader, rsl.RangeLastGCKey(),
		hlc.Timestamp{}, true, nil, &t)
	return t, err
}

func (rsl replicaStateLoader) setGCThreshold(
	ctx context.Context, eng engine.ReadWriter, ms *enginepb.MVCCStats, threshold *hlc.Timestamp,
) error {
	if threshold == nil {
		return errors.New("cannot persist nil GCThreshold")
	}
	return engine.MVCCPutProto(ctx, eng, ms,
		rsl.RangeLastGCKey(), hlc.Timestamp{}, nil, threshold)
}

func (rsl replicaStateLoader) loadTxnSpanGCThreshold(
	ctx context.Context, reader engine.Reader,
) (hlc.Timestamp, error) {
	var t hlc.Timestamp
	_, err := engine.MVCCGetProto(ctx, reader, rsl.RangeTxnSpanGCThresholdKey(),
		hlc.Timestamp{}, true, nil, &t)
	return t, err
}

func (rsl replicaStateLoader) setTxnSpanGCThreshold(
	ctx context.Context, eng engine.ReadWriter, ms *enginepb.MVCCStats, threshold *hlc.Timestamp,
) error {
	if threshold == nil {
		return errors.New("cannot persist nil TxnSpanGCThreshold")
	}

	return engine.MVCCPutProto(ctx, eng, ms,
		rsl.RangeTxnSpanGCThresholdKey(), hlc.Timestamp{}, nil, threshold)
}

func (rsl replicaStateLoader) loadMVCCStats(
	ctx context.Context, reader engine.Reader,
) (enginepb.MVCCStats, error) {
	var ms enginepb.MVCCStats
	_, err := engine.MVCCGetProto(ctx, reader, rsl.RangeStatsKey(), hlc.Timestamp{}, true, nil, &ms)
	return ms, err
}

func (rsl replicaStateLoader) setMVCCStats(
	ctx context.Context, eng engine.ReadWriter, newMS *enginepb.MVCCStats,
) error {
	return engine.MVCCPutProto(ctx, eng, nil, rsl.RangeStatsKey(), hlc.Timestamp{}, nil, newMS)
}

// The rest is not technically part of ReplicaState.
// TODO(tschottdorf): more consolidation of ad-hoc structures: last index and
// hard state. These are closely coupled with ReplicaState (and in particular
// with its TruncatedState) but are different in that they are not consistently
// updated through Raft.

func loadLastIndex(
	ctx context.Context, reader engine.Reader, rangeID roachpb.RangeID,
) (uint64, error) {
	rsl := makeReplicaStateLoader(rangeID)
	return rsl.loadLastIndex(ctx, reader)
}

func (rsl replicaStateLoader) loadLastIndex(
	ctx context.Context, reader engine.Reader,
) (uint64, error) {
	var lastIndex uint64
	v, _, err := engine.MVCCGet(ctx, reader, rsl.RaftLastIndexKey(),
		hlc.Timestamp{}, true /* consistent */, nil)
	if err != nil {
		return 0, err
	}
	if v != nil {
		int64LastIndex, err := v.GetInt()
		if err != nil {
			return 0, err
		}
		lastIndex = uint64(int64LastIndex)
	} else {
		// The log is empty, which means we are either starting from scratch
		// or the entire log has been truncated away.
		lastEnt, err := rsl.loadTruncatedState(ctx, reader)
		if err != nil {
			return 0, err
		}
		lastIndex = lastEnt.Index
	}
	return lastIndex, nil
}

func (rsl replicaStateLoader) setLastIndex(
	ctx context.Context, eng engine.ReadWriter, lastIndex uint64,
) error {
	var value roachpb.Value
	value.SetInt(int64(lastIndex))
	return engine.MVCCPut(ctx, eng, nil, rsl.RaftLastIndexKey(),
		hlc.Timestamp{}, value, nil /* txn */)
}

// loadReplicaDestroyedError loads the replica destroyed error for the specified
// range. If there is no error, nil is returned.
func (rsl replicaStateLoader) loadReplicaDestroyedError(
	ctx context.Context, reader engine.Reader,
) (*roachpb.Error, error) {
	var v roachpb.Error
	found, err := engine.MVCCGetProto(ctx, reader,
		rsl.RangeReplicaDestroyedErrorKey(),
		hlc.Timestamp{}, true /* consistent */, nil, &v)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &v, nil
}

// setReplicaDestroyedError sets an error indicating that the replica has been
// destroyed.
func (rsl replicaStateLoader) setReplicaDestroyedError(
	ctx context.Context, eng engine.ReadWriter, err *roachpb.Error,
) error {
	return engine.MVCCPutProto(ctx, eng, nil,
		rsl.RangeReplicaDestroyedErrorKey(), hlc.Timestamp{}, nil /* txn */, err)
}

func loadHardState(
	ctx context.Context, reader engine.Reader, rangeID roachpb.RangeID,
) (raftpb.HardState, error) {
	rsl := makeReplicaStateLoader(rangeID)
	return rsl.loadHardState(ctx, reader)
}

func (rsl replicaStateLoader) loadHardState(
	ctx context.Context, reader engine.Reader,
) (raftpb.HardState, error) {
	var hs raftpb.HardState
	found, err := engine.MVCCGetProto(ctx, reader,
		rsl.RaftHardStateKey(),
		hlc.Timestamp{}, true, nil, &hs)

	if !found || err != nil {
		return raftpb.HardState{}, err
	}
	return hs, nil
}

func (rsl replicaStateLoader) setHardState(
	ctx context.Context, batch engine.ReadWriter, st raftpb.HardState,
) error {
	return engine.MVCCPutProto(ctx, batch, nil,
		rsl.RaftHardStateKey(), hlc.Timestamp{}, nil, &st)
}

// synthesizeHardState synthesizes a HardState from the given ReplicaState and
// any existing on-disk HardState in the context of a snapshot, while verifying
// that the application of the snapshot does not violate Raft invariants. It
// must be called after the supplied state and ReadWriter have been updated
// with the result of the snapshot.
// If there is an existing HardState, we must respect it and we must not apply
// a snapshot that would move the state backwards.
func (rsl replicaStateLoader) synthesizeHardState(
	ctx context.Context, eng engine.ReadWriter, s storagebase.ReplicaState, oldHS raftpb.HardState,
) error {
	newHS := raftpb.HardState{
		Term: s.TruncatedState.Term,
		// Note that when applying a Raft snapshot, the applied index is
		// equal to the Commit index represented by the snapshot.
		Commit: s.RaftAppliedIndex,
	}

	if oldHS.Commit > newHS.Commit {
		return errors.Errorf("can't decrease HardState.Commit from %d to %d",
			oldHS.Commit, newHS.Commit)
	}
	if oldHS.Term > newHS.Term {
		// The existing HardState is allowed to be ahead of us, which is
		// relevant in practice for the split trigger. We already checked above
		// that we're not rewinding the acknowledged index, and we haven't
		// updated votes yet.
		newHS.Term = oldHS.Term
	}
	// If the existing HardState voted in this term, remember that.
	if oldHS.Term == newHS.Term {
		newHS.Vote = oldHS.Vote
	}
	err := rsl.setHardState(ctx, eng, newHS)
	return errors.Wrapf(err, "writing HardState %+v", &newHS)
}

// writeInitialState bootstraps a new Raft group (i.e. it is called when we
// bootstrap a Range, or when setting up the right hand side of a split).
// Its main task is to persist a consistent Raft (and associated Replica) state
// which does not start from zero but presupposes a few entries already having
// applied.
// The supplied MVCCStats are used for the Stats field after adjusting for
// persisting the state itself, and the updated stats are returned.
func writeInitialState(
	ctx context.Context,
	eng engine.ReadWriter,
	ms enginepb.MVCCStats,
	desc roachpb.RangeDescriptor,
	oldHS raftpb.HardState,
	lease *roachpb.Lease,
) (enginepb.MVCCStats, error) {
	rsl := makeReplicaStateLoader(desc.RangeID)

	var s storagebase.ReplicaState
	s.TruncatedState = &roachpb.RaftTruncatedState{
		Term:  raftInitialLogTerm,
		Index: raftInitialLogIndex,
	}
	s.RaftAppliedIndex = s.TruncatedState.Index
	s.Desc = &roachpb.RangeDescriptor{
		RangeID: desc.RangeID,
	}
	s.Stats = ms
	s.Lease = lease

	if existingLease, err := rsl.loadLease(ctx, eng); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading lease")
	} else if (existingLease != roachpb.Lease{}) {
		log.Fatalf(ctx, "expected trivial lease, but found %+v", existingLease)
	}

	newMS, err := rsl.save(ctx, eng, s)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	if err := rsl.synthesizeHardState(ctx, eng, s, oldHS); err != nil {
		return enginepb.MVCCStats{}, err
	}

	if err := rsl.setLastIndex(ctx, eng, s.TruncatedState.Index); err != nil {
		return enginepb.MVCCStats{}, err
	}

	return newMS, nil
}

// ReplicaEvalContext is the interface through which command
// evaluation accesses the in-memory state of a Replica. Any state
// that corresponds to (mutable) on-disk data must be registered in
// the SpanSet if one is given.
type ReplicaEvalContext struct {
	repl *Replica
	ss   *SpanSet
}

// In-memory state, immutable fields, and debugging methods are accessed directly.

// NodeID returns the Replica's NodeID.
func (rec ReplicaEvalContext) NodeID() roachpb.NodeID {
	return rec.repl.NodeID()
}

// StoreID returns the Replica's StoreID.
func (rec ReplicaEvalContext) StoreID() roachpb.StoreID {
	return rec.repl.store.StoreID()
}

// RangeID returns the Replica's RangeID.
func (rec ReplicaEvalContext) RangeID() roachpb.RangeID {
	return rec.repl.RangeID
}

// IsFirstRange returns true if this replica is the first range in the
// system.
func (rec ReplicaEvalContext) IsFirstRange() bool {
	return rec.repl.IsFirstRange()
}

// String returns a string representation of the Replica.
func (rec ReplicaEvalContext) String() string {
	return rec.repl.String()
}

// StoreTestingKnobs returns the Replica's StoreTestingKnobs.
func (rec ReplicaEvalContext) StoreTestingKnobs() StoreTestingKnobs {
	return rec.repl.store.cfg.TestingKnobs
}

// Tracer returns the Replica's Tracer.
func (rec ReplicaEvalContext) Tracer() opentracing.Tracer {
	return rec.repl.store.Tracer()
}

// DB returns the Replica's client DB.
func (rec ReplicaEvalContext) DB() *client.DB {
	return rec.repl.store.DB()
}

// Engine returns the Replica's underlying Engine. In most cases the
// evaluation Batch should be used instead.
func (rec ReplicaEvalContext) Engine() engine.Engine {
	return rec.repl.store.Engine()
}

// AbortCache returns the Replica's AbortCache.
func (rec ReplicaEvalContext) AbortCache() *AbortCache {
	// Despite its name, the abort cache doesn't hold on-disk data in
	// memory. It just provides methods that take a Batch, so SpanSet
	// declarations are enforced there.
	return rec.repl.abortCache
}

// stateLoader returns the Replica's replicaStateLoader.
func (rec ReplicaEvalContext) stateLoader() replicaStateLoader {
	// replicaStateLoader's methods take a Batch argument; SpanSet
	// declarations are enforced there.
	return rec.repl.stateLoader
}

// pushTxnQueue returns the Replica's pushTxnQueue.
func (rec ReplicaEvalContext) pushTxnQueue() *pushTxnQueue {
	return rec.repl.pushTxnQueue
}

// FirstIndex returns the oldest index in the raft log.
func (rec ReplicaEvalContext) FirstIndex() (uint64, error) {
	return rec.repl.FirstIndex()
}

// Term returns the term of the given entry in the raft log.
func (rec ReplicaEvalContext) Term(i uint64) (uint64, error) {
	return rec.repl.Term(i)
}

// Fields backed by on-disk data must be registered in the SpanSet.

// Desc returns the Replica's RangeDescriptor.
func (rec ReplicaEvalContext) Desc() (*roachpb.RangeDescriptor, error) {
	rec.repl.mu.RLock()
	defer rec.repl.mu.RUnlock()
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeDescriptorKey(rec.repl.mu.state.Desc.StartKey)},
		); err != nil {
			return nil, err
		}
	}
	return rec.repl.mu.state.Desc, nil
}

// ContainsKey returns true if the given key is within the Replica's range.
//
// TODO(bdarnell): Replace this method with one on Desc(). See comment
// on Replica.ContainsKey.
func (rec ReplicaEvalContext) ContainsKey(key roachpb.Key) (bool, error) {
	rec.repl.mu.RLock()
	defer rec.repl.mu.RUnlock()
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeDescriptorKey(rec.repl.mu.state.Desc.StartKey)},
		); err != nil {
			return false, err
		}
	}
	return containsKey(*rec.repl.mu.state.Desc, key), nil
}

// GetMVCCStats returns the Replica's MVCCStats.
func (rec ReplicaEvalContext) GetMVCCStats() (enginepb.MVCCStats, error) {
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeStatsKey(rec.RangeID())},
		); err != nil {
			return enginepb.MVCCStats{}, err
		}
	}
	return rec.repl.GetMVCCStats(), nil
}

// GCThreshold returns the GC threshold of the Range, typically updated when
// keys are garbage collected. Reads and writes at timestamps <= this time will
// not be served.
func (rec ReplicaEvalContext) GCThreshold() (hlc.Timestamp, error) {
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeLastGCKey(rec.RangeID())},
		); err != nil {
			return hlc.Timestamp{}, err
		}
	}
	rec.repl.mu.RLock()
	defer rec.repl.mu.RUnlock()
	return rec.repl.mu.state.GCThreshold, nil
}

// TxnSpanGCThreshold returns the time of the Replica's last
// transaction span GC.
func (rec ReplicaEvalContext) TxnSpanGCThreshold() (hlc.Timestamp, error) {
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeTxnSpanGCThresholdKey(rec.RangeID())},
		); err != nil {
			return hlc.Timestamp{}, err
		}
	}
	rec.repl.mu.RLock()
	defer rec.repl.mu.RUnlock()
	return rec.repl.mu.state.TxnSpanGCThreshold, nil
}

// GetLastReplicaGCTimestamp returns the last time the Replica was
// considered for GC.
func (rec ReplicaEvalContext) GetLastReplicaGCTimestamp(
	ctx context.Context,
) (hlc.Timestamp, error) {
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeLastReplicaGCTimestampKey(rec.RangeID())},
		); err != nil {
			return hlc.Timestamp{}, err
		}
	}
	return rec.repl.getLastReplicaGCTimestamp(ctx)
}

// GetLease returns the Replica's current and next lease (if any).
func (rec ReplicaEvalContext) GetLease() (*roachpb.Lease, *roachpb.Lease, error) {
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeLeaseKey(rec.RangeID())},
		); err != nil {
			return nil, nil, err
		}
	}
	lease, nextLease := rec.repl.getLease()
	return lease, nextLease, nil
}

// GetTempPrefix proxies Replica.GetTempDir
func (rec ReplicaEvalContext) GetTempPrefix() string {
	return rec.repl.GetTempPrefix()
}
