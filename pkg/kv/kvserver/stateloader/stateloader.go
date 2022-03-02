// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stateloader

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// StateLoader contains accessor methods to read or write the
// fields of kvserverbase.ReplicaState. It contains an internal buffer
// which is reused to avoid an allocation on frequently-accessed code
// paths.
//
// Because of this internal buffer, this struct is not safe for
// concurrent use, and the return values of methods that return keys
// are invalidated the next time any method is called.
//
// It is safe to have multiple replicaStateLoaders for the same
// Replica. Reusable replicaStateLoaders are typically found in a
// struct with a mutex, and temporary loaders may be created when
// locking is less desirable than an allocation.
type StateLoader struct {
	keys.RangeIDPrefixBuf
}

// Make creates a a StateLoader.
func Make(rangeID roachpb.RangeID) StateLoader {
	rsl := StateLoader{
		RangeIDPrefixBuf: keys.MakeRangeIDPrefixBuf(rangeID),
	}
	return rsl
}

// Load a ReplicaState from disk. The exception is the Desc field, which is
// updated transactionally, and is populated from the supplied RangeDescriptor
// under the convention that that is the latest committed version.
func (rsl StateLoader) Load(
	ctx context.Context, reader storage.Reader, desc *roachpb.RangeDescriptor,
) (kvserverpb.ReplicaState, error) {
	var s kvserverpb.ReplicaState
	// TODO(tschottdorf): figure out whether this is always synchronous with
	// on-disk state (likely iffy during Split/ChangeReplica triggers).
	s.Desc = protoutil.Clone(desc).(*roachpb.RangeDescriptor)
	// Read the range lease.
	lease, err := rsl.LoadLease(ctx, reader)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	s.Lease = &lease

	if s.GCThreshold, err = rsl.LoadGCThreshold(ctx, reader); err != nil {
		return kvserverpb.ReplicaState{}, err
	}

	as, err := rsl.LoadRangeAppliedState(ctx, reader)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	s.RaftAppliedIndex = as.RaftAppliedIndex
	s.RaftAppliedIndexTerm = as.RaftAppliedIndexTerm
	s.LeaseAppliedIndex = as.LeaseAppliedIndex
	ms := as.RangeStats.ToStats()
	s.Stats = &ms
	if as.RaftClosedTimestamp != nil {
		s.RaftClosedTimestamp = *as.RaftClosedTimestamp
	}

	// The truncated state should not be optional (i.e. the pointer is
	// pointless), but it is and the migration is not worth it.
	truncState, err := rsl.LoadRaftTruncatedState(ctx, reader)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	s.TruncatedState = &truncState

	version, err := rsl.LoadVersion(ctx, reader)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	if (version != roachpb.Version{}) {
		s.Version = &version
	}

	return s, nil
}

// Save persists the given ReplicaState to disk. It assumes that the contained
// Stats are up-to-date and returns the stats which result from writing the
// updated State.
//
// As an exception to the rule, the Desc field (whose on-disk state is special
// in that it's a full MVCC value and updated transactionally) is only used for
// its RangeID.
//
// TODO(tschottdorf): test and assert that none of the optional values are
// missing whenever save is called. Optional values should be reserved
// strictly for use in Result. Do before merge.
func (rsl StateLoader) Save(
	ctx context.Context, readWriter storage.ReadWriter, state kvserverpb.ReplicaState,
) (enginepb.MVCCStats, error) {
	ms := state.Stats
	if err := rsl.SetLease(ctx, readWriter, ms, *state.Lease); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.SetGCThreshold(ctx, readWriter, ms, state.GCThreshold); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.SetRaftTruncatedState(ctx, readWriter, state.TruncatedState); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if state.Version != nil {
		if err := rsl.SetVersion(ctx, readWriter, ms, state.Version); err != nil {
			return enginepb.MVCCStats{}, err
		}
	}
	rai, lai, rait, ct := state.RaftAppliedIndex, state.LeaseAppliedIndex, state.RaftAppliedIndexTerm,
		&state.RaftClosedTimestamp
	if err := rsl.SetRangeAppliedState(ctx, readWriter, rai, lai, rait, ms, ct); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return *ms, nil
}

// LoadLease loads the lease.
func (rsl StateLoader) LoadLease(
	ctx context.Context, reader storage.Reader,
) (roachpb.Lease, error) {
	var lease roachpb.Lease
	_, err := storage.MVCCGetProto(ctx, reader, rsl.RangeLeaseKey(),
		hlc.Timestamp{}, &lease, storage.MVCCGetOptions{})
	return lease, err
}

// SetLease persists a lease.
func (rsl StateLoader) SetLease(
	ctx context.Context, readWriter storage.ReadWriter, ms *enginepb.MVCCStats, lease roachpb.Lease,
) error {
	return storage.MVCCPutProto(ctx, readWriter, ms, rsl.RangeLeaseKey(),
		hlc.Timestamp{}, nil, &lease)
}

// LoadRangeAppliedState loads the Range applied state.
func (rsl StateLoader) LoadRangeAppliedState(
	ctx context.Context, reader storage.Reader,
) (enginepb.RangeAppliedState, error) {
	var as enginepb.RangeAppliedState
	_, err := storage.MVCCGetProto(ctx, reader, rsl.RangeAppliedStateKey(), hlc.Timestamp{}, &as,
		storage.MVCCGetOptions{})
	return as, err
}

// LoadMVCCStats loads the MVCC stats.
func (rsl StateLoader) LoadMVCCStats(
	ctx context.Context, reader storage.Reader,
) (enginepb.MVCCStats, error) {
	// Check the applied state key.
	as, err := rsl.LoadRangeAppliedState(ctx, reader)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	return as.RangeStats.ToStats(), nil
}

// SetRangeAppliedState overwrites the range applied state. This state is a
// combination of the Raft and lease applied indices, along with the MVCC stats.
//
// The applied indices and the stats used to be stored separately in different
// keys. We now deem those keys to be "legacy" because they have been replaced
// by the range applied state key.
//
// TODO(andrei): raftClosedTimestamp is a pointer to avoid an allocation when
// putting it in RangeAppliedState. Once RangeAppliedState.RaftClosedTimestamp
// is made non-nullable (see comments on the field), this argument should be
// taken by value.
func (rsl StateLoader) SetRangeAppliedState(
	ctx context.Context,
	readWriter storage.ReadWriter,
	appliedIndex, leaseAppliedIndex, appliedIndexTerm uint64,
	newMS *enginepb.MVCCStats,
	raftClosedTimestamp *hlc.Timestamp,
) error {
	as := enginepb.RangeAppliedState{
		RaftAppliedIndex:     appliedIndex,
		LeaseAppliedIndex:    leaseAppliedIndex,
		RangeStats:           newMS.ToPersistentStats(),
		RaftAppliedIndexTerm: appliedIndexTerm,
	}
	if raftClosedTimestamp != nil && !raftClosedTimestamp.IsEmpty() {
		as.RaftClosedTimestamp = raftClosedTimestamp
	}
	// The RangeAppliedStateKey is not included in stats. This is also reflected
	// in C.MVCCComputeStats and ComputeStatsForRange.
	ms := (*enginepb.MVCCStats)(nil)
	return storage.MVCCPutProto(ctx, readWriter, ms, rsl.RangeAppliedStateKey(), hlc.Timestamp{}, nil, &as)
}

// SetMVCCStats overwrites the MVCC stats. This needs to perform a read on the
// RangeAppliedState key before overwriting the stats. Use SetRangeAppliedState
// when performance is important.
func (rsl StateLoader) SetMVCCStats(
	ctx context.Context, readWriter storage.ReadWriter, newMS *enginepb.MVCCStats,
) error {
	as, err := rsl.LoadRangeAppliedState(ctx, readWriter)
	if err != nil {
		return err
	}
	return rsl.SetRangeAppliedState(
		ctx, readWriter, as.RaftAppliedIndex, as.LeaseAppliedIndex, as.RaftAppliedIndexTerm, newMS,
		as.RaftClosedTimestamp)
}

// SetClosedTimestamp overwrites the closed timestamp.
func (rsl StateLoader) SetClosedTimestamp(
	ctx context.Context, readWriter storage.ReadWriter, closedTS *hlc.Timestamp,
) error {
	as, err := rsl.LoadRangeAppliedState(ctx, readWriter)
	if err != nil {
		return err
	}
	return rsl.SetRangeAppliedState(
		ctx, readWriter, as.RaftAppliedIndex, as.LeaseAppliedIndex, as.RaftAppliedIndexTerm,
		as.RangeStats.ToStatsPtr(), closedTS)
}

// LoadGCThreshold loads the GC threshold.
func (rsl StateLoader) LoadGCThreshold(
	ctx context.Context, reader storage.Reader,
) (*hlc.Timestamp, error) {
	var t hlc.Timestamp
	_, err := storage.MVCCGetProto(ctx, reader, rsl.RangeGCThresholdKey(),
		hlc.Timestamp{}, &t, storage.MVCCGetOptions{})
	return &t, err
}

// SetGCThreshold sets the GC threshold.
func (rsl StateLoader) SetGCThreshold(
	ctx context.Context,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	threshold *hlc.Timestamp,
) error {
	if threshold == nil {
		return errors.New("cannot persist nil GCThreshold")
	}
	return storage.MVCCPutProto(ctx, readWriter, ms,
		rsl.RangeGCThresholdKey(), hlc.Timestamp{}, nil, threshold)
}

// LoadVersion loads the replica version.
func (rsl StateLoader) LoadVersion(
	ctx context.Context, reader storage.Reader,
) (roachpb.Version, error) {
	var version roachpb.Version
	_, err := storage.MVCCGetProto(ctx, reader, rsl.RangeVersionKey(),
		hlc.Timestamp{}, &version, storage.MVCCGetOptions{})
	return version, err
}

// SetVersion sets the replica version.
func (rsl StateLoader) SetVersion(
	ctx context.Context,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	version *roachpb.Version,
) error {
	return storage.MVCCPutProto(ctx, readWriter, ms,
		rsl.RangeVersionKey(), hlc.Timestamp{}, nil, version)
}

// The rest is not technically part of ReplicaState.

// LoadLastIndex loads the last index.
func (rsl StateLoader) LoadLastIndex(ctx context.Context, reader storage.Reader) (uint64, error) {
	prefix := rsl.RaftLogPrefix()
	// NB: raft log has no intents.
	iter := reader.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{LowerBound: prefix})
	defer iter.Close()

	var lastIndex uint64
	iter.SeekLT(storage.MakeMVCCMetadataKey(rsl.RaftLogKey(math.MaxUint64)))
	if ok, _ := iter.Valid(); ok {
		key := iter.Key()
		var err error
		_, lastIndex, err = encoding.DecodeUint64Ascending(key.Key[len(prefix):])
		if err != nil {
			log.Fatalf(ctx, "unable to decode Raft log index key: %s", key)
		}
	}

	if lastIndex == 0 {
		// The log is empty, which means we are either starting from scratch
		// or the entire log has been truncated away.
		lastEnt, err := rsl.LoadRaftTruncatedState(ctx, reader)
		if err != nil {
			return 0, err
		}
		lastIndex = lastEnt.Index
	}
	return lastIndex, nil
}

// LoadRaftTruncatedState loads the truncated state.
func (rsl StateLoader) LoadRaftTruncatedState(
	ctx context.Context, reader storage.Reader,
) (roachpb.RaftTruncatedState, error) {
	var truncState roachpb.RaftTruncatedState
	if _, err := storage.MVCCGetProto(
		ctx, reader, rsl.RaftTruncatedStateKey(), hlc.Timestamp{}, &truncState, storage.MVCCGetOptions{},
	); err != nil {
		return roachpb.RaftTruncatedState{}, err
	}
	return truncState, nil
}

// SetRaftTruncatedState overwrites the truncated state.
func (rsl StateLoader) SetRaftTruncatedState(
	ctx context.Context, writer storage.Writer, truncState *roachpb.RaftTruncatedState,
) error {
	if (*truncState == roachpb.RaftTruncatedState{}) {
		return errors.New("cannot persist empty RaftTruncatedState")
	}
	// "Blind" because ms == nil and timestamp.IsEmpty().
	return storage.MVCCBlindPutProto(
		ctx,
		writer,
		nil, /* ms */
		rsl.RaftTruncatedStateKey(),
		hlc.Timestamp{}, /* timestamp */
		truncState,
		nil, /* txn */
	)
}

// LoadHardState loads the HardState.
func (rsl StateLoader) LoadHardState(
	ctx context.Context, reader storage.Reader,
) (raftpb.HardState, error) {
	var hs raftpb.HardState
	found, err := storage.MVCCGetProto(ctx, reader, rsl.RaftHardStateKey(),
		hlc.Timestamp{}, &hs, storage.MVCCGetOptions{})

	if !found || err != nil {
		return raftpb.HardState{}, err
	}
	return hs, nil
}

// SetHardState overwrites the HardState.
func (rsl StateLoader) SetHardState(
	ctx context.Context, writer storage.Writer, hs raftpb.HardState,
) error {
	// "Blind" because ms == nil and timestamp.IsEmpty().
	return storage.MVCCBlindPutProto(
		ctx,
		writer,
		nil, /* ms */
		rsl.RaftHardStateKey(),
		hlc.Timestamp{}, /* timestamp */
		&hs,
		nil, /* txn */
	)
}

// SynthesizeRaftState creates a Raft state which synthesizes both a HardState
// and a lastIndex from pre-seeded data in the engine (typically created via
// WriteInitialReplicaState and, on a split, perhaps the activity of an
// uninitialized Raft group)
func (rsl StateLoader) SynthesizeRaftState(
	ctx context.Context, readWriter storage.ReadWriter,
) error {
	hs, err := rsl.LoadHardState(ctx, readWriter)
	if err != nil {
		return err
	}
	truncState, err := rsl.LoadRaftTruncatedState(ctx, readWriter)
	if err != nil {
		return err
	}
	as, err := rsl.LoadRangeAppliedState(ctx, readWriter)
	if err != nil {
		return err
	}
	return rsl.SynthesizeHardState(ctx, readWriter, hs, truncState, as.RaftAppliedIndex)
}

// SynthesizeHardState synthesizes an on-disk HardState from the given input,
// taking care that a HardState compatible with the existing data is written.
func (rsl StateLoader) SynthesizeHardState(
	ctx context.Context,
	readWriter storage.ReadWriter,
	oldHS raftpb.HardState,
	truncState roachpb.RaftTruncatedState,
	raftAppliedIndex uint64,
) error {
	newHS := raftpb.HardState{
		Term: truncState.Term,
		// Note that when applying a Raft snapshot, the applied index is
		// equal to the Commit index represented by the snapshot.
		Commit: raftAppliedIndex,
	}

	if oldHS.Commit > newHS.Commit {
		return errors.Newf("can't decrease HardState.Commit from %d to %d",
			redact.Safe(oldHS.Commit), redact.Safe(newHS.Commit))
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
	err := rsl.SetHardState(ctx, readWriter, newHS)
	return errors.Wrapf(err, "writing HardState %+v", &newHS)
}

// SetRaftReplicaID overwrites the RaftReplicaID.
func (rsl StateLoader) SetRaftReplicaID(
	ctx context.Context, writer storage.Writer, replicaID roachpb.ReplicaID,
) error {
	rid := roachpb.RaftReplicaID{ReplicaID: replicaID}
	// "Blind" because ms == nil and timestamp.IsEmpty().
	return storage.MVCCBlindPutProto(
		ctx,
		writer,
		nil, /* ms */
		rsl.RaftReplicaIDKey(),
		hlc.Timestamp{}, /* timestamp */
		&rid,
		nil, /* txn */
	)
}

// LoadRaftReplicaID loads the RaftReplicaID.
func (rsl StateLoader) LoadRaftReplicaID(
	ctx context.Context, reader storage.Reader,
) (replicaID roachpb.RaftReplicaID, found bool, err error) {
	found, err = storage.MVCCGetProto(ctx, reader, rsl.RaftReplicaIDKey(),
		hlc.Timestamp{}, &replicaID, storage.MVCCGetOptions{})
	return
}
