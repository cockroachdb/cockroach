// Copyright 2016 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// StateLoader gives access to read or write the state of the Raft log. It
// contains an internal buffer which is reused to avoid an allocation on
// frequently-accessed code paths.
//
// Because of this internal buffer, this struct is not safe for concurrent use,
// and the return values of methods that return keys are invalidated the next
// time any method is called.
//
// It is safe to have multiple state loaders for the same replica. Reusable
// loaders are typically found in a struct with a mutex, and temporary loaders
// may be created when locking is less desirable than an allocation.
type StateLoader struct {
	keys.RangeIDPrefixBuf
}

// NewStateLoader creates a StateLoader.
func NewStateLoader(rangeID roachpb.RangeID) StateLoader {
	rsl := StateLoader{
		RangeIDPrefixBuf: keys.MakeRangeIDPrefixBuf(rangeID),
	}
	return rsl
}

// LoadLastIndex loads the last index.
func (rsl StateLoader) LoadLastIndex(ctx context.Context, reader storage.Reader) (uint64, error) {
	prefix := rsl.RaftLogPrefix()
	// NB: raft log has no intents.
	iter := reader.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{LowerBound: prefix})
	defer iter.Close()

	var lastIndex uint64
	iter.SeekLT(storage.MakeMVCCMetadataKey(keys.RaftLogKeyFromPrefix(prefix, math.MaxUint64)))
	if ok, _ := iter.Valid(); ok {
		key := iter.UnsafeKey().Key
		if len(key) < len(prefix) {
			log.Fatalf(ctx, "unable to decode Raft log index key: len(%s) < len(%s)", key.String(), prefix.String())
		}
		suffix := key[len(prefix):]
		var err error
		lastIndex, err = keys.DecodeRaftLogKeyFromSuffix(suffix)
		if err != nil {
			log.Fatalf(ctx, "unable to decode Raft log index key: %s; %v", key.String(), err)
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
		hlc.Timestamp{},      /* timestamp */
		hlc.ClockTimestamp{}, /* localTimestamp */
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
		hlc.Timestamp{},      /* timestamp */
		hlc.ClockTimestamp{}, /* localTimestamp */
		&hs,
		nil, /* txn */
	)
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
		hlc.Timestamp{},      /* timestamp */
		hlc.ClockTimestamp{}, /* localTimestamp */
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
