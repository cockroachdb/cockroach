// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type singleEngineRangeStorage struct {
	startKey *roachpb.RKey // nil if uninited, notably different from pointer to empty slice (which is r1)
	id       storage.FullReplicaID
	eng      storage.Engine
}

var _ storage.RangeStorage = (*singleEngineRangeStorage)(nil)

func (s *singleEngineRangeStorage) FullReplicaID() storage.FullReplicaID {
	return s.id
}

func (s *singleEngineRangeStorage) State() storage.ReplicaState {
	nextReplicaID, err := getRangeTombstone(s.eng, s.id.RangeID)
	if err != nil {
		// TODO(tbg): should this method be infallible, i.e. replicaID be loaded on
		// creation and stored? That would probably be necessary in practice anyway,
		// for perf reasons.
		panic(err)
	}
	if nextReplicaID > s.id.ReplicaID {
		return storage.DeletedReplica
	}
	if s.startKey == nil {
		return storage.UninitializedStateMachine
	}
	return storage.InitializedStateMachine
}

func (s *singleEngineRangeStorage) CurrentRaftEntriesRange() (lo uint64, hi uint64, err error) {
	panic("lift some code here or use https://github.com/cockroachdb/cockroach/pull/76126")
}

func (s *singleEngineRangeStorage) GetHardState() (raftpb.HardState, error) {
	// NB: making a stateloader is rather allocation-heavy so optimize this sometime.
	return stateloader.Make(s.id.RangeID).LoadHardState(context.Background(), s.eng)
}

func (s *singleEngineRangeStorage) CanTruncateRaftIfStateMachineIsDurable(index uint64) {
	panic("guess I just have to save this on `s`? Are there concurrent callers? Hopefully not.")
}

func (s *singleEngineRangeStorage) DoRaftMutation(rBatch storage.RaftMutationBatch) error {
	// Basically need to lift:
	_ = (*Replica).append
	panic("lift some code here or use https://github.com/cockroachdb/cockroach/pull/76126")
}

func (s *singleEngineRangeStorage) IngestRangeSnapshot(
	span roachpb.Span,
	raftAppliedIndex uint64,
	raftAppliedIndexTerm uint64,
	sstPaths []string,
	subsumedRangeIDs []roachpb.RangeID,
) error {
	// Basically need to lift:
	_ = (*Replica).applySnapshot
	panic("lift some code here")
}

func (s *singleEngineRangeStorage) ApplyCommittedUsingIngest(
	sstPaths []string, highestRaftIndex uint64,
) error {
	_ = addSSTablePreApply // already free-standing, so should be "easy"
	// ... but we need to durably persist highestRaftIndex as committed as well,
	// before ingesting, i.e. durably forward HardState.Commit first.
	panic("implement me")
}

func (s *singleEngineRangeStorage) ApplyCommittedBatch(smBatch storage.MutationBatch) error {
	// The batch is currently committed in:
	_ = (*replicaAppBatch).ApplyToStateMachine
	// ... so that would be the method calling into here (and we need to lift code out
	// of the above method). I think we mostly need to (conditionally) call:
	_ = (*replicaAppBatch).addAppliedStateKeyToBatch
	// as is done in that method, then commit.
	panic("implement me")
}

func (s *singleEngineRangeStorage) SyncStateMachine() error {
	// TODO(tbg): check that this isn't accidentally a noop.
	return s.eng.NewUnindexedBatch(true /* writeOnly */).Commit(true)
}
