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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type singleEngineReplicasStorage struct {
	id  roachpb.StoreID
	eng storage.Engine
}

var _ storage.ReplicasStorage = (*singleEngineReplicasStorage)(nil)

func (s *singleEngineReplicasStorage) Init() {
	// No-op for now but this would really have to apply all entries
	// that are in HardState.Committed until it sees the first nontrivial
	// entry (split, merge, etc).
	// This should be possible after #76126, #76130, and more generally #75729.
}

func (s *singleEngineReplicasStorage) CurrentRanges() []storage.ReplicaInfo {
	iwds := s.currentRangesInternal()
	var sl []storage.ReplicaInfo
	for _, iwd := range iwds {
		sl = append(sl, iwd.info)
	}
	return sl
}

type replicaInfoWithDesc struct {
	info storage.ReplicaInfo
	desc *roachpb.RangeDescriptor
}

func (s *singleEngineReplicasStorage) currentRangesInternal() []replicaInfoWithDesc {
	var sl []replicaInfoWithDesc
	if err := IterateRangeDescriptorsFromDisk(context.Background(), s.eng, func(desc roachpb.RangeDescriptor) error {
		replDesc, ok := desc.GetReplicaDescriptor(s.id)
		if !ok {
			panic("TODO(tbg)")
		}
		sl = append(sl, replicaInfoWithDesc{info: storage.ReplicaInfo{
			FullReplicaID: storage.FullReplicaID{
				RangeID:   desc.RangeID,
				ReplicaID: replDesc.ReplicaID,
			},
			State: storage.InitializedStateMachine,
		}, desc: &desc})
		return nil
	}); err != nil {
		panic(err) // TODO(tbg)
	}
	// TODO(tbg): discover uninitialized replicas by scanning ...
	_ = keys.LocalRangeIDPrefix
	// ... for ...
	_ = keys.RaftHardStateKey
	return sl
}

func getRangeTombstone(
	eng storage.Reader, rangeID roachpb.RangeID,
) (nextReplicaID roachpb.ReplicaID, _ error) {
	tombstoneKey := keys.RangeTombstoneKey(rangeID)
	var tombstone roachpb.RangeTombstone
	_, err := storage.MVCCGetProto(
		context.Background(), eng, tombstoneKey, hlc.Timestamp{}, &tombstone, storage.MVCCGetOptions{},
	)
	if err != nil {
		return 0, err
	}
	return tombstone.NextReplicaID, nil
}

func (s *singleEngineReplicasStorage) GetRangeTombstone(
	rangeID roachpb.RangeID,
) (nextReplicaID roachpb.ReplicaID, _ error) {
	return getRangeTombstone(s.eng, rangeID)
}

func (s *singleEngineReplicasStorage) GetHandle(
	rr storage.FullReplicaID,
) (storage.RangeStorage, error) {
	for _, iwd := range s.currentRangesInternal() {
		if iwd.info.FullReplicaID == rr {
			impl := &singleEngineRangeStorage{
				eng:      s.eng,
				startKey: &iwd.desc.StartKey,
				id:       rr,
			}
			return impl, nil
		}
	}
	panic("TODO(tbg): is it an assertion failure to fail to find the replica here?")
}

func (s *singleEngineReplicasStorage) CreateUninitializedRange(
	rr storage.FullReplicaID,
) (storage.RangeStorage, error) {
	//TODO implement me
	panic("implement me")
}

func (s *singleEngineReplicasStorage) SplitReplica(
	r storage.RangeStorage,
	rhsRR storage.FullReplicaID,
	rhsSpan roachpb.Span,
	smBatch storage.MutationBatch,
) (storage.RangeStorage, error) {
	// TODO the code that would have to be here is in splitTriggerHelper.
	panic("implement me")
}

func (s *singleEngineReplicasStorage) MergeReplicas(
	lhsRS storage.RangeStorage, rhsRS storage.RangeStorage, smBatch storage.MutationBatch,
) error {
	//TODO the code that would have to be here is in batcheval.mergeTrigger.
	panic("implement me")
}

func (s *singleEngineReplicasStorage) DiscardReplica(
	r storage.RangeStorage, nextReplicaID roachpb.ReplicaID,
) error {
	//TODO (*Replica).destroyRaftMuLocked.
	panic("implement me")
}
