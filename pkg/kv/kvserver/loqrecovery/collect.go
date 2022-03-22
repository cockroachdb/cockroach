// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// CollectReplicaInfo captures states of all replicas in all stores for the sake of quorum recovery.
func CollectReplicaInfo(
	ctx context.Context, stores []storage.Engine,
) (loqrecoverypb.NodeReplicaInfo, error) {
	if len(stores) == 0 {
		return loqrecoverypb.NodeReplicaInfo{}, errors.New("no stores were provided for info collection")
	}

	var replicas []loqrecoverypb.ReplicaInfo
	for _, reader := range stores {
		storeIdent, err := kvserver.ReadStoreIdent(ctx, reader)
		if err != nil {
			return loqrecoverypb.NodeReplicaInfo{}, err
		}
		if err = kvserver.IterateRangeDescriptorsFromDisk(ctx, reader, func(desc roachpb.RangeDescriptor) error {
			rsl := stateloader.Make(desc.RangeID)
			rstate, err := rsl.Load(ctx, reader, &desc)
			if err != nil {
				return err
			}
			hstate, err := rsl.LoadHardState(ctx, reader)
			if err != nil {
				return err
			}
			// Check raft log for un-applied range descriptor changes. We start from
			// applied+1 (inclusive) and read until the end of the log. We also look
			// at potentially uncommitted entries as we have no way to determine their
			// outcome, and they will become committed as soon as the replica is
			// designated as a survivor.
			rangeUpdates, err := GetDescriptorChangesFromRaftLog(desc.RangeID,
				rstate.RaftAppliedIndex+1, math.MaxInt64, reader)
			if err != nil {
				return err
			}

			replicaData := loqrecoverypb.ReplicaInfo{
				StoreID:                  storeIdent.StoreID,
				NodeID:                   storeIdent.NodeID,
				Desc:                     desc,
				RaftAppliedIndex:         rstate.RaftAppliedIndex,
				RaftCommittedIndex:       hstate.Commit,
				RaftLogDescriptorChanges: rangeUpdates,
			}
			replicas = append(replicas, replicaData)
			return nil
		}); err != nil {
			return loqrecoverypb.NodeReplicaInfo{}, err
		}
	}
	return loqrecoverypb.NodeReplicaInfo{Replicas: replicas}, nil
}

// GetDescriptorChangesFromRaftLog iterates over raft log between indicies
// lo (inclusive) and hi (exclusive) and searches for changes to range
// descriptor. Changes are identified by commit trigger content which is
// extracted either from EntryNormal where change updates key range info
// (split/merge) or from EntryConfChange* for changes in replica set.
func GetDescriptorChangesFromRaftLog(
	rangeID roachpb.RangeID, lo, hi uint64, reader storage.Reader,
) ([]loqrecoverypb.DescriptorChangeInfo, error) {
	var changes []loqrecoverypb.DescriptorChangeInfo

	key := keys.RaftLogKey(rangeID, lo)
	endKey := keys.RaftLogKey(rangeID, hi)
	iter := reader.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		UpperBound: endKey,
	})
	defer iter.Close()

	var meta enginepb.MVCCMetadata
	var ent raftpb.Entry

	decodeRaftChange := func(ccI raftpb.ConfChangeI) ([]byte, error) {
		var ccC kvserverpb.ConfChangeContext
		if err := protoutil.Unmarshal(ccI.AsV2().Context, &ccC); err != nil {
			return nil, errors.Wrap(err, "while unmarshaling CCContext")
		}
		return ccC.Payload, nil
	}

	iter.SeekGE(storage.MakeMVCCMetadataKey(key))
	for ; ; iter.Next() {
		ok, err := iter.Valid()
		if err != nil {
			return nil, err
		}
		if !ok {
			return changes, nil
		}
		if err := protoutil.Unmarshal(iter.UnsafeValue(), &meta); err != nil {
			return nil, errors.Wrap(err, "unable to decode raft log MVCCMetadata")
		}
		if err := storage.MakeValue(meta).GetProto(&ent); err != nil {
			return nil, errors.Wrap(err, "unable to unmarshal raft Entry")
		}
		if len(ent.Data) == 0 {
			continue
		}
		// Following code extracts our raft command from raft log entry. Depending
		// on entry type we either need to extract encoded command from configuration
		// change (for replica changes) or from normal command (for splits and
		// merges).
		var payload []byte
		switch ent.Type {
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := protoutil.Unmarshal(ent.Data, &cc); err != nil {
				return nil, errors.Wrap(err, "while unmarshaling ConfChange")
			}
			payload, err = decodeRaftChange(cc)
			if err != nil {
				return nil, err
			}
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := protoutil.Unmarshal(ent.Data, &cc); err != nil {
				return nil, errors.Wrap(err, "while unmarshaling ConfChangeV2")
			}
			payload, err = decodeRaftChange(cc)
			if err != nil {
				return nil, err
			}
		case raftpb.EntryNormal:
			_, payload = kvserverbase.DecodeRaftCommand(ent.Data)
		default:
			continue
		}
		if len(payload) == 0 {
			continue
		}
		var raftCmd kvserverpb.RaftCommand
		if err := protoutil.Unmarshal(payload, &raftCmd); err != nil {
			return nil, errors.Wrap(err, "unable to unmarshal raft command")
		}
		switch {
		case raftCmd.ReplicatedEvalResult.Split != nil:
			changes = append(changes,
				loqrecoverypb.DescriptorChangeInfo{
					ChangeType: loqrecoverypb.DescriptorChangeType_Split,
					Desc:       &raftCmd.ReplicatedEvalResult.Split.LeftDesc,
					OtherDesc:  &raftCmd.ReplicatedEvalResult.Split.RightDesc,
				})
		case raftCmd.ReplicatedEvalResult.Merge != nil:
			changes = append(changes,
				loqrecoverypb.DescriptorChangeInfo{
					ChangeType: loqrecoverypb.DescriptorChangeType_Merge,
					Desc:       &raftCmd.ReplicatedEvalResult.Merge.LeftDesc,
					OtherDesc:  &raftCmd.ReplicatedEvalResult.Merge.RightDesc,
				})
		case raftCmd.ReplicatedEvalResult.ChangeReplicas != nil:
			changes = append(changes, loqrecoverypb.DescriptorChangeInfo{
				ChangeType: loqrecoverypb.DescriptorChangeType_ReplicaChange,
				Desc:       raftCmd.ReplicatedEvalResult.ChangeReplicas.Desc,
			})
		}
	}
}
