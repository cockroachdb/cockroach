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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
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
		storeIdent, err := kvstorage.ReadStoreIdent(ctx, reader)
		if err != nil {
			return loqrecoverypb.NodeReplicaInfo{}, err
		}
		if err = kvstorage.IterateRangeDescriptorsFromDisk(ctx, reader, func(desc roachpb.RangeDescriptor) error {
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

// GetDescriptorChangesFromRaftLog iterates over raft log between indices
// lo (inclusive) and hi (exclusive) and searches for changes to range
// descriptors, as identified by presence of a commit trigger.
func GetDescriptorChangesFromRaftLog(
	rangeID roachpb.RangeID, lo, hi uint64, reader storage.Reader,
) ([]loqrecoverypb.DescriptorChangeInfo, error) {
	var changes []loqrecoverypb.DescriptorChangeInfo
	if err := raftlog.Visit(reader, rangeID, lo, hi, func(ent raftpb.Entry) error {
		e, err := raftlog.NewEntry(ent)
		if err != nil {
			return err
		}
		raftCmd := e.Cmd
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
		return nil
	}); err != nil {
		return nil, err
	}
	return changes, nil
}
