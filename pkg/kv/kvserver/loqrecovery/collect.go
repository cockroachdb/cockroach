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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
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
			replicaData := loqrecoverypb.ReplicaInfo{
				StoreID:            storeIdent.StoreID,
				NodeID:             storeIdent.NodeID,
				Desc:               desc,
				RaftAppliedIndex:   rstate.RaftAppliedIndex,
				RaftCommittedIndex: hstate.Commit,
				// TODO(oleg): #73282 Track presence of uncommitted descriptors
				HasUncommittedDescriptors: false,
			}
			replicas = append(replicas, replicaData)
			return nil
		}); err != nil {
			return loqrecoverypb.NodeReplicaInfo{}, err
		}
	}
	return loqrecoverypb.NodeReplicaInfo{Replicas: replicas}, nil
}
