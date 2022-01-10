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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
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
			// the position after applied (inclusive) and stop at position after
			// committed (exclusive) - hence +1 on index values.
			hasUnappliedDesc, err := checkUncommittedDescriptorsInRaftLog(desc.RangeID, desc.StartKey,
				rstate.RaftAppliedIndex+1, hstate.Commit+1, reader)
			if err != nil {
				return err
			}

			replicaData := loqrecoverypb.ReplicaInfo{
				StoreID:                   storeIdent.StoreID,
				NodeID:                    storeIdent.NodeID,
				Desc:                      desc,
				RaftAppliedIndex:          rstate.RaftAppliedIndex,
				RaftCommittedIndex:        hstate.Commit,
				HasUncommittedDescriptors: hasUnappliedDesc,
			}
			replicas = append(replicas, replicaData)
			return nil
		}); err != nil {
			return loqrecoverypb.NodeReplicaInfo{}, err
		}
	}
	return loqrecoverypb.NodeReplicaInfo{Replicas: replicas}, nil
}

// checkUncommittedDescriptorsInRaftLog iterates over committed but not applied
// part of raft log and searches for writes to range descriptor. If any value
// referencing local range descriptor key is found then we have a potential
// pending update.
func checkUncommittedDescriptorsInRaftLog(
	rangeID roachpb.RangeID,
	rangeKey roachpb.RKey,
	startIndex uint64,
	endIndex uint64,
	reader storage.Reader,
) (bool, error) {
	rangeDescKey := keys.RangeDescriptorKey(rangeKey)

	key := keys.RaftLogKey(rangeID, startIndex)
	endKey := keys.RaftLogKey(rangeID, endIndex)
	iter := reader.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		UpperBound: endKey,
	})
	defer iter.Close()

	var meta enginepb.MVCCMetadata
	var ent raftpb.Entry

	iter.SeekGE(storage.MakeMVCCMetadataKey(key))
	for ; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil || !ok {
			return false, err
		}
		if err := protoutil.Unmarshal(iter.UnsafeValue(), &meta); err != nil {
			return false, errors.Wrap(err, "unable to decode raft log MVCCMetadata")
		}
		if err := storage.MakeValue(meta).GetProto(&ent); err != nil {
			return false, errors.Wrap(err, "unable to unmarshal raft Entry")
		}
		if ent.Type != raftpb.EntryNormal || len(ent.Data) == 0 {
			continue
		}
		_, cmdData := kvserver.DecodeRaftCommand(ent.Data)
		if len(cmdData) == 0 {
			continue
		}
		var raftCmd kvserverpb.RaftCommand
		if err := protoutil.Unmarshal(cmdData, &raftCmd); err != nil {
			return false, errors.Wrap(err, "unable to unmarshal raft command")
		}
		batchReader, err := storage.NewRocksDBBatchReader(raftCmd.WriteBatch.Data)
		if err != nil {
			return false, errors.Wrap(err, "unable to read entries from raft command")
		}
		for batchReader.Next() {
			switch batchReader.BatchType() {
			case storage.BatchTypeValue:
				mvccKey, err := batchReader.MVCCKey()
				if err == nil {
					if rangeDescKey.Equal(mvccKey.Key) {
						// TODO(oleg): don't forget to remove this debug info
						fmt.Printf("Found descriptor update at %s\n", mvccKey)
						return true, nil
					}
				}
			}
		}
	}
}
