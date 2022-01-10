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
			// the position after applied (inclusive) and read till the end of the log.
			// We also look on potentially uncommitted entries as we have to way to
			// determine their outcome and they will turn into committed as soon as the
			// replica designated as a survivor.
			rangeUpdates, err := getDescriptorChangesFromRaftLog(desc.RangeID,
				roachpb.Key(desc.StartKey), rstate.RaftAppliedIndex+1, math.MaxInt64, reader)
			if err != nil {
				return err
			}

			replicaData := loqrecoverypb.ReplicaInfo{
				StoreID:                   storeIdent.StoreID,
				NodeID:                    storeIdent.NodeID,
				Desc:                      desc,
				RaftAppliedIndex:          rstate.RaftAppliedIndex,
				RaftCommittedIndex:        hstate.Commit,
				HasUncommittedDescriptors: isUpdateAllowed(rangeUpdates),
			}
			replicas = append(replicas, replicaData)
			return nil
		}); err != nil {
			return loqrecoverypb.NodeReplicaInfo{}, err
		}
	}
	return loqrecoverypb.NodeReplicaInfo{Replicas: replicas}, nil
}

type descriptorChangeType int

const (
	raftChange descriptorChangeType = iota
	descriptorUpdate
	descriptorLockUpdate
	descriptorLockClear
)

func (c descriptorChangeType) String() string {
	switch c {
	case raftChange:
		return "raftChange"
	case descriptorUpdate:
		return "descriptorUpdate"
	case descriptorLockUpdate:
		return "descriptorLockUpdate"
	case descriptorLockClear:
		return "descriptorLockClear"
	}
	return "unknown"
}

// getDescriptorChangesFromRaftLog iterates over committed but not applied
// part of raft log and searches for writes to range descriptor and update
// to raft config. Former is identified by entry type, latter is by looking
// into the content of the batch to see if there are values referencing local
// range descriptor key or its lock table. If any of such changes found then
// we have a potential pending update.
func getDescriptorChangesFromRaftLog(
	rangeID roachpb.RangeID, startKey roachpb.Key, lo, hi uint64, reader storage.Reader,
) ([]descriptorChangeType, error) {
	rangeDescKey := keys.RangeDescriptorKey(roachpb.RKey(startKey))
	rangeLockKey, _ := keys.LockTableSingleKey(rangeDescKey, nil)

	var changes []descriptorChangeType

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
		if ent.Type == raftpb.EntryConfChange || ent.Type == raftpb.EntryConfChangeV2 {
			changes = append(changes, raftChange)
			continue
		}
		if len(ent.Data) == 0 {
			continue
		}
		_, cmdData := kvserver.DecodeRaftCommand(ent.Data)
		if len(cmdData) == 0 {
			continue
		}
		var raftCmd kvserverpb.RaftCommand
		if err := protoutil.Unmarshal(cmdData, &raftCmd); err != nil {
			return nil, errors.Wrap(err, "unable to unmarshal raft command")
		}
		// We need to introspect raft log entry to check if update batch contains
		// any changes to range descriptor itself or creation or removal of lock
		// entries for the descriptor. Descriptor entries are MVCC keys while
		// lock entries are not, so we could use that criteria to route
		// descriptor key or to lock range comparisons.
		batchReader, err := storage.NewRocksDBBatchReader(raftCmd.WriteBatch.Data)
		if err != nil {
			return nil, errors.Wrap(err, "unable to read entries from raft command")
		}
		for batchReader.Next() {
			var lockUpdateType descriptorChangeType
			switch batchReader.BatchType() {
			case storage.BatchTypeValue:
				lockUpdateType = descriptorLockUpdate
			case storage.BatchTypeDeletion, storage.BatchTypeSingleDeletion:
				lockUpdateType = descriptorLockClear
			default:
				continue
			}
			mvccKey, err := batchReader.MVCCKey()
			if err == nil {
				if rangeDescKey.Equal(mvccKey.Key) {
					changes = append(changes, descriptorUpdate)
					continue
				}
			} else {
				engKey, err := batchReader.EngineKey()
				if err != nil {
					return nil, errors.Wrap(err, "unable to decode entry key from raft command")
				}
				if rangeLockKey.Compare(engKey.Key) <= 0 &&
					rangeLockKey.PrefixEnd().Compare(engKey.Key) > 0 {
					changes = append(changes, lockUpdateType)
					continue
				}
			}
		}
	}
}

// isUpdateAllowed checks if raft log contains unapplied changes to range
// descriptor that could sufficiently change its state and affect recovery
// success.
// We only allow clear intent in the log since we remove it ourselves anyway
// as a part of recovery. Any other change to descriptor or raft group are
// forbidden since they could crash node when applied to doctored state of
// winning replica.
func isUpdateAllowed(updates []descriptorChangeType) bool {
	if len(updates) == 0 {
		return true
	}
	if len(updates) > 1 {
		return false
	}
	if updates[0] == descriptorLockClear {
		return true
	}
	return false
}
