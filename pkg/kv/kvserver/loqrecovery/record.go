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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// writeReplicaRecoveryStoreRecord adds a replica recovery record to store local
// part of key range. This entry is subsequently used on node startup to
// log the data and preserve this information for subsequent debugging as
// needed.
// Record keys have an index suffix. Every recovery run will find first unused
// slot and write records in keys with sequential index.
// See RegisterOfflineRecoveryEvents for details on where these records
// are read and deleted.
func writeReplicaRecoveryStoreRecord(
	uuid uuid.UUID,
	timestamp int64,
	update loqrecoverypb.ReplicaUpdate,
	report PrepareReplicaReport,
	updater *replicaPrepareBatch,
) error {
	record := loqrecoverypb.ReplicaRecoveryRecord{
		Timestamp:    timestamp,
		RangeID:      report.RangeID,
		StartKey:     update.StartKey,
		EndKey:       update.StartKey,
		OldReplicaID: report.OldReplica.ReplicaID,
		NewReplica:   update.NewReplica,
	}

	data, err := protoutil.Marshal(&record)
	if err != nil {
		return errors.Wrap(err, "failed to marshal update record entry")
	}
	if err := updater.readWriter.PutUnversioned(
		keys.StoreReplicaUnsafeRecoveryKey(uuid.GetBytes(), updater.nextUpdateRecordIndex), data); err != nil {
		return err
	}
	updater.nextUpdateRecordIndex++
	return nil
}

// RegisterOfflineRecoveryEvents checks if recovery data was captured in the store and writes
// appropriate structured entries to the log.
// This function is called on startup to ensure that any offline replica recovery actions
// are properly reflected in server logs as needed.
// Any new destinations for this info should be added to registerEvent function.
// If registerEvent returns true, value associated with a key is removed from store.
//
// TODO(oleg): #73679 add events to the rangelog. That would require registering events
// at the later stage of startup when SQL is already available.
func RegisterOfflineRecoveryEvents(
	ctx context.Context,
	readWriter storage.ReadWriter,
	registerEvent func(context.Context, loqrecoverypb.ReplicaRecoveryRecord) bool,
) (int, error) {
	eventCount := 0

	iter := readWriter.NewMVCCIterator(
		storage.MVCCKeyIterKind, storage.IterOptions{
			LowerBound: keys.LocalStoreUnsafeReplicaRecoveryKeyMin,
			UpperBound: keys.LocalStoreUnsafeReplicaRecoveryKeyMax,
		})
	defer iter.Close()

	iter.SeekGE(storage.MVCCKey{Key: keys.LocalStoreUnsafeReplicaRecoveryKeyMin})
	valid, err := iter.Valid()
	for ; valid && err == nil; valid, err = iter.Valid() {
		record := loqrecoverypb.ReplicaRecoveryRecord{}
		if err := iter.ValueProto(&record); err != nil {
			return eventCount, errors.Wrapf(
				err, "failed to deserialize replica recovery event at key %s",
				iter.UnsafeRawKey())
		}
		if registerEvent(ctx, record) {
			eventCount++
			if err := readWriter.ClearUnversioned(iter.UnsafeKey().Key); err != nil {
				return eventCount, errors.Wrapf(
					err, "failed to delete replica recovery record at index %s",
					iter.UnsafeRawKey())
			}
		}
		iter.Next()
	}
	return eventCount, nil
}
