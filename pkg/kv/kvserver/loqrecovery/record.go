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
// See RegisterOfflineRecoveryEvents for details on where these records
// are read and deleted.
func writeReplicaRecoveryStoreRecord(
	uuid uuid.UUID,
	timestamp int64,
	update loqrecoverypb.ReplicaUpdate,
	report PrepareReplicaReport,
	readWriter storage.ReadWriter,
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
	if err := readWriter.PutUnversioned(
		keys.StoreUnsafeReplicaRecoveryKey(uuid), data); err != nil {
		return err
	}
	return nil
}

// RegisterOfflineRecoveryEvents checks if recovery data was captured in the
// store and notifies callback about all registered events. Its up to the
// callback function to send events where appropriate. Events are removed
// from the store unless callback returns false or error. If latter case events
// would be reprocessed on subsequent call to this function.
// This function is called on startup to ensure that any offline replica
// recovery actions are properly reflected in server logs as needed.
//
// TODO(oleg): #73679 add events to the rangelog. That would require registering
// events at the later stage of startup when SQL is already available.
func RegisterOfflineRecoveryEvents(
	ctx context.Context,
	readWriter storage.ReadWriter,
	registerEvent func(context.Context, loqrecoverypb.ReplicaRecoveryRecord) (bool, error),
) (int, error) {
	successCount := 0
	var processingErrors error

	iter := readWriter.NewMVCCIterator(
		storage.MVCCKeyIterKind, storage.IterOptions{
			LowerBound: keys.LocalStoreUnsafeReplicaRecoveryKeyMin,
			UpperBound: keys.LocalStoreUnsafeReplicaRecoveryKeyMax,
		})
	defer iter.Close()

	iter.SeekGE(storage.MVCCKey{Key: keys.LocalStoreUnsafeReplicaRecoveryKeyMin})
	for ; ; iter.Next() {
		valid, err := iter.Valid()
		if err != nil {
			processingErrors = errors.CombineErrors(processingErrors,
				errors.Wrapf(err, "failed to iterate replica recovery record keys"))
			break
		}
		if !valid {
			break
		}

		record := loqrecoverypb.ReplicaRecoveryRecord{}
		if err := iter.ValueProto(&record); err != nil {
			processingErrors = errors.CombineErrors(processingErrors, errors.Wrapf(err,
				"failed to deserialize replica recovery event at key %s", iter.Key()))
			continue
		}
		removeEvent, err := registerEvent(ctx, record)
		if err != nil {
			processingErrors = errors.CombineErrors(processingErrors,
				errors.Wrapf(err, "replica recovery record processing failed"))
			continue
		}
		if removeEvent {
			if err := readWriter.ClearUnversioned(iter.UnsafeKey().Key); err != nil {
				processingErrors = errors.CombineErrors(processingErrors, errors.Wrapf(
					err, "failed to delete replica recovery record at key %s", iter.Key()))
				continue
			}
		}
		successCount++
	}
	if processingErrors != nil {
		return 0, errors.Wrapf(processingErrors,
			"failed to fully process replica recovery records, successfully processed %d", successCount)
	}
	return successCount, nil
}
