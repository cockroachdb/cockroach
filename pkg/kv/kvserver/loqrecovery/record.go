// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"context"
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
		Timestamp:       timestamp,
		RangeID:         report.RangeID(),
		StartKey:        update.StartKey,
		EndKey:          update.StartKey,
		OldReplicaID:    report.OldReplica.ReplicaID,
		NewReplica:      update.NewReplica,
		RangeDescriptor: report.Descriptor,
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
// store and notifies callback about all registered events. It's up to the
// callback function to send events where appropriate. Events are removed
// from the store unless callback returns false or error. If latter case events
// would be reprocessed on subsequent call to this function.
// This function is called on startup to ensure that any offline replica
// recovery actions are properly reflected in server logs as needed.
func RegisterOfflineRecoveryEvents(
	ctx context.Context,
	readWriter storage.ReadWriter,
	registerEvent func(context.Context, loqrecoverypb.ReplicaRecoveryRecord) (bool, error),
) (int, error) {
	successCount := 0
	var processingErrors error

	iter, err := readWriter.NewMVCCIterator(ctx, storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound: keys.LocalStoreUnsafeReplicaRecoveryKeyMin,
		UpperBound: keys.LocalStoreUnsafeReplicaRecoveryKeyMax,
	})
	if err != nil {
		return 0, err
	}
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
				"failed to deserialize replica recovery event at key %s", iter.UnsafeKey()))
			continue
		}
		removeEvent, err := registerEvent(ctx, record)
		if err != nil {
			processingErrors = errors.CombineErrors(processingErrors,
				errors.Wrapf(err, "replica recovery record processing failed"))
			continue
		}
		if removeEvent {
			if err := readWriter.ClearUnversioned(iter.UnsafeKey().Key, storage.ClearOptions{}); err != nil {
				processingErrors = errors.CombineErrors(processingErrors, errors.Wrapf(
					err, "failed to delete replica recovery record at key %s", iter.UnsafeKey()))
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

// UpdateRangeLogWithRecovery inserts a range log update to system.rangelog
// using information from recovery event.
func UpdateRangeLogWithRecovery(
	ctx context.Context,
	sqlExec func(ctx context.Context, stmt string, args ...interface{}) (int, error),
	event loqrecoverypb.ReplicaRecoveryRecord,
) error {
	const insertEventTableStmt = `
	INSERT INTO system.rangelog (
		timestamp, "rangeID", "storeID", "eventType", "otherRangeID", info
	)
	VALUES(
		$1, $2, $3, $4, $5, $6
	)
	`
	updateInfo := kvserverpb.RangeLogEvent_Info{
		UpdatedDesc:  &event.RangeDescriptor,
		AddedReplica: &event.NewReplica,
		Reason:       kvserverpb.ReasonUnsafeRecovery,
		Details:      "Performed unsafe range loss of quorum recovery",
	}
	infoBytes, err := json.Marshal(updateInfo)
	if err != nil {
		return errors.Wrap(err, "failed to serialize a RangeLog info entry")
	}
	args := []interface{}{
		timeutil.Unix(0, event.Timestamp),
		event.RangeID,
		event.NewReplica.StoreID,
		kvserverpb.RangeLogEventType_unsafe_quorum_recovery.String(),
		nil, // otherRangeID
		string(infoBytes),
	}

	rows, err := sqlExec(ctx, insertEventTableStmt, args...)
	if err != nil {
		return errors.Wrap(err, "failed to insert a RangeLog entry")
	}
	if rows != 1 {
		return errors.Errorf("%d row(s) affected by RangeLog insert while expected 1",
			rows)
	}
	return nil
}

func writeNodeRecoveryResults(
	ctx context.Context,
	writer storage.ReadWriter,
	result loqrecoverypb.PlanApplicationResult,
	actions loqrecoverypb.DeferredRecoveryActions,
) error {
	var fullErr error
	err := storage.MVCCPutProto(ctx, writer, keys.StoreLossOfQuorumRecoveryStatusKey(),
		hlc.Timestamp{}, &result, storage.MVCCWriteOptions{})
	fullErr = errors.Wrap(err, "failed to write loss of quorum recovery plan application status")
	if !actions.Empty() {
		err = storage.MVCCPutProto(ctx, writer, keys.StoreLossOfQuorumRecoveryCleanupActionsKey(),
			hlc.Timestamp{}, &actions, storage.MVCCWriteOptions{})
		fullErr = errors.CombineErrors(fullErr,
			errors.Wrap(err, "failed to write loss of quorum recovery cleanup action"))
	} else {
		_, _, err = storage.MVCCDelete(ctx, writer, keys.StoreLossOfQuorumRecoveryCleanupActionsKey(),
			hlc.Timestamp{}, storage.MVCCWriteOptions{})
		fullErr = errors.CombineErrors(fullErr,
			errors.Wrap(err, "failed to clean loss of quorum recovery cleanup action"))
	}
	return fullErr
}

func readNodeRecoveryStatusInfo(
	ctx context.Context, reader storage.Reader,
) (loqrecoverypb.PlanApplicationResult, bool, error) {
	var result loqrecoverypb.PlanApplicationResult
	ok, err := storage.MVCCGetProto(ctx, reader, keys.StoreLossOfQuorumRecoveryStatusKey(),
		hlc.Timestamp{}, &result, storage.MVCCGetOptions{})
	if err != nil {
		log.Error(ctx, "failed to read loss of quorum recovery plan application status")
		return loqrecoverypb.PlanApplicationResult{}, false, err
	}
	return result, ok, nil
}

// ReadCleanupActionsInfo reads cleanup actions info if it is present in the
// reader.
func ReadCleanupActionsInfo(
	ctx context.Context, writer storage.ReadWriter,
) (loqrecoverypb.DeferredRecoveryActions, bool, error) {
	var result loqrecoverypb.DeferredRecoveryActions
	exists, err := storage.MVCCGetProto(ctx, writer, keys.StoreLossOfQuorumRecoveryCleanupActionsKey(),
		hlc.Timestamp{}, &result, storage.MVCCGetOptions{})
	if err != nil {
		log.Errorf(ctx, "failed to read loss of quorum cleanup actions key: %s", err)
		return loqrecoverypb.DeferredRecoveryActions{}, false, err
	}
	return result, exists, nil
}

// RemoveCleanupActionsInfo removes cleanup actions info if it is present in the
// reader.
func RemoveCleanupActionsInfo(ctx context.Context, writer storage.ReadWriter) error {
	_, _, err := storage.MVCCDelete(ctx, writer, keys.StoreLossOfQuorumRecoveryCleanupActionsKey(),
		hlc.Timestamp{}, storage.MVCCWriteOptions{})
	return err
}
