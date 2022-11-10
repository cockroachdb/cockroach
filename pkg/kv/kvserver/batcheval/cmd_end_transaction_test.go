// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestEndTxnUpdatesTransactionRecord tests EndTxn request across its various
// possible transaction record state transitions and error cases.
func TestEndTxnUpdatesTransactionRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	startKey := roachpb.Key("0000")
	endKey := roachpb.Key("9999")
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}
	as := abortspan.New(desc.RangeID)

	k, k2 := roachpb.Key("a"), roachpb.Key("b")
	ts, ts2, ts3 := hlc.Timestamp{WallTime: 1}, hlc.Timestamp{WallTime: 2}, hlc.Timestamp{WallTime: 3}
	txn := roachpb.MakeTransaction("test", k, 0, ts, 0, 1)
	writes := []roachpb.SequencedWrite{{Key: k, Sequence: 0}}
	intents := []roachpb.Span{{Key: k2}}

	headerTxn := txn.Clone()
	pushedHeaderTxn := txn.Clone()
	pushedHeaderTxn.WriteTimestamp.Forward(ts2)
	refreshedHeaderTxn := txn.Clone()
	refreshedHeaderTxn.WriteTimestamp.Forward(ts2)
	refreshedHeaderTxn.ReadTimestamp.Forward(ts2)
	restartedHeaderTxn := txn.Clone()
	restartedHeaderTxn.Restart(-1, 0, ts2)
	restartedAndPushedHeaderTxn := txn.Clone()
	restartedAndPushedHeaderTxn.Restart(-1, 0, ts2)
	restartedAndPushedHeaderTxn.WriteTimestamp.Forward(ts3)
	committedHeaderTxn := txn.Clone()
	committedHeaderTxn.Status = roachpb.COMMITTED

	pendingRecord := func() *roachpb.TransactionRecord {
		record := txn.AsRecord()
		record.Status = roachpb.PENDING
		return &record
	}()
	stagingRecord := func() *roachpb.TransactionRecord {
		record := txn.AsRecord()
		record.Status = roachpb.STAGING
		record.LockSpans = intents
		record.InFlightWrites = writes
		return &record
	}()
	committedRecord := func() *roachpb.TransactionRecord {
		record := txn.AsRecord()
		record.Status = roachpb.COMMITTED
		record.LockSpans = intents
		return &record
	}()
	abortedRecord := func() *roachpb.TransactionRecord {
		record := txn.AsRecord()
		record.Status = roachpb.ABORTED
		record.LockSpans = intents
		return &record
	}()

	testCases := []struct {
		name string
		// Replica state.
		existingTxn  *roachpb.TransactionRecord
		canCreateTxn func() (can bool, minTS hlc.Timestamp)
		// Request state.
		headerTxn      *roachpb.Transaction
		commit         bool
		noLockSpans    bool
		inFlightWrites []roachpb.SequencedWrite
		deadline       hlc.Timestamp
		// Expected result.
		expError string
		expTxn   *roachpb.TransactionRecord
	}{
		{
			// Standard case where a transaction is rolled back when
			// there are intents to clean up.
			name: "record missing, try rollback",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: nil, // not needed
			// Request state.
			headerTxn: headerTxn,
			commit:    false,
			// Expected result.
			// If the transaction record doesn't exist, a rollback that needs
			// to record remote intents will create it.
			expTxn: abortedRecord,
		},
		{
			// Non-standard case. Mimics a transaction being cleaned up
			// when all intents are on the transaction record's range.
			name: "record missing, try rollback without intents",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: nil, // not needed
			// Request state.
			headerTxn:   headerTxn,
			commit:      false,
			noLockSpans: true,
			// Expected result.
			// If the transaction record doesn't exist, a rollback that doesn't
			// need to record any remote intents won't create it.
			expTxn: nil,
		},
		{
			// Either a PushTxn(ABORT) request succeeded or this is a replay
			// and the transaction has already been finalized. Either way,
			// the request isn't allowed to create a new transaction record.
			name: "record missing, can't create, try stage",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return false, hlc.Timestamp{} },
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
		},
		{
			// Either a PushTxn(ABORT) request succeeded or this is a replay
			// and the transaction has already been finalized. Either way,
			// the request isn't allowed to create a new transaction record.
			name: "record missing, can't create, try commit",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return false, hlc.Timestamp{} },
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
		},
		{
			// Standard case where a transaction record is created during a
			// parallel commit.
			name: "record missing, can create, try stage",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, hlc.Timestamp{} },
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expTxn: stagingRecord,
		},
		{
			// Standard case where a transaction record is created during a
			// non-parallel commit.
			name: "record missing, can create, try commit",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, hlc.Timestamp{} },
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expTxn: committedRecord,
		},
		{
			// Standard case where a transaction record is created during a
			// parallel commit when all writes are still in-flight.
			name: "record missing, can create, try stage without intents",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, hlc.Timestamp{} },
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			noLockSpans:    true,
			inFlightWrites: writes,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *stagingRecord
				record.LockSpans = nil
				return &record
			}(),
		},
		{
			// Non-standard case where a transaction record is created during a
			// non-parallel commit when there are no intents. Mimics a transaction
			// being committed when all intents are on the transaction record's
			// range.
			name: "record missing, can create, try commit without intents",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, hlc.Timestamp{} },
			// Request state.
			headerTxn:   headerTxn,
			commit:      true,
			noLockSpans: true,
			// Expected result.
			// If the transaction record doesn't exist, a commit that doesn't
			// need to record any remote intents won't create it.
			expTxn: nil,
		},
		{
			// The transaction's commit timestamp was increased during its
			// lifetime, but it hasn't refreshed up to its new commit timestamp.
			// The stage will be rejected.
			name: "record missing, can create, try stage at pushed timestamp",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, hlc.Timestamp{} },
			// Request state.
			headerTxn:      pushedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// The transaction's commit timestamp was increased during its
			// lifetime, but it hasn't refreshed up to its new commit timestamp.
			// The commit will be rejected.
			name: "record missing, can create, try commit at pushed timestamp",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, hlc.Timestamp{} },
			// Request state.
			headerTxn: pushedHeaderTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// The transaction's commit timestamp was increased during its
			// lifetime and it has refreshed up to this timestamp. The stage
			// will succeed.
			name: "record missing, can create, try stage at pushed timestamp after refresh",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, hlc.Timestamp{} },
			// Request state.
			headerTxn:      refreshedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *stagingRecord
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// The transaction's commit timestamp was increased during its
			// lifetime and it has refreshed up to this timestamp. The commit
			// will succeed.
			name: "record missing, can create, try commit at pushed timestamp after refresh",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, hlc.Timestamp{} },
			// Request state.
			headerTxn: refreshedHeaderTxn,
			commit:    true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *committedRecord
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that the
			// transaction can be created with. This will trigger a retry error.
			name: "record missing, can create with min timestamp, try stage",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, ts2 },
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that the
			// transaction can be created with. This will trigger a retry error.
			name: "record missing, can create with min timestamp, try commit",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, ts2 },
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that
			// the transaction can be created with. Luckily, the transaction has
			// already refreshed above this time, so it can avoid a retry error.
			name: "record missing, can create with min timestamp, try stage at pushed timestamp after refresh",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, ts2 },
			// Request state.
			headerTxn:      refreshedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *stagingRecord
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that
			// the transaction can be created with. Luckily, the transaction has
			// already refreshed above this time, so it can avoid a retry error.
			name: "record missing, can create with min timestamp, try commit at pushed timestamp after refresh",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, ts2 },
			// Request state.
			headerTxn: refreshedHeaderTxn,
			commit:    true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *committedRecord
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// The transaction has run into a WriteTooOld error during its
			// lifetime. The stage will be rejected.
			name: "record missing, can create, try stage after write too old",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, hlc.Timestamp{} },
			// Request state.
			headerTxn: func() *roachpb.Transaction {
				clone := txn.Clone()
				clone.WriteTooOld = true
				return clone
			}(),
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_WRITE_TOO_OLD)",
		},
		{
			// The transaction has run into a WriteTooOld error during its
			// lifetime. The stage will be rejected.
			name: "record missing, can create, try commit after write too old",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, hlc.Timestamp{} },
			// Request state.
			headerTxn: func() *roachpb.Transaction {
				clone := txn.Clone()
				clone.WriteTooOld = true
				return clone
			}(),
			commit: true,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_WRITE_TOO_OLD)",
		},
		{
			// Standard case where a transaction is rolled back. The record
			// already exists because it has been heartbeated.
			name: "record pending, try rollback",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    false,
			// Expected result.
			expTxn: abortedRecord,
		},
		{
			// Standard case where a transaction record is created during a
			// parallel commit. The record already exists because it has been
			// heartbeated.
			name: "record pending, try stage",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expTxn: stagingRecord,
		},
		{
			// Standard case where a transaction record is created during a
			// non-parallel commit. The record already exists because it has
			// been heartbeated.
			name: "record pending, try commit",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expTxn: committedRecord,
		},
		{
			// The transaction's commit timestamp was increased during its
			// lifetime, but it hasn't refreshed up to its new commit timestamp.
			// The stage will be rejected.
			name: "record pending, try stage at pushed timestamp",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn:      pushedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// The transaction's commit timestamp was increased during its
			// lifetime, but it hasn't refreshed up to its new commit timestamp.
			// The commit will be rejected.
			name: "record pending, try commit at pushed timestamp",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: pushedHeaderTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// The transaction's commit timestamp was increased during its
			// lifetime and it has refreshed up to this timestamp. The stage
			// will succeed.
			name: "record pending, try stage at pushed timestamp after refresh",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn:      refreshedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *stagingRecord
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// The transaction's commit timestamp was increased during its
			// lifetime and it has refreshed up to this timestamp. The commit
			// will succeed.
			name: "record pending, try commit at pushed timestamp after refresh",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: refreshedHeaderTxn,
			commit:    true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *committedRecord
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// The transaction has run into a WriteTooOld error during its
			// lifetime. The stage will be rejected.
			name: "record pending, try stage after write too old",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: func() *roachpb.Transaction {
				clone := txn.Clone()
				clone.WriteTooOld = true
				return clone
			}(),
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_WRITE_TOO_OLD)",
		},
		{
			// The transaction has run into a WriteTooOld error during its
			// lifetime. The stage will be rejected.
			name: "record pending, try commit after write too old",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: func() *roachpb.Transaction {
				clone := txn.Clone()
				clone.WriteTooOld = true
				return clone
			}(),
			commit: true,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_WRITE_TOO_OLD)",
		},
		{
			// Standard case where a transaction is rolled back after it has
			// written a record at a lower epoch. The existing record is
			// upgraded.
			name: "record pending, try rollback at higher epoch",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: restartedHeaderTxn,
			commit:    false,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *abortedRecord
				record.Epoch++
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// Standard case where a transaction record is created during a
			// parallel commit after it has written a record at a lower epoch.
			// The existing record is upgraded.
			name: "record pending, try stage at higher epoch",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn:      restartedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *stagingRecord
				record.Epoch++
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// Standard case where a transaction record is created during a
			// non-parallel commit after it has written a record at a lower
			// epoch. The existing record is upgraded.
			name: "record pending, try commit at higher epoch",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: restartedHeaderTxn,
			commit:    true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *committedRecord
				record.Epoch++
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// The transaction's commit timestamp was increased during the
			// current epoch, but it hasn't refreshed up to its new commit
			// timestamp. The stage will be rejected.
			name: "record pending, try stage at higher epoch and pushed timestamp",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn:      restartedAndPushedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// The transaction's commit timestamp was increased during the
			// current epoch, but it hasn't refreshed up to its new commit
			// timestamp. The commit will be rejected.
			name: "record pending, try commit at higher epoch and pushed timestamp",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: restartedAndPushedHeaderTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// Standard case where a transaction is rolled back. The record
			// already exists because of a failed parallel commit attempt in
			// the same epoch.
			//
			// The rollback is not considered an authoritative indication that the
			// transaction is not implicitly committed, so an indeterminate commit
			// error is returned to force transaction recovery to be performed.
			name: "record staging, try rollback at same epoch",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    false,
			// Expected result.
			expError: "found txn in indeterminate STAGING state",
		},
		{
			// Standard case where a transaction record is created during a
			// parallel commit. The record already exists because of a failed
			// parallel commit attempt.
			name: "record staging, try re-stage",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expTxn: stagingRecord,
		},
		{
			// Standard case where a transaction record is created during a
			// non-parallel commit. The record already exists because of a
			// failed parallel commit attempt.
			name: "record staging, try commit",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expTxn: committedRecord,
		},
		{
			// Non-standard case where a transaction record is created during a
			// parallel commit. The record already exists because of a failed
			// parallel commit attempt. The re-stage will fail because of the
			// pushed timestamp.
			name: "record staging, try re-stage at pushed timestamp",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn:      pushedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// Non-standard case where a transaction record is created during
			// a non-parallel commit. The record already exists because of a
			// failed parallel commit attempt. The commit will fail because of
			// the pushed timestamp.
			name: "record staging, try commit at pushed timestamp",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn: pushedHeaderTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// Non-standard case where a transaction is rolled back. The record
			// already exists because of a failed parallel commit attempt in a
			// prior epoch.
			name: "record staging, try rollback at higher epoch",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn: restartedHeaderTxn,
			commit:    false,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *abortedRecord
				record.Epoch++
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// Non-standard case where a transaction record is created during a
			// parallel commit. The record already exists because of a failed
			// parallel commit attempt in a prior epoch.
			name: "record staging, try re-stage at higher epoch",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn:      restartedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *stagingRecord
				record.Epoch++
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// Non-standard case where a transaction record is created during
			// a non-parallel commit. The record already exists because of a
			// failed parallel commit attempt in a prior epoch.
			name: "record staging, try commit at higher epoch",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn: restartedHeaderTxn,
			commit:    true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *committedRecord
				record.Epoch++
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// Non-standard case where a transaction record is created during a
			// parallel commit. The record already exists because of a failed
			// parallel commit attempt in a prior epoch. The re-stage will fail
			// because of the pushed timestamp.
			name: "record staging, try re-stage at higher epoch and pushed timestamp",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn:      restartedAndPushedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// Non-standard case where a transaction record is created during a
			// non-parallel commit. The record already exists because of a
			// failed parallel commit attempt in a prior epoch. The commit will
			// fail because of the pushed timestamp.
			name: "record staging, try commit at higher epoch and pushed timestamp",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn: restartedAndPushedHeaderTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// The transaction has already been aborted. The client will often
			// send a rollback to resolve any intents and start cleaning up the
			// transaction.
			name: "record aborted, try rollback",
			// Replica state.
			existingTxn: abortedRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    false,
			// Expected result.
			expTxn: abortedRecord,
		},
		///////////////////////////////////////////////////////////////////////
		//                    INVALID REQUEST ERROR CASES                    //
		///////////////////////////////////////////////////////////////////////
		{
			name: "record pending, try rollback at lower epoch",
			// Replica state.
			existingTxn: func() *roachpb.TransactionRecord {
				record := *pendingRecord
				record.Epoch++
				return &record
			}(),
			// Request state.
			headerTxn: headerTxn,
			commit:    false,
			// Expected result.
			expError: "programming error: epoch regression",
		},
		{
			name: "record pending, try stage at lower epoch",
			// Replica state.
			existingTxn: func() *roachpb.TransactionRecord {
				record := *pendingRecord
				record.Epoch++
				return &record
			}(),
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "programming error: epoch regression",
		},
		{
			name: "record pending, try commit at lower epoch",
			// Replica state.
			existingTxn: func() *roachpb.TransactionRecord {
				record := *pendingRecord
				record.Epoch++
				return &record
			}(),
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expError: "programming error: epoch regression",
		},
		{
			name: "record committed, try rollback",
			// Replica state.
			existingTxn: committedRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    false,
			// Expected result.
			expError: "TransactionStatusError: already committed (REASON_TXN_COMMITTED)",
		},
		{
			name: "record and header committed, try rollback",
			// Replica state.
			existingTxn: committedRecord,
			// Request state.
			headerTxn: committedHeaderTxn,
			commit:    false,
			// Expected result.
			expError: "TransactionStatusError: cannot perform EndTxn with txn status COMMITTED (REASON_TXN_COMMITTED)",
		},
		{
			name: "record committed, try stage",
			// Replica state.
			existingTxn: committedRecord,
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "TransactionStatusError: already committed (REASON_TXN_COMMITTED)",
		},
		{
			name: "record committed, try commit",
			// Replica state.
			existingTxn: committedRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionStatusError: already committed (REASON_TXN_COMMITTED)",
		},
		{
			name: "record and header committed, try commit",
			// Replica state.
			existingTxn: committedRecord,
			// Request state.
			headerTxn: committedHeaderTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionStatusError: cannot perform EndTxn with txn status COMMITTED (REASON_TXN_COMMITTED)",
		},
		{
			name: "record aborted, try stage",
			// Replica state.
			existingTxn: abortedRecord,
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
		},
		{
			name: "record aborted, try commit",
			// Replica state.
			existingTxn: abortedRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			db := storage.NewDefaultInMemForTesting()
			defer db.Close()
			batch := db.NewBatch()
			defer batch.Close()

			// Write the existing transaction record, if necessary.
			txnKey := keys.TransactionKey(txn.Key, txn.ID)
			if c.existingTxn != nil {
				if err := storage.MVCCPutProto(ctx, batch, nil, txnKey, hlc.Timestamp{}, hlc.ClockTimestamp{}, nil, c.existingTxn); err != nil {
					t.Fatal(err)
				}
			}

			// Sanity check request args.
			if !c.commit {
				require.Nil(t, c.inFlightWrites)
				require.Zero(t, c.deadline)
			}

			// Issue an EndTxn request.
			req := roachpb.EndTxnRequest{
				RequestHeader: roachpb.RequestHeader{Key: txn.Key},
				Commit:        c.commit,

				InFlightWrites: c.inFlightWrites,
				Deadline:       c.deadline,
			}
			if !c.noLockSpans {
				req.LockSpans = intents
			}
			var resp roachpb.EndTxnResponse
			_, err := EndTxn(ctx, batch, CommandArgs{
				EvalCtx: (&MockEvalCtx{
					Desc:      &desc,
					AbortSpan: as,
					CanCreateTxn: func() (bool, hlc.Timestamp, roachpb.TransactionAbortedReason) {
						require.NotNil(t, c.canCreateTxn, "CanCreateTxnRecord unexpectedly called")
						if can, minTS := c.canCreateTxn(); can {
							return true, minTS, 0
						}
						return false, hlc.Timestamp{}, roachpb.ABORT_REASON_ABORTED_RECORD_FOUND
					},
				}).EvalContext(),
				Args: &req,
				Header: roachpb.Header{
					Timestamp: ts,
					Txn:       c.headerTxn,
				},
			}, &resp)

			if c.expError != "" {
				if !testutils.IsError(err, regexp.QuoteMeta(c.expError)) {
					t.Fatalf("expected error %q; found %v", c.expError, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}

				// Assert that the txn record is written as expected.
				var resTxnRecord roachpb.TransactionRecord
				if ok, err := storage.MVCCGetProto(
					ctx, batch, txnKey, hlc.Timestamp{}, &resTxnRecord, storage.MVCCGetOptions{},
				); err != nil {
					t.Fatal(err)
				} else if c.expTxn == nil {
					require.False(t, ok, "unexpected transaction record found")
				} else {
					require.True(t, ok, "expected transaction record, one not found")
					require.Equal(t, *c.expTxn, resTxnRecord)
				}
			}
		})
	}
}

// TestPartialRollbackOnEndTransaction verifies that the intent
// resolution performed synchronously as a side effect of
// EndTransaction request properly takes into account the ignored
// seqnum list.
func TestPartialRollbackOnEndTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	k := roachpb.Key("a")
	ts := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	txn := roachpb.MakeTransaction("test", k, 0, ts, 0, 1)
	endKey := roachpb.Key("z")
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(k),
		EndKey:   roachpb.RKey(endKey),
	}
	intents := []roachpb.Span{{Key: k}}

	// We want to inspect the final txn record after EndTxn, to
	// ascertain that it persists the ignore list.
	defer TestingSetTxnAutoGC(false)()

	testutils.RunTrueAndFalse(t, "withStoredTxnRecord", func(t *testing.T, storeTxnBeforeEndTxn bool) {
		db := storage.NewDefaultInMemForTesting()
		defer db.Close()
		batch := db.NewBatch()
		defer batch.Close()

		var v roachpb.Value

		// Write a first value at key.
		v.SetString("a")
		txn.Sequence = 1
		if err := storage.MVCCPut(ctx, batch, nil, k, ts, hlc.ClockTimestamp{}, v, &txn); err != nil {
			t.Fatal(err)
		}
		// Write another value.
		v.SetString("b")
		txn.Sequence = 2
		if err := storage.MVCCPut(ctx, batch, nil, k, ts, hlc.ClockTimestamp{}, v, &txn); err != nil {
			t.Fatal(err)
		}

		// Partially revert the store above.
		txn.IgnoredSeqNums = []enginepb.IgnoredSeqNumRange{{Start: 2, End: 2}}

		// We test with and without a stored txn record, so as to exercise
		// the two branches of EndTxn() and verify that the ignored seqnum
		// list is properly persisted in the stored transaction record.
		txnKey := keys.TransactionKey(txn.Key, txn.ID)
		if storeTxnBeforeEndTxn {
			txnRec := txn.AsRecord()
			if err := storage.MVCCPutProto(ctx, batch, nil, txnKey, hlc.Timestamp{}, hlc.ClockTimestamp{}, nil, &txnRec); err != nil {
				t.Fatal(err)
			}
		}

		// Issue the end txn command.
		req := roachpb.EndTxnRequest{
			RequestHeader: roachpb.RequestHeader{Key: txn.Key},
			Commit:        true,
			LockSpans:     intents,
		}
		var resp roachpb.EndTxnResponse
		if _, err := EndTxn(ctx, batch, CommandArgs{
			EvalCtx: (&MockEvalCtx{
				Desc: &desc,
				CanCreateTxn: func() (bool, hlc.Timestamp, roachpb.TransactionAbortedReason) {
					return true, ts, 0
				},
			}).EvalContext(),
			Args: &req,
			Header: roachpb.Header{
				Timestamp: ts,
				Txn:       &txn,
			},
		}, &resp); err != nil {
			t.Fatal(err)
		}

		// The second write has been rolled back; verify that the remaining
		// value is from the first write.
		res, i, err := storage.MVCCGet(ctx, batch, k, ts2, storage.MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if i != nil {
			t.Errorf("found intent, expected none: %+v", i)
		}
		if res == nil {
			t.Errorf("no value found, expected one")
		} else {
			s, err := res.GetBytes()
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, "a", string(s))
		}

		// Also verify that the txn record contains the ignore list.
		var txnRec roachpb.TransactionRecord
		hasRec, err := storage.MVCCGetProto(ctx, batch, txnKey, hlc.Timestamp{}, &txnRec, storage.MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !hasRec {
			t.Error("expected txn record remaining after test, found none")
		} else {
			require.Equal(t, txn.IgnoredSeqNums, txnRec.IgnoredSeqNums)
		}
	})
}

// TestAssertNoCommitWaitIfCommitTrigger tests that an EndTxn that carries a
// commit trigger and needs to commit-wait because it has a commit timestamp in
// the future will return an assertion error. Such situations should trigger a
// higher-level hook (maybeCommitWaitBeforeCommitTrigger) to perform the commit
// wait sleep before the request acquires latches and begins evaluating.
func TestCommitWaitBeforeIntentResolutionIfCommitTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "commitTrigger", func(t *testing.T, commitTrigger bool) {
		for _, cfg := range []struct {
			name     string
			commitTS func(now hlc.Timestamp) hlc.Timestamp
			expError bool
		}{
			{
				name:     "past",
				commitTS: func(now hlc.Timestamp) hlc.Timestamp { return now },
				expError: false,
			},
			{
				name:     "past-syn",
				commitTS: func(now hlc.Timestamp) hlc.Timestamp { return now.WithSynthetic(true) },
				expError: false,
			},
			{
				name:     "future-syn",
				commitTS: func(now hlc.Timestamp) hlc.Timestamp { return now.Add(100, 0).WithSynthetic(true) },
				// If the EndTxn carried a commit trigger and its transaction will need
				// to commit-wait because the transaction has a future-time commit
				// timestamp, evaluating the request should return an error.
				expError: commitTrigger,
			},
		} {
			t.Run(cfg.name, func(t *testing.T) {
				ctx := context.Background()
				db := storage.NewDefaultInMemForTesting()
				defer db.Close()
				batch := db.NewBatch()
				defer batch.Close()

				clock := hlc.NewClock(timeutil.NewManualTime(timeutil.Unix(0, 123)), time.Nanosecond /* maxOffset */)
				desc := roachpb.RangeDescriptor{
					RangeID:  99,
					StartKey: roachpb.RKey("a"),
					EndKey:   roachpb.RKey("z"),
				}

				now := clock.Now()
				commitTS := cfg.commitTS(now)
				txn := roachpb.MakeTransaction("test", desc.StartKey.AsRawKey(), 0, now, 0, 1)
				txn.ReadTimestamp = commitTS
				txn.WriteTimestamp = commitTS

				// Issue the end txn command.
				req := roachpb.EndTxnRequest{
					RequestHeader: roachpb.RequestHeader{Key: txn.Key},
					Commit:        true,
				}
				if commitTrigger {
					req.InternalCommitTrigger = &roachpb.InternalCommitTrigger{
						ModifiedSpanTrigger: &roachpb.ModifiedSpanTrigger{NodeLivenessSpan: &roachpb.Span{}},
					}
				}
				var resp roachpb.EndTxnResponse
				_, err := EndTxn(ctx, batch, CommandArgs{
					EvalCtx: (&MockEvalCtx{
						Desc:  &desc,
						Clock: clock,
						CanCreateTxn: func() (bool, hlc.Timestamp, roachpb.TransactionAbortedReason) {
							return true, hlc.Timestamp{}, 0
						},
					}).EvalContext(),
					Args: &req,
					Header: roachpb.Header{
						Timestamp: commitTS,
						Txn:       &txn,
					},
				}, &resp)

				if cfg.expError {
					require.Error(t, err)
					require.Regexp(t, `txn .* with modified-span \(node-liveness\) commit trigger needs commit wait`, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})
}

func TestComputeSplitRangeKeyStatsDelta(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	storage.DisableMetamorphicSimpleValueEncoding(t)

	emptyValue := func() storage.MVCCValue {
		return storage.MVCCValue{}
	}

	localTSValue := func(ts int) storage.MVCCValue {
		var v storage.MVCCValue
		v.MVCCValueHeader.LocalTimestamp = hlc.ClockTimestamp{WallTime: int64(ts)}
		return v
	}

	rangeKV := func(start, end string, ts int, value storage.MVCCValue) storage.MVCCRangeKeyValue {
		valueRaw, err := storage.EncodeMVCCValue(value)
		require.NoError(t, err)
		return storage.MVCCRangeKeyValue{
			RangeKey: storage.MVCCRangeKey{
				StartKey:  roachpb.Key(start),
				EndKey:    roachpb.Key(end),
				Timestamp: hlc.Timestamp{WallTime: int64(ts)},
			},
			Value: valueRaw,
		}
	}

	const nowNanos = 10e9
	lhsDesc := roachpb.RangeDescriptor{StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("l")}
	rhsDesc := roachpb.RangeDescriptor{StartKey: roachpb.RKey("l"), EndKey: roachpb.RKey("z").PrefixEnd()}

	testcases := map[string]struct {
		rangeKVs []storage.MVCCRangeKeyValue
		expect   enginepb.MVCCStats
	}{
		// Empty stats shouldn't do anything.
		"empty": {nil, enginepb.MVCCStats{}},
		// a-z splits into a-l and l-z: simple +1 range key
		"full": {[]storage.MVCCRangeKeyValue{rangeKV("a", "z", 1e9, emptyValue())}, enginepb.MVCCStats{
			RangeKeyCount: 1,
			RangeKeyBytes: 13,
			RangeValCount: 1,
			GCBytesAge:    117,
		}},
		// a-z with local timestamp splits into a-l and l-z: simple +1 range key with value
		"full value": {[]storage.MVCCRangeKeyValue{rangeKV("a", "z", 2e9, localTSValue(1))}, enginepb.MVCCStats{
			RangeKeyCount: 1,
			RangeKeyBytes: 13,
			RangeValCount: 1,
			RangeValBytes: 9,
			GCBytesAge:    176,
		}},
		// foo-zz splits into foo-l and l-zzzz: contribution is same as for short
		// keys, because we have to adjust for the change in LHS end key which ends
		// up only depending on the split key, and that doesn't change.
		"different key length": {[]storage.MVCCRangeKeyValue{rangeKV("foo", "zzzz", 1e9, emptyValue())}, enginepb.MVCCStats{
			RangeKeyCount: 1,
			RangeKeyBytes: 13,
			RangeValCount: 1,
			GCBytesAge:    117,
		}},
		// Two abutting keys at different timestamps at the split point should not
		// require a delta.
		"no straddling, timestamp": {[]storage.MVCCRangeKeyValue{
			rangeKV("a", "l", 1e9, emptyValue()),
			rangeKV("l", "z", 2e9, emptyValue()),
		}, enginepb.MVCCStats{}},
		// Two abutting keys at different local timestamps (values) at the split
		// point should not require a delta.
		"no straddling, value": {[]storage.MVCCRangeKeyValue{
			rangeKV("a", "l", 2e9, localTSValue(1)),
			rangeKV("l", "z", 2e9, localTSValue(2)),
		}, enginepb.MVCCStats{}},
		// Multiple straddling keys.
		"multiple": {
			[]storage.MVCCRangeKeyValue{
				rangeKV("a", "z", 2e9, localTSValue(1)),
				rangeKV("k", "p", 3e9, localTSValue(2)),
				rangeKV("foo", "m", 4e9, emptyValue()),
			}, enginepb.MVCCStats{
				RangeKeyCount: 1,
				RangeKeyBytes: 31,
				RangeValCount: 3,
				RangeValBytes: 18,
				GCBytesAge:    348,
			}},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			engine := storage.NewDefaultInMemForTesting()
			defer engine.Close()

			for _, rkv := range tc.rangeKVs {
				value, err := storage.DecodeMVCCValue(rkv.Value)
				require.NoError(t, err)
				require.NoError(t, engine.PutMVCCRangeKey(rkv.RangeKey, value))
			}

			tc.expect.LastUpdateNanos = nowNanos

			msDelta, err := computeSplitRangeKeyStatsDelta(engine, lhsDesc, rhsDesc)
			require.NoError(t, err)
			msDelta.AgeTo(nowNanos)
			require.Equal(t, tc.expect, msDelta)
		})
	}
}

func TestResolveLocalLocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	intToKey := func(i int) roachpb.Key {
		return roachpb.Key(fmt.Sprintf("%01000d", i))
	}

	const (
		pointMaxKeysNumLocks        = 700
		rangeMaxKeysNumLocks        = 100
		rangeMaxKeysIntentsPerLock  = 7
		pointMaxBytesNumLocks       = 20
		rangeMaxBytesNumLocks       = 5
		rangeMaxBytesIntentsPerLock = 4
		maxBytesNumResolvedLocks    = 12
	)

	lockSpansPointMaxKeys := make([]roachpb.Span, pointMaxKeysNumLocks)
	for i := range lockSpansPointMaxKeys {
		lockSpansPointMaxKeys[i].Key = intToKey(i)
	}
	expectedResolvedLocksPointMaxKeys := make([]roachpb.Span, lockResolutionBatchSize)
	for i := range expectedResolvedLocksPointMaxKeys {
		expectedResolvedLocksPointMaxKeys[i].Key = intToKey(i)
	}
	expectedExternalLocksPointMaxKeys := make([]roachpb.Span, pointMaxKeysNumLocks-lockResolutionBatchSize)
	for i := range expectedExternalLocksPointMaxKeys {
		expectedExternalLocksPointMaxKeys[i].Key = intToKey(i + lockResolutionBatchSize)
	}

	lockSpansRangeMaxKeys := make([]roachpb.Span, rangeMaxKeysNumLocks)
	for i := range lockSpansRangeMaxKeys {
		lockSpansRangeMaxKeys[i].Key = intToKey(rangeMaxKeysIntentsPerLock * i)
		lockSpansRangeMaxKeys[i].EndKey = intToKey(rangeMaxKeysIntentsPerLock * (i + 1))
	}
	expectedResolvedLocksRangeMaxKeys := make([]roachpb.Span, lockResolutionBatchSize/rangeMaxKeysIntentsPerLock+1)
	for i := range expectedResolvedLocksRangeMaxKeys {
		expectedResolvedLocksRangeMaxKeys[i].Key = intToKey(rangeMaxKeysIntentsPerLock * i)
		if i == len(expectedResolvedLocksRangeMaxKeys)-1 {
			expectedResolvedLocksRangeMaxKeys[i].EndKey = intToKey(lockResolutionBatchSize - 1).Next()
		} else {
			expectedResolvedLocksRangeMaxKeys[i].EndKey = intToKey(rangeMaxKeysIntentsPerLock * (i + 1))
		}
	}
	expectedExternalLocksRangeMaxKeys := make([]roachpb.Span, rangeMaxKeysNumLocks-lockResolutionBatchSize/rangeMaxKeysIntentsPerLock)
	for i := range expectedExternalLocksRangeMaxKeys {
		offset := lockResolutionBatchSize / rangeMaxKeysIntentsPerLock
		if i == 0 {
			expectedExternalLocksRangeMaxKeys[i].Key = intToKey(lockResolutionBatchSize - 1).Next()
		} else {
			expectedExternalLocksRangeMaxKeys[i].Key = intToKey(rangeMaxKeysIntentsPerLock * (i + offset))
		}
		expectedExternalLocksRangeMaxKeys[i].EndKey = intToKey(rangeMaxKeysIntentsPerLock * (i + offset + 1))
	}

	lockSpansPointMaxBytes := make([]roachpb.Span, pointMaxBytesNumLocks)
	for i := range lockSpansPointMaxBytes {
		lockSpansPointMaxBytes[i].Key = intToKey(i)
	}
	expectedResolvedLocksPointMaxBytes := make([]roachpb.Span, maxBytesNumResolvedLocks)
	for i := range expectedResolvedLocksPointMaxBytes {
		expectedResolvedLocksPointMaxBytes[i].Key = intToKey(i)
	}
	expectedExternalLocksPointMaxBytes := make([]roachpb.Span, pointMaxBytesNumLocks-maxBytesNumResolvedLocks)
	for i := range expectedExternalLocksPointMaxBytes {
		expectedExternalLocksPointMaxBytes[i].Key = intToKey(i + maxBytesNumResolvedLocks)
	}

	lockSpansRangeMaxBytes := make([]roachpb.Span, rangeMaxBytesNumLocks)
	for i := range lockSpansRangeMaxBytes {
		lockSpansRangeMaxBytes[i].Key = intToKey(rangeMaxBytesIntentsPerLock * i)
		lockSpansRangeMaxBytes[i].EndKey = intToKey(rangeMaxBytesIntentsPerLock * (i + 1))
	}
	expectedResolvedLocksRangeMaxBytes := make([]roachpb.Span, maxBytesNumResolvedLocks/rangeMaxBytesIntentsPerLock)
	for i := range expectedResolvedLocksRangeMaxBytes {
		expectedResolvedLocksRangeMaxBytes[i].Key = intToKey(rangeMaxBytesIntentsPerLock * i)
		expectedResolvedLocksRangeMaxBytes[i].EndKey = intToKey(rangeMaxBytesIntentsPerLock * (i + 1))
	}
	expectedExternalLocksRangeMaxBytes := make([]roachpb.Span, rangeMaxBytesNumLocks-maxBytesNumResolvedLocks/rangeMaxBytesIntentsPerLock)
	for i := range expectedExternalLocksRangeMaxBytes {
		offset := maxBytesNumResolvedLocks / rangeMaxBytesIntentsPerLock
		expectedExternalLocksRangeMaxBytes[i].Key = intToKey(rangeMaxBytesIntentsPerLock * (i + offset))
		expectedExternalLocksRangeMaxBytes[i].EndKey = intToKey(rangeMaxBytesIntentsPerLock * (i + offset + 1))
	}

	testCases := []struct {
		desc                  string
		lockSpans             []roachpb.Span
		targetBytes           int64
		numLocks              int
		numResolvedLocks      int
		expectedResolvedLocks []roachpb.Span
		expectedExternalLocks []roachpb.Span
	}{
		// Point intent resolution with a max keys limit. 700 point intents, 500
		// become resolved locks and 200 become external locks.
		{
			desc:                  "Point span with max keys",
			lockSpans:             lockSpansPointMaxKeys,
			targetBytes:           0,
			numResolvedLocks:      lockResolutionBatchSize,
			numLocks:              pointMaxKeysNumLocks,
			expectedResolvedLocks: expectedResolvedLocksPointMaxKeys,
			expectedExternalLocks: expectedExternalLocksPointMaxKeys,
		},
		// Ranged intent resolution with a max keys limit. 700 ranged intents in
		// the form of 100 lock spans each containing 7 intents, 72 become resolved
		// locks (containing the first 500 intents) and 29 become external locks
		// (containing the last 200 intents). Note that the max key limit splits in
		// between the 72nd lock, so the resolved locks will contain the first part
		// of the 72nd lock span and the external locks will contain the remaining
		// of the 72nd lock span.
		{
			desc:                  "Range span with max keys",
			lockSpans:             lockSpansRangeMaxKeys,
			targetBytes:           0,
			numResolvedLocks:      lockResolutionBatchSize,
			numLocks:              rangeMaxKeysIntentsPerLock * rangeMaxKeysNumLocks,
			expectedResolvedLocks: expectedResolvedLocksRangeMaxKeys,
			expectedExternalLocks: expectedExternalLocksRangeMaxKeys,
		},
		// Point intent resolution with a max bytes limit. 20 point intents, 12
		// become resolved locks and 8 become external locks.
		{
			desc:                  "Point span with max bytes",
			lockSpans:             lockSpansPointMaxBytes,
			targetBytes:           11900,
			numResolvedLocks:      maxBytesNumResolvedLocks,
			numLocks:              pointMaxBytesNumLocks,
			expectedResolvedLocks: expectedResolvedLocksPointMaxBytes,
			expectedExternalLocks: expectedExternalLocksPointMaxBytes,
		},
		// Ranged intent resolution with a max bytes limit. 20 ranged intents in
		// the form of 5 lock spans each containing 4 intents, 3 become resolved
		// locks (containing the first 12 intents) and 2 become external locks
		// (containing the last 8 intents). Note that the max byte limit splits in
		// right after the 3rd lock, so the resolved locks will contain strictly
		// the first 3 lock spans and the external locks will contain strictly the
		// remaining locks.
		{
			desc:                  "Range span with max bytes",
			lockSpans:             lockSpansRangeMaxBytes,
			targetBytes:           11900,
			numResolvedLocks:      maxBytesNumResolvedLocks,
			numLocks:              rangeMaxBytesIntentsPerLock * rangeMaxBytesNumLocks,
			expectedResolvedLocks: expectedResolvedLocksRangeMaxBytes,
			expectedExternalLocks: expectedExternalLocksRangeMaxBytes,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			db := storage.NewDefaultInMemForTesting()
			defer db.Close()
			batch := db.NewBatch()
			defer batch.Close()

			ts := hlc.Timestamp{WallTime: 1}
			txn := roachpb.MakeTransaction("test", roachpb.Key("a"), 0, ts, 0, 1)
			txn.Status = roachpb.COMMITTED

			for i := 0; i < tc.numLocks; i++ {
				err := storage.MVCCPut(ctx, batch, nil, intToKey(i), ts, hlc.ClockTimestamp{}, roachpb.MakeValueFromString("a"), &txn)
				require.NoError(t, err)
			}
			resolvedLocks, externalLocks, numBytes, err := resolveLocalLocks(
				ctx,
				&roachpb.RangeDescriptor{
					StartKey: roachpb.RKeyMin,
					EndKey:   roachpb.RKeyMax,
				},
				batch,
				nil,
				&roachpb.EndTxnRequest{
					LockSpans: tc.lockSpans,
				},
				&txn,
				(&MockEvalCtx{}).EvalContext(),
				tc.targetBytes,
			)
			require.NoError(t, err)
			require.Equal(t, len(tc.expectedResolvedLocks), len(resolvedLocks))
			for i, lock := range resolvedLocks {
				require.Equal(t, tc.expectedResolvedLocks[i].Key, lock.Key)
				require.Equal(t, tc.expectedResolvedLocks[i].EndKey, lock.EndKey, "ERR: %d", i)
			}
			require.Equal(t, len(tc.expectedExternalLocks), len(externalLocks))
			for i, lock := range externalLocks {
				require.Equal(t, tc.expectedExternalLocks[i].Key, lock.Key)
				require.Equal(t, tc.expectedExternalLocks[i].EndKey, lock.EndKey)
			}
			require.Greater(t, numBytes, int64(1000*tc.numResolvedLocks))
			require.Less(t, numBytes, int64(1050*tc.numResolvedLocks))
		})
	}
}
