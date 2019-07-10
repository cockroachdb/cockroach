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
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestEndTransactionUpdatesTransactionRecord tests EndTransaction request
// across its various possible transaction record state transitions and error
// cases.
func TestEndTransactionUpdatesTransactionRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	startKey := roachpb.Key("0000")
	endKey := roachpb.Key("9999")
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}

	k, k2 := roachpb.Key("a"), roachpb.Key("b")
	ts, ts2, ts3 := hlc.Timestamp{WallTime: 1}, hlc.Timestamp{WallTime: 2}, hlc.Timestamp{WallTime: 3}
	txn := roachpb.MakeTransaction("test", k, 0, ts, 0)
	writes := []roachpb.SequencedWrite{{Key: k, Sequence: 0}}
	intents := []roachpb.Span{{Key: k2}}

	headerTxn := txn.Clone()
	pushedHeaderTxn := txn.Clone()
	pushedHeaderTxn.Timestamp.Forward(ts2)
	refreshedHeaderTxn := txn.Clone()
	refreshedHeaderTxn.Timestamp.Forward(ts2)
	refreshedHeaderTxn.RefreshedTimestamp.Forward(ts2)
	restartedHeaderTxn := txn.Clone()
	restartedHeaderTxn.Restart(-1, 0, ts2)
	restartedAndPushedHeaderTxn := txn.Clone()
	restartedAndPushedHeaderTxn.Restart(-1, 0, ts2)
	restartedAndPushedHeaderTxn.Timestamp.Forward(ts3)

	pendingRecord := func() *roachpb.TransactionRecord {
		record := txn.AsRecord()
		record.Status = roachpb.PENDING
		return &record
	}()
	stagingRecord := func() *roachpb.TransactionRecord {
		record := txn.AsRecord()
		record.Status = roachpb.STAGING
		record.IntentSpans = intents
		record.InFlightWrites = writes
		return &record
	}()
	committedRecord := func() *roachpb.TransactionRecord {
		record := txn.AsRecord()
		record.Status = roachpb.COMMITTED
		record.IntentSpans = intents
		return &record
	}()
	abortedRecord := func() *roachpb.TransactionRecord {
		record := txn.AsRecord()
		record.Status = roachpb.ABORTED
		record.IntentSpans = intents
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
		noIntentSpans  bool
		inFlightWrites []roachpb.SequencedWrite
		deadline       *hlc.Timestamp
		noRefreshSpans bool
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
			headerTxn:     headerTxn,
			commit:        false,
			noIntentSpans: true,
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
			noIntentSpans:  true,
			inFlightWrites: writes,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *stagingRecord
				record.IntentSpans = nil
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
			headerTxn:     headerTxn,
			commit:        true,
			noIntentSpans: true,
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
				record.Timestamp.Forward(ts2)
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
				record.Timestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// The transaction's commit timestamp was increased during its
			// lifetime and it has never read anything. The stage will succeed.
			name: "record missing, can create, try stage at pushed timestamp, can forward timestamp",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, hlc.Timestamp{} },
			// Request state.
			headerTxn:      pushedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			noRefreshSpans: true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *stagingRecord
				record.Timestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// The transaction's commit timestamp was increased during its
			// lifetime and it has never read anything. The commit will succeed.
			name: "record missing, can create, try commit at pushed timestamp, can forward timestamp",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, hlc.Timestamp{} },
			// Request state.
			headerTxn:      pushedHeaderTxn,
			commit:         true,
			noRefreshSpans: true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *committedRecord
				record.Timestamp.Forward(ts2)
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
				record.Timestamp.Forward(ts2)
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
				record.Timestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that the
			// transaction can be created with. This will trigger a retry error.
			name: "record missing, can create with min timestamp, try stage, can forward timestamp",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, ts2 },
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			noRefreshSpans: true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *stagingRecord
				record.Timestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that the
			// transaction can be created with. This will trigger a retry error.
			name: "record missing, can create with min timestamp, try commit, can forward timestamp",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: func() (bool, hlc.Timestamp) { return true, ts2 },
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			noRefreshSpans: true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *committedRecord
				record.Timestamp.Forward(ts2)
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
			// lifetime and it has never read anything. The stage will succeed.
			name: "record pending, try stage at pushed timestamp, can forward timestamp",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn:      pushedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			noRefreshSpans: true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *stagingRecord
				record.Timestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// The transaction's commit timestamp was increased during its
			// lifetime and it has never read anything. The commit will succeed.
			name: "record pending, try commit at pushed timestamp, can forward timestamp",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn:      pushedHeaderTxn,
			commit:         true,
			noRefreshSpans: true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *committedRecord
				record.Timestamp.Forward(ts2)
				return &record
			}(),
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
				record.Timestamp.Forward(ts2)
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
				record.Timestamp.Forward(ts2)
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
				record.Timestamp.Forward(ts2)
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
				record.Timestamp.Forward(ts2)
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
				record.Timestamp.Forward(ts2)
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
			// already exists because of a failed parallel commit attempt.
			name: "record staging, try rollback",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    false,
			// Expected result.
			expTxn: abortedRecord,
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
				record.Timestamp.Forward(ts2)
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
				record.Timestamp.Forward(ts2)
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
				record.Timestamp.Forward(ts2)
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
			name: "record pending, try rollback at lower timestamp",
			// Replica state.
			existingTxn: func() *roachpb.TransactionRecord {
				record := *pendingRecord
				record.Timestamp = hlc.Timestamp{Logical: 1}
				return &record
			}(),
			// Request state.
			headerTxn: headerTxn,
			commit:    false,
			// Expected result.
			expError: "programming error: timestamp regression",
		},
		{
			name: "record pending, try stage at lower timestamp",
			// Replica state.
			existingTxn: func() *roachpb.TransactionRecord {
				record := *pendingRecord
				record.Timestamp = hlc.Timestamp{Logical: 1}
				return &record
			}(),
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "programming error: timestamp regression",
		},
		{
			name: "record pending, try commit at lower timestamp",
			// Replica state.
			existingTxn: func() *roachpb.TransactionRecord {
				record := *pendingRecord
				record.Timestamp = hlc.Timestamp{Logical: 1}
				return &record
			}(),
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expError: "programming error: timestamp regression",
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
			db := engine.NewInMem(roachpb.Attributes{}, 10<<20)
			defer db.Close()
			batch := db.NewBatch()
			defer batch.Close()

			// Write the existing transaction record, if necessary.
			txnKey := keys.TransactionKey(txn.Key, txn.ID)
			if c.existingTxn != nil {
				if err := engine.MVCCPutProto(ctx, batch, nil, txnKey, hlc.Timestamp{}, nil, c.existingTxn); err != nil {
					t.Fatal(err)
				}
			}

			// Sanity check request args.
			if !c.commit {
				require.Nil(t, c.inFlightWrites)
				require.Nil(t, c.deadline)
				require.False(t, c.noRefreshSpans)
			}

			// Issue an EndTransaction request.
			req := roachpb.EndTransactionRequest{
				RequestHeader: roachpb.RequestHeader{Key: txn.Key},
				Commit:        c.commit,

				InFlightWrites: c.inFlightWrites,
				Deadline:       c.deadline,
				NoRefreshSpans: c.noRefreshSpans,
			}
			if !c.noIntentSpans {
				req.IntentSpans = intents
			}
			var resp roachpb.EndTransactionResponse
			_, err := EndTransaction(ctx, batch, CommandArgs{
				EvalCtx: &mockEvalCtx{
					desc: &desc,
					canCreateTxnFn: func() (bool, hlc.Timestamp, roachpb.TransactionAbortedReason) {
						require.NotNil(t, c.canCreateTxn, "CanCreateTxnRecord unexpectedly called")
						if can, minTS := c.canCreateTxn(); can {
							return true, minTS, 0
						}
						return false, hlc.Timestamp{}, roachpb.ABORT_REASON_ABORTED_RECORD_FOUND
					},
				},
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
				if ok, err := engine.MVCCGetProto(
					ctx, batch, txnKey, hlc.Timestamp{}, &resTxnRecord, engine.MVCCGetOptions{},
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
