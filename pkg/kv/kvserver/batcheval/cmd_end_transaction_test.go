// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestIsEndTxnExceedingDeadline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name     string
		commitTS int64
		deadline int64
		exp      bool
	}{
		{"no deadline", 10, 0, false},
		{"later deadline", 10, 11, false},
		{"equal deadline", 10, 10, true},
		{"earlier deadline", 10, 9, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			commitTS := hlc.Timestamp{WallTime: tt.commitTS}
			deadline := hlc.Timestamp{WallTime: tt.deadline}
			require.Equal(t, tt.exp, IsEndTxnExceedingDeadline(commitTS, deadline))
		})
	}
}

func TestIsEndTxnTriggeringRetryError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		txnIsoLevel             isolation.Level
		txnWriteTooOld          bool
		txnWriteTimestampPushed bool
		txnExceedingDeadline    bool

		expRetry  bool
		expReason kvpb.TransactionRetryReason
	}{
		{isolation.Serializable, false, false, false, false, 0},
		{isolation.Serializable, false, false, true, true, kvpb.RETRY_COMMIT_DEADLINE_EXCEEDED},
		{isolation.Serializable, false, true, false, true, kvpb.RETRY_SERIALIZABLE},
		{isolation.Serializable, false, true, true, true, kvpb.RETRY_SERIALIZABLE},
		{isolation.Serializable, true, false, false, true, kvpb.RETRY_WRITE_TOO_OLD},
		{isolation.Serializable, true, false, true, true, kvpb.RETRY_WRITE_TOO_OLD},
		{isolation.Serializable, true, true, false, true, kvpb.RETRY_WRITE_TOO_OLD},
		{isolation.Serializable, true, true, true, true, kvpb.RETRY_WRITE_TOO_OLD},
		{isolation.Snapshot, false, false, false, false, 0},
		{isolation.Snapshot, false, false, true, true, kvpb.RETRY_COMMIT_DEADLINE_EXCEEDED},
		{isolation.Snapshot, false, true, false, false, 0},
		{isolation.Snapshot, false, true, true, true, kvpb.RETRY_COMMIT_DEADLINE_EXCEEDED},
		{isolation.Snapshot, true, false, false, true, kvpb.RETRY_WRITE_TOO_OLD},
		{isolation.Snapshot, true, false, true, true, kvpb.RETRY_WRITE_TOO_OLD},
		{isolation.Snapshot, true, true, false, true, kvpb.RETRY_WRITE_TOO_OLD},
		{isolation.Snapshot, true, true, true, true, kvpb.RETRY_WRITE_TOO_OLD},
		{isolation.ReadCommitted, false, false, false, false, 0},
		{isolation.ReadCommitted, false, false, true, true, kvpb.RETRY_COMMIT_DEADLINE_EXCEEDED},
		{isolation.ReadCommitted, false, true, false, false, 0},
		{isolation.ReadCommitted, false, true, true, true, kvpb.RETRY_COMMIT_DEADLINE_EXCEEDED},
		{isolation.ReadCommitted, true, false, false, true, kvpb.RETRY_WRITE_TOO_OLD},
		{isolation.ReadCommitted, true, false, true, true, kvpb.RETRY_WRITE_TOO_OLD},
		{isolation.ReadCommitted, true, true, false, true, kvpb.RETRY_WRITE_TOO_OLD},
		{isolation.ReadCommitted, true, true, true, true, kvpb.RETRY_WRITE_TOO_OLD},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("iso=%s/wto=%t/pushed=%t/deadline=%t",
			tt.txnIsoLevel, tt.txnWriteTooOld, tt.txnWriteTimestampPushed, tt.txnExceedingDeadline)
		t.Run(name, func(t *testing.T) {
			txn := roachpb.MakeTransaction("test", nil, tt.txnIsoLevel, 0, hlc.Timestamp{WallTime: 10}, 0, 1, 0, false /* omitInRangefeeds */)
			if tt.txnWriteTooOld {
				txn.WriteTooOld = true
			}
			if tt.txnWriteTimestampPushed {
				txn.WriteTimestamp = txn.WriteTimestamp.Add(1, 0)
			}
			deadline := txn.WriteTimestamp.Next()
			if tt.txnExceedingDeadline {
				deadline = txn.WriteTimestamp.Prev()
			}

			gotRetry, gotReason, _ := IsEndTxnTriggeringRetryError(&txn, deadline)
			require.Equal(t, tt.expRetry, gotRetry)
			require.Equal(t, tt.expReason, gotReason)
		})
	}
}

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
	txn := roachpb.MakeTransaction("test", k, 0, 0, ts, 0, 1, 0, false /* omitInRangefeeds */)
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
	preparedHeaderTxn := txn.Clone()
	preparedHeaderTxn.Status = roachpb.PREPARED

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
	preparedRecord := func() *roachpb.TransactionRecord {
		record := txn.AsRecord()
		record.Status = roachpb.PREPARED
		record.LockSpans = intents
		return &record
	}()

	testCases := []struct {
		name string
		// Replica state.
		existingTxn    *roachpb.TransactionRecord
		canCreateTxn   bool
		minTxnCommitTS hlc.Timestamp
		// Request state.
		headerTxn      *roachpb.Transaction
		commit         bool
		prepare        bool
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
			existingTxn: nil,
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
			existingTxn: nil,
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
			canCreateTxn: false,
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
			canCreateTxn: false,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
		},
		{
			// Either a PushTxn(ABORT) request succeeded or this is a replay
			// and the transaction has already been finalized. Either way,
			// the request isn't allowed to create a new transaction record.
			name: "record missing, can't create, try prepare",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: false,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
		},
		{
			// Standard case where a transaction record is created during a
			// parallel commit.
			name: "record missing, can create, try stage",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: true,
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
			canCreateTxn: true,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expTxn: committedRecord,
		},
		{
			// Standard case where a transaction record is created in the prepared
			// state during an XA two-phase commit.
			name: "record missing, can create, try prepare",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: true,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expTxn: preparedRecord,
		},
		{
			// Standard case where a transaction record is created during a
			// parallel commit when all writes are still in-flight.
			name: "record missing, can create, try stage without intents",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: true,
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
			canCreateTxn: true,
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
			// Non-standard case where a transaction record is created during the
			// prepare stage of an XA two-phase commit when there are no intents.
			name: "record missing, can create, try prepare without intents",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: true,
			// Request state.
			headerTxn:   headerTxn,
			commit:      true,
			prepare:     true,
			noLockSpans: true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *preparedRecord
				record.LockSpans = nil
				return &record
			}(),
		},
		{
			// The transaction's commit timestamp was increased during its
			// lifetime, but it hasn't refreshed up to its new commit timestamp.
			// The stage will be rejected.
			name: "record missing, can create, try stage at pushed timestamp",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: true,
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
			canCreateTxn: true,
			// Request state.
			headerTxn: pushedHeaderTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// The transaction's commit timestamp was increased during its
			// lifetime, but it hasn't refreshed up to its new commit timestamp.
			// The prepare will be rejected.
			name: "record missing, can create, try prepare at pushed timestamp",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: true,
			// Request state.
			headerTxn: pushedHeaderTxn,
			commit:    true,
			prepare:   true,
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
			canCreateTxn: true,
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
			canCreateTxn: true,
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
			// The transaction's commit timestamp was increased during its
			// lifetime and it has refreshed up to this timestamp. The prepare
			// will succeed.
			name: "record missing, can create, try prepare at pushed timestamp after refresh",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: true,
			// Request state.
			headerTxn: refreshedHeaderTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *preparedRecord
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that the
			// transaction can be committed with. This will trigger a retry error.
			name: "record missing, can commit with min timestamp, try stage",
			// Replica state.
			existingTxn:    nil,
			canCreateTxn:   true,
			minTxnCommitTS: ts2,
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that the
			// transaction can be committed with. This will trigger a retry error.
			name: "record missing, can commit with min timestamp, try commit",
			// Replica state.
			existingTxn:    nil,
			canCreateTxn:   true,
			minTxnCommitTS: ts2,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that the
			// transaction can be committed (or prepared) with. This will trigger
			// a retry error.
			name: "record missing, can commit with min timestamp, try prepare",
			// Replica state.
			existingTxn:    nil,
			canCreateTxn:   true,
			minTxnCommitTS: ts2,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that
			// the transaction can be committed with. Luckily, the transaction has
			// already refreshed above this time, so it can avoid a retry error.
			name: "record missing, can commit with min timestamp, try stage at pushed timestamp after refresh",
			// Replica state.
			existingTxn:    nil,
			canCreateTxn:   true,
			minTxnCommitTS: ts2,
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
			// the transaction can be committed with. Luckily, the transaction has
			// already refreshed above this time, so it can avoid a retry error.
			name: "record missing, can commit with min timestamp, try commit at pushed timestamp after refresh",
			// Replica state.
			existingTxn:    nil,
			canCreateTxn:   true,
			minTxnCommitTS: ts2,
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
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that
			// the transaction can be committed (or prepared) with. Luckily, the
			// transaction has already refreshed above this time, so it can
			// avoid a retry error.
			name: "record missing, can commit with min timestamp, try prepare at pushed timestamp after refresh",
			// Replica state.
			existingTxn:    nil,
			canCreateTxn:   true,
			minTxnCommitTS: ts2,
			// Request state.
			headerTxn: refreshedHeaderTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *preparedRecord
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
			canCreateTxn: true,
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
			// lifetime. The commit will be rejected.
			name: "record missing, can create, try commit after write too old",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: true,
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
			// The transaction has run into a WriteTooOld error during its
			// lifetime. The prepare will be rejected.
			name: "record missing, can create, try prepare after write too old",
			// Replica state.
			existingTxn:  nil,
			canCreateTxn: true,
			// Request state.
			headerTxn: func() *roachpb.Transaction {
				clone := txn.Clone()
				clone.WriteTooOld = true
				return clone
			}(),
			commit:  true,
			prepare: true,
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
			// Standard case where a transaction record is staged by a parallel
			// commit. The record already exists because it has been heartbeated.
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
			// Standard case where a transaction record is committed during a
			// non-parallel commit. The record already exists because it has been
			// heartbeated.
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
			// Standard case where a transaction record is updated to the prepared
			// state during an XA two-phase commit. The record already exists because
			// it has been heartbeated.
			name: "record pending, try prepare",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expTxn: preparedRecord,
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
			// lifetime, but it hasn't refreshed up to its new commit timestamp.
			// The prepare will be rejected.
			name: "record pending, try prepare at pushed timestamp",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: pushedHeaderTxn,
			commit:    true,
			prepare:   true,
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
			// The transaction's commit timestamp was increased during its
			// lifetime and it has refreshed up to this timestamp. The prepare
			// will succeed.
			name: "record pending, try prepare at pushed timestamp after refresh",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: refreshedHeaderTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *preparedRecord
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that the
			// transaction can be committed with. The record already exists because
			// it has been heartbeated. This will trigger a retry error.
			name: "record pending, can commit with min timestamp, try stage",
			// Replica state.
			existingTxn:    pendingRecord,
			minTxnCommitTS: ts2,
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that the
			// transaction can be committed with. The record already exists because
			// it has been heartbeated. This will trigger a retry error.
			name: "record pending, can commit with min timestamp, try commit",
			// Replica state.
			existingTxn:    pendingRecord,
			minTxnCommitTS: ts2,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that the
			// transaction can be committed with. The record already exists because
			// it has been heartbeated. This will trigger a retry error.
			name: "record pending, can commit with min timestamp, try prepare",
			// Replica state.
			existingTxn:    pendingRecord,
			minTxnCommitTS: ts2,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		},
		{
			// A PushTxn(TIMESTAMP) request bumped the minimum timestamp that
			// the transaction can be committed with. The record already exists
			// because it has been heartbeated. Luckily, the transaction has
			// already refreshed above this time, so it can avoid a retry error.
			name: "record pending, can commit with min timestamp, try stage at pushed timestamp after refresh",
			// Replica state.
			existingTxn:    pendingRecord,
			minTxnCommitTS: ts2,
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
			// the transaction can be committed with. The record already exists
			// because it has been heartbeated. Luckily, the transaction has
			// already refreshed above this time, so it can avoid a retry error.
			name: "record pending, can commit with min timestamp, try commit at pushed timestamp after refresh",
			// Replica state.
			existingTxn:    pendingRecord,
			minTxnCommitTS: ts2,
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
			// transaction can be committed (or prepared) with. The record already
			// exists because it has been heartbeated. Luckily, the transaction has
			// already refreshed above this time, so it can avoid a retry error.
			name: "record pending, can commit with min timestamp, try prepare at pushed timestamp after refresh",
			// Replica state.
			existingTxn:    pendingRecord,
			minTxnCommitTS: ts2,
			// Request state.
			headerTxn: refreshedHeaderTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *preparedRecord
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
			// lifetime. The commit will be rejected.
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
			// The transaction has run into a WriteTooOld error during its
			// lifetime. The prepare will be rejected.
			name: "record pending, try prepare after write too old",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: func() *roachpb.Transaction {
				clone := txn.Clone()
				clone.WriteTooOld = true
				return clone
			}(),
			commit:  true,
			prepare: true,
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
			// Standard case where a transaction record is staged during a
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
			// Standard case where a transaction record is committed during a
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
			// Standard case where a transaction record is updated to the prepared
			// state during an XA two-phase commit after it has written a record at a
			// lower epoch. The existing record is upgraded.
			name: "record pending, try prepare at higher epoch",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: restartedHeaderTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *preparedRecord
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
			// The transaction's commit timestamp was increased during the
			// current epoch, but it hasn't refreshed up to its new commit
			// timestamp. The prepare will be rejected.
			name: "record pending, try prepare at higher epoch and pushed timestamp",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: restartedAndPushedHeaderTxn,
			commit:    true,
			prepare:   true,
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
			// Standard case where a transaction record is re-staged during a
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
			// Standard case where a transaction record is explicitly committed.
			// The record already exists and is staging because it was previously
			// implicitly committed.
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
			// Standard case where a transaction record is re-staged during a
			// parallel commit. The record already exists because of a failed
			// parallel commit attempt. The timestamp cache's minimum commit
			// timestamp is ignored when the transaction record is staging.
			name: "record staging, can commit with min timestamp, try re-stage",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn:      headerTxn,
			minTxnCommitTS: ts2,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expTxn: stagingRecord,
		},
		{
			// Non-standard case where a transaction record is explicitly committed.
			// The record already exists and is staging because it was previously
			// implicitly committed. The timestamp cache's minimum commit timestamp is
			// ignored when the transaction record is staging.
			name: "record staging, can commit with min timestamp, try commit",
			// Replica state.
			existingTxn:    stagingRecord,
			minTxnCommitTS: ts2,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expTxn: committedRecord,
		},
		{
			// Non-standard case where a transaction record is re-staged during
			// a parallel commit. The record already exists because of a failed
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
			// Non-standard case where a transaction record is committed during
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
			// Non-standard case where a transaction record is re-staged during
			// a parallel commit. The record already exists because of a failed
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
			// Non-standard case where a transaction record is committed during
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
			name: "record staging, try prepare at higher epoch",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn: restartedHeaderTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expTxn: func() *roachpb.TransactionRecord {
				record := *preparedRecord
				record.Epoch++
				record.WriteTimestamp.Forward(ts2)
				return &record
			}(),
		},
		{
			// Non-standard case where a transaction record is re-staged during
			// a parallel commit. The record already exists because of a failed
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
			// Non-standard case where a transaction record is committed during
			// a non-parallel commit. The record already exists because of a
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
			name: "record staging, try prepare at higher epoch and pushed timestamp",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn: restartedAndPushedHeaderTxn,
			commit:    true,
			prepare:   true,
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
		{
			// The transaction has been prepared. The client rolls it back.
			name: "record prepared, try rollback",
			// Replica state.
			existingTxn: preparedRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    false,
			// Expected result.
			expTxn: abortedRecord,
		},
		{
			// The transaction has been prepared. The client commits it.
			name: "record prepared, try commit",
			// Replica state.
			existingTxn: preparedRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expTxn: committedRecord,
		},
		{
			// The transaction has been prepared. The client sends another prepare
			// request. This is a no-op.
			name: "record prepared, try re-prepare",
			// Replica state.
			existingTxn: preparedRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expTxn: preparedRecord,
		},
		{
			// The transaction has been prepared. The client rolls it back.
			name: "record and header prepared, try rollback",
			// Replica state.
			existingTxn: preparedRecord,
			// Request state.
			headerTxn: preparedHeaderTxn,
			commit:    false,
			// Expected result.
			expTxn: abortedRecord,
		},
		{
			// The transaction has been prepared. The client commits it.
			name: "record and header prepared, try commit",
			// Replica state.
			existingTxn: preparedRecord,
			// Request state.
			headerTxn: preparedHeaderTxn,
			commit:    true,
			// Expected result.
			expTxn: committedRecord,
		},
		{
			// The transaction has been prepared. The client sends another prepare
			// request. This is a no-op.
			name: "record and header prepared, try re-prepare",
			// Replica state.
			existingTxn: preparedRecord,
			// Request state.
			headerTxn: preparedHeaderTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expTxn: preparedRecord,
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
			name: "record pending, try prepare at lower epoch",
			// Replica state.
			existingTxn: func() *roachpb.TransactionRecord {
				record := *pendingRecord
				record.Epoch++
				return &record
			}(),
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expError: "programming error: epoch regression",
		},
		{
			name: "record pending, try prepare without commit",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    false,
			prepare:   true,
			// Expected result.
			expError: "cannot prepare a rollback",
		},
		{
			name: "record pending, try prepare with in-flight writes",
			// Replica state.
			existingTxn: pendingRecord,
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			prepare:        true,
			inFlightWrites: writes,
			// Expected result.
			expError: "cannot prepare a parallel commit",
		},
		{
			name: "record staging, try prepare",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expError: "cannot prepare a staging transaction",
		},
		{
			name: "record staging, try prepare at pushed timestamp",
			// Replica state.
			existingTxn: stagingRecord,
			// Request state.
			headerTxn: pushedHeaderTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expError: "cannot prepare a staging transaction",
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
		{
			name: "record aborted, try prepare",
			// Replica state.
			existingTxn: abortedRecord,
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
		},
		{
			name: "record prepared, try stage",
			// Replica state.
			existingTxn: preparedRecord,
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "cannot parallel commit a prepared transaction",
		},
		{
			name: "record prepared, try rollback at higher epoch",
			// Replica state.
			existingTxn: preparedRecord,
			// Request state.
			headerTxn: restartedHeaderTxn,
			commit:    false,
			// Expected result.
			expError: "epoch mismatch with prepared transaction",
		},
		{
			name: "record prepared, try stage at higher epoch",
			// Replica state.
			existingTxn: preparedRecord,
			// Request state.
			headerTxn:      restartedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "epoch mismatch with prepared transaction",
		},
		{
			name: "record prepared, try commit at higher epoch",
			// Replica state.
			existingTxn: preparedRecord,
			// Request state.
			headerTxn: restartedHeaderTxn,
			commit:    true,
			// Expected result.
			expError: "epoch mismatch with prepared transaction",
		},
		{
			name: "record prepared, try re-prepare at higher epoch",
			// Replica state.
			existingTxn: preparedRecord,
			// Request state.
			headerTxn: restartedHeaderTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expError: "epoch mismatch with prepared transaction",
		},
		{
			name: "record prepared, try rollback at lower epoch",
			// Replica state.
			existingTxn: func() *roachpb.TransactionRecord {
				record := *preparedRecord
				record.Epoch++
				return &record
			}(),
			// Request state.
			headerTxn: headerTxn,
			commit:    false,
			// Expected result.
			expError: "epoch mismatch with prepared transaction",
		},
		{
			name: "record prepared, try stage at lower epoch",
			// Replica state.
			existingTxn: func() *roachpb.TransactionRecord {
				record := *preparedRecord
				record.Epoch++
				return &record
			}(),
			// Request state.
			headerTxn:      headerTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "epoch mismatch with prepared transaction",
		},
		{
			name: "record prepared, try commit at lower epoch",
			// Replica state.
			existingTxn: func() *roachpb.TransactionRecord {
				record := *preparedRecord
				record.Epoch++
				return &record
			}(),
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			// Expected result.
			expError: "epoch mismatch with prepared transaction",
		},
		{
			name: "record prepared, try re-prepare at lower epoch",
			// Replica state.
			existingTxn: func() *roachpb.TransactionRecord {
				record := *preparedRecord
				record.Epoch++
				return &record
			}(),
			// Request state.
			headerTxn: headerTxn,
			commit:    true,
			prepare:   true,
			// Expected result.
			expError: "epoch mismatch with prepared transaction",
		},
		{
			// The transaction has been prepared. The client tries to stage it.
			name: "record and header prepared, try stage",
			// Replica state.
			existingTxn: preparedRecord,
			// Request state.
			headerTxn:      preparedHeaderTxn,
			commit:         true,
			inFlightWrites: writes,
			// Expected result.
			expError: "cannot parallel commit a prepared transaction",
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
				if err := storage.MVCCPutProto(ctx, batch, txnKey, hlc.Timestamp{}, c.existingTxn, storage.MVCCWriteOptions{}); err != nil {
					t.Fatal(err)
				}
			}

			// Sanity check request args.
			if !c.commit {
				require.Nil(t, c.inFlightWrites)
				require.Zero(t, c.deadline)
			}

			// Issue an EndTxn request.
			req := kvpb.EndTxnRequest{
				RequestHeader: kvpb.RequestHeader{Key: txn.Key},
				Commit:        c.commit,
				Prepare:       c.prepare,

				InFlightWrites: c.inFlightWrites,
				Deadline:       c.deadline,
			}
			if !c.noLockSpans {
				req.LockSpans = intents
			}
			var resp kvpb.EndTxnResponse
			_, err := EndTxn(ctx, batch, CommandArgs{
				EvalCtx: (&MockEvalCtx{
					Desc:      &desc,
					AbortSpan: as,
					CanCreateTxnRecordFn: func() (bool, kvpb.TransactionAbortedReason) {
						if c.canCreateTxn {
							return true, 0
						}
						return false, kvpb.ABORT_REASON_ABORTED_RECORD_FOUND
					},
					MinTxnCommitTSFn: func() hlc.Timestamp {
						return c.minTxnCommitTS
					},
				}).EvalContext(),
				Args: &req,
				Header: kvpb.Header{
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
	txn := roachpb.MakeTransaction("test", k, 0, 0, ts, 0, 1, 0, false /* omitInRangefeeds */)
	endKey := roachpb.Key("z")
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(k),
		EndKey:   roachpb.RKey(endKey),
	}
	intents := []roachpb.Span{{Key: k}}

	testutils.RunTrueAndFalse(t, "withStoredTxnRecord", func(t *testing.T, storeTxnBeforeEndTxn bool) {
		db := storage.NewDefaultInMemForTesting()
		defer db.Close()
		batch := db.NewBatch()
		defer batch.Close()

		var v roachpb.Value

		// Write a first value at key.
		v.SetString("a")
		txn.Sequence = 1
		if _, err := storage.MVCCPut(ctx, batch, k, ts, v, storage.MVCCWriteOptions{Txn: &txn}); err != nil {
			t.Fatal(err)
		}
		// Write another value.
		v.SetString("b")
		txn.Sequence = 2
		if _, err := storage.MVCCPut(ctx, batch, k, ts, v, storage.MVCCWriteOptions{Txn: &txn}); err != nil {
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
			if err := storage.MVCCPutProto(ctx, batch, txnKey, hlc.Timestamp{}, &txnRec, storage.MVCCWriteOptions{}); err != nil {
				t.Fatal(err)
			}
		}

		// Issue the end txn command.
		req := kvpb.EndTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: txn.Key},
			Commit:        true,
			LockSpans:     intents,
		}
		var resp kvpb.EndTxnResponse
		if _, err := EndTxn(ctx, batch, CommandArgs{
			EvalCtx: (&MockEvalCtx{
				Desc: &desc,
				// We want to inspect the final txn record after EndTxn, to
				// ascertain that it persists the ignore list.
				EvalKnobs: kvserverbase.BatchEvalTestingKnobs{DisableTxnAutoGC: true},
			}).EvalContext(),
			Args: &req,
			Header: kvpb.Header{
				Timestamp: ts,
				Txn:       &txn,
			},
		}, &resp); err != nil {
			t.Fatal(err)
		}

		// The second write has been rolled back; verify that the remaining
		// value is from the first write.
		res, err := storage.MVCCGet(ctx, batch, k, ts2, storage.MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if res.Intent != nil {
			t.Errorf("found intent, expected none: %+v", res.Intent)
		}
		if res.Value == nil {
			t.Errorf("no value found, expected one")
		} else {
			s, err := res.Value.GetBytes()
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
				name:     "future",
				commitTS: func(now hlc.Timestamp) hlc.Timestamp { return now.Add(100, 0) },
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

				clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123)))
				desc := roachpb.RangeDescriptor{
					RangeID:  99,
					StartKey: roachpb.RKey("a"),
					EndKey:   roachpb.RKey("z"),
				}

				now := clock.Now()
				commitTS := cfg.commitTS(now)
				txn := roachpb.MakeTransaction("test", desc.StartKey.AsRawKey(), 0, 0, now, 0, 1, 0, false /* omitInRangefeeds */)
				txn.ReadTimestamp = commitTS
				txn.WriteTimestamp = commitTS

				// Issue the end txn command.
				req := kvpb.EndTxnRequest{
					RequestHeader: kvpb.RequestHeader{Key: txn.Key},
					Commit:        true,
				}
				if commitTrigger {
					req.InternalCommitTrigger = &roachpb.InternalCommitTrigger{
						ModifiedSpanTrigger: &roachpb.ModifiedSpanTrigger{NodeLivenessSpan: &roachpb.Span{}},
					}
				}
				var resp kvpb.EndTxnResponse
				_, err := EndTxn(ctx, batch, CommandArgs{
					EvalCtx: (&MockEvalCtx{
						Desc:  &desc,
						Clock: clock,
					}).EvalContext(),
					Args: &req,
					Header: kvpb.Header{
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

			msDelta, err := computeSplitRangeKeyStatsDelta(context.Background(), engine, lhsDesc, rhsDesc)
			require.NoError(t, err)
			msDelta.AgeTo(nowNanos)
			require.Equal(t, tc.expect, msDelta)
		})
	}
}

// TestResolveLocalLocks tests resolveLocalLocks for point and ranged intents
// as well as under a max key or max byte limit, ensuring the returned
// resolvedLocks, externalLocks, and numBytes are as expected.
func TestResolveLocalLocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	intToKey := func(i int) roachpb.Key {
		return roachpb.Key(fmt.Sprintf("%01000d", i))
	}
	ceil := func(i int, j int) int {
		return (i + j - 1) / j
	}

	const (
		numKeys             = 20
		keysPerRangedLock   = 4
		maxKeys             = 11 // not divisible by keysPerRangedLock
		targetBytes         = 11900
		keysFromTargetBytes = 12 // divisible by keysPerRangedLock
	)

	pointLocks := make([]roachpb.Span, numKeys)
	for i := range pointLocks {
		pointLocks[i].Key = intToKey(i)
	}
	rangedLocks := make([]roachpb.Span, numKeys/keysPerRangedLock)
	for i := range rangedLocks {
		rangedLocks[i].Key = intToKey(i * keysPerRangedLock)
		rangedLocks[i].EndKey = intToKey((i + 1) * keysPerRangedLock)
	}

	expectedResolvedLocksPointMaxKeys := make([]roachpb.Span, maxKeys)
	for i := range expectedResolvedLocksPointMaxKeys {
		expectedResolvedLocksPointMaxKeys[i].Key = intToKey(i)
	}
	expectedExternalLocksPointMaxKeys := make([]roachpb.Span, numKeys-maxKeys)
	for i := range expectedExternalLocksPointMaxKeys {
		expectedExternalLocksPointMaxKeys[i].Key = intToKey(i + maxKeys)
	}

	expectedResolvedLocksRangedMaxKeys := make([]roachpb.Span, ceil(maxKeys, keysPerRangedLock))
	for i := range expectedResolvedLocksRangedMaxKeys {
		expectedResolvedLocksRangedMaxKeys[i].Key = intToKey(i * keysPerRangedLock)
		if i == len(expectedResolvedLocksRangedMaxKeys)-1 {
			expectedResolvedLocksRangedMaxKeys[i].EndKey = intToKey(maxKeys - 1).Next()
		} else {
			expectedResolvedLocksRangedMaxKeys[i].EndKey = intToKey((i + 1) * keysPerRangedLock)
		}
	}
	expectedExternalLocksRangedMaxKeys := make([]roachpb.Span, ceil(numKeys, keysPerRangedLock)-ceil(maxKeys, keysPerRangedLock)+1)
	for i := range expectedExternalLocksRangedMaxKeys {
		offset := maxKeys / keysPerRangedLock
		if i == 0 {
			expectedExternalLocksRangedMaxKeys[i].Key = intToKey(maxKeys - 1).Next()
		} else {
			expectedExternalLocksRangedMaxKeys[i].Key = intToKey((i + offset) * keysPerRangedLock)
		}
		expectedExternalLocksRangedMaxKeys[i].EndKey = intToKey((i + offset + 1) * keysPerRangedLock)
	}

	expectedResolvedLocksPointTargetBytes := make([]roachpb.Span, keysFromTargetBytes)
	for i := range expectedResolvedLocksPointTargetBytes {
		expectedResolvedLocksPointTargetBytes[i].Key = intToKey(i)
	}
	expectedExternalLocksPointTargetBytes := make([]roachpb.Span, numKeys-keysFromTargetBytes)
	for i := range expectedExternalLocksPointTargetBytes {
		expectedExternalLocksPointTargetBytes[i].Key = intToKey(i + keysFromTargetBytes)
	}

	expectedResolvedLocksRangedTargetBytes := make([]roachpb.Span, keysFromTargetBytes/keysPerRangedLock)
	for i := range expectedResolvedLocksRangedTargetBytes {
		expectedResolvedLocksRangedTargetBytes[i].Key = intToKey(i * keysPerRangedLock)
		expectedResolvedLocksRangedTargetBytes[i].EndKey = intToKey((i + 1) * keysPerRangedLock)
	}
	expectedExternalLocksRangedTargetBytes := make([]roachpb.Span, ceil(numKeys, keysPerRangedLock)-keysFromTargetBytes/keysPerRangedLock)
	for i := range expectedExternalLocksRangedTargetBytes {
		offset := keysFromTargetBytes / keysPerRangedLock
		expectedExternalLocksRangedTargetBytes[i].Key = intToKey((i + offset) * keysPerRangedLock)
		expectedExternalLocksRangedTargetBytes[i].EndKey = intToKey((i + offset + 1) * keysPerRangedLock)
	}

	expectedResolvedLocksNoLimit := make([]roachpb.Span, numKeys)
	for i := range expectedResolvedLocksNoLimit {
		expectedResolvedLocksNoLimit[i].Key = intToKey(i)
	}
	expectedExternalLocksNoLimit := make([]roachpb.Span, 0)

	testCases := []struct {
		desc                  string
		lockSpans             []roachpb.Span
		resolveAllowance      int64
		targetBytes           int64
		expectedResolvedLocks []roachpb.Span
		expectedExternalLocks []roachpb.Span
	}{
		// Point intent resolution with a max keys limit. 20 point intents, 11
		// become resolved locks and 9 become external locks.
		{
			desc:                  "Point locks with max keys",
			lockSpans:             pointLocks,
			resolveAllowance:      maxKeys,
			targetBytes:           0,
			expectedResolvedLocks: expectedResolvedLocksPointMaxKeys,
			expectedExternalLocks: expectedExternalLocksPointMaxKeys,
		},
		// Ranged intent resolution with a max keys limit. 5 ranged locks (each
		// containing 4 keys), 3 become resolved locks (containing the first 2
		// locks and part of the 3rd lock) and 3 become external locks (containing
		// the remaining part of the 3rd lock and the last 2 locks). Note that the
		// max key limit splits in between the 3rd lock, so the resolved locks will
		// contain the first part of the 3rd lock span and the external locks will
		// contain the remaining part of the 3rd lock span.
		{
			desc:                  "Ranged locks with max keys",
			lockSpans:             rangedLocks,
			resolveAllowance:      maxKeys,
			targetBytes:           0,
			expectedResolvedLocks: expectedResolvedLocksRangedMaxKeys,
			expectedExternalLocks: expectedExternalLocksRangedMaxKeys,
		},
		// Point intent resolution with a target bytes limit. 20 point intents, 12
		// become resolved locks and 8 become external locks.
		{
			desc:                  "Point span with target bytes",
			lockSpans:             pointLocks,
			resolveAllowance:      0,
			targetBytes:           targetBytes,
			expectedResolvedLocks: expectedResolvedLocksPointTargetBytes,
			expectedExternalLocks: expectedExternalLocksPointTargetBytes,
		},
		// Ranged intent resolution with a target bytes limit. 5 ranged locks (each
		// containing 4 keys), 3 become resolved locks (containing the first 3
		// locks) and 2 become external locks (containing the last 2 locks). Note
		// that the target byte limit does not split in between any locks, so the
		// resolved and external locks do not contain part of a lock span for any
		// lock.
		{
			desc:                  "Ranged span with target bytes",
			lockSpans:             rangedLocks,
			resolveAllowance:      0,
			targetBytes:           targetBytes,
			expectedResolvedLocks: expectedResolvedLocksRangedTargetBytes,
			expectedExternalLocks: expectedExternalLocksRangedTargetBytes,
		},
		// Point intent resolution without any limit. 20 point intents, 20 become
		// resolved locks and 0 become external locks.
		{
			desc:                  "No key or byte limit",
			lockSpans:             pointLocks,
			resolveAllowance:      0,
			targetBytes:           0,
			expectedResolvedLocks: expectedResolvedLocksNoLimit,
			expectedExternalLocks: expectedExternalLocksNoLimit,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			db := storage.NewDefaultInMemForTesting()
			defer db.Close()
			batch := db.NewBatch()
			defer batch.Close()

			ts := hlc.Timestamp{WallTime: 1}
			txn := roachpb.MakeTransaction("test", roachpb.Key("a"), 0, 0, ts, 0, 1, 0, false /* omitInRangefeeds */)
			txn.Status = roachpb.COMMITTED

			for i := 0; i < numKeys; i++ {
				_, err := storage.MVCCPut(ctx, batch, intToKey(i), ts, roachpb.MakeValueFromString("a"), storage.MVCCWriteOptions{Txn: &txn})
				require.NoError(t, err)
			}
			resolvedLocks, releasedReplLocks, externalLocks, err := resolveLocalLocksWithPagination(
				ctx,
				batch,
				(&MockEvalCtx{
					Desc: &roachpb.RangeDescriptor{
						StartKey: roachpb.RKeyMin,
						EndKey:   roachpb.RKeyMax,
					}}).EvalContext(),
				nil,
				&kvpb.EndTxnRequest{
					LockSpans:             tc.lockSpans,
					InternalCommitTrigger: &roachpb.InternalCommitTrigger{},
				},
				&txn,
				tc.resolveAllowance,
				tc.targetBytes,
			)
			require.NoError(t, err)
			require.Equal(t, len(tc.expectedResolvedLocks), len(resolvedLocks))
			for i, lock := range resolvedLocks {
				require.Equal(t, tc.expectedResolvedLocks[i].Key, lock.Key)
				require.Equal(t, tc.expectedResolvedLocks[i].EndKey, lock.EndKey)
			}
			require.Len(t, releasedReplLocks, 0)
			require.Equal(t, len(tc.expectedExternalLocks), len(externalLocks))
			for i, lock := range externalLocks {
				require.Equal(t, tc.expectedExternalLocks[i].Key, lock.Key)
				require.Equal(t, tc.expectedExternalLocks[i].EndKey, lock.EndKey)
			}
		})
	}
}

// TestSplitTriggerWritesInitialReplicaState tests that a split trigger sets up
// the split's right-hand side range by writing the initial replica state into
// the evaluation write batch.
func TestSplitTriggerWritesInitialReplicaState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	version := st.Version.LatestVersion()
	manual := timeutil.NewManualTime(timeutil.Unix(0, 10))
	clock := hlc.NewClockForTesting(manual)

	db := storage.NewDefaultInMemForTesting()
	defer db.Close()
	batch := db.NewBatch()
	defer batch.Close()

	rangeLeaseDuration := 99 * time.Nanosecond
	startKey := roachpb.Key("0000")
	endKey := roachpb.Key("9999")
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}
	desc.AddReplica(1, 1, roachpb.VOTER_FULL)
	lease := roachpb.Lease{
		Replica: desc.InternalReplicas[0],
		// The range was using a leader lease. The split will need to swap this to
		// an expiration-based lease.
		Term:          10,
		MinExpiration: hlc.Timestamp{WallTime: 100},
	}
	gcThreshold := hlc.Timestamp{WallTime: 4}
	lastGCTimestamp := hlc.Timestamp{WallTime: 5}
	gcHint := roachpb.GCHint{GCTimestamp: gcThreshold}
	abortSpanTxnID := uuid.MakeV4()
	as := abortspan.New(desc.RangeID)
	sl := stateloader.Make(desc.RangeID)
	rec := (&MockEvalCtx{
		ClusterSettings:        st,
		Desc:                   &desc,
		Clock:                  clock,
		AbortSpan:              as,
		LastReplicaGCTimestamp: lastGCTimestamp,
		RangeLeaseDuration:     rangeLeaseDuration,
	}).EvalContext()

	splitKey := roachpb.RKey("5555")
	leftDesc, rightDesc := desc, desc
	leftDesc.EndKey = splitKey
	rightDesc.RangeID++
	rightDesc.StartKey = splitKey
	rightDesc.InternalReplicas = slices.Clone(leftDesc.InternalReplicas)
	rightDesc.InternalReplicas[0].ReplicaID++
	split := &roachpb.SplitTrigger{
		LeftDesc:  leftDesc,
		RightDesc: rightDesc,
	}

	// Write the range state that will be consulted and copied during the split.
	err := as.Put(ctx, batch, nil, abortSpanTxnID, &roachpb.AbortSpanEntry{})
	require.NoError(t, err)
	err = sl.SetLease(ctx, batch, nil, lease)
	require.NoError(t, err)
	err = sl.SetGCThreshold(ctx, batch, nil, &gcThreshold)
	require.NoError(t, err)
	err = sl.SetGCHint(ctx, batch, nil, &gcHint)
	require.NoError(t, err)
	err = sl.SetVersion(ctx, batch, nil, &version)
	require.NoError(t, err)

	// Run the split trigger, which is normally run as a subset of EndTxn request
	// evaluation.
	_, _, err = splitTrigger(ctx, rec, batch, enginepb.MVCCStats{}, split, hlc.Timestamp{})
	require.NoError(t, err)

	// Verify that range state was migrated to the right-hand side properly.
	asRight := abortspan.New(rightDesc.RangeID)
	slRight := stateloader.Make(rightDesc.RangeID)
	// The abort span should have been transferred over.
	ok, err := asRight.Get(ctx, batch, abortSpanTxnID, &roachpb.AbortSpanEntry{})
	require.NoError(t, err)
	require.True(t, ok)
	// The lease should be present, pointing at the replica in the right-hand side
	// range, and switched to an expiration-based lease.
	expLease := roachpb.Lease{
		Replica:    rightDesc.InternalReplicas[0],
		Expiration: &hlc.Timestamp{WallTime: manual.Now().Add(rangeLeaseDuration).UnixNano()},
	}
	loadedLease, err := slRight.LoadLease(ctx, batch)
	require.NoError(t, err)
	require.Equal(t, expLease, loadedLease)
	loadedGCThreshold, err := slRight.LoadGCThreshold(ctx, batch)
	require.NoError(t, err)
	require.NotNil(t, loadedGCThreshold)
	require.Equal(t, gcThreshold, *loadedGCThreshold)
	loadedGCHint, err := slRight.LoadGCHint(ctx, batch)
	require.NoError(t, err)
	require.NotNil(t, loadedGCHint)
	require.Equal(t, gcHint, *loadedGCHint)
	expTruncState := kvserverpb.RaftTruncatedState{
		Term:  stateloader.RaftInitialLogTerm,
		Index: stateloader.RaftInitialLogIndex,
	}
	loadedTruncState, err := slRight.LoadRaftTruncatedState(ctx, batch)
	require.NoError(t, err)
	require.Equal(t, expTruncState, loadedTruncState)
	loadedVersion, err := slRight.LoadVersion(ctx, batch)
	require.NoError(t, err)
	require.Equal(t, version, loadedVersion)
	expAppliedState := kvserverpb.RangeAppliedState{
		RaftAppliedIndexTerm: stateloader.RaftInitialLogTerm,
		RaftAppliedIndex:     stateloader.RaftInitialLogIndex,
		LeaseAppliedIndex:    stateloader.InitialLeaseAppliedIndex,
	}
	loadedAppliedState, err := slRight.LoadRangeAppliedState(ctx, batch)
	require.NoError(t, err)
	require.NotNil(t, loadedAppliedState)
	loadedAppliedState.RangeStats = kvserverpb.MVCCPersistentStats{} // ignore
	require.Equal(t, &expAppliedState, loadedAppliedState)
}
