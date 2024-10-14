// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvpb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPrepareTransactionForRetry(t *testing.T) {
	isolation.RunEachLevel(t, testPrepareTransactionForRetry)
}

func testPrepareTransactionForRetry(t *testing.T, isoLevel isolation.Level) {
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	tsClock := hlc.Timestamp{WallTime: 3}
	txn := roachpb.MakeTransaction("test", nil, isoLevel, -1, ts1, 0, 99, 0, false /* omitInRangefeeds */)
	txn2ID := uuid.MakeV4() // used if txn is aborted
	tests := []struct {
		name   string
		err    *Error
		expTxn roachpb.Transaction
		expErr bool
	}{
		{
			name:   "no error",
			err:    nil,
			expErr: true,
		},
		{
			name:   "no txn",
			err:    NewError(errors.New("random")),
			expErr: true,
		},
		{
			name:   "random error",
			err:    NewErrorWithTxn(errors.New("random"), &txn),
			expErr: true,
		},
		{
			name: "txn aborted error",
			err:  NewErrorWithTxn(&TransactionAbortedError{}, &txn),
			expTxn: func() roachpb.Transaction {
				nextTxn := txn
				nextTxn.ID = txn2ID
				nextTxn.ReadTimestamp = tsClock
				nextTxn.WriteTimestamp = tsClock
				nextTxn.MinTimestamp = tsClock
				nextTxn.LastHeartbeat = tsClock
				nextTxn.GlobalUncertaintyLimit = tsClock
				return nextTxn
			}(),
		},
		{
			name: "read within uncertainty error",
			err:  NewErrorWithTxn(&ReadWithinUncertaintyIntervalError{ValueTimestamp: ts2}, &txn),
			expTxn: func() roachpb.Transaction {
				nextTxn := txn
				if isoLevel != isolation.ReadCommitted {
					nextTxn.Epoch++
				}
				nextTxn.ReadTimestamp = ts2.Next()
				nextTxn.WriteTimestamp = ts2.Next()
				return nextTxn
			}(),
		},
		{
			name: "txn push error",
			err: NewErrorWithTxn(&TransactionPushError{
				PusheeTxn: roachpb.Transaction{TxnMeta: enginepb.TxnMeta{WriteTimestamp: ts2, Priority: 3}},
			}, &txn),
			expTxn: func() roachpb.Transaction {
				nextTxn := txn
				if isoLevel != isolation.ReadCommitted {
					nextTxn.Epoch++
				}
				nextTxn.ReadTimestamp = ts2
				nextTxn.WriteTimestamp = ts2
				nextTxn.Priority = 2
				return nextTxn
			}(),
		},
		{
			name: "txn retry error (reason: write too old)",
			err:  NewErrorWithTxn(&TransactionRetryError{Reason: RETRY_WRITE_TOO_OLD}, &txn),
			expTxn: func() roachpb.Transaction {
				nextTxn := txn
				if isoLevel != isolation.ReadCommitted {
					nextTxn.Epoch++
				}
				return nextTxn
			}(),
		},
		{
			name: "txn retry error (reason: serializable)",
			err:  NewErrorWithTxn(&TransactionRetryError{Reason: RETRY_SERIALIZABLE}, &txn),
			expTxn: func() roachpb.Transaction {
				nextTxn := txn
				if isoLevel != isolation.ReadCommitted {
					nextTxn.Epoch++
				}
				nextTxn.ReadTimestamp = tsClock
				nextTxn.WriteTimestamp = tsClock
				return nextTxn
			}(),
		},
		{
			name: "write too old error",
			err:  NewErrorWithTxn(&WriteTooOldError{ActualTimestamp: ts2}, &txn),
			expTxn: func() roachpb.Transaction {
				nextTxn := txn
				if isoLevel != isolation.ReadCommitted {
					nextTxn.Epoch++
				}
				nextTxn.ReadTimestamp = ts2
				nextTxn.WriteTimestamp = ts2
				return nextTxn
			}(),
		},
		{
			name:   "intent missing error",
			err:    NewErrorWithTxn(&IntentMissingError{}, &txn),
			expErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, tsClock.WallTime)))
			nextTxn, err := PrepareTransactionForRetry(tt.err, -1 /* pri */, clock)
			if tt.expErr {
				require.Error(t, err)
				require.True(t, errors.IsAssertionFailure(err))
				require.Zero(t, nextTxn)
			} else {
				require.NoError(t, err)
				if nextTxn.ID != txn.ID {
					// Eliminate randomness from ID generation.
					nextTxn.ID = txn2ID
				}
				require.Equal(t, tt.expTxn, nextTxn)
			}
		})
	}
}

func TestTransactionRefreshTimestamp(t *testing.T) {
	isolation.RunEachLevel(t, testTransactionRefreshTimestamp)
}

func testTransactionRefreshTimestamp(t *testing.T, isoLevel isolation.Level) {
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	txn := roachpb.MakeTransaction("test", nil, isoLevel, 1, ts1, 0, 99, 0, false /* omitInRangefeeds */)
	tests := []struct {
		name  string
		err   *Error
		expOk bool
		expTs hlc.Timestamp
	}{
		{
			name:  "no error",
			err:   nil,
			expOk: false,
			expTs: hlc.Timestamp{},
		},
		{
			name:  "no txn",
			err:   NewError(errors.New("random")),
			expOk: false,
			expTs: hlc.Timestamp{},
		},
		{
			name:  "random error",
			err:   NewErrorWithTxn(errors.New("random"), &txn),
			expOk: false,
			expTs: hlc.Timestamp{},
		},
		{
			name:  "txn aborted error",
			err:   NewErrorWithTxn(&TransactionAbortedError{}, &txn),
			expOk: false,
			expTs: hlc.Timestamp{},
		},
		{
			name:  "txn retry error (reason: unknown)",
			err:   NewErrorWithTxn(&TransactionRetryError{Reason: RETRY_REASON_UNKNOWN}, &txn),
			expOk: false,
			expTs: hlc.Timestamp{},
		},
		{
			name:  "txn retry error (reason: write too old)",
			err:   NewErrorWithTxn(&TransactionRetryError{Reason: RETRY_WRITE_TOO_OLD}, &txn),
			expOk: true,
			expTs: ts1,
		},
		{
			name:  "txn retry error (reason: serializable)",
			err:   NewErrorWithTxn(&TransactionRetryError{Reason: RETRY_SERIALIZABLE}, &txn),
			expOk: true,
			expTs: ts1,
		},
		{
			name:  "txn retry error (reason: async write failure)",
			err:   NewErrorWithTxn(&TransactionRetryError{Reason: RETRY_ASYNC_WRITE_FAILURE}, &txn),
			expOk: false,
			expTs: hlc.Timestamp{},
		},
		{
			name:  "txn retry error (reason: commit deadline exceeded)",
			err:   NewErrorWithTxn(&TransactionRetryError{Reason: RETRY_COMMIT_DEADLINE_EXCEEDED}, &txn),
			expOk: false,
			expTs: hlc.Timestamp{},
		},
		{
			name:  "write too old error",
			err:   NewErrorWithTxn(&WriteTooOldError{ActualTimestamp: ts2}, &txn),
			expOk: true,
			expTs: ts2,
		},
		{
			name:  "read within uncertainty error",
			err:   NewErrorWithTxn(&ReadWithinUncertaintyIntervalError{ValueTimestamp: ts2}, &txn),
			expOk: true,
			expTs: ts2.Next(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, ts := TransactionRefreshTimestamp(tt.err)
			require.Equal(t, tt.expOk, ok)
			require.Equal(t, tt.expTs, ts)
		})
	}
}
