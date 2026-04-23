// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnfeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestEndTxnWriteTooOldBelowClosedTS verifies that when creating a new
// transaction record during commit, if the transaction's WriteTimestamp is at
// or below the closed timestamp, EndTxn returns a WriteTooOldError. This
// ensures the TxnFeed resolved timestamp never needs to regress when new
// transaction records appear.
func TestEndTxnWriteTooOldBelowClosedTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))

	startKey := roachpb.Key("0000")
	endKey := roachpb.Key("9999")
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}
	as := abortspan.New(desc.RangeID)

	txnKey := roachpb.Key("a")
	ts := hlc.Timestamp{WallTime: 1}
	txn := roachpb.MakeTransaction(
		"test", txnKey, 0, 0, ts, 0, 1, 0, false, /* omitInRangefeeds */
	)

	tests := []struct {
		name        string
		closedTS    hlc.Timestamp
		commit      bool
		expectedErr bool
	}{
		{
			name:        "commit below closed timestamp",
			closedTS:    hlc.Timestamp{WallTime: 10},
			commit:      true,
			expectedErr: true,
		},
		{
			name:        "commit at closed timestamp",
			closedTS:    ts,
			commit:      true,
			expectedErr: true,
		},
		{
			name:        "commit above closed timestamp",
			closedTS:    hlc.Timestamp{WallTime: 0, Logical: 1},
			commit:      true,
			expectedErr: false,
		},
		{
			name:        "abort below closed timestamp",
			closedTS:    hlc.Timestamp{WallTime: 10},
			commit:      false,
			expectedErr: false,
		},
		{
			name:        "empty closed timestamp",
			closedTS:    hlc.Timestamp{},
			commit:      true,
			expectedErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := storage.NewDefaultInMemForTesting()
			defer db.Close()
			batch := db.NewBatch()
			defer batch.Close()

			st := cluster.MakeTestingClusterSettings()

			req := kvpb.EndTxnRequest{
				RequestHeader: kvpb.RequestHeader{Key: txn.Key},
				Commit:        tc.commit,
				LockSpans:     []roachpb.Span{{Key: roachpb.Key("b")}},
			}

			var resp kvpb.EndTxnResponse
			_, err := EndTxn(ctx, batch, CommandArgs{
				EvalCtx: (&MockEvalCtx{
					Desc:            &desc,
					Clock:           clock,
					AbortSpan:       as,
					ClusterSettings: st,
					ClosedTimestamp: tc.closedTS,
					CanCreateTxnRecordFn: func() (bool, kvpb.TransactionAbortedReason) {
						return true, 0
					},
				}).EvalContext(),
				Args: &req,
				Header: kvpb.Header{
					Timestamp: ts,
					Txn:       txn.Clone(),
				},
			}, &resp)

			if tc.expectedErr {
				var wtoErr *kvpb.WriteTooOldError
				require.ErrorAs(t, err, &wtoErr)
				require.Equal(t, ts, wtoErr.Timestamp,
					"WriteTooOldError.Timestamp should be the txn's WriteTimestamp")
				require.Equal(t, tc.closedTS.Next(), wtoErr.ActualTimestamp,
					"WriteTooOldError.ActualTimestamp should be closedTS.Next()")
			} else {
				if err != nil {
					// Rollbacks may return other errors (e.g., the txn is
					// written as aborted), but should not return
					// WriteTooOldError.
					var wtoErr *kvpb.WriteTooOldError
					require.False(t, errors.As(err, &wtoErr),
						"should not return WriteTooOldError, got: %v", err)
				}
			}
		})
	}
}

// TestEndTxnTxnFeedOps verifies that EndTxn emits the correct TxnFeedOps
// for parallel commits (STAGING → RECORD_WRITTEN) and explicit aborts
// (ABORTED → ABORTED op).
func TestEndTxnTxnFeedOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))
	now := clock.Now()

	startKey := roachpb.Key("0000")
	endKey := roachpb.Key("9999")
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}
	as := abortspan.New(desc.RangeID)
	txnKey := roachpb.Key("a")

	st := cluster.MakeTestingClusterSettings()
	txnfeed.Enabled.Override(ctx, &st.SV, true)

	t.Run("staging emits RECORD_WRITTEN", func(t *testing.T) {
		db := storage.NewDefaultInMemForTesting()
		defer db.Close()
		batch := db.NewBatch()
		defer batch.Close()

		txn := roachpb.MakeTransaction(
			"test", txnKey, 0, 0, now, 0, 1, 0, false, /* omitInRangefeeds */
		)

		req := kvpb.EndTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: txnKey},
			Commit:        true,
			LockSpans:     []roachpb.Span{{Key: roachpb.Key("b")}},
			InFlightWrites: []roachpb.SequencedWrite{
				{Key: roachpb.Key("b"), Sequence: 1},
			},
		}

		var resp kvpb.EndTxnResponse
		res, err := EndTxn(ctx, batch, CommandArgs{
			EvalCtx: (&MockEvalCtx{
				Desc:            &desc,
				Clock:           clock,
				AbortSpan:       as,
				ClusterSettings: st,
				CanCreateTxnRecordFn: func() (bool, kvpb.TransactionAbortedReason) {
					return true, 0
				},
			}).EvalContext(),
			Args: &req,
			Header: kvpb.Header{
				Timestamp: now,
				Txn:       txn.Clone(),
			},
		}, &resp)
		require.NoError(t, err)
		require.Equal(t, roachpb.STAGING, resp.Txn.Status)

		require.NotNil(t, res.Replicated.TxnFeedOps)
		require.Len(t, res.Replicated.TxnFeedOps.Ops, 1)
		op := res.Replicated.TxnFeedOps.Ops[0]
		require.Equal(t, kvserverpb.TxnFeedOp_RECORD_WRITTEN, op.Type)
		require.Equal(t, resp.Txn.ID, op.TxnID)
		require.Equal(t, txnKey, op.AnchorKey)
		require.Equal(t, resp.Txn.WriteTimestamp, op.WriteTimestamp)
	})

	t.Run("abort with existing record emits ABORTED", func(t *testing.T) {
		db := storage.NewDefaultInMemForTesting()
		defer db.Close()

		txn := roachpb.MakeTransaction(
			"test", txnKey, 0, 0, now, 0, 1, 0, false, /* omitInRangefeeds */
		)

		// Pre-write a PENDING transaction record so recordAlreadyExisted is true.
		txnRecord := txn.AsRecord()
		txnRecordKey := keys.TransactionKey(txnKey, txn.ID)
		require.NoError(t, storage.MVCCPutProto(
			ctx, db, txnRecordKey, clock.Now(), &txnRecord,
			storage.MVCCWriteOptions{Category: fs.BatchEvalReadCategory},
		))

		batch := db.NewBatch()
		defer batch.Close()

		req := kvpb.EndTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: txnKey},
			Commit:        false,
			LockSpans:     []roachpb.Span{{Key: roachpb.Key("b")}},
		}

		var resp kvpb.EndTxnResponse
		res, err := EndTxn(ctx, batch, CommandArgs{
			EvalCtx: (&MockEvalCtx{
				Desc:            &desc,
				Clock:           clock,
				AbortSpan:       as,
				ClusterSettings: st,
				CanCreateTxnRecordFn: func() (bool, kvpb.TransactionAbortedReason) {
					return true, 0
				},
			}).EvalContext(),
			Args: &req,
			Header: kvpb.Header{
				Timestamp: now,
				Txn:       txn.Clone(),
			},
		}, &resp)
		require.NoError(t, err)
		require.Equal(t, roachpb.ABORTED, resp.Txn.Status)

		require.NotNil(t, res.Replicated.TxnFeedOps)
		require.Len(t, res.Replicated.TxnFeedOps.Ops, 1)
		op := res.Replicated.TxnFeedOps.Ops[0]
		require.Equal(t, kvserverpb.TxnFeedOp_ABORTED, op.Type)
		require.Equal(t, resp.Txn.ID, op.TxnID)
	})

	t.Run("abort without existing record does not emit", func(t *testing.T) {
		db := storage.NewDefaultInMemForTesting()
		defer db.Close()
		batch := db.NewBatch()
		defer batch.Close()

		txn := roachpb.MakeTransaction(
			"test", txnKey, 0, 0, now, 0, 1, 0, false, /* omitInRangefeeds */
		)

		req := kvpb.EndTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: txnKey},
			Commit:        false,
			LockSpans:     []roachpb.Span{{Key: roachpb.Key("b")}},
		}

		var resp kvpb.EndTxnResponse
		res, err := EndTxn(ctx, batch, CommandArgs{
			EvalCtx: (&MockEvalCtx{
				Desc:            &desc,
				Clock:           clock,
				AbortSpan:       as,
				ClusterSettings: st,
				CanCreateTxnRecordFn: func() (bool, kvpb.TransactionAbortedReason) {
					return true, 0
				},
			}).EvalContext(),
			Args: &req,
			Header: kvpb.Header{
				Timestamp: now,
				Txn:       txn.Clone(),
			},
		}, &resp)
		require.NoError(t, err)
		require.Equal(t, roachpb.ABORTED, resp.Txn.Status)
		require.Nil(t, res.Replicated.TxnFeedOps)
	})
}
