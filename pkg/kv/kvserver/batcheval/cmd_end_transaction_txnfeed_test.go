// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
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
