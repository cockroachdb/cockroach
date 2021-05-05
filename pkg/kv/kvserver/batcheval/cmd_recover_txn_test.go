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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestRecoverTxn tests RecoverTxn request in its base case where no concurrent
// actors have modified the transaction record that it is attempting to recover.
// It tests the case where all of the txn's in-flight writes were successful and
// the case where one of the txn's in-flight writes was found missing and
// prevented.
func TestRecoverTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	k, k2 := roachpb.Key("a"), roachpb.Key("b")
	ts := hlc.Timestamp{WallTime: 1}
	txn := roachpb.MakeTransaction("test", k, 0, ts, 0)
	txn.Status = roachpb.STAGING
	txn.LockSpans = []roachpb.Span{{Key: k}}
	txn.InFlightWrites = []roachpb.SequencedWrite{{Key: k2, Sequence: 0}}

	testutils.RunTrueAndFalse(t, "missing write", func(t *testing.T, missingWrite bool) {
		db := storage.NewDefaultInMemForTesting()
		defer db.Close()

		// Write the transaction record.
		txnKey := keys.TransactionKey(txn.Key, txn.ID)
		txnRecord := txn.AsRecord()
		if err := storage.MVCCPutProto(ctx, db, nil, txnKey, hlc.Timestamp{}, nil, &txnRecord); err != nil {
			t.Fatal(err)
		}

		// Issue a RecoverTxn request.
		var resp roachpb.RecoverTxnResponse
		if _, err := RecoverTxn(ctx, db, CommandArgs{
			Args: &roachpb.RecoverTxnRequest{
				RequestHeader:       roachpb.RequestHeader{Key: txn.Key},
				Txn:                 txn.TxnMeta,
				ImplicitlyCommitted: !missingWrite,
			},
			Header: roachpb.Header{
				Timestamp: ts,
			},
		}, &resp); err != nil {
			t.Fatal(err)
		}

		// Assert that the response is correct.
		expTxnRecord := txn.AsRecord()
		expTxn := expTxnRecord.AsTransaction()
		// Merge the in-flight writes into the lock spans.
		expTxn.LockSpans = []roachpb.Span{{Key: k}, {Key: k2}}
		expTxn.InFlightWrites = nil
		// Set the correct status.
		if !missingWrite {
			expTxn.Status = roachpb.COMMITTED
		} else {
			expTxn.Status = roachpb.ABORTED
		}
		require.Equal(t, expTxn, resp.RecoveredTxn)

		// Assert that the updated txn record was persisted correctly.
		var resTxnRecord roachpb.Transaction
		if _, err := storage.MVCCGetProto(
			ctx, db, txnKey, hlc.Timestamp{}, &resTxnRecord, storage.MVCCGetOptions{},
		); err != nil {
			t.Fatal(err)
		}
		require.Equal(t, expTxn, resTxnRecord)
	})
}

// TestRecoverTxnRecordChanged tests that RecoverTxn requests are no-ops when
// they find that the transaction record that they are attempting to recover is
// different than what they expected it to be, which would be either due to an
// active transaction coordinator or due to a concurrent recovery.
func TestRecoverTxnRecordChanged(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	k := roachpb.Key("a")
	ts := hlc.Timestamp{WallTime: 1}
	txn := roachpb.MakeTransaction("test", k, 0, ts, 0)
	txn.Status = roachpb.STAGING

	testCases := []struct {
		name                string
		implicitlyCommitted bool
		expError            string
		changedTxn          roachpb.Transaction
	}{
		{
			name:                "transaction commit after all writes found",
			implicitlyCommitted: true,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Status = roachpb.COMMITTED
				txnCopy.InFlightWrites = nil
				return txnCopy
			}(),
		},
		{
			name:                "transaction abort after all writes found",
			implicitlyCommitted: true,
			expError:            "found ABORTED record for implicitly committed transaction",
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Status = roachpb.ABORTED
				txnCopy.InFlightWrites = nil
				return txnCopy
			}(),
		},
		{
			name:                "transaction restart after all writes found",
			implicitlyCommitted: true,
			expError:            "epoch change by implicitly committed transaction: 0->1",
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.BumpEpoch()
				return txnCopy
			}(),
		},
		{
			name:                "transaction timestamp increase after all writes found",
			implicitlyCommitted: true,
			expError:            "timestamp change by implicitly committed transaction: 0.000000001,0->0.000000002,0",
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.WriteTimestamp = txnCopy.WriteTimestamp.Add(1, 0)
				return txnCopy
			}(),
		},
		{
			name:                "transaction commit after write prevented",
			implicitlyCommitted: false,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Status = roachpb.COMMITTED
				txnCopy.InFlightWrites = nil
				return txnCopy
			}(),
		},
		{
			name:                "transaction abort after write prevented",
			implicitlyCommitted: false,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Status = roachpb.ABORTED
				txnCopy.InFlightWrites = nil
				return txnCopy
			}(),
		},
		{
			name:                "transaction restart (pending) after write prevented",
			implicitlyCommitted: false,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.BumpEpoch()
				txnCopy.Status = roachpb.PENDING
				return txnCopy
			}(),
		},
		{
			name:                "transaction restart (staging) after write prevented",
			implicitlyCommitted: false,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.BumpEpoch()
				return txnCopy
			}(),
		},
		{
			name:                "transaction timestamp increase (pending) after write prevented",
			implicitlyCommitted: false,
			expError:            "cannot recover PENDING transaction in same epoch",
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Status = roachpb.PENDING
				txnCopy.InFlightWrites = nil
				txnCopy.WriteTimestamp = txnCopy.WriteTimestamp.Add(1, 0)
				return txnCopy
			}(),
		},
		{
			name:                "transaction timestamp increase (staging) after write prevented",
			implicitlyCommitted: false,
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.WriteTimestamp = txnCopy.WriteTimestamp.Add(1, 0)
				return txnCopy
			}(),
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			db := storage.NewDefaultInMemForTesting()
			defer db.Close()

			// Write the modified transaction record, simulating a concurrent
			// actor changing the transaction record before the RecoverTxn
			// request is evaluated.
			txnKey := keys.TransactionKey(txn.Key, txn.ID)
			txnRecord := c.changedTxn.AsRecord()
			if err := storage.MVCCPutProto(ctx, db, nil, txnKey, hlc.Timestamp{}, nil, &txnRecord); err != nil {
				t.Fatal(err)
			}

			// Issue a RecoverTxn request.
			var resp roachpb.RecoverTxnResponse
			_, err := RecoverTxn(ctx, db, CommandArgs{
				Args: &roachpb.RecoverTxnRequest{
					RequestHeader:       roachpb.RequestHeader{Key: txn.Key},
					Txn:                 txn.TxnMeta,
					ImplicitlyCommitted: c.implicitlyCommitted,
				},
				Header: roachpb.Header{
					Timestamp: ts,
				},
			}, &resp)

			if c.expError != "" {
				if !testutils.IsError(err, c.expError) {
					t.Fatalf("expected error %q; found %v", c.expError, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}

				// Assert that the response is correct.
				expTxnRecord := c.changedTxn.AsRecord()
				expTxn := expTxnRecord.AsTransaction()
				require.Equal(t, expTxn, resp.RecoveredTxn)

				// Assert that the txn record was not modified.
				var resTxnRecord roachpb.Transaction
				if _, err := storage.MVCCGetProto(
					ctx, db, txnKey, hlc.Timestamp{}, &resTxnRecord, storage.MVCCGetOptions{},
				); err != nil {
					t.Fatal(err)
				}
				require.Equal(t, expTxn, resTxnRecord)
			}
		})
	}
}
