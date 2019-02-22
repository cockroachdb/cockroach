// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestRecoverTxn tests RecoverTxn request in its base case where no concurrent
// actors have modified the transaction record that it is attempting to recover.
// It tests the case where all of the txn's in-flight writes were successful and
// the case where one of the txn's in-flight writes was found missing and
// prevented.
func TestRecoverTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	k := roachpb.Key("a")
	ts := hlc.Timestamp{WallTime: 1}
	txn := roachpb.MakeTransaction("test", k, 0, ts, 0)
	txn.Status = roachpb.STAGING

	testutils.RunTrueAndFalse(t, "missing write", func(t *testing.T, missingWrite bool) {
		db := engine.NewInMem(roachpb.Attributes{}, 10<<20)
		defer db.Close()

		// Write the transaction record.
		txnKey := keys.TransactionKey(txn.Key, txn.ID)
		txnRecord := txn.AsRecord()
		if err := engine.MVCCPutProto(ctx, db, nil, txnKey, hlc.Timestamp{}, nil, &txnRecord); err != nil {
			t.Fatal(err)
		}

		// Issue a RecoverTxn request.
		var resp roachpb.RecoverTxnResponse
		if _, err := RecoverTxn(ctx, db, CommandArgs{
			Args: &roachpb.RecoverTxnRequest{
				RequestHeader:  roachpb.RequestHeader{Key: txn.Key},
				Txn:            txn.TxnMeta,
				AllWritesFound: !missingWrite,
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
		if !missingWrite {
			expTxn.Status = roachpb.COMMITTED
		} else {
			expTxn.Status = roachpb.ABORTED
		}
		require.Equal(t, expTxn, resp.RecoveredTxn)

		// Assert that the updated txn record was persisted correctly.
		var resTxnRecord roachpb.Transaction
		if _, err := engine.MVCCGetProto(
			ctx, db, txnKey, hlc.Timestamp{}, &resTxnRecord, engine.MVCCGetOptions{},
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

	ctx := context.Background()
	k := roachpb.Key("a")
	ts := hlc.Timestamp{WallTime: 1}
	txn := roachpb.MakeTransaction("test", k, 0, ts, 0)
	txn.Status = roachpb.STAGING

	testCases := []struct {
		name       string
		changedTxn roachpb.Transaction
	}{
		{
			name: "transaction commit",
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Status = roachpb.COMMITTED
				txnCopy.InFlightWrites = nil
				return txnCopy
			}(),
		},
		{
			name: "transaction abort",
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Status = roachpb.ABORTED
				txnCopy.InFlightWrites = nil
				return txnCopy
			}(),
		},
		{
			name: "transaction restart",
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.BumpEpoch()
				return txnCopy
			}(),
		},
		{
			name: "transaction timestamp increase",
			changedTxn: func() roachpb.Transaction {
				txnCopy := txn
				txnCopy.Timestamp = txnCopy.Timestamp.Add(1, 0)
				return txnCopy
			}(),
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			db := engine.NewInMem(roachpb.Attributes{}, 10<<20)
			defer db.Close()

			// Write the modified transaction record, simulating a concurrent
			// actor changing the transaction record before the RecoverTxn
			// request is evaluated.
			txnKey := keys.TransactionKey(txn.Key, txn.ID)
			txnRecord := c.changedTxn.AsRecord()
			if err := engine.MVCCPutProto(ctx, db, nil, txnKey, hlc.Timestamp{}, nil, &txnRecord); err != nil {
				t.Fatal(err)
			}

			// Issue a RecoverTxn request.
			var resp roachpb.RecoverTxnResponse
			if _, err := RecoverTxn(ctx, db, CommandArgs{
				Args: &roachpb.RecoverTxnRequest{
					RequestHeader:  roachpb.RequestHeader{Key: txn.Key},
					Txn:            txn.TxnMeta,
					AllWritesFound: true,
				},
				Header: roachpb.Header{
					Timestamp: ts,
				},
			}, &resp); err != nil {
				t.Fatal(err)
			}

			// Assert that the response is correct.
			expTxnRecord := c.changedTxn.AsRecord()
			expTxn := expTxnRecord.AsTransaction()
			require.Equal(t, expTxn, resp.RecoveredTxn)

			// Assert that the txn record was not modified.
			var resTxnRecord roachpb.Transaction
			if _, err := engine.MVCCGetProto(
				ctx, db, txnKey, hlc.Timestamp{}, &resTxnRecord, engine.MVCCGetOptions{},
			); err != nil {
				t.Fatal(err)
			}
			require.Equal(t, expTxn, resTxnRecord)
		})
	}
}
