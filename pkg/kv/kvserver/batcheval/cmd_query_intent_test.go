// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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

func TestQueryIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	db := storage.NewDefaultInMemForTesting()
	defer db.Close()

	makeTS := func(ts int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: ts}
	}

	writeIntent := func(k roachpb.Key, ts int64) roachpb.Transaction {
		txn := roachpb.MakeTransaction("test", k, 0, makeTS(ts), 0, 1)
		_, err := storage.MVCCDelete(ctx, db, nil, k, makeTS(ts), hlc.ClockTimestamp{}, &txn)
		require.NoError(t, err)
		return txn
	}

	// Write three keys in three separate transactions.
	keyA := roachpb.Key("a")
	keyAA := roachpb.Key("aa")
	keyB := roachpb.Key("b")
	keyC := roachpb.Key("c")

	txA := writeIntent(keyA, 5)
	txAA := writeIntent(keyAA, 6)
	txB := writeIntent(keyB, 7)

	st := cluster.MakeTestingClusterSettings()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 10)))
	evalCtx := &MockEvalCtx{ClusterSettings: st, Clock: clock}

	// Since we can't move the intents clock after they are written, created
	// cloned transactions with the clock shifted instead.
	txABack := *txA.Clone()
	txAForward := *txA.Clone()
	txABack.WriteTimestamp = txABack.WriteTimestamp.Add(-2, 0)
	txAForward.WriteTimestamp = txAForward.WriteTimestamp.Add(20, 0)

	type Success struct{}
	type NotFound struct{}
	success := Success{}
	notFound := NotFound{}

	tests := []struct {
		name           string
		hTransaction   roachpb.Transaction
		argTransaction roachpb.Transaction
		key            roachpb.Key
		errorFlag      bool
		response       interface{}
	}{
		// Perform standard reading of all three keys.
		{"readA", txA, txA, keyA, true, success},
		{"readAA", txAA, txAA, keyAA, true, success},
		{"readB", txB, txB, keyB, true, success},

		{"readC", txA, txA, keyC, true, &kvpb.IntentMissingError{}},

		// This tries reading a different key than this tx was written with. The
		// returned error depends on the error flag setting.
		{"wrongTxE", txA, txA, keyB, true, &kvpb.IntentMissingError{}},
		{"wrongTx", txA, txA, keyB, false, notFound},

		// This sets a mismatch for transactions in the header and the body. An
		// error is returned regardless of the errorFlag.
		{"mismatchTxE", txA, txB, keyA, true, errors.AssertionFailedf("")},
		{"mismatchTx", txA, txB, keyA, false, errors.AssertionFailedf("")},

		// This simulates pushed intents by moving the tx clock backwards in time.
		// An error is only returned if the error flag is set.
		{"clockBackE", txABack, txABack, keyA, true, &kvpb.TransactionRetryError{}},
		{"clockBack", txABack, txABack, keyA, false, notFound},

		// This simulates pushed transactions by moving the tx clock forward in time.
		{"clockFwd", txAForward, txAForward, keyA, true, success},

		// This simulates a mismatch in the header and arg write timestamps. This is
		// always an error regardless of flag.
		{"mismatchTxClockE", txA, txAForward, keyA, true, errors.AssertionFailedf("")},
		{"mismatchTxClock", txA, txAForward, keyA, false, errors.AssertionFailedf("")},

		// It is OK if the time on the arg transaction is moved backwards, its
		// unclear if this happens in practice.
		{"mismatchTxClock", txA, txABack, keyA, true, success},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cArgs := CommandArgs{
				Header: kvpb.Header{Timestamp: clock.Now(), Txn: &test.hTransaction},
				Args: &kvpb.QueryIntentRequest{
					RequestHeader:  kvpb.RequestHeader{Key: test.key},
					Txn:            test.argTransaction.TxnMeta,
					ErrorIfMissing: test.errorFlag,
				},
			}
			cArgs.EvalCtx = evalCtx.EvalContext()
			var resp kvpb.QueryIntentResponse
			_, err := QueryIntent(ctx, db, cArgs, &resp)
			switch test.response {
			case success:
				require.NoError(t, err)
				require.True(t, resp.FoundIntent)
			case notFound:
				require.NoError(t, err)
				require.False(t, resp.FoundIntent)
			default:
				require.IsType(t, test.response, err, "received %v", err)
				require.False(t, resp.FoundIntent)
			}
		})
	}
}
