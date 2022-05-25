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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
		require.NoError(t, storage.MVCCDelete(ctx, db, nil, k, makeTS(ts), hlc.ClockTimestamp{}, &txn))
		return txn
	}

	// Write three keys in three separate transactions.
	keyA := roachpb.Key("a")
	keyAA := roachpb.Key("aa")
	keyB := roachpb.Key("b")

	txA := writeIntent(keyA, 5)
	txAA := writeIntent(keyAA, 6)
	txB := writeIntent(keyB, 7)

	st := cluster.MakeTestingClusterSettings()
	clock := hlc.NewClock(hlc.NewManualClock(10).UnixNano, time.Nanosecond)
	evalCtx := &MockEvalCtx{ClusterSettings: st, Clock: clock}

	// Since we can't move the intents clock after they are written, created
	// cloned transactions with the clock shifted instead.
	txABack := *txA.Clone()
	txAForward := *txA.Clone()
	txABack.WriteTimestamp = txABack.WriteTimestamp.Add(-2, 0)
	txAForward.WriteTimestamp = txAForward.WriteTimestamp.Add(20, 0)

	type ResponseType int
	const (
		Error ResponseType = iota
		NotFound
		Success
	)

	tests := []struct {
		name           string
		hTransaction   roachpb.Transaction
		argTransaction roachpb.Transaction
		key            roachpb.Key
		errorFlag      bool
		response       ResponseType
	}{
		// Perform standard reading of all three keys.
		{"readA", txA, txA, keyA, true, Success},
		{"readAA", txAA, txAA, keyAA, true, Success},
		{"readB", txB, txB, keyB, true, Success},

		// This tries reading a different key than this tx was written with. The
		// returned error depends on the error flag setting.
		{"wrongTxE", txA, txA, keyB, true, Error},
		{"wrongTx", txA, txA, keyB, false, NotFound},

		// This sets a mismatch for transactions in the header and the body. An
		// error is returned regardless of the errorFlag.
		{"mismatchTxE", txA, txB, keyA, true, Error},
		{"mismatchTx", txA, txB, keyA, false, Error},

		// This simulates pushed intents by moving the tx clock backwards in time.
		// An error is only returned if the error flag is set.
		{"clockBackE", txABack, txABack, keyA, true, Error},
		{"clockBack", txABack, txABack, keyA, false, NotFound},

		// This simulates pushed transactions by moving the tx clock forward in time.
		{"clockFwd", txAForward, txAForward, keyA, true, Success},

		// This simulates a mismatch in the header and arg write timestamps. This is
		// always an error regardless of flag.
		{"mismatchTxClockE", txA, txAForward, keyA, true, Error},
		{"mismatchTxClock", txA, txAForward, keyA, false, Error},

		// It is OK if the time on the arg transaction is moved backwards, its
		// unclear if this happens in practice.
		{"mismatchTxClock", txA, txABack, keyA, true, Success},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cArgs := CommandArgs{
				Header: roachpb.Header{Timestamp: clock.Now(), Txn: &test.hTransaction},
				Args: &roachpb.QueryIntentRequest{
					RequestHeader:  roachpb.RequestHeader{Key: test.key},
					Txn:            test.argTransaction.TxnMeta,
					ErrorIfMissing: test.errorFlag,
				},
			}
			cArgs.EvalCtx = evalCtx.EvalContext()
			var resp roachpb.QueryIntentResponse
			_, err := QueryIntent(ctx, db, cArgs, &resp)
			switch test.response {
			case Error:
				require.Error(t, err)
				require.False(t, resp.FoundIntent)
			case NotFound:
				require.NoError(t, err)
				require.False(t, resp.FoundIntent)
			case Success:
				require.NoError(t, err)
				require.True(t, resp.FoundIntent)
			}
		})
	}
}
