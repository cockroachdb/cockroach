// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		txn := roachpb.MakeTransaction("test", k, 0, 0, makeTS(ts), 0, 1, 0, false /* omitInRangefeeds */)
		_, _, err := storage.MVCCDelete(ctx, db, k, makeTS(ts), storage.MVCCWriteOptions{Txn: &txn})
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

	// Since we can't move the intents timestamp after they are written, created
	// cloned transactions with the timestamp shifted instead.
	txABack := *txA.Clone()
	txAForward := *txA.Clone()
	txABack.WriteTimestamp = txABack.WriteTimestamp.Add(-2, 0)
	txAForward.WriteTimestamp = txAForward.WriteTimestamp.Add(20, 0)

	type response int
	const (
		_ response = iota
		expAssertionError
		expIntentMissingError
		expNotFound
		expFoundIntent
		expFoundUnpushedIntent
	)

	tests := []struct {
		name           string
		hTransaction   roachpb.Transaction
		argTransaction roachpb.Transaction
		key            roachpb.Key
		errorFlag      bool
		resp           response
	}{
		// Perform standard reading of all three keys.
		{"readA", txA, txA, keyA, true, expFoundUnpushedIntent},
		{"readAA", txAA, txAA, keyAA, true, expFoundUnpushedIntent},
		{"readB", txB, txB, keyB, true, expFoundUnpushedIntent},
		{"readC", txA, txA, keyC, true, expIntentMissingError},

		// This tries reading a different key than this tx was written with. The
		// returned error depends on the error flag setting.
		{"wrongTxErr", txA, txA, keyB, true, expIntentMissingError},
		{"wrongTx", txA, txA, keyB, false, expNotFound},

		// This sets a mismatch for transactions in the header and the body. An
		// error is returned regardless of the errorFlag.
		{"mismatchTxErr", txA, txB, keyA, true, expAssertionError},
		{"mismatchTx", txA, txB, keyA, false, expAssertionError},

		// This simulates pushed intents by moving the tx timestamp backwards in time.
		// An error is not returned, regardless of the error flag.
		{"timestampBackErr", txABack, txABack, keyA, true, expFoundIntent},
		{"timestampBack", txABack, txABack, keyA, false, expFoundIntent},

		// This simulates pushed transactions by moving the tx timestamp forward in time.
		// In two of the cases, the header timestamp leads the argument's timestamp.
		{"timestampFwd", txAForward, txAForward, keyA, true, expFoundUnpushedIntent},
		{"timestampFwdHeaderAhead", txAForward, txA, keyA, true, expFoundUnpushedIntent},
		{"timestampEqualHeaderAhead", txA, txABack, keyA, true, expFoundUnpushedIntent},

		// This simulates a mismatch in the header and arg write timestamps. This is
		// always an error regardless of flag.
		{"headerBehindErr", txA, txAForward, keyA, true, expAssertionError},
		{"headerBehind", txA, txAForward, keyA, false, expAssertionError},
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
			switch test.resp {
			case expAssertionError:
				require.NotNil(t, err)
				require.IsType(t, errors.AssertionFailedf(""), err)
				require.False(t, resp.FoundIntent)
				require.False(t, resp.FoundUnpushedIntent)
				require.Nil(t, resp.Txn)
			case expIntentMissingError:
				require.NotNil(t, err)
				require.IsType(t, &kvpb.IntentMissingError{}, err)
				require.False(t, resp.FoundIntent)
				require.False(t, resp.FoundUnpushedIntent)
				require.Nil(t, resp.Txn)
			case expNotFound:
				require.Nil(t, err)
				require.False(t, resp.FoundIntent)
				require.False(t, resp.FoundUnpushedIntent)
				require.Nil(t, resp.Txn)
			case expFoundIntent:
				require.Nil(t, err)
				require.True(t, resp.FoundIntent)
				require.False(t, resp.FoundUnpushedIntent)
				// If the intent was found but was pushed, the response also carries the
				// updated write timestamp.
				require.NotNil(t, resp.Txn)
				require.True(t, test.hTransaction.WriteTimestamp.Less(resp.Txn.WriteTimestamp))
			case expFoundUnpushedIntent:
				require.Nil(t, err)
				require.True(t, resp.FoundIntent)
				require.True(t, resp.FoundUnpushedIntent)
				require.Nil(t, resp.Txn)
			default:
				t.Fatalf("unexpected response: %v", test.resp)
			}
		})
	}
}
