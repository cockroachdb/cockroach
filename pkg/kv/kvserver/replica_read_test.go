// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestVirtualizedIntentResolution exercises read-only request execution and
// checks that reads have the correct behavior with and without intents to
// resolve virtually, returned by the concurrency (and lock table) guard.
func TestVirtualizedIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tsc := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

	// The manual clock starts at time 123.
	ts200 := makeTS(200, 0)
	ts300 := makeTS(300, 0)
	ts400 := makeTS(400, 0)
	ts500 := makeTS(500, 0)
	ts600 := makeTS(600, 0)

	// Setup: write an original value at time 200.
	k := roachpb.Key("a")
	pArgs := putArgs(k, []byte("original"))
	_, pErr := tc.SendWrappedWith(kvpb.Header{Timestamp: ts200}, &pArgs)
	require.NoError(t, pErr.GoError())

	// Setup: write an intent at time 300.
	writeTxn := newTransaction("writeTxn", k, 1, nil)
	pArgs = putArgs(k, []byte("intent"))
	writeTxn.ReadTimestamp = ts300
	writeTxn.WriteTimestamp = ts300
	_, pErr = tc.SendWrappedWith(kvpb.Header{Txn: writeTxn}, &pArgs)
	require.NoError(t, pErr.GoError())

	type res struct {
		value string
		error string
	}
	type toResolve struct {
		txnStatus  roachpb.TransactionStatus
		txnWriteTs hlc.Timestamp
		obsTs      hlc.Timestamp
		obsNode    int
	}
	testCases := []struct {
		readTs    hlc.Timestamp
		readGUL   hlc.Timestamp
		toResolve *toResolve
		exp       res
	}{
		// Reading below the intent's ts results in seeing the original value.
		{readTs: ts200, exp: res{value: "original"}},
		// Reading at or above the intent's ts results in a lock conflict error.
		{readTs: ts300, exp: res{error: "conflicting locks"}},
		{readTs: ts400, exp: res{error: "conflicting locks"}},
		// Next, we look at cases where we have intents to resolve virtually.
		// Reading below the pushed writeTxn's ts results in the original value
		// since these reads have no uncertainty interval.
		{readTs: ts300, toResolve: &toResolve{txnStatus: roachpb.PENDING, txnWriteTs: ts400}, exp: res{value: "original"}},
		{readTs: ts300, toResolve: &toResolve{txnStatus: roachpb.STAGING, txnWriteTs: ts400}, exp: res{value: "original"}},
		{readTs: ts300, toResolve: &toResolve{txnStatus: roachpb.COMMITTED, txnWriteTs: ts400}, exp: res{value: "original"}},
		{readTs: ts300, toResolve: &toResolve{txnStatus: roachpb.ABORTED, txnWriteTs: ts400}, exp: res{value: "original"}},
		// Reading at or above the pushed writeTxn's ts results in a lock conflict
		// error if the txn is not finalized. Otherwise, the intent value is
		// expected if the txn is committed, and the original value is expected if
		// the txn is aborted.
		{readTs: ts400, toResolve: &toResolve{txnStatus: roachpb.PENDING, txnWriteTs: ts400}, exp: res{error: "conflicting lock"}},
		{readTs: ts400, toResolve: &toResolve{txnStatus: roachpb.STAGING, txnWriteTs: ts400}, exp: res{error: "conflicting lock"}},
		{readTs: ts400, toResolve: &toResolve{txnStatus: roachpb.COMMITTED, txnWriteTs: ts400}, exp: res{value: "intent"}},
		{readTs: ts400, toResolve: &toResolve{txnStatus: roachpb.ABORTED, txnWriteTs: ts400}, exp: res{value: "original"}},
		// Next, we look at more subtle cases with uncertainty intervals. To do so,
		// the read is now a transactional read with a global uncertainty limit.
		// Reading below the pushed txn's write ts is no longer enough to avoid
		// conflict because the read GUL is above that ts.
		{readTs: ts300, readGUL: ts500, toResolve: &toResolve{txnStatus: roachpb.PENDING, txnWriteTs: ts400}, exp: res{error: "uncertainty interval"}},
		{readTs: ts300, readGUL: ts500, toResolve: &toResolve{txnStatus: roachpb.STAGING, txnWriteTs: ts400}, exp: res{error: "uncertainty interval"}},
		{readTs: ts300, readGUL: ts500, toResolve: &toResolve{txnStatus: roachpb.COMMITTED, txnWriteTs: ts400}, exp: res{error: "uncertainty interval"}},
		{readTs: ts300, readGUL: ts500, toResolve: &toResolve{txnStatus: roachpb.ABORTED, txnWriteTs: ts400}, exp: res{value: "original"}},
		// To circumvent the uncertainty restart, we can pass in a clock observation
		// via the intents to resolve. Using a clock observation to set the intent's
		// local timestamp for non-pending transactions is not safe. See the comment
		// in pushLockTxn in lock_table_waiter.go.
		{readTs: ts300, readGUL: ts600, toResolve: &toResolve{txnStatus: roachpb.PENDING, txnWriteTs: ts500, obsTs: ts400, obsNode: 1}, exp: res{value: "original"}},
		// A clock observation from a different node doesn't help.
		{readTs: ts300, readGUL: ts600, toResolve: &toResolve{txnStatus: roachpb.PENDING, txnWriteTs: ts500, obsTs: ts400, obsNode: 2}, exp: res{error: "uncertainty interval"}},
	}

	for _, c := range testCases {
		// Build a mock guard, injecting any intents to resolve virtually.
		var intents []roachpb.LockUpdate
		if c.toResolve != nil {
			writeTxn.TxnMeta.WriteTimestamp = c.toResolve.txnWriteTs
			status := c.toResolve.txnStatus
			obs := roachpb.ObservedTimestamp{Timestamp: hlc.ClockTimestamp(c.toResolve.obsTs), NodeID: roachpb.NodeID(c.toResolve.obsNode)}
			intents = []roachpb.LockUpdate{{Span: roachpb.Span{Key: k}, Status: status, Txn: writeTxn.TxnMeta, ClockWhilePending: obs}}
		}
		g := mockGuard{
			req: concurrency.Request{
				LatchSpans: allSpans(),
				LockSpans:  lockspanset.New(),
			},
			intentsToResolveVirtually: intents,
		}
		// Construct the read-only request.
		ba := &kvpb.BatchRequest{}
		ba.Timestamp = c.readTs
		// If the read wants to set a GUL, use a transactional read.
		if !c.readGUL.IsEmpty() {
			readTxn := newTransaction("readTxn", k, 1, nil)
			readTxn.ReadTimestamp = c.readTs
			readTxn.GlobalUncertaintyLimit = c.readGUL
			// We need a fairly old observation here to make sure the read's local
			// uncertainty limit lets it read under the intent with a clock
			// observation while pending. This observation needs to be lower than the
			// local timestamp set on the intent (set using ClockWhilePending).
			// The txn's observed timestamp is usually set when the request is first
			// received by the store, so here we'll record it as the read's ts.
			readTxn.UpdateObservedTimestamp(roachpb.NodeID(1), c.readTs.UnsafeToClockTimestamp())
			ba.Txn = readTxn
		}
		gArgs := getArgs(k)
		ba.Add(&gArgs)
		br, _, _, pErr := tc.repl.executeReadOnlyBatch(ctx, ba, g, kvadmission.AdmissionInfo{})

		if c.exp.error == "" {
			require.NoError(t, pErr.GoError())
			require.Regexp(t, c.exp.value, string(br.Responses[0].GetGet().Value.RawBytes))
		} else {
			require.Nil(t, br)
			require.Regexp(t, c.exp.error, pErr.GoError())
		}
	}
}
