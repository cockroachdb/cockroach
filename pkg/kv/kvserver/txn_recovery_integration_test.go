// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// TestTxnRecoveryFromStaging tests the recovery process for a transaction that
// stages its transaction record immediately before its coordinator dies. It
// tests that concurrent transactions are able to recover from the indeterminate
// commit state after it becomes clear that the original transaction is no
// longer live. The test checks both the case where the parallel commit was
// successful and the case where the parallel commit failed.
func TestTxnRecoveryFromStaging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	for i, tc := range []struct {
		// implicitCommit says whether we expect the transaction to satisfy the
		// implicit-commit condition.
		implicitCommit bool
		// If implicitCommit is false, writeTooOld dictates what kind of push will
		// be experienced by one of the txn's intents. An intent being pushed is the
		// reason why the implicit-commit condition is expected to fail. We simulate
		// both pushes by the timestamp cache, and by deferred write-too-old
		// conditions.
		writeTooOld bool
		// futureWrites dictates whether the transaction has been writing at the
		// present time or whether it has been writing into the future with a
		// synthetic timestamp.
		futureWrites bool
	}{
		{
			implicitCommit: true,
		},
		{
			implicitCommit: false,
			writeTooOld:    false,
		},
		{
			implicitCommit: false,
			writeTooOld:    true,
		},
		{
			implicitCommit: true,
			futureWrites:   true,
		},
		{
			implicitCommit: false,
			writeTooOld:    false,
			futureWrites:   true,
		},
		{
			implicitCommit: false,
			writeTooOld:    true,
			futureWrites:   true,
		},
	} {
		name := fmt.Sprintf("%d-commit:%t,writeTooOld:%t,futureWrites:%t", i, tc.implicitCommit, tc.writeTooOld, tc.futureWrites)
		t.Run(name, func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			manual := hlc.NewManualClock(123)
			cfg := TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
			// Set the RecoverIndeterminateCommitsOnFailedPushes flag to true so
			// that a push on a STAGING transaction record immediately launches
			// the transaction recovery process.
			cfg.TestingKnobs.EvalKnobs.RecoverIndeterminateCommitsOnFailedPushes = true
			store := createTestStoreWithConfig(t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

			// Create a transaction that will get stuck performing a parallel commit.
			txn := newTransaction("txn", keyA, 1, store.Clock())

			// If the transaction is writing into the future, bump its write
			// timestamp. Also, bump its read timestamp to simulate a situation
			// where it has refreshed up to its write timestamp in preparation
			// to commit.
			if tc.futureWrites {
				txn.WriteTimestamp = txn.ReadTimestamp.Add(50, 0).WithSynthetic(true)
				txn.ReadTimestamp = txn.WriteTimestamp // simulate refresh
			}

			// Issue two writes, which will be considered in-flight at the time of
			// the transaction's EndTxn request.
			keyAVal := []byte("value")
			pArgs := putArgs(keyA, keyAVal)
			pArgs.Sequence = 1
			h := roachpb.Header{Txn: txn}
			if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &pArgs); pErr != nil {
				t.Fatal(pErr)
			}

			// If we don't want this transaction to commit successfully, perform a
			// conflicting operation on keyB to prevent the transaction's write to
			// keyB from writing at its desired timestamp. This prevents an implicit
			// commit state.
			conflictH := roachpb.Header{Timestamp: txn.WriteTimestamp.Next()}
			if !tc.implicitCommit {
				if !tc.writeTooOld {
					gArgs := getArgs(keyB)
					if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), conflictH, &gArgs); pErr != nil {
						t.Fatal(pErr)
					}
				} else {
					pArgs = putArgs(keyB, []byte("pusher val"))
					if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), conflictH, &pArgs); pErr != nil {
						t.Fatal(pErr)
					}
				}
			}

			pArgs = putArgs(keyB, []byte("value2"))
			pArgs.Sequence = 2
			if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &pArgs); pErr != nil {
				t.Fatal(pErr)
			}

			// Issue a parallel commit, which will put the transaction into a
			// STAGING state. Include both writes as the EndTxn's in-flight writes.
			et, etH := endTxnArgs(txn, true)
			et.InFlightWrites = []roachpb.SequencedWrite{
				{Key: keyA, Sequence: 1},
				{Key: keyB, Sequence: 2},
			}
			etReply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), etH, &et)
			if pErr != nil {
				t.Fatal(pErr)
			}
			if replyTxn := etReply.Header().Txn; replyTxn.Status != roachpb.STAGING {
				t.Fatalf("expected STAGING txn, found %v", replyTxn)
			}

			// Pretend the transaction coordinator for the parallel commit died at this
			// point. Typically, we would have to wait out the TxnLivenessThreshold. But
			// since we set RecoverIndeterminateCommitsOnFailedPushes, we don't need to
			// wait. So issue a read on one of the keys that the transaction wrote. This
			// will result in a transaction push and eventually a full transaction
			// recovery in order to resolve the indeterminate commit.

			gArgs := getArgs(keyA)
			gReply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), conflictH, &gArgs)
			if pErr != nil {
				t.Fatal(pErr)
			}
			if tc.implicitCommit {
				if val := gReply.(*roachpb.GetResponse).Value; val == nil {
					t.Fatalf("expected non-nil value when reading key %v", keyA)
				} else if valBytes, err := val.GetBytes(); err != nil {
					t.Fatal(err)
				} else if !bytes.Equal(valBytes, keyAVal) {
					t.Fatalf("actual value %q did not match expected value %q", valBytes, keyAVal)
				}
			} else {
				if val := gReply.(*roachpb.GetResponse).Value; val != nil {
					t.Fatalf("expected nil value when reading key %v; found %v", keyA, val)
				}
			}

			// Query the transaction and verify that it has the right status.
			qtArgs := queryTxnArgs(txn.TxnMeta, false /* waitForUpdate */)
			qtReply, pErr := kv.SendWrapped(ctx, store.TestSender(), &qtArgs)
			if pErr != nil {
				t.Fatal(pErr)
			}
			status := qtReply.(*roachpb.QueryTxnResponse).QueriedTxn.Status
			expStatus := roachpb.ABORTED
			if tc.implicitCommit {
				expStatus = roachpb.COMMITTED
			}
			if status != expStatus {
				t.Fatalf("expected transaction status %v; found %v", expStatus, status)
			}
		})
	}
}

// TestTxnRecoveryFromStagingWithHighPriority tests the transaction recovery
// process initiated by a high-priority operation which encounters a staging
// transaction. The test contains a subtest for each of the combinations of the
// following boolean options:
//
// - pushAbort: configures whether or not the high-priority operation is a
//     read (false) or a write (true), which dictates the kind of push
//     operation dispatched against the staging transaction.
//
// - newEpoch: configures whether or not the staging transaction wrote the
//     intent which the high-priority operation conflicts with at a higher
//     epoch than it is staged at. If true, the staging transaction is not
//     implicitly committed.
//
// - newTimestamp: configures whether or not the staging transaction wrote the
//     intent which the high-priority operation conflicts with at a higher
//     timestamp than it is staged at. If true, the staging transaction is not
//     implicitly committed.
//
func TestTxnRecoveryFromStagingWithHighPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	run := func(t *testing.T, pushAbort, newEpoch, newTimestamp bool) {
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		manual := hlc.NewManualClock(123)
		cfg := TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
		store := createTestStoreWithConfig(t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

		// Create a transaction that will get stuck performing a parallel
		// commit.
		keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
		txn := newTransaction("txn", keyA, 1, store.Clock())

		// Issue two writes, which will be considered in-flight at the time of
		// the transaction's EndTxn request.
		keyAVal := []byte("value")
		pArgs := putArgs(keyA, keyAVal)
		pArgs.Sequence = 1
		h := roachpb.Header{Txn: txn}
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &pArgs)
		require.Nil(t, pErr, "error: %s", pErr)

		// The second write may or may not be bumped.
		pArgs = putArgs(keyB, []byte("value2"))
		pArgs.Sequence = 2
		h2 := roachpb.Header{Txn: txn.Clone()}
		if newEpoch {
			h2.Txn.BumpEpoch()
		}
		if newTimestamp {
			manual.Increment(100)
			h2.Txn.WriteTimestamp = store.Clock().Now()
		}
		_, pErr = kv.SendWrappedWith(ctx, store.TestSender(), h2, &pArgs)
		require.Nil(t, pErr, "error: %s", pErr)

		// Issue a parallel commit, which will put the transaction into a
		// STAGING state. Include both writes as the EndTxn's in-flight writes.
		et, etH := endTxnArgs(txn, true)
		et.InFlightWrites = []roachpb.SequencedWrite{
			{Key: keyA, Sequence: 1},
			{Key: keyB, Sequence: 2},
		}
		etReply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), etH, &et)
		require.Nil(t, pErr, "error: %s", pErr)
		require.Equal(t, roachpb.STAGING, etReply.Header().Txn.Status)

		// Issue a conflicting, high-priority operation.
		var conflictArgs roachpb.Request
		if pushAbort {
			pArgs = putArgs(keyB, []byte("value3"))
			conflictArgs = &pArgs
		} else {
			gArgs := getArgs(keyB)
			conflictArgs = &gArgs
		}
		manual.Increment(100)
		conflictH := roachpb.Header{
			UserPriority: roachpb.MaxUserPriority,
			Timestamp:    store.Clock().Now(),
		}
		_, pErr = kv.SendWrappedWith(ctx, store.TestSender(), conflictH, conflictArgs)
		require.Nil(t, pErr, "error: %s", pErr)

		// Query the transaction and verify that it has the right state.
		qtArgs := queryTxnArgs(txn.TxnMeta, false /* waitForUpdate */)
		qtReply, pErr := kv.SendWrapped(ctx, store.TestSender(), &qtArgs)
		require.Nil(t, pErr, "error: %s", pErr)
		qtTxn := qtReply.(*roachpb.QueryTxnResponse).QueriedTxn

		if !newEpoch && !newTimestamp {
			// The transaction was implicitly committed at its initial epoch and
			// timestamp.
			require.Equal(t, roachpb.COMMITTED, qtTxn.Status)
			require.Equal(t, txn.Epoch, qtTxn.Epoch)
			require.Equal(t, txn.WriteTimestamp, qtTxn.WriteTimestamp)
		} else if newEpoch {
			// The transaction is aborted if that's what the high-priority
			// request wants. Otherwise, the transaction's record is bumped to
			// the new epoch pulled from its intent and pushed above the
			// high-priority request's timestamp.
			if pushAbort {
				require.Equal(t, roachpb.ABORTED, qtTxn.Status)
			} else /* pushTimestamp */ {
				require.Equal(t, roachpb.PENDING, qtTxn.Status)
				require.Equal(t, txn.Epoch+1, qtTxn.Epoch)
				require.Equal(t, conflictH.Timestamp.Next(), qtTxn.WriteTimestamp)
			}
		} else /* if newTimestamp */ {
			// The transaction is aborted, even if the high-priority request
			// only needed it to be pushed to a higher timestamp. This is
			// because we don't allow a STAGING transaction record to move back
			// to PENDING in the same epoch.
			require.Equal(t, roachpb.ABORTED, qtTxn.Status)
		}
	}

	testutils.RunTrueAndFalse(t, "push_abort", func(t *testing.T, pushAbort bool) {
		testutils.RunTrueAndFalse(t, "new_epoch", func(t *testing.T, newEpoch bool) {
			testutils.RunTrueAndFalse(t, "new_timestamp", func(t *testing.T, newTimestamp bool) {
				run(t, pushAbort, newEpoch, newTimestamp)
			})
		})
	})
}

// TestTxnClearRangeIntents tests whether a ClearRange call blindly removes
// write intents. This can cause it to remove an intent from an implicitly
// committed STAGING txn. When txn recovery kicks in, it will fail to find the
// expected intent, causing it to roll back a committed txn (including any
// values outside of the cleared range).
//
// Because the fix for this relies on separated intents, the bug will continue
// to be present until the planned migration in 21.2. Since tests currently
// enable separated intents at random, we assert the buggy behavior when these
// are disabled. See also: https://github.com/cockroachdb/cockroach/issues/46764
func TestTxnClearRangeIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(nil)
	// Immediately launch transaction recovery when pushing a STAGING txn.
	cfg.TestingKnobs.EvalKnobs.RecoverIndeterminateCommitsOnFailedPushes = true
	store := createTestStoreWithConfig(t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

	// Set up a couple of keys to write, and a range to clear that covers
	// B and its intent.
	keyA, valueA := roachpb.Key("a"), []byte("value1")
	keyB, valueB := roachpb.Key("b"), []byte("value2")
	clearFrom, clearTo := roachpb.Key("aa"), roachpb.Key("x")

	// Create a transaction that will get stuck performing a parallel commit.
	txn := newTransaction("txn", keyA, 1, store.Clock())
	txnHeader := roachpb.Header{Txn: txn}

	// Issue two writes, which will be considered in-flight at the time of the
	// transaction's EndTxn request.
	put := putArgs(keyA, valueA)
	put.Sequence = 1
	_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), txnHeader, &put)
	require.Nil(t, pErr, "error: %s", pErr)

	put = putArgs(keyB, valueB)
	put.Sequence = 2
	_, pErr = kv.SendWrappedWith(ctx, store.TestSender(), txnHeader, &put)
	require.Nil(t, pErr, "error: %s", pErr)

	// Issue a parallel commit, which will put the transaction into a STAGING
	// state. Include both writes as the EndTxn's in-flight writes.
	endTxn, endTxnHeader := endTxnArgs(txn, true)
	endTxn.InFlightWrites = []roachpb.SequencedWrite{
		{Key: keyA, Sequence: 1},
		{Key: keyB, Sequence: 2},
	}
	reply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), endTxnHeader, &endTxn)
	require.Nil(t, pErr, pErr)
	require.Equal(t, roachpb.STAGING, reply.Header().Txn.Status, "expected STAGING txn")

	// Make sure intents exists for keys A and B.
	queryIntent := queryIntentArgs(keyA, txn.TxnMeta, false)
	reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{}, &queryIntent)
	require.Nil(t, pErr, "error: %s", pErr)
	require.True(t, reply.(*roachpb.QueryIntentResponse).FoundIntent, "intent missing for %q", keyA)

	queryIntent = queryIntentArgs(keyB, txn.TxnMeta, false)
	reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{}, &queryIntent)
	require.Nil(t, pErr, "error: %s", pErr)
	require.True(t, reply.(*roachpb.QueryIntentResponse).FoundIntent, "intent missing for %q", keyB)

	// Call ClearRange covering key B and its intent.
	clearRange := clearRangeArgs(clearFrom, clearTo)
	_, pErr = kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{}, &clearRange)
	require.Nil(t, pErr, "error: %s", pErr)

	// If separated intents are enabled, all should be well.
	if store.engine.IsSeparatedIntentsEnabledForTesting(ctx) {
		// Reading A should succeed, but B should be gone.
		get := getArgs(keyA)
		reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{}, &get)
		require.Nil(t, pErr, "error: %s", pErr)
		require.NotNil(t, reply.(*roachpb.GetResponse).Value, "expected value for A")
		value, err := reply.(*roachpb.GetResponse).Value.GetBytes()
		require.NoError(t, err)
		require.Equal(t, value, valueA)

		get = getArgs(keyB)
		reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{}, &get)
		require.Nil(t, pErr, "error: %s", pErr)
		require.Nil(t, reply.(*roachpb.GetResponse).Value, "unexpected value for B")

		// Query the original transaction, which should now be committed.
		queryTxn := queryTxnArgs(txn.TxnMeta, false)
		reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{}, &queryTxn)
		require.Nil(t, pErr, "error: %s", pErr)
		require.Equal(t, roachpb.COMMITTED, reply.(*roachpb.QueryTxnResponse).QueriedTxn.Status)

	} else {
		// If separated intents are disabled, ClearRange will have removed B's
		// intent without resolving it. When we read A, txn recovery will expect
		// to find B's intent, but when missing it assumes the txn did not
		// complete and aborts it, rolling back all writes (including A).
		get := getArgs(keyA)
		reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{}, &get)
		require.Nil(t, pErr, "error: %s", pErr)
		require.Nil(t, reply.(*roachpb.GetResponse).Value, "unexpected value for A")

		get = getArgs(keyB)
		reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{}, &get)
		require.Nil(t, pErr, "error: %s", pErr)
		require.Nil(t, reply.(*roachpb.GetResponse).Value, "unexpected value for B")

		// Query the original transaction, which should now be aborted.
		queryTxn := queryTxnArgs(txn.TxnMeta, false)
		reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{}, &queryTxn)
		require.Nil(t, pErr, "error: %s", pErr)
		require.Equal(t, roachpb.ABORTED, reply.(*roachpb.QueryTxnResponse).QueriedTxn.Status)
	}
}
