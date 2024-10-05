// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
		// futureWrites dictates whether the transaction has been writing at the
		// present time or whether it has been writing into the future.
		futureWrites bool
	}{
		{
			implicitCommit: true,
		},
		{
			implicitCommit: false,
		},
		{
			implicitCommit: true,
			futureWrites:   true,
		},
		{
			implicitCommit: false,
			futureWrites:   true,
		},
	} {
		name := fmt.Sprintf("%d-commit:%t,futureWrites:%t", i, tc.implicitCommit, tc.futureWrites)
		t.Run(name, func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			cfg := TestStoreConfig(hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123))))
			// Set the RecoverIndeterminateCommitsOnFailedPushes flag to true so
			// that a push on a STAGING transaction record immediately launches
			// the transaction recovery process.
			cfg.TestingKnobs.EvalKnobs.RecoverIndeterminateCommitsOnFailedPushes = true
			store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

			// Create a transaction that will get stuck performing a parallel commit.
			txn := newTransaction("txn", keyA, 1, store.Clock())

			// If the transaction is writing into the future, bump its write
			// timestamp. Also, bump its read timestamp to simulate a situation
			// where it has refreshed up to its write timestamp in preparation
			// to commit.
			if tc.futureWrites {
				txn.WriteTimestamp = txn.ReadTimestamp.Add(50, 0)
				txn.ReadTimestamp = txn.WriteTimestamp // simulate refresh
			}

			// Issue two writes, which will be considered in-flight at the time of
			// the transaction's EndTxn request.
			keyAVal := []byte("value")
			pArgs := putArgs(keyA, keyAVal)
			pArgs.Sequence = 1
			h := kvpb.Header{Txn: txn}
			if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &pArgs); pErr != nil {
				t.Fatal(pErr)
			}

			// If we don't want this transaction to commit successfully, perform a
			// conflicting operation on keyB to prevent the transaction's write to
			// keyB from writing at its desired timestamp. This prevents an implicit
			// commit state.
			conflictH := kvpb.Header{Timestamp: txn.WriteTimestamp.Next()}
			if !tc.implicitCommit {
				gArgs := getArgs(keyB)
				if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), conflictH, &gArgs); pErr != nil {
					t.Fatal(pErr)
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
				if val := gReply.(*kvpb.GetResponse).Value; val == nil {
					t.Fatalf("expected non-nil value when reading key %v", keyA)
				} else if valBytes, err := val.GetBytes(); err != nil {
					t.Fatal(err)
				} else if !bytes.Equal(valBytes, keyAVal) {
					t.Fatalf("actual value %q did not match expected value %q", valBytes, keyAVal)
				}
			} else {
				if val := gReply.(*kvpb.GetResponse).Value; val != nil {
					t.Fatalf("expected nil value when reading key %v; found %v", keyA, val)
				}
			}

			// Query the transaction and verify that it has the right status.
			qtArgs := queryTxnArgs(txn.TxnMeta, false /* waitForUpdate */)
			qtReply, pErr := kv.SendWrapped(ctx, store.TestSender(), &qtArgs)
			if pErr != nil {
				t.Fatal(pErr)
			}
			status := qtReply.(*kvpb.QueryTxnResponse).QueriedTxn.Status
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
//   - pushAbort: configures whether or not the high-priority operation is a
//     read (false) or a write (true), which dictates the kind of push
//     operation dispatched against the staging transaction.
//
//   - newEpoch: configures whether or not the staging transaction wrote the
//     intent which the high-priority operation conflicts with at a higher
//     epoch than it is staged at. If true, the staging transaction is not
//     implicitly committed.
//
//   - newTimestamp: configures whether or not the staging transaction wrote the
//     intent which the high-priority operation conflicts with at a higher
//     timestamp than it is staged at. If true, the staging transaction is not
//     implicitly committed.
func TestTxnRecoveryFromStagingWithHighPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	run := func(t *testing.T, pushAbort, newEpoch, newTimestamp bool) {
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
		cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
		store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

		// Create a transaction that will get stuck performing a parallel
		// commit.
		keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
		txn := newTransaction("txn", keyA, 1, store.Clock())

		// Issue two writes, which will be considered in-flight at the time of
		// the transaction's EndTxn request.
		keyAVal := []byte("value")
		pArgs := putArgs(keyA, keyAVal)
		pArgs.Sequence = 1
		h := kvpb.Header{Txn: txn}
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &pArgs)
		require.Nil(t, pErr, "error: %s", pErr)

		// The second write may or may not be bumped.
		pArgs = putArgs(keyB, []byte("value2"))
		pArgs.Sequence = 2
		h2 := kvpb.Header{Txn: txn.Clone()}
		if newEpoch {
			h2.Txn.BumpEpoch()
		}
		if newTimestamp {
			manual.Advance(100)
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
		var conflictArgs kvpb.Request
		if pushAbort {
			pArgs = putArgs(keyB, []byte("value3"))
			conflictArgs = &pArgs
		} else {
			gArgs := getArgs(keyB)
			conflictArgs = &gArgs
		}
		manual.Advance(100)
		conflictH := kvpb.Header{
			UserPriority: roachpb.MaxUserPriority,
			Timestamp:    store.Clock().Now(),
		}
		_, pErr = kv.SendWrappedWith(ctx, store.TestSender(), conflictH, conflictArgs)
		require.Nil(t, pErr, "error: %s", pErr)

		// Query the transaction and verify that it has the right state.
		qtArgs := queryTxnArgs(txn.TxnMeta, false /* waitForUpdate */)
		qtReply, pErr := kv.SendWrapped(ctx, store.TestSender(), &qtArgs)
		require.Nil(t, pErr, "error: %s", pErr)
		qtTxn := qtReply.(*kvpb.QueryTxnResponse).QueriedTxn

		if !newEpoch && !newTimestamp {
			// The transaction was implicitly committed at its initial epoch and
			// timestamp.
			require.Equal(t, roachpb.COMMITTED, qtTxn.Status)
			require.Equal(t, txn.Epoch, qtTxn.Epoch)
			require.Equal(t, txn.WriteTimestamp, qtTxn.WriteTimestamp)
		} else {
			// The pusher knows that the transaction is not implicitly committed, so
			// it is aborted.
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

// TestTxnRecoveryFromStagingWithoutHighPriority tests that the transaction
// recovery process is NOT initiated by a normal-priority operation which
// encounters a staging transaction. Instead, the normal-priority operation
// waits for the committing transaction to complete. The test contains a subtest
// for each of the combinations of the following options:
//
//   - pusheeIsoLevel: configures the isolation level of the pushee (committing)
//     transaction. Isolation levels affect the behavior of pushes of pending
//     transactions, but not of staging transactions.
//
//   - pusheeCommits: configures whether or not the staging transaction is
//     implicitly and, eventually, explicitly committed or not.
//
//   - pusherWriting: configures whether or not the conflicting operation is a
//     read (false) or a write (true), which dictates the kind of push operation
//     dispatched against the staging transaction.
func TestTxnRecoveryFromStagingWithoutHighPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	run := func(t *testing.T, pusheeIsoLevel isolation.Level, pusheeCommits, pusherWriting bool) {
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
		cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
		store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

		// Create a transaction that will get stuck performing a parallel
		// commit.
		keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
		txn := newTransaction("txn", keyA, 1, store.Clock())
		txn.IsoLevel = pusheeIsoLevel

		// Issue two writes, which will be considered in-flight at the time of
		// the transaction's EndTxn request.
		keyAVal := []byte("value")
		pArgs := putArgs(keyA, keyAVal)
		pArgs.Sequence = 1
		h := kvpb.Header{Txn: txn}
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &pArgs)
		require.Nil(t, pErr, "error: %s", pErr)

		pArgs = putArgs(keyB, []byte("value2"))
		pArgs.Sequence = 2
		h2 := kvpb.Header{Txn: txn.Clone()}
		if !pusheeCommits {
			// If we're not going to have the pushee commit, make sure it never enters
			// the implicit commit state by bumping the timestamp of one of its writes.
			manual.Advance(100)
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

		// Issue a conflicting, normal-priority operation.
		var conflictArgs kvpb.Request
		if pusherWriting {
			pArgs = putArgs(keyB, []byte("value3"))
			conflictArgs = &pArgs
		} else {
			gArgs := getArgs(keyB)
			conflictArgs = &gArgs
		}
		manual.Advance(100)
		pErrC := make(chan *kvpb.Error, 1)
		require.NoError(t, stopper.RunAsyncTask(ctx, "conflict", func(ctx context.Context) {
			_, pErr := kv.SendWrapped(ctx, store.TestSender(), conflictArgs)
			pErrC <- pErr
		}))

		// Wait for the conflict to push and be queued in the txn wait queue.
		testutils.SucceedsSoon(t, func() error {
			select {
			case pErr := <-pErrC:
				t.Fatalf("conflicting operation unexpectedly completed: pErr=%s", pErr)
			default:
			}
			if v := store.txnWaitMetrics.PusherWaiting.Value(); v != 1 {
				return errors.Errorf("expected 1 pusher waiting, found %d", v)
			}
			return nil
		})

		// Finalize the STAGING txn, either by committing it or by aborting it.
		et2, et2H := endTxnArgs(txn, pusheeCommits)
		etReply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), et2H, &et2)
		require.Nil(t, pErr, "error: %s", pErr)
		expStatus := roachpb.COMMITTED
		if !pusheeCommits {
			expStatus = roachpb.ABORTED
		}
		require.Equal(t, expStatus, etReply.Header().Txn.Status)

		// This will unblock the conflicting operation, which should succeed.
		pErr = <-pErrC
		require.Nil(t, pErr, "error: %s", pErr)
	}

	for _, pusheeIsoLevel := range isolation.Levels() {
		t.Run("pushee_iso_level="+pusheeIsoLevel.String(), func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "pushee_commits", func(t *testing.T, pusheeCommits bool) {
				testutils.RunTrueAndFalse(t, "pusher_writing", func(t *testing.T, pusherWriting bool) {
					run(t, pusheeIsoLevel, pusheeCommits, pusherWriting)
				})
			})
		})
	}
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
	store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

	// Set up a couple of keys to write, and a range to clear that covers
	// B and its intent.
	keyA, valueA := roachpb.Key("a"), []byte("value1")
	keyB, valueB := roachpb.Key("b"), []byte("value2")
	clearFrom, clearTo := roachpb.Key("aa"), roachpb.Key("x")

	// Create a transaction that will get stuck performing a parallel commit.
	txn := newTransaction("txn", keyA, 1, store.Clock())
	txnHeader := kvpb.Header{Txn: txn}

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
	reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{}, &queryIntent)
	require.Nil(t, pErr, "error: %s", pErr)
	require.True(t, reply.(*kvpb.QueryIntentResponse).FoundUnpushedIntent, "intent missing for %q", keyA)

	queryIntent = queryIntentArgs(keyB, txn.TxnMeta, false)
	reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{}, &queryIntent)
	require.Nil(t, pErr, "error: %s", pErr)
	require.True(t, reply.(*kvpb.QueryIntentResponse).FoundUnpushedIntent, "intent missing for %q", keyB)

	// Call ClearRange covering key B and its intent.
	clearRange := clearRangeArgs(clearFrom, clearTo)
	_, pErr = kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{}, &clearRange)
	require.Nil(t, pErr, "error: %s", pErr)

	// If separated intents are enabled, all should be well.
	// Reading A should succeed, but B should be gone.
	get := getArgs(keyA)
	reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{}, &get)
	require.Nil(t, pErr, "error: %s", pErr)
	require.NotNil(t, reply.(*kvpb.GetResponse).Value, "expected value for A")
	value, err := reply.(*kvpb.GetResponse).Value.GetBytes()
	require.NoError(t, err)
	require.Equal(t, value, valueA)

	get = getArgs(keyB)
	reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{}, &get)
	require.Nil(t, pErr, "error: %s", pErr)
	require.Nil(t, reply.(*kvpb.GetResponse).Value, "unexpected value for B")

	// Query the original transaction, which should now be committed.
	queryTxn := queryTxnArgs(txn.TxnMeta, false)
	reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{}, &queryTxn)
	require.Nil(t, pErr, "error: %s", pErr)
	require.Equal(t, roachpb.COMMITTED, reply.(*kvpb.QueryTxnResponse).QueriedTxn.Status)
}
