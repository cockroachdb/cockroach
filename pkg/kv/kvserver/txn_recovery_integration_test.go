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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestTxnRecoveryFromStaging tests the recovery process for a transaction that
// stages its transaction record immediately before its coordinator dies. It
// tests that concurrent transactions are able to recover from the indeterminate
// commit state after it becomes clear that the original transaction is no
// longer live. The test checks both the case where the parallel commit was
// successful and the case where the parallel commit failed.
func TestTxnRecoveryFromStaging(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
	} {
		t.Run(fmt.Sprintf("%d-commit:%t,writeTooOld:%t", i, tc.writeTooOld, tc.implicitCommit), func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			store, manual := createTestStore(t, testStoreOpts{createSystemRanges: true}, stopper)

			// Create a transaction that will get stuck performing a parallel commit.
			txn := newTransaction("txn", keyA, 1, store.Clock())

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
			// read on keyB to prevent the transaction's write to keyB from writing
			// at its desired timestamp. This prevents an implicit commit state.
			if !tc.implicitCommit {
				if !tc.writeTooOld {
					gArgs := getArgs(keyB)
					if _, pErr := kv.SendWrapped(ctx, store.TestSender(), &gArgs); pErr != nil {
						t.Fatal(pErr)
					}
				} else {
					pArgs = putArgs(keyB, []byte("pusher val"))
					if _, pErr := kv.SendWrapped(ctx, store.TestSender(), &pArgs); pErr != nil {
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

			// Pretend the transaction coordinator for the parallel commit died at this point.
			// Wait for longer than the TxnLivenessThreshold and then issue a read on one of
			// the keys that the transaction wrote. This will result in a transaction push and
			// eventually a full transaction recovery in order to resolve the indeterminate
			// commit.
			manual.Increment(txnwait.TxnLivenessThreshold.Nanoseconds() + 1)

			gArgs := getArgs(keyA)
			gReply, pErr := kv.SendWrapped(ctx, store.TestSender(), &gArgs)
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
