// Copyright 2016 The Cockroach Authors.
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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func beginTransaction(
	t *testing.T, store *Store, pri roachpb.UserPriority, key roachpb.Key, putKey bool,
) *roachpb.Transaction {
	txn := newTransaction("test", key, pri, store.Clock())
	if !putKey {
		return txn
	}

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: txn}
	put := putArgs(key, []byte("value"))
	ba.Add(&put)
	assignSeqNumsForReqs(txn, &put)
	br, pErr := store.TestSender().Send(context.Background(), ba)
	if pErr != nil {
		t.Fatal(pErr)
	}
	return br.Txn
}

// TestContendedIntentWithDependencyCycle verifies that a queue of
// writers on a contended key will still notice a dependency cycle.
// In this case, txn3 writes "a", then txn1 writes "b" and "a", then
// txn2 writes "b", then txn3 writes "b". The deadlock is broken by
// an aborted transaction.
//
// Additional non-transactional reads on the same contended key are
// inserted to verify they do not interfere with writing transactions
// always pushing to ensure the dependency cycle can be detected.
//
// This test is something of an integration test which exercises the
// IntentResolver as well as the concurrency Manager's lockTable and
// txnWaitQueue.
func TestContendedIntentWithDependencyCycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(t, testStoreOpts{createSystemRanges: true}, stopper)

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	spanA := roachpb.Span{Key: keyA}
	spanB := roachpb.Span{Key: keyB}

	// Create the three transactions; at this point, none of them have
	// conflicts. Txn1 has written "b", Txn3 has written "a".
	txn1 := beginTransaction(t, store, -3, keyB, true /* putKey */)
	txn2 := beginTransaction(t, store, -2, keyB, false /* putKey */)
	txn3 := beginTransaction(t, store, -1, keyA, true /* putKey */)

	// Send txn1 put, followed by an end transaction.
	txnCh1 := make(chan error, 1)
	go func() {
		put := putArgs(keyA, []byte("value"))
		assignSeqNumsForReqs(txn1, &put)
		if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn1}, &put); pErr != nil {
			txnCh1 <- pErr.GoError()
			return
		}

		et, h := endTxnArgs(txn1, true)
		et.LockSpans = []roachpb.Span{spanA, spanB}
		assignSeqNumsForReqs(txn1, &et)
		h.CanForwardReadTimestamp = true
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &et)
		txnCh1 <- pErr.GoError()
	}()

	// Send a non-transactional read to keyB. This adds an early waiter
	// to the intent resolver on keyB which txn2 must skip in order to
	// properly register itself as a dependency by pushing txn1.
	readCh1 := make(chan error, 1)
	go func() {
		get := getArgs(keyB)
		_, pErr := kv.SendWrapped(ctx, store.TestSender(), &get)
		readCh1 <- pErr.GoError()
	}()

	// Send txn2 put, followed by an end transaction.
	txnCh2 := make(chan error, 1)
	go func() {
		put := putArgs(keyB, []byte("value"))
		assignSeqNumsForReqs(txn2, &put)
		repl, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn2}, &put)
		if pErr != nil {
			txnCh2 <- pErr.GoError()
			return
		}

		txn2Copy := *repl.Header().Txn
		txn2 = &txn2Copy
		et, h := endTxnArgs(txn2, true)
		et.LockSpans = []roachpb.Span{spanB}
		assignSeqNumsForReqs(txn2, &et)
		h.CanForwardReadTimestamp = true
		_, pErr = kv.SendWrappedWith(ctx, store.TestSender(), h, &et)
		txnCh2 <- pErr.GoError()
	}()

	// Send another non-transactional read to keyB to add a waiter in
	// between txn2 and txn3. Txn3 must wait on txn2, instead of getting
	// queued behind this reader, in order to establish the dependency cycle.
	readCh2 := make(chan error, 1)
	go func() {
		get := getArgs(keyB)
		_, pErr := kv.SendWrapped(ctx, store.TestSender(), &get)
		readCh2 <- pErr.GoError()
	}()

	// Send txn3. Pause for 10ms to make it more likely that we have a
	// dependency cycle of length 3, although we don't mind testing
	// either way.
	time.Sleep(10 * time.Millisecond)
	txnCh3 := make(chan error, 1)
	go func() {
		put := putArgs(keyB, []byte("value"))
		assignSeqNumsForReqs(txn3, &put)
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn3}, &put)
		txnCh3 <- pErr.GoError()
	}()

	// The third transaction will always be aborted.
	err := <-txnCh3
	if !errors.HasType(err, (*roachpb.UnhandledRetryableError)(nil)) {
		t.Fatalf("expected transaction aborted error; got %T", err)
	}
	if err := <-txnCh1; err != nil {
		t.Fatal(err)
	}
	if err := <-txnCh2; err != nil {
		t.Fatal(err)
	}
	if err := <-readCh1; err != nil {
		t.Fatal(err)
	}
	if err := <-readCh2; err != nil {
		t.Fatal(err)
	}
}

// Regression test for https://github.com/cockroachdb/cockroach/issues/64092
// which makes sure that synchronous ranged intent resolution during rollback
// completes in a reasonable time.
func TestRollbackSyncRangedIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)
	skip.UnderStress(t)

	ctx := context.Background()
	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &StoreTestingKnobs{
				DisableLoadBasedSplitting: true,
				IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
					ForceSyncIntentResolution: true,
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	txn := srv.DB().NewTxn(ctx, "test")
	for i := 0; i < 100000; i++ {
		require.NoError(t, txn.Put(ctx, []byte(fmt.Sprintf("key%v", i)), []byte("value")))
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	require.NoError(t, txn.Rollback(ctx))
	require.NoError(t, ctx.Err())
}
