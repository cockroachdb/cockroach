// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
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
// writers on a contended key, each pushing the prior writer, will
// still notice a dependency cycle. In this case, txn3 writes "a",
// then txn1 writes "b" and "a", then txn2 writes "b", then txn3
// writes "b". The deadlock is broken by an aborted transaction.
//
// Additional non-transactional reads on the same contended key are
// inserted to verify they do not interfere with writing transactions
// always pushing to ensure the dependency cycle can be detected.
//
// This test is something of an integration test which exercises both
// the IntentResolver as well as the txnWaitQueue.
func TestContendedIntentWithDependencyCycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		if _, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn1}, &put); pErr != nil {
			txnCh1 <- pErr.GoError()
			return
		}
		et, _ := endTxnArgs(txn1, true)
		et.IntentSpans = []roachpb.Span{spanA, spanB}
		et.CanCommitAtHigherTimestamp = true
		assignSeqNumsForReqs(txn1, &et)
		_, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn1}, &et)
		txnCh1 <- pErr.GoError()
	}()

	// Send a non-transactional read to keyB. This adds an early waiter
	// to the intent resolver on keyB which txn2 must skip in order to
	// properly register itself as a dependency by pushing txn1.
	readCh1 := make(chan error, 1)
	go func() {
		get := getArgs(keyB)
		_, pErr := client.SendWrapped(ctx, store.TestSender(), &get)
		readCh1 <- pErr.GoError()
	}()

	// Send txn2 put, followed by an end transaction.
	txnCh2 := make(chan error, 1)
	go func() {
		put := putArgs(keyB, []byte("value"))
		assignSeqNumsForReqs(txn2, &put)
		repl, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn2}, &put)
		if pErr != nil {
			txnCh2 <- pErr.GoError()
			return
		}
		txn2Copy := *repl.Header().Txn
		txn2 = &txn2Copy
		et, _ := endTxnArgs(txn2, true)
		et.IntentSpans = []roachpb.Span{spanB}
		et.CanCommitAtHigherTimestamp = true
		assignSeqNumsForReqs(txn2, &et)
		_, pErr = client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn2}, &et)
		txnCh2 <- pErr.GoError()
	}()

	// Send another non-transactional read to keyB to add a waiter in
	// between txn2 and txn3. Txn3 must wait on txn2, instead of getting
	// queued behind this reader, in order to establish the dependency cycle.
	readCh2 := make(chan error, 1)
	go func() {
		get := getArgs(keyB)
		_, pErr := client.SendWrapped(ctx, store.TestSender(), &get)
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
		_, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn3}, &put)
		txnCh3 <- pErr.GoError()
	}()

	// The third transaction will always be aborted.
	err := <-txnCh3
	if _, ok := err.(*roachpb.UnhandledRetryableError); !ok {
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

// TestContendedIntentChangesOnRetry verifies that a batch which observes a
// WriteIntentError for one key and then a WriteIntentError for a different
// key doesn't pollute the old key's contentionQueue state.
//
// This also serves as a regression test for #32582. In that issue, we
// saw a transaction wait in the contentionQueue without pushing the
// transaction that it was deadlocked on. This was because of a bug in
// how the queue handled WriteIntentErrors for different intents on
// the re-evaluation of a batch.
//
// The scenario requires 5 unique transactions:
// 1.  txn1 writes to keyA.
// 2.  txn2 writes to keyB.
// 3.  txn4 writes to keyC and keyB in the same batch. The batch initially
//     fails with a WriteIntentError on keyA. It enters the contentionQueue
//     and becomes the front of a contendedKey list.
// 4.  txn5 writes to keyB. It enters the contentionQueue behind txn4.
// 5.  txn3 writes to keyC.
// 6.  txn2 is committed and the intent on keyB is resolved.
// 7.  txn4 exits the contentionQueue and re-evaluates. This time, it hits
//     a WriteIntentError on the first request in its batch: keyC. HOWEVER,
//     before it updates the contentionQueue, steps 8-10 occur.
// 8.  txn3 writes to keyB. It never enters the contentionQueue. txn3 then
//     writes to keyA in a separate batch and gets stuck waiting on txn1.
// 9.  txn2 writes to keyB. It observes txn3's intent and informs the
//     contentionQueue of the new txn upon its entrance.
// 10. txn5, the new front of the contendedKey list, pushes txn2 and gets
//     stuck in the txnWaitQueue.
// 11. txn4 finally updates the contentionQueue. A bug previously existed
//     where it would set the contendedKey's lastTxnMeta to nil because it
//     saw a WriteIntentError for a different key.
// 12. txn1 notices the nil lastTxnMeta and does not push txn2. This prevents
//     cycle detection from succeeding and we observe a deadlock.
//
func TestContendedIntentChangesOnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(t, testStoreOpts{createSystemRanges: true}, stopper)

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	keyC := roachpb.Key("c")
	keyD := roachpb.Key("d")
	spanA := roachpb.Span{Key: keyA}
	spanB := roachpb.Span{Key: keyB}
	spanC := roachpb.Span{Key: keyC}

	// Steps 1 and 2.
	//
	// Create the five transactions; at this point, none of them have
	// conflicts. Txn1 has written "a", Txn2 has written "b".
	txn1 := beginTransaction(t, store, -5, keyA, true /* putKey */)
	txn2 := beginTransaction(t, store, -4, keyB, true /* putKey */)
	txn3 := beginTransaction(t, store, -3, keyC, false /* putKey */)
	txn4 := beginTransaction(t, store, -5, keyD, false /* putKey */)
	txn5 := beginTransaction(t, store, -1, keyD, false /* putKey */)

	t.Log(txn1.ID, txn2.ID, txn3.ID, txn4.ID, txn5.ID)

	txnCh1 := make(chan error, 1)
	txnCh3 := make(chan error, 1)
	txnCh4 := make(chan error, 1)
	txnCh5 := make(chan error, 1)

	// waitForContended waits until the provided key has the specified
	// number of pushers in the contentionQueue.
	waitForContended := func(key roachpb.Key, count int) {
		testutils.SucceedsSoon(t, func() error {
			contentionCount := store.intentResolver.NumContended(key)
			if contentionCount != count {
				return errors.Errorf("expected len %d; got %d", count, contentionCount)
			}
			return nil
		})
	}

	// Steps 3, 7, and 11.
	//
	// Send txn4's puts in a single batch, followed by an end transaction.
	// This txn will hit a WriteIntentError on its second request during the
	// first time that it evaluates the batch and will hit a WriteIntentError
	// on its first request during the second time that it evaluates. This
	// second WriteIntentError must not be entangled with the contentionQueue
	// entry for the first key.
	{
		go func() {
			putC := putArgs(keyC, []byte("value")) // will hit intent on 2nd iter
			putB := putArgs(keyB, []byte("value")) // will hit intent on 1st iter
			assignSeqNumsForReqs(txn4, &putC)
			assignSeqNumsForReqs(txn4, &putB)
			ba := roachpb.BatchRequest{}
			ba.Header = roachpb.Header{Txn: txn4}
			ba.Add(&putC, &putB)
			br, pErr := store.TestSender().Send(ctx, ba)
			if pErr != nil {
				txnCh4 <- pErr.GoError()
				return
			}
			txn4.Update(br.Txn)

			et, _ := endTxnArgs(txn4, true)
			et.IntentSpans = []roachpb.Span{spanB, spanC}
			et.CanCommitAtHigherTimestamp = true
			assignSeqNumsForReqs(txn4, &et)
			_, pErr = client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn4}, &et)
			txnCh4 <- pErr.GoError()
		}()

		waitForContended(keyB, 1)
		t.Log("txn4 in contentionQueue")
	}

	// Steps 4 and 10.
	//
	// Send txn5's put, followed by an end transaction. This request will
	// wait at the head of the contention queue once txn2 is committed.
	{
		go func() {
			// Write keyB to create a cycle with txn3.
			putB := putArgs(keyB, []byte("value"))
			assignSeqNumsForReqs(txn5, &putB)
			repl, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn5}, &putB)
			if pErr != nil {
				txnCh5 <- pErr.GoError()
				return
			}
			txn5.Update(repl.Header().Txn)

			et, _ := endTxnArgs(txn5, true)
			et.IntentSpans = []roachpb.Span{spanB}
			et.CanCommitAtHigherTimestamp = true
			assignSeqNumsForReqs(txn5, &et)
			_, pErr = client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn5}, &et)
			txnCh5 <- pErr.GoError()
		}()

		waitForContended(keyB, 2)
		t.Log("txn5 in contentionQueue")
	}

	// Step 5.
	//
	// Write to keyC, which will block txn4 on its re-evaluation.
	{
		putC := putArgs(keyC, []byte("value"))
		assignSeqNumsForReqs(txn3, &putC)
		if _, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn3}, &putC); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Step 6.
	//
	// Commit txn2, which should set off a chain reaction of movement. txn3 should
	// write an intent at keyB and go on to write at keyA. In doing so, it should
	// create a cycle with txn1. This cycle should be detected.
	//
	// In #32582 we saw a case where txn4 would hit a different WriteIntentError
	// during its re-evaluation (keyC instead of keyB). This caused it to remove
	// the lastTxnMeta from keyB's contendedKey record, which prevented txn1 from
	// pushing txn3 and in turn prevented cycle detection from finding the deadlock.
	{
		// Sleeping for dependencyCyclePushDelay before committing txn2 makes the
		// failure reproduce more easily.
		time.Sleep(100 * time.Millisecond)

		et, _ := endTxnArgs(txn2, true)
		et.IntentSpans = []roachpb.Span{spanB}
		et.CanCommitAtHigherTimestamp = true
		assignSeqNumsForReqs(txn2, &et)
		if _, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn2}, &et); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Step 8.
	//
	// Send txn3's two other put requests, followed by an end transaction. This
	// txn will first write to keyB before writing to keyA. This will create a
	// cycle between txn1 and txn3.
	{
		// Write to keyB, which will hit an intent from txn2.
		putB := putArgs(keyB, []byte("value"))
		assignSeqNumsForReqs(txn3, &putB)
		repl, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn3}, &putB)
		if pErr != nil {
			txnCh3 <- pErr.GoError()
			return
		}
		txn3.Update(repl.Header().Txn)

		go func() {
			// Write keyA, which will hit an intent from txn1 and create a cycle.
			putA := putArgs(keyA, []byte("value"))
			assignSeqNumsForReqs(txn3, &putA)
			repl, pErr = client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn3}, &putA)
			if pErr != nil {
				txnCh3 <- pErr.GoError()
				return
			}
			txn3.Update(repl.Header().Txn)

			et, _ := endTxnArgs(txn3, true)
			et.IntentSpans = []roachpb.Span{spanA, spanB}
			et.CanCommitAtHigherTimestamp = true
			assignSeqNumsForReqs(txn3, &et)
			_, pErr = client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn3}, &et)
			txnCh3 <- pErr.GoError()
		}()
	}

	// Step 9.
	//
	// Send txn1's put request to keyB, which completes the cycle between txn1
	// and txn3.
	{
		go func() {
			// Write keyB to create a cycle with txn3.
			putB := putArgs(keyB, []byte("value"))
			assignSeqNumsForReqs(txn1, &putB)
			repl, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
				Txn: txn1,
			}, &putB)
			if pErr != nil {
				txnCh1 <- pErr.GoError()
				return
			}
			txn1.Update(repl.Header().Txn)

			et, _ := endTxnArgs(txn1, true)
			et.IntentSpans = []roachpb.Span{spanB}
			et.CanCommitAtHigherTimestamp = true
			assignSeqNumsForReqs(txn1, &et)
			_, pErr = client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn1}, &et)
			txnCh1 <- pErr.GoError()
		}()
	}

	// The third transaction will always be aborted because it has
	// a lower priority than the first.
	err := <-txnCh3
	if _, ok := err.(*roachpb.UnhandledRetryableError); !ok {
		t.Fatalf("expected transaction aborted error; got %T", err)
	}
	if err := <-txnCh1; err != nil {
		t.Fatal(err)
	}
	if err := <-txnCh4; err != nil {
		// txn4 can end up being aborted due to a perceived deadlock. This
		// is rare and isn't important to the test, so we allow it.
		if _, ok := err.(*roachpb.UnhandledRetryableError); !ok {
			t.Fatal(err)
		}
	}
	if err := <-txnCh5; err != nil {
		// txn5 can end up being aborted due to a perceived deadlock. This
		// is rare and isn't important to the test, so we allow it.
		if _, ok := err.(*roachpb.UnhandledRetryableError); !ok {
			t.Fatal(err)
		}
	}
}

// TestContendedIntentPushedByHighPriorityScan verifies that a queue of readers
// and writers for a contended key will not prevent a high priority scan from
// pushing the head of the queue and reading a committed value.
func TestContendedIntentPushedByHighPriorityScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(t, testStoreOpts{createSystemRanges: true}, stopper)

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	spanA := roachpb.Span{Key: keyA}
	spanB := roachpb.Span{Key: keyB}

	txn1 := beginTransaction(t, store, -3, keyA, true /* putKey */)
	txn2 := beginTransaction(t, store, -2, keyB, true /* putKey */)

	// txn1 already wrote an intent at keyA.
	_ = txn1

	// Send txn2 put, followed by an end transaction. This should add
	// txn2 to the contentionQueue, blocking on the result of txn1.
	txnCh2 := make(chan error, 1)
	go func() {
		put := putArgs(keyA, []byte("value"))
		assignSeqNumsForReqs(txn2, &put)
		if _, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
			Txn: txn2,
		}, &put); pErr != nil {
			txnCh2 <- pErr.GoError()
			return
		}
		et, _ := endTxnArgs(txn2, true)
		et.IntentSpans = []roachpb.Span{spanA, spanB}
		et.CanCommitAtHigherTimestamp = true
		assignSeqNumsForReqs(txn2, &et)
		_, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn2}, &et)
		txnCh2 <- pErr.GoError()
	}()

	// Wait for txn2 to enter the contentionQueue and begin pushing txn1.
	testutils.SucceedsSoon(t, func() error {
		contentionCount := store.intentResolver.NumContended(keyA)
		if exp := 1; contentionCount != exp {
			return errors.Errorf("expected len %d; got %d", exp, contentionCount)
		}
		return nil
	})

	// Perform a scan over the same keyspace with a high priority transaction.
	txnHigh := newTransaction("high-priority", nil, roachpb.MaxUserPriority, store.Clock())
	scan := scanArgs(keyA, keyB)
	scanResp, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txnHigh}, scan)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if kvs := scanResp.(*roachpb.ScanResponse).Rows; len(kvs) > 0 {
		t.Fatalf("expected kvs returned from scan %v", kvs)
	}

	// Commit txn1. This succeeds even though the txn was pushed because the
	// transaction has no refresh spans.
	et, _ := endTxnArgs(txn1, true)
	et.IntentSpans = []roachpb.Span{spanA}
	et.CanCommitAtHigherTimestamp = true
	assignSeqNumsForReqs(txn1, &et)
	if _, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn1}, &et); pErr != nil {
		t.Fatal(pErr)
	}

	// Wait for txn2 to commit. Again, this succeeds even though the txn was
	// pushed because the transaction has no refresh spans.
	if err := <-txnCh2; err != nil {
		t.Fatal(err)
	}
}
