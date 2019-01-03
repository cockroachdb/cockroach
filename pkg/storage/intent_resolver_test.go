// Copyright 2016 The Cockroach Authors.
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

package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
)

// TestPushTransactionsWithNonPendingIntent verifies that maybePushIntents
// returns an error when a non-pending intent is passed.
func TestPushTransactionsWithNonPendingIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	testCases := [][]roachpb.Intent{
		{{Span: roachpb.Span{Key: roachpb.Key("a")}, Status: roachpb.PENDING},
			{Span: roachpb.Span{Key: roachpb.Key("b")}, Status: roachpb.ABORTED}},
		{{Span: roachpb.Span{Key: roachpb.Key("a")}, Status: roachpb.PENDING},
			{Span: roachpb.Span{Key: roachpb.Key("b")}, Status: roachpb.COMMITTED}},
	}
	for _, intents := range testCases {
		if _, pErr := tc.store.intentResolver.maybePushIntents(
			context.Background(), intents, roachpb.Header{}, roachpb.PUSH_TOUCH, true,
		); !testutils.IsPError(pErr, "unexpected (ABORTED|COMMITTED) intent") {
			t.Errorf("expected error on aborted/resolved intent, but got %s", pErr)
		}
		if cnt := len(tc.store.intentResolver.mu.inFlightPushes); cnt != 0 {
			t.Errorf("expected no inflight pushes refcount map entries, found %d", cnt)
		}
	}
}

func beginTransaction(
	t *testing.T, store *Store, pri roachpb.UserPriority, key roachpb.Key, putKey bool,
) *roachpb.Transaction {
	txn := newTransaction("test", key, pri, store.Clock())

	var ba roachpb.BatchRequest
	bt, header := beginTxnArgs(key, txn)
	ba.Header = header
	ba.Add(&bt)
	assignSeqNumsForReqs(txn, &bt)
	if putKey {
		put := putArgs(key, []byte("value"))
		ba.Add(&put)
		assignSeqNumsForReqs(txn, &put)
	}
	br, pErr := store.TestSender().Send(context.Background(), ba)
	if pErr != nil {
		t.Fatal(pErr)
	}
	txn = br.Txn

	return txn
}

// TestContendedIntent verifies that multiple transactions, some actively
// writing and others read-only, are queued if processing write intent
// errors on a contended key. The test verifies the expected ordering in
// the queue.
func TestContendedIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store, _ := createTestStore(t, testStoreOpts{createSystemRanges: true}, stopper)
	ctx, cancel := context.WithCancel(context.Background())

	key := roachpb.Key("a")
	span := roachpb.Span{Key: key}
	origTxn := beginTransaction(t, store, 1, key, true /* putKey */)

	roTxn1 := newTransaction("test", key, 1, store.Clock())
	roTxn2 := newTransaction("test", key, 1, store.Clock())
	roTxn3 := newTransaction("test", key, 1, store.Clock())
	rwTxn1 := beginTransaction(t, store, 1, roachpb.Key("b"), true /* putKey */)
	rwTxn2 := beginTransaction(t, store, 1, roachpb.Key("c"), true /* putKey */)

	testCases := []struct {
		pusher  *roachpb.Transaction
		expTxns []*roachpb.Transaction
	}{
		// First establish a chain of three read-only txns.
		{pusher: roTxn1, expTxns: []*roachpb.Transaction{roTxn1}},
		{pusher: roTxn2, expTxns: []*roachpb.Transaction{roTxn1, roTxn2}},
		{pusher: roTxn3, expTxns: []*roachpb.Transaction{roTxn1, roTxn2, roTxn3}},
		// Now, verify that a writing txn is inserted at the end of the queue.
		{pusher: rwTxn1, expTxns: []*roachpb.Transaction{roTxn1, roTxn2, roTxn3, rwTxn1}},
		// And a second writing txn is inserted after it.
		{pusher: rwTxn2, expTxns: []*roachpb.Transaction{roTxn1, roTxn2, roTxn3, rwTxn1, rwTxn2}},
	}

	var wg sync.WaitGroup
	ir := store.intentResolver
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			wiErr := &roachpb.WriteIntentError{Intents: []roachpb.Intent{{Txn: origTxn.TxnMeta, Span: span}}}
			h := roachpb.Header{Txn: tc.pusher}
			wg.Add(1)
			go func() {
				_, pErr := ir.processWriteIntentError(ctx, roachpb.NewError(wiErr), nil, h, roachpb.PUSH_ABORT)
				if pErr != nil && !testutils.IsPError(pErr, "context canceled") {
					panic(pErr)
				}
				wg.Done()
			}()
			testutils.SucceedsSoon(t, func() error {
				ir.contentionQ.mu.Lock()
				defer ir.contentionQ.mu.Unlock()
				contended, ok := ir.contentionQ.mu.keys[string(key)]
				if !ok {
					return errors.Errorf("key not contended")
				}
				if lc, let := contended.ll.Len(), len(tc.expTxns); lc != let {
					return errors.Errorf("expected len %d; got %d", let, lc)
				}
				var idx int
				for e := contended.ll.Front(); e != nil; e = e.Next() {
					p := e.Value.(*pusher)
					if p.txn != tc.expTxns[idx] {
						return errors.Errorf("expected txn %s at index %d; got %s", tc.expTxns[idx], idx, p.txn)
					}
					idx++
				}
				return nil
			})
		})
	}

	// Free up all waiters to complete the test.
	cancel()
	wg.Wait()
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
func TestContendedIntentWithDependencyCycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store, _ := createTestStore(t, testStoreOpts{createSystemRanges: true}, stopper)
	ctx := context.Background()

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
		et.NoRefreshSpans = true
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
		et.NoRefreshSpans = true
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store, _ := createTestStore(t, testStoreOpts{createSystemRanges: true}, stopper)
	ctx := context.Background()

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

	fmt.Println(txn1.ID, txn2.ID, txn3.ID, txn4.ID, txn5.ID)

	txnCh1 := make(chan error, 1)
	txnCh3 := make(chan error, 1)
	txnCh4 := make(chan error, 1)
	txnCh5 := make(chan error, 1)

	// waitForContended waits until the provided key has the specified
	// number of pushers in the contentionQueue.
	waitForContended := func(key roachpb.Key, count int) {
		testutils.SucceedsSoon(t, func() error {
			ir := store.intentResolver
			ir.contentionQ.mu.Lock()
			defer ir.contentionQ.mu.Unlock()
			contended, ok := ir.contentionQ.mu.keys[string(key)]
			if !ok {
				return errors.Errorf("key not contended")
			}
			if lc := contended.ll.Len(); lc != count {
				return errors.Errorf("expected len %d; got %d", count, lc)
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
			et.NoRefreshSpans = true
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
			et.NoRefreshSpans = true
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
		et.NoRefreshSpans = true
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
			et.NoRefreshSpans = true
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
			repl, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn1}, &putB)
			if pErr != nil {
				txnCh1 <- pErr.GoError()
				return
			}
			txn1.Update(repl.Header().Txn)

			et, _ := endTxnArgs(txn1, true)
			et.IntentSpans = []roachpb.Span{spanB}
			et.NoRefreshSpans = true
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
