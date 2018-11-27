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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
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
	txn := newTransaction("test", key, pri, enginepb.SERIALIZABLE, store.Clock())

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
	store, _ := createTestStore(t, stopper)
	ctx, cancel := context.WithCancel(context.Background())

	key := roachpb.Key("a")
	span := roachpb.Span{Key: key}
	origTxn := beginTransaction(t, store, 1, key, true /* putKey */)

	roTxn1 := newTransaction("test", key, 1, enginepb.SERIALIZABLE, store.Clock())
	roTxn2 := newTransaction("test", key, 1, enginepb.SERIALIZABLE, store.Clock())
	roTxn3 := newTransaction("test", key, 1, enginepb.SERIALIZABLE, store.Clock())
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
	store, _ := createTestStore(t, stopper)
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
