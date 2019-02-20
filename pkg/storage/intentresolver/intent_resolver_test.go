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

package intentresolver

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

// TestPushTransactionsWithNonPendingIntent verifies that maybePushIntents
// returns an error when a non-pending intent is passed.
func TestPushTransactionsWithNonPendingIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	db := client.NewDB(log.AmbientContext{
		Tracer: tracing.NewTracer(),
	}, client.NonTransactionalFactoryFunc(func(context.Context, roachpb.BatchRequest) (
		*roachpb.BatchResponse, *roachpb.Error) {
		return nil, nil
	}), clock)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	ir := New(Config{
		Stopper: stopper,
		DB:      db,
		Clock:   clock,
	})

	testCases := [][]roachpb.Intent{
		{{Span: roachpb.Span{Key: roachpb.Key("a")}, Status: roachpb.PENDING},
			{Span: roachpb.Span{Key: roachpb.Key("b")}, Status: roachpb.ABORTED}},
		{{Span: roachpb.Span{Key: roachpb.Key("a")}, Status: roachpb.PENDING},
			{Span: roachpb.Span{Key: roachpb.Key("b")}, Status: roachpb.COMMITTED}},
	}
	for _, intents := range testCases {
		if _, pErr := ir.maybePushIntents(
			context.Background(), intents, roachpb.Header{}, roachpb.PUSH_TOUCH, true,
		); !testutils.IsPError(pErr, "unexpected (ABORTED|COMMITTED) intent") {
			t.Errorf("expected error on aborted/resolved intent, but got %s", pErr)
		}
		if cnt := len(ir.mu.inFlightPushes); cnt != 0 {
			t.Errorf("expected no inflight pushes refcount map entries, found %d", cnt)
		}
	}
}

// TestCleanupTxnIntentsOnGCAsync exercises the code which is used to
// asynchronously clean up transaction intents and then transaction records.
// This method is invoked from the storage GC queue.
func TestCleanupTxnIntentsOnGCAsync(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	cfg := Config{
		Stopper: stopper,
		Clock:   clock,
	}
	type testCase struct {
		txn           *roachpb.Transaction
		intents       []roachpb.Intent
		sendFuncs     []sendFunc
		expectPushed  bool
		expectSucceed bool
	}

	// This test creates 3 transaction for use in the below test cases.
	// A new intent resolver is created for each test case so they operate
	// completely independently.
	key := roachpb.Key("a")
	// Txn0 is in the pending state and is not old enough to have expired to the
	// code ought to send nothing.
	txn0 := beginTransaction(t, clock, 1, key, true /* putKey */)
	// Txn1 is in the pending state but is expired.
	txn1 := beginTransaction(t, clock, 1, key, true /* putKey */)
	txn1.OrigTimestamp.WallTime -= int64(100 * time.Second)
	txn1.LastHeartbeat = txn1.OrigTimestamp
	// Txn2 is in the Committed state
	txn2 := beginTransaction(t, clock, 1, key, true /* putKey */)
	txn2.Status = roachpb.COMMITTED
	cases := []*testCase{
		// This one has an unexpired pending transaction so it's skipped
		{txn: txn0},
		// Txn1 is pending and expired so the code should attempt to push the txn.
		// The provided sender will fail on the first request. The callback should
		// indicate that the transaction was pushed but that the resolution was not
		// successful.
		{
			txn:          txn1,
			sendFuncs:    []sendFunc{failSendFunc},
			expectPushed: true,
		},
		// Txn1 is pending and expired so the code should attempt to push the txn
		// and then work to resolve its intents. The intent resolution happens in
		// different requests for individual keys and then for spans. This case will
		// allow the individual key intent to be resolved completely but will fail
		// to resolve the span. The callback should indicate that the transaction
		// has been pushed but that the garbage collection was not successful.
		{
			txn: txn1,
			intents: []roachpb.Intent{
				{
					Span: roachpb.Span{Key: key},
					Txn:  txn1.TxnMeta,
				},
				{
					Span: roachpb.Span{Key: key, EndKey: roachpb.Key("b")},
					Txn:  txn1.TxnMeta,
				},
			},
			sendFuncs: []sendFunc{
				singlePushTxnSendFunc,
				resolveIntentsSendFunc,
				failSendFunc,
			},
			expectPushed: true,
		},
		// Txn1 is pending and expired so the code should attempt to push the txn
		// and then work to resolve its intents. The intent resolution happens in
		// different requests for individual keys and then for spans. This case will
		// allow the individual key intents to be resolved in one request and for
		// the span request to be resolved in another. Finally it will succeed on
		// the GCRequest. This is a positive case and the callback should indicate
		// that the txn has both been pushed and successfully resolved.
		{
			txn: txn1,
			intents: []roachpb.Intent{
				{Span: roachpb.Span{Key: key}, Txn: txn1.TxnMeta},
				{Span: roachpb.Span{Key: roachpb.Key("aa")}, Txn: txn1.TxnMeta},
				{Span: roachpb.Span{Key: key, EndKey: roachpb.Key("b")}, Txn: txn1.TxnMeta},
			},
			sendFuncs: []sendFunc{
				singlePushTxnSendFunc,
				resolveIntentsSendFunc,
				resolveIntentsSendFunc,
				gcSendFunc,
			},
			expectPushed:  true,
			expectSucceed: true,
		},
		// Txn2 is committed so it should not be pushed. Also it has no intents so
		// it should only send a GCRequest. The callback should indicate that there
		// is no push but that the gc has occurred successfully.
		{
			txn:           txn2,
			intents:       []roachpb.Intent{},
			sendFuncs:     []sendFunc{gcSendFunc},
			expectSucceed: true,
		},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			sf := &sendFuncs{sendFuncs: c.sendFuncs}
			ir := newIntentResolverWithSendFuncs(cfg, sf)
			var didPush, didSucceed bool
			done := make(chan struct{})
			onComplete := func(pushed, succeeded bool) {
				didPush, didSucceed = pushed, succeeded
				close(done)
			}
			err := ir.CleanupTxnIntentsOnGCAsync(ctx, 1, c.txn, c.intents, clock.Now(), onComplete)
			if err != nil {
				t.Fatalf("unexpected error sending async transaction")
			}
			<-done
			if sf.len() != 0 {
				t.Errorf("Not all send funcs called")
			}
			if didSucceed != c.expectSucceed {
				t.Fatalf("unexpected success value: got %v, expected %v", didSucceed, c.expectSucceed)
			}
			if didPush != c.expectPushed {
				t.Fatalf("unexpected pushed value: got %v, expected %v", didPush, c.expectPushed)
			}
		})
	}
}

// TestContendedIntent verifies that multiple transactions, some actively
// writing and others read-only, are queued if processing write intent
// errors on a contended key. The test verifies the expected ordering in
// the queue and then works to invoke the corresponding cleanup functions
// to exercise the various transaction pushing and intent resolution which
// should occur.
func TestContendedIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	// The intent resolver's db uses a sender which always resolves intents
	// and pushes transactions to aborted successfully. All requests are sent to
	// reqChan so that the test can inspect all sent requests. Given the buffer
	// size of 2, it is important that the test take care to consume from reqChan
	// when it expects requests
	reqChan := make(chan roachpb.BatchRequest, 2)
	sender := client.NonTransactionalFactoryFunc(
		func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			resp := &roachpb.BatchResponse{}
			switch req := ba.Requests[0].GetInner().(type) {
			case *roachpb.PushTxnRequest:
				resp.Add(&roachpb.PushTxnResponse{
					PusheeTxn: roachpb.Transaction{
						TxnMeta: req.PusheeTxn,
						Status:  roachpb.ABORTED,
					},
				})
			case *roachpb.ResolveIntentRequest:
				resp.Add(&roachpb.ResolveIntentRangeResponse{})
			default:
				t.Errorf("Unexpected request of type %T", req)
			}
			reqChan <- ba
			return resp, nil
		})
	verifyPushTxn := func(ba roachpb.BatchRequest, pusher, pushee uuid.UUID) {
		if req, ok := ba.Requests[0].GetInner().(*roachpb.PushTxnRequest); ok {
			if req.PusherTxn.ID != pusher {
				t.Fatalf("expected pusher to be %v, got %v", pusher, req.PusherTxn.ID)
			} else if req.PusheeTxn.ID != pushee {
				t.Fatalf("expected pushee to be %v, got %v", pushee, req.PusheeTxn.ID)
			}
		} else {
			t.Fatalf("expected PushTxnRequest, got %T", ba.Requests[0].GetInner())
		}
	}
	verifyResolveIntent := func(ba roachpb.BatchRequest, key roachpb.Key) {
		if req, ok := ba.Requests[0].GetInner().(*roachpb.ResolveIntentRequest); ok {
			key.Equal(req.Key)
		} else {
			t.Fatalf("expected PushTxnRequest, got %T", ba.Requests[0].GetInner())
		}
	}

	db := client.NewDB(log.AmbientContext{
		Tracer: tracing.NewTracer(),
	}, sender, clock)
	ir := New(Config{
		Stopper: stopper,
		DB:      db,
		Clock:   clock,
	})

	// The below code constructs 7 transactions which will encounter
	// WriteIntentErrors for origTxn. These transactions all contend with each
	// other. The test begins by creating goroutines for each of the 7 txns to
	// record a WriteIntentError for an intent for keyA. We verify that these
	// transactions queue up behind eachother in the contention queue.
	// We then proceed to process their returned CleanupFuncs and verify that
	// the corresponding requests are sent.
	//
	// The expectation which is verified below is that the roTxn1 will lead to
	// a PushTxn request for origTxn followed by a ResolveIntents request for
	// origTxn's intent. The rest of the read-only transactions when rapidly
	// cleaned up should send no requests. Note that roTxn4 gets canceled after
	// roTxn1 is cleaned up in order to ensure that the cancellation code path
	// does not interfere with correct code behavior.
	//
	// Interesting nuances in resolution begin with rwTxn1 for which we call
	// the CleanupFunc with now a new WriteIntentError with the unrelatedRWTxn
	// as the associated transaction. This leads all future push requests to
	// push the unrelatedRWTxn rather than the origTxn though intent resolution
	// requests will be for the original key.
	//
	// For rwTxn2 we wait for the dependencyCyclePushDelay to trigger the sending
	// of another Push request for the unrelatedRWTxn and then we call cleanup
	// for rwTxn2 with a new WriteIntentError on a different intent key which
	// should not result in any new requests.
	// Lastly we rapidly resolve the last rwTxn3 with (nil, nil) which also should
	// lead to no requests being sent.

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	keyC := roachpb.Key("c")
	origTxn := beginTransaction(t, clock, 1, keyA, true /* putKey */)
	unrelatedRWTxn := beginTransaction(t, clock, 1, keyB, true /* putKey */)

	roTxn1 := newTransaction("test", keyA, 1, clock)
	roTxn2 := newTransaction("test", keyA, 1, clock)
	roTxn3 := newTransaction("test", keyA, 1, clock)
	roTxn4 := newTransaction("test", keyA, 1, clock) // this one gets canceled
	rwTxn1 := beginTransaction(t, clock, 1, keyB, true /* putKey */)
	rwTxn2 := beginTransaction(t, clock, 1, keyC, true /* putKey */)
	rwTxn3 := beginTransaction(t, clock, 1, keyB, true /* putKey */)

	testCases := []struct {
		pusher     *roachpb.Transaction
		expTxns    []*roachpb.Transaction
		cancelFunc context.CancelFunc
	}{
		// First establish a chain of four read-only txns.
		{pusher: roTxn1, expTxns: []*roachpb.Transaction{roTxn1}},
		{pusher: roTxn2, expTxns: []*roachpb.Transaction{roTxn1, roTxn2}},
		{pusher: roTxn3, expTxns: []*roachpb.Transaction{roTxn1, roTxn2, roTxn3}},
		// The fourth txn will be canceled before its predecessor is cleaned up to
		// excersize the cancellation code path.
		{pusher: roTxn4, expTxns: []*roachpb.Transaction{roTxn1, roTxn2, roTxn3, roTxn4}},
		// Now, verify that a writing txn is inserted at the end of the queue.
		{pusher: rwTxn1, expTxns: []*roachpb.Transaction{roTxn1, roTxn2, roTxn3, roTxn4, rwTxn1}},
		// And a second writing txn is inserted after it.
		{pusher: rwTxn2, expTxns: []*roachpb.Transaction{roTxn1, roTxn2, roTxn3, roTxn4, rwTxn1, rwTxn2}},
		{pusher: rwTxn3, expTxns: []*roachpb.Transaction{roTxn1, roTxn2, roTxn3, roTxn4, rwTxn1, rwTxn2, rwTxn3}},
	}
	var wg sync.WaitGroup
	cleanupFuncs := make(chan CleanupFunc)
	for i, tc := range testCases {
		testCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		testCases[i].cancelFunc = cancel
		t.Run(tc.pusher.ID.String(), func(t *testing.T) {
			wiErr := &roachpb.WriteIntentError{Intents: []roachpb.Intent{{
				Txn:  origTxn.TxnMeta,
				Span: roachpb.Span{Key: keyA},
			}}}
			h := roachpb.Header{Txn: tc.pusher}
			wg.Add(1)
			go func() {
				cleanupFunc, pErr := ir.ProcessWriteIntentError(testCtx, roachpb.NewError(wiErr), nil, h, roachpb.PUSH_ABORT)
				if pErr != nil {
					panic(pErr)
				}
				cleanupFuncs <- cleanupFunc
				wg.Done()
			}()
			testutils.SucceedsSoon(t, func() error {
				if lc, let := ir.NumContended(keyA), len(tc.expTxns); lc != let {
					return errors.Errorf("expected len %d; got %d", let, lc)
				}
				ir.contentionQ.mu.Lock()
				defer ir.contentionQ.mu.Unlock()
				contended, ok := ir.contentionQ.mu.keys[string(keyA)]
				if !ok {
					return errors.Errorf("key not contended")
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

	// Wait until all of the WriteIntentErrors have been processed to stop
	// processing the cleanupFuncs.
	go func() { wg.Wait(); close(cleanupFuncs) }()
	i := 0
	for f := range cleanupFuncs {
		switch i {
		// The read only transactions should be cleaned up with nil, nil.
		case 0:
			// There should be a push of orig and then a resolve of intent.
			verifyPushTxn(<-reqChan, roTxn1.ID, origTxn.ID)
			verifyResolveIntent(<-reqChan, keyA)
			fallthrough
		case 1, 2, 3:
			// The remaining roTxns should not do anything upon cleanup.
			f(nil, nil)
			if i == 1 {
				testCases[3].cancelFunc()
			}
		case 4:
			// Call the CleanupFunc with a new WriteIntentError with a different
			// transaction. This should lean to a new push on the new transaction and
			// an intent resolution of the original intent.
			f(&roachpb.WriteIntentError{Intents: []roachpb.Intent{{
				Span: roachpb.Span{Key: keyA},
				Txn:  unrelatedRWTxn.TxnMeta,
			}}}, nil)
			verifyPushTxn(<-reqChan, rwTxn2.ID, unrelatedRWTxn.ID)
			verifyResolveIntent(<-reqChan, rwTxn1.Key)
		case 5:
			verifyPushTxn(<-reqChan, rwTxn3.ID, unrelatedRWTxn.ID)
			f(&roachpb.WriteIntentError{Intents: []roachpb.Intent{{
				Span: roachpb.Span{Key: keyB},
				Txn:  rwTxn1.TxnMeta,
			}}}, nil)
		case 6:
			f(nil, &testCases[i].pusher.TxnMeta)
		}
		i++
	}
}

// TestCleanupIntentsAsync verifies that CleanupIntentsAsync either runs
// synchronously or returns an error when there are too many concurrently
// running tasks.
func TestCleanupIntentsAsyncThrottled(t *testing.T) {
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	cfg := Config{
		Stopper: stopper,
		Clock:   clock,
	}
	txn := beginTransaction(t, clock, 1, roachpb.Key("a"), true /* putKey */)
	sf := newSendFuncs(
		pushTxnSendFunc(1),
		resolveIntentsSendFunc,
	)
	ir := newIntentResolverWithSendFuncs(cfg, sf)
	// Run defaultTaskLimit tasks which will block until blocker is closed.
	blocker := make(chan struct{})
	defer close(blocker)
	var wg sync.WaitGroup
	wg.Add(defaultTaskLimit)
	for i := 0; i < defaultTaskLimit; i++ {
		if err := ir.runAsyncTask(context.Background(), false, func(context.Context) {
			wg.Done()
			<-blocker
		}); err != nil {
			t.Fatalf("Failed to run blocking async task: %v", err)
		}
	}
	wg.Wait()
	testIntentsWithArg := []result.IntentsWithArg{
		{Intents: []roachpb.Intent{
			{Span: roachpb.Span{Key: roachpb.Key("a")}, Txn: txn.TxnMeta},
		}},
	}
	// Running with allowSyncProcessing = false should result in an error and no
	// requests being sent.
	err := ir.CleanupIntentsAsync(context.Background(), testIntentsWithArg, false)
	assert.Equal(t, errors.Cause(err), stop.ErrThrottled)
	// Running with allowSyncProcessing = true should result in the synchronous
	// processing of the intents resulting in no error and the consumption of the
	// sendFuncs.
	err = ir.CleanupIntentsAsync(context.Background(), testIntentsWithArg, true)
	assert.Nil(t, err)
	assert.Equal(t, sf.len(), 0)
}

// TestCleanupIntentsAsync verifies that CleanupIntentsAsync sends the expected
// requests.
func TestCleanupIntentsAsync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		intents   []result.IntentsWithArg
		sendFuncs []sendFunc
	}
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	txn := beginTransaction(t, clock, 1, roachpb.Key("a"), true /* putKey */)
	testIntentsWithArg := []result.IntentsWithArg{
		{Intents: []roachpb.Intent{
			{Span: roachpb.Span{Key: roachpb.Key("a")}, Txn: txn.TxnMeta},
		}},
	}
	cases := []testCase{
		{
			intents: testIntentsWithArg,
			sendFuncs: []sendFunc{
				singlePushTxnSendFunc,
				resolveIntentsSendFunc,
			},
		},
		{
			intents: testIntentsWithArg,
			sendFuncs: []sendFunc{
				singlePushTxnSendFunc,
				failSendFunc,
			},
		},
		{
			intents: testIntentsWithArg,
			sendFuncs: []sendFunc{
				failSendFunc,
			},
		},
	}
	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			stopper := stop.NewStopper()
			sf := newSendFuncs(c.sendFuncs...)
			cfg := Config{
				Stopper: stopper,
				Clock:   clock,
			}
			ir := newIntentResolverWithSendFuncs(cfg, sf)
			err := ir.CleanupIntentsAsync(context.Background(), c.intents, true)
			testutils.SucceedsSoon(t, func() error {
				if l := sf.len(); l > 0 {
					return fmt.Errorf("Still have %d funcs to send", l)
				}
				return nil
			})
			stopper.Stop(context.Background())
			assert.Nil(t, err, "error from CleanupIntentsAsync")
		})
	}
}

func newSendFuncs(sf ...sendFunc) *sendFuncs {
	return &sendFuncs{sendFuncs: sf}
}

type sendFuncs struct {
	mu        syncutil.Mutex
	sendFuncs []sendFunc
}

func (sf *sendFuncs) len() int {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return len(sf.sendFuncs)
}

func (sf *sendFuncs) pop() sendFunc {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	if len(sf.sendFuncs) == 0 {
		panic("no send funcs left!")
	}
	ret := sf.sendFuncs[0]
	sf.sendFuncs = sf.sendFuncs[1:]
	return ret
}

// TestCleanupTxnIntentsAsync verifies that CleanupTxnIntentsAsync sends the
// expected requests.
func TestCleanupTxnIntentsAsync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		intents   []result.EndTxnIntents
		before    func(*testCase, *IntentResolver) func()
		sendFuncs []sendFunc
	}
	testEndTxnIntents := []result.EndTxnIntents{
		{
			Txn: roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					ID: uuid.MakeV4(),
				},
				Intents: []roachpb.Span{
					{Key: roachpb.Key("a")},
				},
			},
		},
	}

	cases := []testCase{
		{
			intents:   testEndTxnIntents,
			sendFuncs: []sendFunc{},
			before: func(tc *testCase, ir *IntentResolver) func() {
				_, f := ir.lockInFlightTxnCleanup(context.Background(), tc.intents[0].Txn.ID)
				return f
			},
		},
		{
			intents: testEndTxnIntents,
			sendFuncs: []sendFunc{
				resolveIntentsSendFunc,
				gcSendFunc,
			},
		},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			stopper := stop.NewStopper()
			clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
			cfg := Config{
				Stopper: stopper,
				Clock:   clock,
			}
			var sendFuncCalled int64
			numSendFuncs := int64(len(c.sendFuncs))
			sf := newSendFuncs(counterSendFuncs(&sendFuncCalled, c.sendFuncs)...)
			ir := newIntentResolverWithSendFuncs(cfg, sf)
			if c.before != nil {
				defer c.before(&c, ir)()
			}
			err := ir.CleanupTxnIntentsAsync(context.Background(), 1, c.intents, false)
			testutils.SucceedsSoon(t, func() error {
				if called := atomic.LoadInt64(&sendFuncCalled); called < numSendFuncs {
					return fmt.Errorf("still waiting for %d calls", numSendFuncs-called)
				}
				return nil
			})
			stopper.Stop(context.Background())
			assert.Nil(t, err)
			assert.Equal(t, sf.len(), 0)
		})
	}
}

func counterSendFuncs(counter *int64, funcs []sendFunc) []sendFunc {
	for i, f := range funcs {
		funcs[i] = counterSendFunc(counter, f)
	}
	return funcs
}

func counterSendFunc(counter *int64, f sendFunc) sendFunc {
	return func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		defer atomic.AddInt64(counter, 1)
		return f(ba)
	}
}

// TestCleanupIntents verifies that CleanupIntents sends the expected requests
// and returns the appropriate errors.
func TestCleanupIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	txn := beginTransaction(t, clock, roachpb.MinUserPriority, roachpb.Key("a"), true)
	// Set txn.ID to a very small value so it's sorted deterministically first.
	txn.ID = uuid.UUID{15: 0x01}
	testIntents := []roachpb.Intent{
		{Span: roachpb.Span{Key: roachpb.Key("a")}, Txn: txn.TxnMeta},
	}
	type testCase struct {
		intents     []roachpb.Intent
		sendFuncs   []sendFunc
		expectedErr bool
		expectedNum int
		cfg         Config
	}
	cases := []testCase{
		{
			intents: testIntents,
			sendFuncs: []sendFunc{
				singlePushTxnSendFunc,
				resolveIntentsSendFunc,
			},
			expectedNum: 1,
		},
		{
			intents: testIntents,
			sendFuncs: []sendFunc{
				failSendFunc,
			},
			expectedErr: true,
		},
		{
			intents: append(makeTxnIntents(t, clock, 3*intentResolverBatchSize),
				// Three intents with the same transaction will only attempt to push the
				// txn 1 time. Hence 3 full batches plus 1 extra.
				testIntents[0], testIntents[0], testIntents[0]),
			sendFuncs: []sendFunc{
				pushTxnSendFunc(intentResolverBatchSize),
				resolveIntentsSendFunc,
				resolveIntentsSendFunc,
				pushTxnSendFunc(intentResolverBatchSize),
				resolveIntentsSendFunc,
				pushTxnSendFunc(intentResolverBatchSize),
				resolveIntentsSendFunc,
				pushTxnSendFunc(1),
				resolveIntentsSendFunc,
			},
			expectedNum: 3*intentResolverBatchSize + 3,
			// Under stress sometimes it can take more than 10ms to even call send.
			// The batch wait is disabled and batch idle increased during this test
			// to eliminate flakiness and ensure that all requests make it to the
			// batcher in a timely manner.
			cfg: Config{
				MaxIntentResolutionBatchWait: -1, // disabled
				MaxIntentResolutionBatchIdle: 20 * time.Millisecond,
			},
		},
	}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			c.cfg.Stopper = stopper
			c.cfg.Clock = clock
			ir := newIntentResolverWithSendFuncs(c.cfg, newSendFuncs(c.sendFuncs...))
			num, err := ir.CleanupIntents(context.Background(), c.intents, clock.Now(), roachpb.PUSH_ABORT)
			assert.Equal(t, num, c.expectedNum, "number of resolved intents")
			assert.Equal(t, err != nil, c.expectedErr, "error during CleanupIntents: %v", err)
		})
	}
}

func beginTxnArgs(
	key []byte, txn *roachpb.Transaction,
) (roachpb.BeginTransactionRequest, roachpb.Header) {
	return roachpb.BeginTransactionRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
	}, roachpb.Header{Txn: txn}
}

func putArgs(key roachpb.Key, value []byte) roachpb.PutRequest {
	return roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(value),
	}
}

func beginTransaction(
	t *testing.T, clock *hlc.Clock, pri roachpb.UserPriority, key roachpb.Key, putKey bool,
) *roachpb.Transaction {
	txn := newTransaction("test", key, pri, clock)

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
	return txn
}

func newTransaction(
	name string, baseKey roachpb.Key, userPriority roachpb.UserPriority, clock *hlc.Clock,
) *roachpb.Transaction {
	var offset int64
	var now hlc.Timestamp
	if clock != nil {
		offset = clock.MaxOffset().Nanoseconds()
		now = clock.Now()
	}
	txn := roachpb.MakeTransaction(name, baseKey, userPriority, now, offset)
	return &txn
}

// assignSeqNumsForReqs sets sequence numbers for each of the provided requests
// given a transaction proto. It also updates the proto to reflect the incremented
// sequence number.
func assignSeqNumsForReqs(txn *roachpb.Transaction, reqs ...roachpb.Request) {
	for _, ru := range reqs {
		txn.Sequence++
		oldHeader := ru.Header()
		oldHeader.Sequence = txn.Sequence
		ru.SetHeader(oldHeader)
	}
}

// makeTxnIntents creates a slice of Intent which each have a unique txn.
func makeTxnIntents(t *testing.T, clock *hlc.Clock, numIntents int) []roachpb.Intent {
	ret := make([]roachpb.Intent, 0, numIntents)
	for i := 0; i < numIntents; i++ {
		txn := beginTransaction(t, clock, 1, roachpb.Key("a"), true /* putKey */)
		ret = append(ret,
			roachpb.Intent{Span: roachpb.Span{Key: txn.Key}, Txn: txn.TxnMeta})
	}
	return ret
}

// sendFunc is a function used to control behavior for a specific request that
// the IntentResolver tries to send. They are used in conjunction with the below
// function to create an IntentResolver with a slice of sendFuncs.
// A library of useful sendFuncs are defined below.
type sendFunc func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)

func newIntentResolverWithSendFuncs(c Config, sf *sendFuncs) *IntentResolver {
	txnSenderFactory := client.NonTransactionalFactoryFunc(
		func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			f := sf.pop()
			return f(ba)
		})
	db := client.NewDB(log.AmbientContext{
		Tracer: tracing.NewTracer(),
	}, txnSenderFactory, c.Clock)
	c.DB = db
	c.MaxGCBatchWait = time.Nanosecond
	return New(c)
}

var (
	pushTxnSendFunc = func(numPushes int) sendFunc {
		return func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			if len(ba.Requests) != numPushes {
				panic(fmt.Errorf("expected %d PushTxnRequests in batch, got %d",
					numPushes, len(ba.Requests)))
			}
			resp := &roachpb.BatchResponse{}
			for _, r := range ba.Requests {
				req := r.GetInner().(*roachpb.PushTxnRequest)
				txn := req.PusheeTxn
				resp.Add(&roachpb.PushTxnResponse{
					PusheeTxn: roachpb.Transaction{
						Status:  roachpb.ABORTED,
						TxnMeta: txn,
					},
				})
			}
			return resp, nil
		}
	}
	singlePushTxnSendFunc           = pushTxnSendFunc(1)
	resolveIntentsSendFunc sendFunc = func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		resp := &roachpb.BatchResponse{}
		for _, r := range ba.Requests {
			if _, ok := r.GetInner().(*roachpb.ResolveIntentRequest); ok {
				resp.Add(&roachpb.ResolveIntentResponse{})
			} else if _, ok := r.GetInner().(*roachpb.ResolveIntentRangeRequest); ok {
				resp.Add(&roachpb.ResolveIntentRangeResponse{})
			} else {
				panic(fmt.Errorf("Unexpected request in batch for intent resolution: %T", r.GetInner()))
			}
		}
		return resp, nil
	}
	failSendFunc sendFunc = func(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		return nil, roachpb.NewError(fmt.Errorf("boom"))
	}
	gcSendFunc sendFunc = func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		resp := &roachpb.BatchResponse{}
		for _, r := range ba.Requests {
			if _, ok := r.GetInner().(*roachpb.GCRequest); ok {
				resp.Add(&roachpb.GCResponse{})
			} else {
				panic(fmt.Errorf("Unexpected request type %T, expecte GCRequest", r.GetInner()))
			}
		}
		return resp, nil
	}
)
