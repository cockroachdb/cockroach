// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package intentresolver

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
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
			{Span: roachpb.Span{Key: roachpb.Key("b")}, Status: roachpb.STAGING}},
		{{Span: roachpb.Span{Key: roachpb.Key("a")}, Status: roachpb.PENDING},
			{Span: roachpb.Span{Key: roachpb.Key("b")}, Status: roachpb.ABORTED}},
		{{Span: roachpb.Span{Key: roachpb.Key("a")}, Status: roachpb.PENDING},
			{Span: roachpb.Span{Key: roachpb.Key("b")}, Status: roachpb.COMMITTED}},
	}
	for _, intents := range testCases {
		if _, pErr := ir.maybePushIntents(
			context.Background(), intents, roachpb.Header{}, roachpb.PUSH_TOUCH, true,
		); !testutils.IsPError(pErr, "unexpected (STAGING|ABORTED|COMMITTED) intent") {
			t.Errorf("expected error on non-pending intent, but got %s", pErr)
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
		sendFuncs     *sendFuncs
		expectPushed  bool
		expectSucceed bool
	}

	// This test creates 3 transaction for use in the below test cases.
	// A new intent resolver is created for each test case so they operate
	// completely independently.
	key := roachpb.Key("a")
	// Txn0 is in the pending state and is not old enough to have expired so the
	// code ought to send nothing.
	txn0 := newTransaction("txn0", key, 1, clock)
	// Txn1 is in the pending state but is expired.
	txn1 := newTransaction("txn1", key, 1, clock)
	txn1.ReadTimestamp.WallTime -= int64(100 * time.Second)
	txn1.DeprecatedOrigTimestamp = txn1.ReadTimestamp
	txn1.LastHeartbeat = txn1.ReadTimestamp
	// Txn2 is in the staging state and is not old enough to have expired so the
	// code ought to send nothing.
	txn2 := newTransaction("txn2", key, 1, clock)
	txn2.Status = roachpb.STAGING
	// Txn3 is in the staging state but is expired.
	txn3 := newTransaction("txn3", key, 1, clock)
	txn3.Status = roachpb.STAGING
	txn3.ReadTimestamp.WallTime -= int64(100 * time.Second)
	txn3.DeprecatedOrigTimestamp = txn3.ReadTimestamp
	txn3.LastHeartbeat = txn3.ReadTimestamp
	// Txn4 is in the committed state.
	txn4 := newTransaction("txn4", key, 1, clock)
	txn4.Status = roachpb.COMMITTED
	cases := []*testCase{
		// This one has an unexpired pending transaction so it's skipped.
		{
			txn:       txn0,
			sendFuncs: newSendFuncs(t),
		},
		// Txn1 is pending and expired so the code should attempt to push the txn.
		// The provided sender will fail on the first request. The callback should
		// indicate that the transaction was pushed but that the resolution was not
		// successful.
		{
			txn:          txn1,
			sendFuncs:    newSendFuncs(t, failSendFunc),
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
				roachpb.MakeIntent(txn1, roachpb.Span{Key: key}),
				roachpb.MakeIntent(txn1, roachpb.Span{Key: key, EndKey: roachpb.Key("b")}),
			},
			sendFuncs: newSendFuncs(t,
				singlePushTxnSendFunc(t),
				resolveIntentsSendFunc(t),
				failSendFunc,
			),
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
				roachpb.MakeIntent(txn1, roachpb.Span{Key: key}),
				roachpb.MakeIntent(txn1, roachpb.Span{Key: roachpb.Key("aa")}),
				roachpb.MakeIntent(txn1, roachpb.Span{Key: key, EndKey: roachpb.Key("b")}),
			},
			sendFuncs: func() *sendFuncs {
				s := newSendFuncs(t)
				s.pushFrontLocked(
					singlePushTxnSendFunc(t),
					resolveIntentsSendFuncs(s, 3, 2),
					gcSendFunc(t),
				)
				return s
			}(),
			expectPushed:  true,
			expectSucceed: true,
		},
		// This one has an unexpired staging transaction so it's skipped.
		{
			txn:       txn2,
			sendFuncs: newSendFuncs(t),
		},
		// Txn3 is staging and expired so the code should attempt to push the txn.
		// The provided sender will fail on the first request. The callback should
		// indicate that the transaction was pushed but that the resolution was not
		// successful.
		{
			txn:          txn3,
			sendFuncs:    newSendFuncs(t, failSendFunc),
			expectPushed: true,
		},
		// Txn3 is staging and expired so the code should attempt to push the txn
		// and then work to resolve its intents. The intent resolution happens in
		// different requests for individual keys and then for spans. This case will
		// allow the individual key intent to be resolved completely but will fail
		// to resolve the span. The callback should indicate that the transaction
		// has been pushed but that the garbage collection was not successful.
		{
			txn: txn3,
			intents: []roachpb.Intent{
				roachpb.MakeIntent(txn3, roachpb.Span{Key: key}),
				roachpb.MakeIntent(txn3, roachpb.Span{Key: key, EndKey: roachpb.Key("b")}),
			},
			sendFuncs: newSendFuncs(t,
				singlePushTxnSendFunc(t),
				resolveIntentsSendFunc(t),
				failSendFunc,
			),
			expectPushed: true,
		},
		// Txn3 is staging and expired so the code should attempt to push the txn
		// and then work to resolve its intents. The intent resolution happens in
		// different requests for individual keys and then for spans. This case will
		// allow the individual key intents to be resolved in one request and for
		// the span request to be resolved in another. Finally it will succeed on
		// the GCRequest. This is a positive case and the callback should indicate
		// that the txn has both been pushed and successfully resolved.
		{
			txn: txn3,
			intents: []roachpb.Intent{
				roachpb.MakeIntent(txn3, roachpb.Span{Key: key}),
				roachpb.MakeIntent(txn3, roachpb.Span{Key: roachpb.Key("aa")}),
				roachpb.MakeIntent(txn3, roachpb.Span{Key: key, EndKey: roachpb.Key("b")}),
			},
			sendFuncs: func() *sendFuncs {
				s := newSendFuncs(t)
				s.pushFrontLocked(
					singlePushTxnSendFunc(t),
					resolveIntentsSendFuncs(s, 3, 2),
					gcSendFunc(t),
				)
				return s
			}(),
			expectPushed:  true,
			expectSucceed: true,
		},
		// Txn4 is committed so it should not be pushed. Also it has no intents so
		// it should only send a GCRequest. The callback should indicate that there
		// is no push but that the gc has occurred successfully.
		{
			txn:           txn4,
			intents:       []roachpb.Intent{},
			sendFuncs:     newSendFuncs(t, gcSendFunc(t)),
			expectSucceed: true,
		},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			ir := newIntentResolverWithSendFuncs(cfg, c.sendFuncs)
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
			if c.sendFuncs.len() != 0 {
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
			t.Fatalf("expected ResolveIntentRequest, got %T", ba.Requests[0].GetInner())
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
	origTxn := newTransaction("orig", keyA, 1, clock)
	unrelatedRWTxn := newTransaction("unrel", keyB, 1, clock)

	roTxn1 := newTransaction("ro-txn1", nil, 1, clock)
	roTxn2 := newTransaction("ro-txn2", nil, 1, clock)
	roTxn3 := newTransaction("ro-txn3", nil, 1, clock)
	roTxn4 := newTransaction("ro-txn4", nil, 1, clock) // this one gets canceled
	rwTxn1 := newTransaction("rw-txn1", keyB, 1, clock)
	rwTxn2 := newTransaction("rw-txn2", keyC, 1, clock)
	rwTxn3 := newTransaction("rw-txn3", keyB, 1, clock)

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
		// exercise the cancellation code path.
		{pusher: roTxn4, expTxns: []*roachpb.Transaction{roTxn1, roTxn2, roTxn3, roTxn4}},
		// Now, verify that a writing txn is inserted at the end of the queue.
		{pusher: rwTxn1, expTxns: []*roachpb.Transaction{roTxn1, roTxn2, roTxn3, roTxn4, rwTxn1}},
		// And other writing txns are inserted after it.
		{pusher: rwTxn2, expTxns: []*roachpb.Transaction{roTxn1, roTxn2, roTxn3, roTxn4, rwTxn1, rwTxn2}},
		{pusher: rwTxn3, expTxns: []*roachpb.Transaction{roTxn1, roTxn2, roTxn3, roTxn4, rwTxn1, rwTxn2, rwTxn3}},
	}
	var wg sync.WaitGroup
	type intentResolverResp struct {
		idx  int
		fn   CleanupFunc
		pErr *roachpb.Error
	}
	resps := make(chan intentResolverResp, 1)
	for i, tc := range testCases {
		testCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		testCases[i].cancelFunc = cancel
		t.Run(tc.pusher.ID.String(), func(t *testing.T) {
			wiErr := &roachpb.WriteIntentError{Intents: []roachpb.Intent{
				roachpb.MakePendingIntent(&origTxn.TxnMeta, roachpb.Span{Key: keyA})}}
			h := roachpb.Header{Txn: tc.pusher}
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				cleanupFunc, pErr := ir.ProcessWriteIntentError(testCtx, roachpb.NewError(wiErr), h, roachpb.PUSH_ABORT)
				resps <- intentResolverResp{idx: idx, fn: cleanupFunc, pErr: pErr}
			}(i)
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
	// processing the resps.
	go func() { wg.Wait(); close(resps) }()
	expIdx := 0
	for resp := range resps {
		// Check index.
		idx := resp.idx
		if idx != expIdx {
			t.Errorf("expected response from request %d, found %d", expIdx, idx)
		}
		expIdx++

		// Check error.
		pErr := resp.pErr
		switch idx {
		case 3:
			// Expected to be canceled.
			if !testutils.IsPError(pErr, context.Canceled.Error()) {
				t.Errorf("expected context canceled error; got %v", pErr)
			}
		default:
			if pErr != nil {
				t.Errorf("unexpected error from ProcessWriteIntentError: %v", pErr)
			}
		}

		// Call cleanup function.
		f := resp.fn
		switch idx {
		// The read only transactions should be cleaned up with nil, nil.
		case 0:
			// There should be a push of orig and then a resolve of intent.
			verifyPushTxn(<-reqChan, roTxn1.ID, origTxn.ID)
			verifyResolveIntent(<-reqChan, keyA)

			// Minimum and maximum priority txns pass through without queuing.
			// This is a convenient place to perform this check because roTxn1
			// push is still blocking all other txn pushes, but the min and max
			// priority txn's requests won't mix with requests.
			for name, pusher := range map[string]*roachpb.Transaction{
				"min priority": newTransaction("min-txn", keyA, roachpb.MinUserPriority, clock),
				"max priority": newTransaction("max-txn", keyA, roachpb.MaxUserPriority, clock),
			} {
				t.Run(name, func(t *testing.T) {
					wiErr := &roachpb.WriteIntentError{Intents: []roachpb.Intent{
						roachpb.MakePendingIntent(&origTxn.TxnMeta, roachpb.Span{Key: keyA})}}
					h := roachpb.Header{Txn: pusher}
					cleanupFunc, pErr := ir.ProcessWriteIntentError(ctx, roachpb.NewError(wiErr), h, roachpb.PUSH_ABORT)
					if pErr != nil {
						panic(pErr)
					}
					if cleanupFunc != nil {
						t.Fatal("unexpected cleanup func; should not have entered contentionQueue")
					}
					verifyPushTxn(<-reqChan, pusher.ID, origTxn.ID)
					verifyResolveIntent(<-reqChan, keyA)
				})
			}
			f(nil, nil)
		case 1, 2, 3:
			// The remaining roTxns should not do anything upon cleanup.
			if idx == 2 {
				// Cancel request 3 before request 2 cleans up. Wait for
				// it to return an error before continuing.
				testCases[3].cancelFunc()
				cf3 := <-resps
				resps <- cf3
			}
			f(nil, nil)
		case 4:
			// Call the CleanupFunc with a new WriteIntentError with a different
			// transaction. This should lean to a new push on the new transaction and
			// an intent resolution of the original intent.
			f(&roachpb.WriteIntentError{Intents: []roachpb.Intent{
				roachpb.MakePendingIntent(&unrelatedRWTxn.TxnMeta, roachpb.Span{Key: keyA})}}, nil)
			verifyPushTxn(<-reqChan, rwTxn2.ID, unrelatedRWTxn.ID)
			verifyResolveIntent(<-reqChan, rwTxn1.Key)
		case 5:
			verifyPushTxn(<-reqChan, rwTxn3.ID, unrelatedRWTxn.ID)
			f(&roachpb.WriteIntentError{Intents: []roachpb.Intent{
				roachpb.MakePendingIntent(&rwTxn1.TxnMeta, roachpb.Span{Key: keyB})}}, nil)
		case 6:
			f(nil, &testCases[idx].pusher.TxnMeta)
		default:
			t.Fatalf("unexpected response %d", idx)
		}
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
	txn := newTransaction("txn", roachpb.Key("a"), 1, clock)
	sf := newSendFuncs(t,
		pushTxnSendFunc(t, 1),
		resolveIntentsSendFunc(t),
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
			t.Fatalf("Failed to run blocking async task: %+v", err)
		}
	}
	wg.Wait()
	testIntents := []roachpb.Intent{
		roachpb.MakeIntent(txn, roachpb.Span{Key: roachpb.Key("a")}),
	}
	// Running with allowSyncProcessing = false should result in an error and no
	// requests being sent.
	err := ir.CleanupIntentsAsync(context.Background(), testIntents, false)
	assert.Equal(t, errors.Cause(err), stop.ErrThrottled)
	// Running with allowSyncProcessing = true should result in the synchronous
	// processing of the intents resulting in no error and the consumption of the
	// sendFuncs.
	err = ir.CleanupIntentsAsync(context.Background(), testIntents, true)
	assert.Nil(t, err)
	assert.Equal(t, sf.len(), 0)
}

// TestCleanupIntentsAsync verifies that CleanupIntentsAsync sends the expected
// requests.
func TestCleanupIntentsAsync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		intents   []roachpb.Intent
		sendFuncs []sendFunc
	}
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	txn := newTransaction("txn", roachpb.Key("a"), 1, clock)
	testIntents := []roachpb.Intent{
		roachpb.MakeIntent(txn, roachpb.Span{Key: roachpb.Key("a")}),
	}
	cases := []testCase{
		{
			intents: testIntents,
			sendFuncs: []sendFunc{
				singlePushTxnSendFunc(t),
				resolveIntentsSendFunc(t),
			},
		},
		{
			intents: testIntents,
			sendFuncs: []sendFunc{
				singlePushTxnSendFunc(t),
				failSendFunc,
			},
		},
		{
			intents: testIntents,
			sendFuncs: []sendFunc{
				failSendFunc,
			},
		},
	}
	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			stopper := stop.NewStopper()
			sf := newSendFuncs(t, c.sendFuncs...)
			cfg := Config{
				Stopper: stopper,
				Clock:   clock,
			}
			ir := newIntentResolverWithSendFuncs(cfg, sf)
			err := ir.CleanupIntentsAsync(context.Background(), c.intents, true)
			sf.drain(t)
			stopper.Stop(context.Background())
			assert.Nil(t, err, "error from CleanupIntentsAsync")
		})
	}
}

// TestCleanupMultipleIntentsAsync verifies that CleanupIntentsAsync sends the
// expected requests when multiple IntentsWithArg are provided to it.
func TestCleanupMultipleIntentsAsync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	txn1 := newTransaction("txn1", roachpb.Key("a"), 1, clock)
	txn2 := newTransaction("txn2", roachpb.Key("c"), 1, clock)
	testIntents := []roachpb.Intent{
		{Span: roachpb.Span{Key: roachpb.Key("a")}, Txn: txn1.TxnMeta},
		{Span: roachpb.Span{Key: roachpb.Key("b")}, Txn: txn1.TxnMeta},
		{Span: roachpb.Span{Key: roachpb.Key("c")}, Txn: txn2.TxnMeta},
		{Span: roachpb.Span{Key: roachpb.Key("d")}, Txn: txn2.TxnMeta},
	}

	// We expect to see a single PushTxn req for all four intents and a
	// ResolveIntent req for each intent. However, because these requests are
	// all async, it's unclear which order these will be issued in. Handle all
	// orders and record the resolved intents.
	var reqs struct {
		syncutil.Mutex
		pushed   []string
		resolved []string
	}
	pushOrResolveFunc := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		switch ba.Requests[0].GetInner().Method() {
		case roachpb.PushTxn:
			for _, ru := range ba.Requests {
				reqs.Lock()
				reqs.pushed = append(reqs.pushed, string(ru.GetPushTxn().Key))
				reqs.Unlock()
			}
			return pushTxnSendFunc(t, len(ba.Requests))(ba)
		case roachpb.ResolveIntent:
			for _, ru := range ba.Requests {
				reqs.Lock()
				reqs.resolved = append(reqs.resolved, string(ru.GetResolveIntent().Key))
				reqs.Unlock()
			}
			return resolveIntentsSendFunc(t)(ba)
		default:
			return nil, roachpb.NewErrorf("unexpected")
		}
	}
	sf := newSendFuncs(t, repeat(pushOrResolveFunc, 5)...)

	stopper := stop.NewStopper()
	cfg := Config{
		Stopper: stopper,
		Clock:   clock,
		// Don't let the  intent resolution requests be batched with each other.
		// This would make it harder to determine how to drain sf.
		TestingKnobs: storagebase.IntentResolverTestingKnobs{
			MaxIntentResolutionBatchSize: 1,
		},
	}
	ir := newIntentResolverWithSendFuncs(cfg, sf)
	err := ir.CleanupIntentsAsync(ctx, testIntents, false)
	sf.drain(t)
	stopper.Stop(ctx)
	assert.Nil(t, err)

	// Both txns should be pushed and all four intents should be resolved.
	sort.Strings(reqs.pushed)
	sort.Strings(reqs.resolved)
	assert.Equal(t, []string{"a", "c"}, reqs.pushed)
	assert.Equal(t, []string{"a", "b", "c", "d"}, reqs.resolved)
}

func repeat(f sendFunc, n int) []sendFunc {
	fns := make([]sendFunc, n)
	for i := range fns {
		fns[i] = f
	}
	return fns
}

func newSendFuncs(t *testing.T, sf ...sendFunc) *sendFuncs {
	return &sendFuncs{t: t, sendFuncs: sf}
}

type sendFuncs struct {
	t         *testing.T
	mu        syncutil.Mutex
	sendFuncs []sendFunc
}

func (sf *sendFuncs) len() int {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return len(sf.sendFuncs)
}

func (sf *sendFuncs) pushFrontLocked(f ...sendFunc) {
	sf.sendFuncs = append(f, sf.sendFuncs...)
}

func (sf *sendFuncs) popLocked() sendFunc {
	if len(sf.sendFuncs) == 0 {
		sf.t.Errorf("No send funcs left!")
	}
	ret := sf.sendFuncs[0]
	sf.sendFuncs = sf.sendFuncs[1:]
	return ret
}

func (sf *sendFuncs) drain(t *testing.T) {
	testutils.SucceedsSoon(t, func() error {
		if l := sf.len(); l > 0 {
			return errors.Errorf("still have %d funcs to send", l)
		}
		return nil
	})
}

// TestTxnCleanupIntentsAsyncWithPartialRollback verifies that
// CleanupIntentsAsync properly forwards the ignored seqnum list in
// the resolve intent requests.
func TestCleanupTxnIntentsAsyncWithPartialRollback(t *testing.T) {
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	txn := newTransaction("txn", roachpb.Key("a"), 1, clock)
	txn.IntentSpans = []roachpb.Span{
		{Key: roachpb.Key("a")},
		{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
	}
	txn.IgnoredSeqNums = []enginepb.IgnoredSeqNumRange{{Start: 1, End: 1}}

	var gotResolveIntent, gotResolveIntentRange int32
	check := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		for _, r := range ba.Requests {
			if ri, ok := r.GetInner().(*roachpb.ResolveIntentRequest); ok {
				atomic.StoreInt32(&gotResolveIntent, 1)
				if !reflect.DeepEqual(ri.IgnoredSeqNums, txn.IgnoredSeqNums) {
					t.Errorf("expected ignored list %v, got %v", txn.IgnoredSeqNums, ri.IgnoredSeqNums)
				}
			} else if rir, ok := r.GetInner().(*roachpb.ResolveIntentRangeRequest); ok {
				atomic.StoreInt32(&gotResolveIntentRange, 1)
				if !reflect.DeepEqual(rir.IgnoredSeqNums, txn.IgnoredSeqNums) {
					t.Errorf("expected ignored list %v, got %v", txn.IgnoredSeqNums, rir.IgnoredSeqNums)
				}
			}
		}
		return respForResolveIntentBatch(t, ba), nil
	}
	sf := newSendFuncs(t,
		sendFunc(check),
		sendFunc(check),
		gcSendFunc(t),
	)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	cfg := Config{
		Stopper: stopper,
		Clock:   clock,
	}
	ir := newIntentResolverWithSendFuncs(cfg, sf)

	intents := []result.EndTxnIntents{{Txn: txn}}

	if err := ir.CleanupTxnIntentsAsync(context.Background(), 1, intents, true /*allowAsyncProcessing*/); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		if atomic.LoadInt32(&gotResolveIntent) == 0 {
			return errors.New("still waiting for resolve intent req")
		}
		if atomic.LoadInt32(&gotResolveIntentRange) == 0 {
			return errors.New("still waiting for resolve intent range req")
		}
		return nil
	})
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
			Txn: &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					ID:           uuid.MakeV4(),
					MinTimestamp: hlc.Timestamp{WallTime: 123},
				},
				IntentSpans: []roachpb.Span{
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
				resolveIntentsSendFunc(t),
				gcSendFunc(t),
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
			sf := newSendFuncs(t, counterSendFuncs(&sendFuncCalled, c.sendFuncs)...)
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

// TestCleanupMultipleTxnIntentsAsync verifies that CleanupTxnIntentsAsync sends
// the expected requests when multiple EndTxnIntents are provided to it.
func TestCleanupMultipleTxnIntentsAsync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	txn1 := newTransaction("txn1", roachpb.Key("a"), 1, clock)
	txn2 := newTransaction("txn2", roachpb.Key("c"), 1, clock)
	testEndTxnIntents := []result.EndTxnIntents{
		{
			Txn: &roachpb.Transaction{
				TxnMeta: txn1.TxnMeta,
				IntentSpans: []roachpb.Span{
					{Key: roachpb.Key("a")},
					{Key: roachpb.Key("b")},
				},
			},
		},
		{
			Txn: &roachpb.Transaction{
				TxnMeta: txn2.TxnMeta,
				IntentSpans: []roachpb.Span{
					{Key: roachpb.Key("c")},
					{Key: roachpb.Key("d")},
				},
			},
		},
	}

	// We expect to see a ResolveIntent req for each intent and a GC req for
	// each txn. However, because these requests are all async, it's unclear
	// which order these will be issued in. Handle all orders and record the
	// GCed transaction records.
	var reqs struct {
		syncutil.Mutex
		resolved []string
		gced     []string
	}
	resolveOrGCFunc := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if len(ba.Requests) != 1 {
			return nil, roachpb.NewErrorf("unexpected")
		}
		ru := ba.Requests[0]
		switch ru.GetInner().Method() {
		case roachpb.ResolveIntent:
			reqs.Lock()
			reqs.resolved = append(reqs.resolved, string(ru.GetResolveIntent().Key))
			reqs.Unlock()
			return resolveIntentsSendFunc(t)(ba)
		case roachpb.GC:
			reqs.Lock()
			reqs.gced = append(reqs.gced, string(ru.GetGc().Key))
			reqs.Unlock()
			return gcSendFunc(t)(ba)
		default:
			return nil, roachpb.NewErrorf("unexpected")
		}
	}
	sf := newSendFuncs(t, repeat(resolveOrGCFunc, 6)...)

	stopper := stop.NewStopper()
	cfg := Config{
		Stopper: stopper,
		Clock:   clock,
		// Don't let the transaction record GC requests or the intent resolution
		// requests be batched with each other. This would make it harder to
		// determine how to drain sf.
		TestingKnobs: storagebase.IntentResolverTestingKnobs{
			MaxGCBatchSize:               1,
			MaxIntentResolutionBatchSize: 1,
		},
	}
	ir := newIntentResolverWithSendFuncs(cfg, sf)
	err := ir.CleanupTxnIntentsAsync(ctx, 1, testEndTxnIntents, false)
	sf.drain(t)
	stopper.Stop(ctx)
	assert.Nil(t, err)

	// All four intents should be resolved and both txn records should be GCed.
	sort.Strings(reqs.resolved)
	sort.Strings(reqs.gced)
	assert.Equal(t, []string{"a", "b", "c", "d"}, reqs.resolved)
	assert.Equal(t, []string{"a", "c"}, reqs.gced)
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
	txn := newTransaction("txn", roachpb.Key("a"), roachpb.MinUserPriority, clock)
	// Set txn.ID to a very small value so it's sorted deterministically first.
	txn.ID = uuid.UUID{15: 0x01}
	testIntents := []roachpb.Intent{
		roachpb.MakeIntent(txn, roachpb.Span{Key: roachpb.Key("a")}),
	}
	type testCase struct {
		intents     []roachpb.Intent
		sendFuncs   *sendFuncs
		expectedErr bool
		expectedNum int
		cfg         Config
	}
	cases := []testCase{
		{
			intents: testIntents,
			sendFuncs: newSendFuncs(t,
				singlePushTxnSendFunc(t),
				resolveIntentsSendFunc(t),
			),
			expectedNum: 1,
		},
		{
			intents: testIntents,
			sendFuncs: newSendFuncs(t,
				failSendFunc,
			),
			expectedErr: true,
		},
		{
			intents: append(makeTxnIntents(t, clock, 3*intentResolverBatchSize),
				// Three intents with the same transaction will only attempt to push the
				// txn 1 time. Hence 3 full batches plus 1 extra.
				testIntents[0], testIntents[0], testIntents[0]),
			sendFuncs: func() *sendFuncs {
				sf := newSendFuncs(t)
				sf.pushFrontLocked( // don't need to lock
					pushTxnSendFuncs(sf, intentResolverBatchSize),
					resolveIntentsSendFuncs(sf, 102 /* numIntents */, 2 /* minNumReqs */),
					pushTxnSendFuncs(sf, intentResolverBatchSize),
					resolveIntentsSendFuncs(sf, 100 /* numIntents */, 1 /* minNumReqs */),
					pushTxnSendFuncs(sf, intentResolverBatchSize),
					resolveIntentsSendFuncs(sf, 100 /* numIntents */, 1 /* minNumReqs */),
					pushTxnSendFuncs(sf, 1),
					resolveIntentsSendFuncs(sf, 1 /* numIntents */, 1 /* minNumReqs */),
				)
				return sf
			}(),
			expectedNum: 3*intentResolverBatchSize + 3,
			cfg: Config{
				MaxIntentResolutionBatchWait: -1, // disabled
				MaxIntentResolutionBatchIdle: 1 * time.Microsecond,
			},
		},
	}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			c.cfg.Stopper = stopper
			c.cfg.Clock = clock
			ir := newIntentResolverWithSendFuncs(c.cfg, c.sendFuncs)
			num, err := ir.CleanupIntents(context.Background(), c.intents, clock.Now(), roachpb.PUSH_ABORT)
			assert.Equal(t, num, c.expectedNum, "number of resolved intents")
			assert.Equal(t, err != nil, c.expectedErr, "error during CleanupIntents: %v", err)
		})
	}
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

// makeTxnIntents creates a slice of Intent which each have a unique txn.
func makeTxnIntents(t *testing.T, clock *hlc.Clock, numIntents int) []roachpb.Intent {
	ret := make([]roachpb.Intent, 0, numIntents)
	for i := 0; i < numIntents; i++ {
		txn := newTransaction("test", roachpb.Key("a"), 1, clock)
		ret = append(ret,
			roachpb.MakeIntent(txn, roachpb.Span{Key: txn.Key}))
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
			sf.mu.Lock()
			defer sf.mu.Unlock()
			f := sf.popLocked()
			return f(ba)
		})
	db := client.NewDB(log.AmbientContext{
		Tracer: tracing.NewTracer(),
	}, txnSenderFactory, c.Clock)
	c.DB = db
	c.MaxGCBatchWait = time.Nanosecond
	return New(c)
}

// pushTxnSendFuncs allows the pushing of N txns across several invocations.
func pushTxnSendFuncs(sf *sendFuncs, N int) sendFunc {
	toPush := int64(N)
	var f sendFunc
	f = func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if remaining := atomic.LoadInt64(&toPush); len(ba.Requests) > int(remaining) {
			sf.t.Errorf("expected at most %d PushTxnRequests in batch, got %d",
				remaining, len(ba.Requests))
		}
		nowRemaining := atomic.AddInt64(&toPush, -1*int64(len(ba.Requests)))
		if nowRemaining > 0 {
			sf.pushFrontLocked(f)
		}
		return respForPushTxnBatch(sf.t, ba), nil
	}
	return f
}

func pushTxnSendFunc(t *testing.T, numPushes int) sendFunc {
	return func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if len(ba.Requests) != numPushes {
			t.Errorf("expected %d PushTxnRequests in batch, got %d",
				numPushes, len(ba.Requests))
		}
		return respForPushTxnBatch(t, ba), nil
	}
}

func singlePushTxnSendFunc(t *testing.T) sendFunc {
	return pushTxnSendFunc(t, 1)
}

func resolveIntentsSendFuncs(sf *sendFuncs, numIntents int, minRequests int) sendFunc {
	toResolve := int64(numIntents)
	reqsSeen := int64(0)
	var f sendFunc
	f = func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if remaining := atomic.LoadInt64(&toResolve); len(ba.Requests) > int(remaining) {
			sf.t.Errorf("expected at most %d ResolveIntentRequests in batch, got %d",
				remaining, len(ba.Requests))
		}
		nowRemaining := atomic.AddInt64(&toResolve, -1*int64(len(ba.Requests)))
		seen := atomic.AddInt64(&reqsSeen, 1)
		if nowRemaining > 0 {
			sf.pushFrontLocked(f)
		} else if seen < int64(minRequests) {
			sf.t.Errorf("expected at least %d requests to resolve %d intents, only saw %d",
				minRequests, numIntents, seen)
		}
		return respForResolveIntentBatch(sf.t, ba), nil
	}
	return f
}

func resolveIntentsSendFunc(t *testing.T) sendFunc {
	return func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		return respForResolveIntentBatch(t, ba), nil
	}
}

func failSendFunc(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	return nil, roachpb.NewError(fmt.Errorf("boom"))
}

func gcSendFunc(t *testing.T) sendFunc {
	return func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		resp := &roachpb.BatchResponse{}
		for _, r := range ba.Requests {
			if _, ok := r.GetInner().(*roachpb.GCRequest); !ok {
				t.Errorf("Unexpected request type %T, expected GCRequest", r.GetInner())
			}
			resp.Add(&roachpb.GCResponse{})
		}
		return resp, nil
	}
}

func respForPushTxnBatch(t *testing.T, ba roachpb.BatchRequest) *roachpb.BatchResponse {
	resp := &roachpb.BatchResponse{}
	for _, r := range ba.Requests {
		var txn enginepb.TxnMeta
		if req, ok := r.GetInner().(*roachpb.PushTxnRequest); ok {
			txn = req.PusheeTxn
		} else {
			t.Errorf("Unexpected request type %T, expected PushTxnRequest", r.GetInner())
		}
		resp.Add(&roachpb.PushTxnResponse{
			PusheeTxn: roachpb.Transaction{
				Status:  roachpb.ABORTED,
				TxnMeta: txn,
			},
		})
	}
	return resp
}

func respForResolveIntentBatch(t *testing.T, ba roachpb.BatchRequest) *roachpb.BatchResponse {
	resp := &roachpb.BatchResponse{}
	for _, r := range ba.Requests {
		if _, ok := r.GetInner().(*roachpb.ResolveIntentRequest); ok {
			resp.Add(&roachpb.ResolveIntentResponse{})
		} else if _, ok := r.GetInner().(*roachpb.ResolveIntentRangeRequest); ok {
			resp.Add(&roachpb.ResolveIntentRangeResponse{})
		} else {
			t.Errorf("Unexpected request in batch for intent resolution: %T", r.GetInner())
		}
	}
	return resp
}
