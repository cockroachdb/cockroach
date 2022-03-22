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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

// TestCleanupTxnIntentsOnGCAsync exercises the code which is used to
// asynchronously clean up transaction intents and then transaction records.
// This method is invoked from the MVCC GC queue.
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
		txn *roachpb.Transaction
		// intentSpans, if set, are appended to txn.LockSpans. They'll result in
		// ResolveIntent requests.
		intentSpans   []roachpb.Span
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
	txn1.LastHeartbeat = txn1.ReadTimestamp
	// Txn2 is in the staging state and is not old enough to have expired so the
	// code ought to send nothing.
	txn2 := newTransaction("txn2", key, 1, clock)
	txn2.Status = roachpb.STAGING
	// Txn3 is in the staging state but is expired.
	txn3 := newTransaction("txn3", key, 1, clock)
	txn3.Status = roachpb.STAGING
	txn3.ReadTimestamp.WallTime -= int64(100 * time.Second)
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
			intentSpans: []roachpb.Span{
				{Key: key},
				{Key: key, EndKey: roachpb.Key("b")},
			},
			sendFuncs: newSendFuncs(t,
				singlePushTxnSendFunc(t),
				resolveIntentsSendFuncEx(t, checkTxnAborted),
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
			intentSpans: []roachpb.Span{
				{Key: key},
				{Key: roachpb.Key("aa")},
				{Key: key, EndKey: roachpb.Key("b")},
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
			intentSpans: []roachpb.Span{
				{Key: key},
				{Key: key, EndKey: roachpb.Key("b")},
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
			intentSpans: []roachpb.Span{
				{Key: key},
				{Key: roachpb.Key("aa")},
				{Key: key, EndKey: roachpb.Key("b")},
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
			intentSpans:   []roachpb.Span{},
			sendFuncs:     newSendFuncs(t, gcSendFunc(t)),
			expectSucceed: true,
		},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			ir := newIntentResolverWithSendFuncs(cfg, c.sendFuncs, stopper)
			var didPush, didSucceed bool
			done := make(chan struct{})
			onComplete := func(pushed, succeeded bool) {
				didPush, didSucceed = pushed, succeeded
				close(done)
			}
			txn := c.txn.Clone()
			txn.LockSpans = append([]roachpb.Span{}, c.intentSpans...)
			err := ir.CleanupTxnIntentsOnGCAsync(ctx, 1, txn, clock.Now(), onComplete)
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
	ir := newIntentResolverWithSendFuncs(cfg, sf, stopper)
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
		roachpb.MakeIntent(&txn.TxnMeta, roachpb.Key("a")),
	}
	// Running with allowSyncProcessing = false should result in an error and no
	// requests being sent.
	err := ir.CleanupIntentsAsync(context.Background(), testIntents, false)
	assert.True(t, errors.Is(err, stop.ErrThrottled))
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
		roachpb.MakeIntent(&txn.TxnMeta, roachpb.Key("a")),
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
			ir := newIntentResolverWithSendFuncs(cfg, sf, stopper)
			err := ir.CleanupIntentsAsync(context.Background(), c.intents, true)
			sf.drain(t)
			stopper.Stop(context.Background())
			assert.Nil(t, err, "error from CleanupIntentsAsync")
		})
	}
}

// TestCleanupMultipleIntentsAsync verifies that CleanupIntentsAsync sends the
// expected requests when multiple intents are provided to it.
func TestCleanupMultipleIntentsAsync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	txn1 := newTransaction("txn1", roachpb.Key("a"), 1, clock)
	txn2 := newTransaction("txn2", roachpb.Key("c"), 1, clock)
	testIntents := []roachpb.Intent{
		roachpb.MakeIntent(&txn1.TxnMeta, roachpb.Key("a")),
		roachpb.MakeIntent(&txn1.TxnMeta, roachpb.Key("b")),
		roachpb.MakeIntent(&txn2.TxnMeta, roachpb.Key("c")),
		roachpb.MakeIntent(&txn2.TxnMeta, roachpb.Key("d")),
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
		TestingKnobs: kvserverbase.IntentResolverTestingKnobs{
			MaxIntentResolutionBatchSize: 1,
		},
	}
	ir := newIntentResolverWithSendFuncs(cfg, sf, stopper)
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
	txn.LockSpans = []roachpb.Span{
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
		return respForResolveIntentBatch(t, ba, dontCheckTxnStatus), nil
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
	ir := newIntentResolverWithSendFuncs(cfg, sf, stopper)

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
		sendFuncs *sendFuncs
	}
	testEndTxnIntents := []result.EndTxnIntents{
		{
			Txn: &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					ID:           uuid.MakeV4(),
					MinTimestamp: hlc.Timestamp{WallTime: 123},
				},
				LockSpans: []roachpb.Span{
					{Key: roachpb.Key("a")},
					{Key: roachpb.Key("b")},
					{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
					{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")},
				},
			},
		},
	}

	cases := []testCase{
		{
			intents:   testEndTxnIntents,
			sendFuncs: newSendFuncs(t),
			before: func(tc *testCase, ir *IntentResolver) func() {
				_, f := ir.lockInFlightTxnCleanup(context.Background(), tc.intents[0].Txn.ID)
				return f
			},
		},
		{
			intents: testEndTxnIntents,
			sendFuncs: func() *sendFuncs {
				s := newSendFuncs(t)
				s.pushFrontLocked(
					resolveIntentsSendFuncs(s, 4, 2),
					gcSendFunc(t),
				)
				return s
			}(),
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
			ir := newIntentResolverWithSendFuncs(cfg, c.sendFuncs, stopper)
			if c.before != nil {
				defer c.before(&c, ir)()
			}
			err := ir.CleanupTxnIntentsAsync(context.Background(), 1, c.intents, false)
			testutils.SucceedsSoon(t, func() error {
				if left := c.sendFuncs.len(); left != 0 {
					return fmt.Errorf("still waiting for %d calls", left)
				}
				return nil
			})
			stopper.Stop(context.Background())
			assert.Nil(t, err)
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
				LockSpans: []roachpb.Span{
					{Key: roachpb.Key("a")},
					{Key: roachpb.Key("b")},
					{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				},
			},
		},
		{
			Txn: &roachpb.Transaction{
				TxnMeta: txn2.TxnMeta,
				LockSpans: []roachpb.Span{
					{Key: roachpb.Key("e")},
					{Key: roachpb.Key("f")},
					{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")},
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
		case roachpb.ResolveIntentRange:
			reqs.Lock()
			req := ru.GetResolveIntentRange()
			reqs.resolved = append(reqs.resolved,
				fmt.Sprintf("%s-%s", string(req.Key), string(req.EndKey)))
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
	sf := newSendFuncs(t, repeat(resolveOrGCFunc, 8)...)

	stopper := stop.NewStopper()
	cfg := Config{
		Stopper: stopper,
		Clock:   clock,
		// Don't let the transaction record GC requests or the intent resolution
		// requests be batched with each other. This would make it harder to
		// determine how to drain sf.
		TestingKnobs: kvserverbase.IntentResolverTestingKnobs{
			MaxGCBatchSize:               1,
			MaxIntentResolutionBatchSize: 1,
		},
	}
	ir := newIntentResolverWithSendFuncs(cfg, sf, stopper)
	err := ir.CleanupTxnIntentsAsync(ctx, 1, testEndTxnIntents, false)
	sf.drain(t)
	stopper.Stop(ctx)
	assert.Nil(t, err)

	// All four intents should be resolved and both txn records should be GCed.
	sort.Strings(reqs.resolved)
	sort.Strings(reqs.gced)
	assert.Equal(t, []string{"a", "b", "c-d", "e", "f", "g-h"}, reqs.resolved)
	assert.Equal(t, []string{"a", "c"}, reqs.gced)
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
		roachpb.MakeIntent(&txn.TxnMeta, roachpb.Key("a")),
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
			ir := newIntentResolverWithSendFuncs(c.cfg, c.sendFuncs, stopper)
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
	txn := roachpb.MakeTransaction(name, baseKey, userPriority, now, offset, 1 /* coordinatorNodeID */)
	return &txn
}

// makeTxnIntents creates a slice of Intent which each have a unique txn.
func makeTxnIntents(t *testing.T, clock *hlc.Clock, numIntents int) []roachpb.Intent {
	ret := make([]roachpb.Intent, 0, numIntents)
	for i := 0; i < numIntents; i++ {
		txn := newTransaction("test", roachpb.Key("a"), 1, clock)
		ret = append(ret,
			roachpb.MakeIntent(&txn.TxnMeta, txn.Key))
	}
	return ret
}

// sendFunc is a function used to control behavior for a specific request that
// the IntentResolver tries to send. They are used in conjunction with the below
// function to create an IntentResolver with a slice of sendFuncs.
// A library of useful sendFuncs are defined below.
type sendFunc func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)

func newIntentResolverWithSendFuncs(
	c Config, sf *sendFuncs, stopper *stop.Stopper,
) *IntentResolver {
	txnSenderFactory := kv.NonTransactionalFactoryFunc(
		func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			sf.mu.Lock()
			defer sf.mu.Unlock()
			f := sf.popLocked()
			return f(ba)
		})
	db := kv.NewDB(log.MakeTestingAmbientCtxWithNewTracer(), txnSenderFactory, c.Clock, stopper)
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

// checkTxnStatusOpt specifies whether some mock handlers for ResolveIntent(s)
// request should assert the intent's status before resolving it, or not.
type checkTxnStatusOpt bool

const (
	// checkTxnAborted makes the mock ResolveIntent check that the intent's txn is
	// aborted (and so the intent would be discarded by the production code).
	checkTxnAborted checkTxnStatusOpt = true

	// NOTE: There should be a checkTxnCommitted option, but no test currently
	// uses it.

	// A bunch of tests use dontCheckTxnStatus because they take shortcuts that
	// causes intents to not be cleaned with a txn that was properly finalized.
	dontCheckTxnStatus checkTxnStatusOpt = false
)

func resolveIntentsSendFuncsEx(
	sf *sendFuncs, numIntents int, minRequests int, opt checkTxnStatusOpt,
) sendFunc {
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
		return respForResolveIntentBatch(sf.t, ba, opt), nil
	}
	return f
}

func resolveIntentsSendFuncEx(t *testing.T, checkTxnStatusOpt checkTxnStatusOpt) sendFunc {
	return func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		return respForResolveIntentBatch(t, ba, checkTxnStatusOpt), nil
	}
}

// resolveIntentsSendFuncs is like resolveIntentsSendFuncsEx, except it never checks
// the intents' txn status.
func resolveIntentsSendFuncs(sf *sendFuncs, numIntents int, minRequests int) sendFunc {
	return resolveIntentsSendFuncsEx(sf, numIntents, minRequests, dontCheckTxnStatus)
}

// resolveIntentsSendFunc is like resolveIntentsSendFuncEx, but it never checks
// the intents' txn status.
func resolveIntentsSendFunc(t *testing.T) sendFunc {
	return resolveIntentsSendFuncEx(t, dontCheckTxnStatus)
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

func respForResolveIntentBatch(
	t *testing.T, ba roachpb.BatchRequest, checkTxnStatusOpt checkTxnStatusOpt,
) *roachpb.BatchResponse {
	resp := &roachpb.BatchResponse{}
	var status roachpb.TransactionStatus
	for _, r := range ba.Requests {
		if rir, ok := r.GetInner().(*roachpb.ResolveIntentRequest); ok {
			status = rir.AsLockUpdate().Status
			resp.Add(&roachpb.ResolveIntentResponse{})
		} else if rirr, ok := r.GetInner().(*roachpb.ResolveIntentRangeRequest); ok {
			status = rirr.AsLockUpdate().Status
			resp.Add(&roachpb.ResolveIntentRangeResponse{})
		} else {
			t.Errorf("Unexpected request in batch for intent resolution: %T", r.GetInner())
		}
	}
	if checkTxnStatusOpt == checkTxnAborted && status != roachpb.ABORTED {
		t.Errorf("expected txn to be finalized, got status: %s", status)
	}
	return resp
}
