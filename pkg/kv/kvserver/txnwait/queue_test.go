// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnwait

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldPushImmediately(t *testing.T) {
	defer leaktest.AfterTest(t)()

	min := enginepb.MinTxnPriority
	max := enginepb.MaxTxnPriority
	mid := enginepb.TxnPriority(1)
	testCases := []struct {
		force      bool
		typ        roachpb.PushTxnType
		pusherPri  enginepb.TxnPriority
		pusheePri  enginepb.TxnPriority
		shouldPush bool
	}{
		{false, roachpb.PUSH_ABORT, min, min, false},
		{false, roachpb.PUSH_ABORT, min, mid, false},
		{false, roachpb.PUSH_ABORT, min, max, false},
		{false, roachpb.PUSH_ABORT, mid, min, true},
		{false, roachpb.PUSH_ABORT, mid, mid, false},
		{false, roachpb.PUSH_ABORT, mid, max, false},
		{false, roachpb.PUSH_ABORT, max, min, true},
		{false, roachpb.PUSH_ABORT, max, mid, true},
		{false, roachpb.PUSH_ABORT, max, max, false},
		{false, roachpb.PUSH_TIMESTAMP, min, min, false},
		{false, roachpb.PUSH_TIMESTAMP, min, mid, false},
		{false, roachpb.PUSH_TIMESTAMP, min, max, false},
		{false, roachpb.PUSH_TIMESTAMP, mid, min, true},
		{false, roachpb.PUSH_TIMESTAMP, mid, mid, false},
		{false, roachpb.PUSH_TIMESTAMP, mid, max, false},
		{false, roachpb.PUSH_TIMESTAMP, max, min, true},
		{false, roachpb.PUSH_TIMESTAMP, max, mid, true},
		{false, roachpb.PUSH_TIMESTAMP, max, max, false},
		{false, roachpb.PUSH_TOUCH, min, min, true},
		{false, roachpb.PUSH_TOUCH, min, mid, true},
		{false, roachpb.PUSH_TOUCH, min, max, true},
		{false, roachpb.PUSH_TOUCH, mid, min, true},
		{false, roachpb.PUSH_TOUCH, mid, mid, true},
		{false, roachpb.PUSH_TOUCH, mid, max, true},
		{false, roachpb.PUSH_TOUCH, max, min, true},
		{false, roachpb.PUSH_TOUCH, max, mid, true},
		{false, roachpb.PUSH_TOUCH, max, max, true},
		// Force pushes always push immediately.
		{true, roachpb.PUSH_ABORT, min, min, true},
		{true, roachpb.PUSH_ABORT, min, mid, true},
		{true, roachpb.PUSH_ABORT, min, max, true},
		{true, roachpb.PUSH_ABORT, mid, min, true},
		{true, roachpb.PUSH_ABORT, mid, mid, true},
		{true, roachpb.PUSH_ABORT, mid, max, true},
		{true, roachpb.PUSH_ABORT, max, min, true},
		{true, roachpb.PUSH_ABORT, max, mid, true},
		{true, roachpb.PUSH_ABORT, max, max, true},
		{true, roachpb.PUSH_TIMESTAMP, min, min, true},
		{true, roachpb.PUSH_TIMESTAMP, min, mid, true},
		{true, roachpb.PUSH_TIMESTAMP, min, max, true},
		{true, roachpb.PUSH_TIMESTAMP, mid, min, true},
		{true, roachpb.PUSH_TIMESTAMP, mid, mid, true},
		{true, roachpb.PUSH_TIMESTAMP, mid, max, true},
		{true, roachpb.PUSH_TIMESTAMP, max, min, true},
		{true, roachpb.PUSH_TIMESTAMP, max, mid, true},
		{true, roachpb.PUSH_TIMESTAMP, max, max, true},
		{true, roachpb.PUSH_TOUCH, min, min, true},
		{true, roachpb.PUSH_TOUCH, min, mid, true},
		{true, roachpb.PUSH_TOUCH, min, max, true},
		{true, roachpb.PUSH_TOUCH, mid, min, true},
		{true, roachpb.PUSH_TOUCH, mid, mid, true},
		{true, roachpb.PUSH_TOUCH, mid, max, true},
		{true, roachpb.PUSH_TOUCH, max, min, true},
		{true, roachpb.PUSH_TOUCH, max, mid, true},
		{true, roachpb.PUSH_TOUCH, max, max, true},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			req := roachpb.PushTxnRequest{
				Force:    test.force,
				PushType: test.typ,
				PusherTxn: roachpb.Transaction{
					TxnMeta: enginepb.TxnMeta{
						Priority: test.pusherPri,
					},
				},
				PusheeTxn: enginepb.TxnMeta{
					Priority: test.pusheePri,
				},
			}
			if shouldPush := ShouldPushImmediately(&req); shouldPush != test.shouldPush {
				t.Errorf("expected %t; got %t", test.shouldPush, shouldPush)
			}
		})
	}
}

func makeTS(w int64, l int32) hlc.Timestamp {
	return hlc.Timestamp{WallTime: w, Logical: l}
}

func TestIsPushed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		typ          roachpb.PushTxnType
		pushTo       hlc.Timestamp
		txnStatus    roachpb.TransactionStatus
		txnTimestamp hlc.Timestamp
		isPushed     bool
	}{
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.PENDING, hlc.Timestamp{}, false},
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.STAGING, hlc.Timestamp{}, false},
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.ABORTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.COMMITTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, hlc.Timestamp{}, false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, hlc.Timestamp{}, false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.ABORTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.COMMITTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 0), false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 1), true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 2), true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, makeTS(10, 0), false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, makeTS(10, 1), true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, makeTS(10, 2), true},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			req := roachpb.PushTxnRequest{
				PushType: test.typ,
				PushTo:   test.pushTo,
			}
			txn := roachpb.Transaction{
				Status: test.txnStatus,
				TxnMeta: enginepb.TxnMeta{
					WriteTimestamp: test.txnTimestamp,
				},
			}
			if isPushed := isPushed(&req, &txn); isPushed != test.isPushed {
				t.Errorf("expected %t; got %t", test.isPushed, isPushed)
			}
		})
	}
}

func makeConfig(s kv.SenderFunc, stopper *stop.Stopper) Config {
	var cfg Config
	cfg.RangeDesc = &roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin, EndKey: roachpb.RKeyMax,
	}
	manual := hlc.NewManualClock(123)
	cfg.Clock = hlc.NewClock(manual.UnixNano, time.Nanosecond)
	cfg.Stopper = stopper
	cfg.Metrics = NewMetrics(time.Minute)
	if s != nil {
		factory := kv.NonTransactionalFactoryFunc(s)
		cfg.DB = kv.NewDB(testutils.MakeAmbientCtx(), factory, cfg.Clock, stopper)
	}
	return cfg
}

// TestMaybeWaitForQueryWithContextCancellation adds a new waiting query to the
// queue and cancels its context. It then verifies that the query was cleaned
// up. Regression test against #28849, before which the waiting query would
// leak.
func TestMaybeWaitForQueryWithContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := makeConfig(nil, stopper)
	q := NewQueue(cfg)
	q.Enable(1 /* leaseSeq */)

	waitingRes := make(chan *roachpb.Error)
	go func() {
		req := &roachpb.QueryTxnRequest{WaitForUpdate: true}
		waitingRes <- q.MaybeWaitForQuery(ctx, req)
	}()

	cancel()
	if pErr := <-waitingRes; !testutils.IsPError(pErr, "context canceled") {
		t.Errorf("unexpected error %v", pErr)
	}
	if len(q.mu.queries) != 0 {
		t.Errorf("expected no waiting queries, found %v", q.mu.queries)
	}

	metrics := cfg.Metrics
	allMetricsAreZero := metrics.PusheeWaiting.Value() == 0 &&
		metrics.PusherWaiting.Value() == 0 &&
		metrics.QueryWaiting.Value() == 0 &&
		metrics.PusherSlow.Value() == 0

	if !allMetricsAreZero {
		t.Errorf("expected all metric gauges to be zero, got some that aren't")
	}
}

// TestPushersReleasedAfterAnyQueryTxnFindsAbortedTxn tests that if any
// QueryTxn on a pushee txn returns an aborted transaction status, all
// pushees of that transaction are informed of the aborted status and
// released.
func TestPushersReleasedAfterAnyQueryTxnFindsAbortedTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	var mockSender kv.SenderFunc
	cfg := makeConfig(func(
		ctx context.Context, ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error) {
		return mockSender(ctx, ba)
	}, stopper)
	q := NewQueue(cfg)
	q.Enable(1 /* leaseSeq */)

	// Set an extremely high transaction liveness threshold so that the pushee
	// is only queried once per pusher.
	defer TestingOverrideTxnLivenessThreshold(time.Hour)()

	// Enqueue pushee transaction in the queue.
	txn := roachpb.MakeTransaction("test", nil, 0, cfg.Clock.Now(), 0)
	q.EnqueueTxn(&txn)

	const numPushees = 3
	var queryTxnCount int32
	mockSender = func(
		ctx context.Context, ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error) {
		br := ba.CreateReply()
		resp := br.Responses[0].GetInner().(*roachpb.QueryTxnResponse)
		resp.QueriedTxn = txn
		if atomic.AddInt32(&queryTxnCount, 1) == numPushees {
			// Only the last pusher's query observes an ABORTED transaction. As
			// mentioned in the corresponding comment in MaybeWaitForPush, this
			// isn't expected without an associated update to the pushee's
			// transaction record. However, it is possible if the pushee hasn't
			// written a transaction record yet and the timestamp cache loses
			// resolution due to memory pressure. While rare, we need to handle
			// this case correctly.
			resp.QueriedTxn.Status = roachpb.ABORTED
		}
		return br, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < numPushees; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			req := roachpb.PushTxnRequest{PusheeTxn: txn.TxnMeta, PushType: roachpb.PUSH_ABORT}
			res, err := q.MaybeWaitForPush(ctx, &req)
			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, roachpb.ABORTED, res.PusheeTxn.Status)
		}()
	}
	wg.Wait()
}

// TestChildTxnBehavior is a relatively ad-hoc and low-level test to ensure
// that the code behaves as expected in the face of transactions with parents.
//
// TODO(ajwerner): Refactor this to make it a bit less brittle so that subtests
// can exercise some different request flows.
func TestChildTxnBehavior(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	var mockSender kv.SenderFunc
	cfg := makeConfig(func(
		ctx context.Context, ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error) {
		return mockSender(ctx, ba)
	}, stopper)
	q := NewQueue(cfg)
	q.Enable(1 /* leaseSeq */)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set an extremely high transaction liveness threshold so that the pushee
	// is only queried once per pusher.
	defer TestingOverrideTxnLivenessThreshold(time.Hour)()

	type response struct {
		br   *roachpb.BatchResponse
		pErr *roachpb.Error
	}
	type request struct {
		ba     *roachpb.BatchRequest
		respCh chan<- response
	}
	reqCh := make(chan request)

	clone := func(txn *roachpb.Transaction) *roachpb.Transaction {
		t.Helper()
		encoded, err := protoutil.Marshal(txn)
		require.NoError(t, err)
		var clone roachpb.Transaction
		require.NoError(t, protoutil.Unmarshal(encoded, &clone))
		return &clone
	}
	type pushResult struct {
		resp *roachpb.PushTxnResponse
		pErr *roachpb.Error
	}
	runPush := func(pusher *roachpb.Transaction, pushee enginepb.TxnMeta) chan pushResult {
		respCh := make(chan pushResult, 1)
		go func() {
			resp, pErr := q.MaybeWaitForPush(ctx,
				&roachpb.PushTxnRequest{
					PusherTxn: *pusher,
					PusheeTxn: pushee,
					PushType:  roachpb.PUSH_ABORT,
				})
			respCh <- pushResult{resp: resp, pErr: pErr}
		}()
		return respCh
	}

	// We're going to create a cycle whereby A is the great-grandparent of A3
	// which is pushing B which is blocked on A. A2 does not have a key and
	// thus, is not queried.
	//
	//  < - - : QueryTxn
	//  <-----: PushTxn
	//
	//   / - - - - - - - - - - - \
	//  |     - - - - - - - - - \ \
	//  v    v            v - - \||
	// [A]  [A1]  [A2]  [A3]---->[B]
	//  ^_______________________/

	txnB := roachpb.MakeTransaction("B", roachpb.Key("B"), 0, cfg.Clock.Now(), 0)
	txnA := roachpb.MakeTransaction("A", roachpb.Key("A"), 0, cfg.Clock.Now(), 0)

	txnA1 := roachpb.MakeTransaction("A1", roachpb.Key("A1"), 0, txnA.ReadTimestamp, 0)
	txnA1.Parent = clone(&txnA)
	txnA2 := roachpb.MakeTransaction("A2", nil, 0, txnA.ReadTimestamp, 0)
	txnA2.Parent = clone(&txnA1)
	txnA3 := roachpb.MakeTransaction("A2", roachpb.Key("A3"), 0, txnA.ReadTimestamp, 0)
	txnA3.Parent = clone(&txnA2)

	// We want to see a request for each of txnA, txnA1, txnA2, and txnB.
	expectedQueryIDs := map[uuid.UUID]*roachpb.Transaction{
		txnB.ID:  &txnB,
		txnA.ID:  &txnA,
		txnA1.ID: &txnA1,
		txnA3.ID: &txnA3,
	}
	seenQueries := struct {
		syncutil.Mutex
		ids map[uuid.UUID]struct{}
	}{
		ids: map[uuid.UUID]struct{}{},
	}
	mockSender = func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		query, ok := ba.Requests[0].GetInner().(*roachpb.QueryTxnRequest)
		if !ok {
			respCh := make(chan response)
			req := request{
				ba:     &ba,
				respCh: respCh,
			}
			select {
			case reqCh <- req:
			case <-ctx.Done():
				return nil, roachpb.NewError(ctx.Err())
			}
			select {
			case resp := <-respCh:
				return resp.br, resp.pErr
			case <-ctx.Done():
				return nil, roachpb.NewError(ctx.Err())
			}
		}
		seenQueries.Lock()
		defer seenQueries.Unlock()
		queried, ok := expectedQueryIDs[query.Txn.ID]
		require.Truef(t, ok, "unexpected ID %b", query.Txn.ID)
		br := roachpb.BatchResponse{}
		deps := q.GetDependents(query.Txn.ID)
		br.Add(&roachpb.QueryTxnResponse{
			QueriedTxn:      *queried,
			TxnRecordExists: true,
			WaitingTxns:     deps,
		})
		seenQueries.ids[query.Txn.ID] = struct{}{}
		return &br, nil
	}

	// Enqueue pushee transaction in the queue.
	q.EnqueueTxn(&txnB)
	q.EnqueueTxn(&txnA)
	pushBCh := runPush(clone(&txnA3), txnB.TxnMeta)
	// There's nothing interesting to return to any of them at this point.
	// In practice what would happen is that the requests would block until the
	// maxWait (50ms) and then would query the transactions and discover that
	// there are no dependencies. If, in the meantime, a push from B came in to
	// any of the three transaction, then a deadlock would be detected because
	// the QueryTxn
	testutils.SucceedsSoon(t, func() error {
		seenQueries.Lock()
		defer seenQueries.Unlock()
		if !assert.EqualValues(noopT{}, seenQueries.ids, map[uuid.UUID]struct{}{
			txnA.ID:  {},
			txnA1.ID: {},
			txnA3.ID: {},
			txnB.ID:  {},
		}) {
			return errors.Errorf("haven't seen them all yet: %v", seenQueries.ids)
		}
		return nil
	})

	// We do not expect the push of B to be complete.
	select {
	case pushDone := <-pushBCh:
		t.Fatalf("unexpected push done: %v", pushDone)
	case req := <-reqCh:
		t.Fatalf("unexpected req: %v", req)
	default:
	}

	pushACh := runPush(clone(&txnB), txnA.TxnMeta)

	// Now we expect one of the two transactions to abort soon.
	req := <-reqCh
	forcePushReq, ok := req.ba.Requests[0].GetInner().(*roachpb.PushTxnRequest)
	require.Truef(t, ok, "%T", req.ba.Requests[0].GetInner())
	require.True(t, forcePushReq.Force)
	var br roachpb.BatchResponse
	var pushee *roachpb.Transaction
	bIsPusher := txnA.Priority < txnB.Priority ||
		(txnA.Priority == txnB.Priority && bytes.Compare(txnA.ID[:], txnB.ID[:]) < 0)
	if bIsPusher {
		require.Equal(t, forcePushReq.PusherTxn.ID, txnB.ID)
		pushee = &txnA
	} else {
		require.Equal(t, forcePushReq.PusherTxn.ID, txnA.ID)
		pushee = &txnB
	}
	seenQueries.Lock()
	pushee.Status = roachpb.ABORTED
	seenQueries.Unlock()
	q.UpdateTxn(ctx, pushee)
	br.Add(&roachpb.PushTxnResponse{PusheeTxn: *pushee})
	req.respCh <- response{br: &br}
	var pusherRes, abortedRes pushResult
	if bIsPusher {
		pusherRes, abortedRes = <-pushACh, <-pushBCh
	} else {
		pusherRes, abortedRes = <-pushBCh, <-pushACh
	}
	require.Nil(t, pusherRes.pErr)
	require.NotNil(t, abortedRes.pErr)
}

// noopT implements assert.TestingT so that its tests can be used without
// failing the test inside a SucceedsSoon.
type noopT struct{}

func (n noopT) Errorf(format string, args ...interface{}) {}

var _ assert.TestingT = noopT{}
