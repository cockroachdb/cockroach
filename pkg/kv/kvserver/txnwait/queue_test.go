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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldPushImmediately(t *testing.T) {
	defer leaktest.AfterTest(t)()

	min := enginepb.MinTxnPriority
	max := enginepb.MaxTxnPriority
	mid1 := enginepb.TxnPriority(1)
	mid2 := enginepb.TxnPriority(2)
	testCases := []struct {
		force      bool
		typ        kvpb.PushTxnType
		pusherPri  enginepb.TxnPriority
		pusheePri  enginepb.TxnPriority
		shouldPush bool
	}{
		{false, kvpb.PUSH_ABORT, min, min, false},
		{false, kvpb.PUSH_ABORT, min, mid1, false},
		{false, kvpb.PUSH_ABORT, min, mid2, false},
		{false, kvpb.PUSH_ABORT, min, max, false},
		{false, kvpb.PUSH_ABORT, mid1, min, true},
		{false, kvpb.PUSH_ABORT, mid1, mid1, false},
		{false, kvpb.PUSH_ABORT, mid1, mid2, false},
		{false, kvpb.PUSH_ABORT, mid1, max, false},
		{false, kvpb.PUSH_ABORT, mid2, min, true},
		{false, kvpb.PUSH_ABORT, mid2, mid1, false},
		{false, kvpb.PUSH_ABORT, mid2, mid2, false},
		{false, kvpb.PUSH_ABORT, mid2, max, false},
		{false, kvpb.PUSH_ABORT, max, min, true},
		{false, kvpb.PUSH_ABORT, max, mid1, true},
		{false, kvpb.PUSH_ABORT, max, mid2, true},
		{false, kvpb.PUSH_ABORT, max, max, false},
		{false, kvpb.PUSH_TIMESTAMP, min, min, false},
		{false, kvpb.PUSH_TIMESTAMP, min, mid1, false},
		{false, kvpb.PUSH_TIMESTAMP, min, mid2, false},
		{false, kvpb.PUSH_TIMESTAMP, min, max, false},
		{false, kvpb.PUSH_TIMESTAMP, mid1, min, true},
		{false, kvpb.PUSH_TIMESTAMP, mid1, mid1, false},
		{false, kvpb.PUSH_TIMESTAMP, mid1, mid2, false},
		{false, kvpb.PUSH_TIMESTAMP, mid1, max, false},
		{false, kvpb.PUSH_TIMESTAMP, mid2, min, true},
		{false, kvpb.PUSH_TIMESTAMP, mid2, mid1, false},
		{false, kvpb.PUSH_TIMESTAMP, mid2, mid2, false},
		{false, kvpb.PUSH_TIMESTAMP, mid2, max, false},
		{false, kvpb.PUSH_TIMESTAMP, max, min, true},
		{false, kvpb.PUSH_TIMESTAMP, max, mid1, true},
		{false, kvpb.PUSH_TIMESTAMP, max, mid2, true},
		{false, kvpb.PUSH_TIMESTAMP, max, max, false},
		{false, kvpb.PUSH_TOUCH, min, min, true},
		{false, kvpb.PUSH_TOUCH, min, mid1, true},
		{false, kvpb.PUSH_TOUCH, min, mid2, true},
		{false, kvpb.PUSH_TOUCH, min, max, true},
		{false, kvpb.PUSH_TOUCH, mid1, min, true},
		{false, kvpb.PUSH_TOUCH, mid1, mid1, true},
		{false, kvpb.PUSH_TOUCH, mid1, mid2, true},
		{false, kvpb.PUSH_TOUCH, mid1, max, true},
		{false, kvpb.PUSH_TOUCH, mid2, min, true},
		{false, kvpb.PUSH_TOUCH, mid2, mid1, true},
		{false, kvpb.PUSH_TOUCH, mid2, mid2, true},
		{false, kvpb.PUSH_TOUCH, mid2, max, true},
		{false, kvpb.PUSH_TOUCH, max, min, true},
		{false, kvpb.PUSH_TOUCH, max, mid1, true},
		{false, kvpb.PUSH_TOUCH, max, mid2, true},
		{false, kvpb.PUSH_TOUCH, max, max, true},
		// Force pushes always push immediately.
		{true, kvpb.PUSH_ABORT, min, min, true},
		{true, kvpb.PUSH_ABORT, min, mid1, true},
		{true, kvpb.PUSH_ABORT, min, mid2, true},
		{true, kvpb.PUSH_ABORT, min, max, true},
		{true, kvpb.PUSH_ABORT, mid1, min, true},
		{true, kvpb.PUSH_ABORT, mid1, mid1, true},
		{true, kvpb.PUSH_ABORT, mid1, mid2, true},
		{true, kvpb.PUSH_ABORT, mid1, max, true},
		{true, kvpb.PUSH_ABORT, mid2, min, true},
		{true, kvpb.PUSH_ABORT, mid2, mid1, true},
		{true, kvpb.PUSH_ABORT, mid2, mid2, true},
		{true, kvpb.PUSH_ABORT, mid2, max, true},
		{true, kvpb.PUSH_ABORT, max, min, true},
		{true, kvpb.PUSH_ABORT, max, mid1, true},
		{true, kvpb.PUSH_ABORT, max, mid2, true},
		{true, kvpb.PUSH_ABORT, max, max, true},
		{true, kvpb.PUSH_TIMESTAMP, min, min, true},
		{true, kvpb.PUSH_TIMESTAMP, min, mid1, true},
		{true, kvpb.PUSH_TIMESTAMP, min, mid2, true},
		{true, kvpb.PUSH_TIMESTAMP, min, max, true},
		{true, kvpb.PUSH_TIMESTAMP, mid1, min, true},
		{true, kvpb.PUSH_TIMESTAMP, mid1, mid1, true},
		{true, kvpb.PUSH_TIMESTAMP, mid1, mid2, true},
		{true, kvpb.PUSH_TIMESTAMP, mid1, max, true},
		{true, kvpb.PUSH_TIMESTAMP, mid2, min, true},
		{true, kvpb.PUSH_TIMESTAMP, mid2, mid1, true},
		{true, kvpb.PUSH_TIMESTAMP, mid2, mid2, true},
		{true, kvpb.PUSH_TIMESTAMP, mid2, max, true},
		{true, kvpb.PUSH_TIMESTAMP, max, min, true},
		{true, kvpb.PUSH_TIMESTAMP, max, mid1, true},
		{true, kvpb.PUSH_TIMESTAMP, max, mid2, true},
		{true, kvpb.PUSH_TIMESTAMP, max, max, true},
		{true, kvpb.PUSH_TOUCH, min, min, true},
		{true, kvpb.PUSH_TOUCH, min, mid1, true},
		{true, kvpb.PUSH_TOUCH, min, mid2, true},
		{true, kvpb.PUSH_TOUCH, min, max, true},
		{true, kvpb.PUSH_TOUCH, mid1, min, true},
		{true, kvpb.PUSH_TOUCH, mid1, mid1, true},
		{true, kvpb.PUSH_TOUCH, mid1, mid2, true},
		{true, kvpb.PUSH_TOUCH, mid1, max, true},
		{true, kvpb.PUSH_TOUCH, mid2, min, true},
		{true, kvpb.PUSH_TOUCH, mid2, mid1, true},
		{true, kvpb.PUSH_TOUCH, mid2, mid2, true},
		{true, kvpb.PUSH_TOUCH, mid2, max, true},
		{true, kvpb.PUSH_TOUCH, max, min, true},
		{true, kvpb.PUSH_TOUCH, max, mid1, true},
		{true, kvpb.PUSH_TOUCH, max, mid2, true},
		{true, kvpb.PUSH_TOUCH, max, max, true},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			req := kvpb.PushTxnRequest{
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
			shouldPush := ShouldPushImmediately(&req)
			require.Equal(t, test.shouldPush, shouldPush)
		})
	}
}

func TestCanPushWithPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()

	min := enginepb.MinTxnPriority
	max := enginepb.MaxTxnPriority
	mid1 := enginepb.TxnPriority(1)
	mid2 := enginepb.TxnPriority(2)
	testCases := []struct {
		pusher enginepb.TxnPriority
		pushee enginepb.TxnPriority
		exp    bool
	}{
		{min, min, false},
		{min, mid1, false},
		{min, mid2, false},
		{min, max, false},
		{mid1, min, true},
		{mid1, mid1, false},
		{mid1, mid2, false},
		{mid1, max, false},
		{mid2, min, true},
		{mid2, mid1, false},
		{mid2, mid2, false},
		{mid2, max, false},
		{max, min, true},
		{max, mid1, true},
		{max, mid2, true},
		{max, max, false},
	}
	for _, test := range testCases {
		name := fmt.Sprintf("pusher=%d/pushee=%d", test.pusher, test.pushee)
		t.Run(name, func(t *testing.T) {
			canPush := CanPushWithPriority(test.pusher, test.pushee)
			require.Equal(t, test.exp, canPush)
		})
	}
}

func makeTS(w int64, l int32) hlc.Timestamp {
	return hlc.Timestamp{WallTime: w, Logical: l}
}

func TestIsPushed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		typ          kvpb.PushTxnType
		pushTo       hlc.Timestamp
		txnStatus    roachpb.TransactionStatus
		txnTimestamp hlc.Timestamp
		isPushed     bool
	}{
		{kvpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.PENDING, hlc.Timestamp{}, false},
		{kvpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.STAGING, hlc.Timestamp{}, false},
		{kvpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.ABORTED, hlc.Timestamp{}, true},
		{kvpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.COMMITTED, hlc.Timestamp{}, true},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, hlc.Timestamp{}, false},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, hlc.Timestamp{}, false},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.ABORTED, hlc.Timestamp{}, true},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.COMMITTED, hlc.Timestamp{}, true},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 0), false},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 1), true},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 2), true},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, makeTS(10, 0), false},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, makeTS(10, 1), true},
		{kvpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, makeTS(10, 2), true},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			req := kvpb.PushTxnRequest{
				PushType: test.typ,
				PushTo:   test.pushTo,
			}
			txn := roachpb.Transaction{
				Status: test.txnStatus,
				TxnMeta: enginepb.TxnMeta{
					WriteTimestamp: test.txnTimestamp,
				},
			}
			isPushed := isPushed(&req, &txn)
			require.Equal(t, test.isPushed, isPushed)
		})
	}
}

func makeConfig(s kv.SenderFunc, stopper *stop.Stopper) Config {
	var cfg Config
	cfg.RangeDesc = &roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin, EndKey: roachpb.RKeyMax,
	}
	cfg.Clock = hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123)))
	cfg.Stopper = stopper
	cfg.Metrics = NewMetrics(time.Minute)
	if s != nil {
		factory := kv.NonTransactionalFactoryFunc(s)
		cfg.DB = kv.NewDB(log.MakeTestingAmbientCtxWithNewTracer(), factory, cfg.Clock, stopper)
	}
	return cfg
}

// TestMaybeWaitForPushWithContextCancellation adds a new waiting push to the
// queue and cancels its context. It then verifies that the push was cleaned up
// and that this was properly reflected in the metrics.
func TestMaybeWaitForPushWithContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	var mockSender kv.SenderFunc
	cfg := makeConfig(func(
		ctx context.Context, ba *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error) {
		return mockSender(ctx, ba)
	}, stopper)
	q := NewQueue(cfg)
	q.Enable(1 /* leaseSeq */)

	// Enqueue pushee transaction in the queue.
	txn := roachpb.MakeTransaction("test", nil, 0, cfg.Clock.Now(), 0, 0)
	q.EnqueueTxn(&txn)

	// Mock out responses to any QueryTxn requests.
	mockSender = func(
		ctx context.Context, ba *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error) {
		br := ba.CreateReply()
		resp := br.Responses[0].GetInner().(*kvpb.QueryTxnResponse)
		resp.QueriedTxn = txn
		return br, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	waitingRes := make(chan *kvpb.Error)
	go func() {
		req := kvpb.PushTxnRequest{PusheeTxn: txn.TxnMeta, PushType: kvpb.PUSH_ABORT}
		_, err := q.MaybeWaitForPush(ctx, &req)
		waitingRes <- err
	}()

	cancel()
	pErr := <-waitingRes
	require.NotNil(t, pErr)
	s := pErr.String()
	_ = s
	require.Regexp(t, context.Canceled.Error(), pErr)
	require.Equal(t, 0, q.mu.txns[txn.ID].waitingPushes.Len())

	m := cfg.Metrics
	require.Equal(t, int64(1), m.PusheeWaiting.Value())
	require.Equal(t, int64(0), m.PusherWaiting.Value())
	require.Equal(t, int64(0), m.QueryWaiting.Value())
	require.Equal(t, int64(0), m.PusherSlow.Value())
}

// TestMaybeWaitForQueryWithContextCancellation adds a new waiting query to the
// queue and cancels its context. It then verifies that the query was cleaned up
// and that this was properly reflected in the metrics. Regression test against
// #28849, before which the waiting query would leak.
func TestMaybeWaitForQueryWithContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := makeConfig(nil, stopper)
	q := NewQueue(cfg)
	q.Enable(1 /* leaseSeq */)

	ctx, cancel := context.WithCancel(context.Background())
	waitingRes := make(chan *kvpb.Error)
	go func() {
		req := &kvpb.QueryTxnRequest{WaitForUpdate: true}
		waitingRes <- q.MaybeWaitForQuery(ctx, req)
	}()

	cancel()
	pErr := <-waitingRes
	require.NotNil(t, pErr)
	require.Regexp(t, context.Canceled.Error(), pErr)
	require.Equal(t, 0, len(q.mu.queries))

	m := cfg.Metrics
	require.Equal(t, int64(0), m.PusheeWaiting.Value())
	require.Equal(t, int64(0), m.PusherWaiting.Value())
	require.Equal(t, int64(0), m.QueryWaiting.Value())
	require.Equal(t, int64(0), m.PusherSlow.Value())
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
		ctx context.Context, ba *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error) {
		return mockSender(ctx, ba)
	}, stopper)
	q := NewQueue(cfg)
	q.Enable(1 /* leaseSeq */)

	// Set an extremely high transaction liveness threshold so that the pushee
	// is only queried once per pusher.
	defer TestingOverrideTxnLivenessThreshold(time.Hour)()

	// Enqueue pushee transaction in the queue.
	txn := roachpb.MakeTransaction("test", nil, 0, cfg.Clock.Now(), 0, 0)
	q.EnqueueTxn(&txn)

	const numPushees = 3
	var queryTxnCount int32
	mockSender = func(
		ctx context.Context, ba *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error) {
		br := ba.CreateReply()
		resp := br.Responses[0].GetInner().(*kvpb.QueryTxnResponse)
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
			req := kvpb.PushTxnRequest{PusheeTxn: txn.TxnMeta, PushType: kvpb.PUSH_ABORT}
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
		ctx context.Context, ba *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error) {
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
		br   *kvpb.BatchResponse
		pErr *kvpb.Error
	}
	type request struct {
		ba     *kvpb.BatchRequest
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
		resp *kvpb.PushTxnResponse
		pErr *kvpb.Error
	}
	runPush := func(pusher *roachpb.Transaction, pushee enginepb.TxnMeta) chan pushResult {
		respCh := make(chan pushResult, 1)
		go func() {
			resp, pErr := q.MaybeWaitForPush(ctx,
				&kvpb.PushTxnRequest{
					PusherTxn: *pusher,
					PusheeTxn: pushee,
					PushType:  kvpb.PUSH_ABORT,
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

	txnB := roachpb.MakeTransaction("B", roachpb.Key("B"), 0, cfg.Clock.Now(), 0, 1)
	txnA := roachpb.MakeTransaction("A", roachpb.Key("A"), 0, cfg.Clock.Now(), 0, 1)

	txnA1 := roachpb.MakeTransaction("A1", roachpb.Key("A1"), 0, txnA.ReadTimestamp, 0, 1)
	txnA1.Parent = clone(&txnA)
	txnA1.Priority = txnA.Priority
	txnA2 := roachpb.MakeTransaction("A2", nil, 0, txnA.ReadTimestamp, 0, 1)
	txnA2.Parent = clone(&txnA1)
	txnA2.Priority = txnA.Priority
	txnA3 := roachpb.MakeTransaction("A3", roachpb.Key("A3"), 0, txnA.ReadTimestamp, 0, 1)
	txnA3.Parent = clone(&txnA2)
	txnA3.Priority = txnA.Priority

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
	mockSender = func(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		query, ok := ba.Requests[0].GetInner().(*kvpb.QueryTxnRequest)
		if !ok {
			respCh := make(chan response)
			req := request{
				ba:     ba,
				respCh: respCh,
			}
			select {
			case reqCh <- req:
			case <-ctx.Done():
				return nil, kvpb.NewError(ctx.Err())
			}
			select {
			case resp := <-respCh:
				return resp.br, resp.pErr
			case <-ctx.Done():
				return nil, kvpb.NewError(ctx.Err())
			}
		}
		seenQueries.Lock()
		defer seenQueries.Unlock()
		queried, ok := expectedQueryIDs[query.Txn.ID]
		require.Truef(t, ok, "unexpected ID %b", query.Txn.ID)
		br := kvpb.BatchResponse{}
		deps := q.GetDependents(query.Txn.ID)
		br.Add(&kvpb.QueryTxnResponse{
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
	forcePushReq, ok := req.ba.Requests[0].GetInner().(*kvpb.PushTxnRequest)
	require.Truef(t, ok, "%T", req.ba.Requests[0].GetInner())
	require.True(t, forcePushReq.Force)
	var br kvpb.BatchResponse
	var pushee *roachpb.Transaction
	bIsPusher := txnA.Priority < txnB.Priority ||
		(txnA.Priority == txnB.Priority && bytes.Compare(txnA.ID[:], txnB.ID[:]) < 0)
	if bIsPusher {
		require.Equal(t, forcePushReq.PusherTxn.ID, txnB.ID)
		pushee = &txnA
	} else {
		require.Equal(t, forcePushReq.PusherTxn.ID, txnA3.ID)
		pushee = &txnB
	}
	seenQueries.Lock()
	pushee.Status = roachpb.ABORTED
	seenQueries.Unlock()
	q.UpdateTxn(ctx, pushee)
	br.Add(&kvpb.PushTxnResponse{PusheeTxn: *pushee})
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
