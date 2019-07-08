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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestShouldPushImmediately(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		typ        roachpb.PushTxnType
		pusherPri  enginepb.TxnPriority
		pusheePri  enginepb.TxnPriority
		shouldPush bool
	}{
		{roachpb.PUSH_ABORT, enginepb.MinTxnPriority, enginepb.MinTxnPriority, false},
		{roachpb.PUSH_ABORT, enginepb.MinTxnPriority, 1, false},
		{roachpb.PUSH_ABORT, enginepb.MinTxnPriority, enginepb.MaxTxnPriority, false},
		{roachpb.PUSH_ABORT, 1, enginepb.MinTxnPriority, true},
		{roachpb.PUSH_ABORT, 1, 1, false},
		{roachpb.PUSH_ABORT, 1, enginepb.MaxTxnPriority, false},
		{roachpb.PUSH_ABORT, enginepb.MaxTxnPriority, enginepb.MinTxnPriority, true},
		{roachpb.PUSH_ABORT, enginepb.MaxTxnPriority, 1, true},
		{roachpb.PUSH_ABORT, enginepb.MaxTxnPriority, enginepb.MaxTxnPriority, false},
		{roachpb.PUSH_TIMESTAMP, enginepb.MinTxnPriority, enginepb.MinTxnPriority, false},
		{roachpb.PUSH_TIMESTAMP, enginepb.MinTxnPriority, 1, false},
		{roachpb.PUSH_TIMESTAMP, enginepb.MinTxnPriority, enginepb.MaxTxnPriority, false},
		{roachpb.PUSH_TIMESTAMP, 1, enginepb.MinTxnPriority, true},
		{roachpb.PUSH_TIMESTAMP, 1, 1, false},
		{roachpb.PUSH_TIMESTAMP, 1, enginepb.MaxTxnPriority, false},
		{roachpb.PUSH_TIMESTAMP, enginepb.MaxTxnPriority, enginepb.MinTxnPriority, true},
		{roachpb.PUSH_TIMESTAMP, enginepb.MaxTxnPriority, 1, true},
		{roachpb.PUSH_TIMESTAMP, enginepb.MaxTxnPriority, enginepb.MaxTxnPriority, false},
		{roachpb.PUSH_TOUCH, enginepb.MinTxnPriority, enginepb.MinTxnPriority, true},
		{roachpb.PUSH_TOUCH, enginepb.MinTxnPriority, 1, true},
		{roachpb.PUSH_TOUCH, enginepb.MinTxnPriority, enginepb.MaxTxnPriority, true},
		{roachpb.PUSH_TOUCH, 1, enginepb.MinTxnPriority, true},
		{roachpb.PUSH_TOUCH, 1, 1, true},
		{roachpb.PUSH_TOUCH, 1, enginepb.MaxTxnPriority, true},
		{roachpb.PUSH_TOUCH, enginepb.MaxTxnPriority, enginepb.MinTxnPriority, true},
		{roachpb.PUSH_TOUCH, enginepb.MaxTxnPriority, 1, true},
		{roachpb.PUSH_TOUCH, enginepb.MaxTxnPriority, enginepb.MaxTxnPriority, true},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			req := roachpb.PushTxnRequest{
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
					Timestamp: test.txnTimestamp,
				},
			}
			if isPushed := isPushed(&req, &txn); isPushed != test.isPushed {
				t.Errorf("expected %t; got %t", test.isPushed, isPushed)
			}
		})
	}
}

// mockRepl implements the ReplicaInterface interface.
type mockRepl struct{}

func (mockRepl) ContainsKey(_ roachpb.Key) bool { return true }

// mockStore implements the StoreInterface interface.
type mockStore struct {
	manual  *hlc.ManualClock
	clock   *hlc.Clock
	stopper *stop.Stopper
	db      *client.DB
	metrics *Metrics
}

func newMockStore(s client.SenderFunc) StoreInterface {
	var ms mockStore
	ms.manual = hlc.NewManualClock(123)
	ms.clock = hlc.NewClock(ms.manual.UnixNano, time.Nanosecond)
	ms.stopper = stop.NewStopper()
	ms.metrics = NewMetrics(time.Minute)
	if s != nil {
		factory := client.NonTransactionalFactoryFunc(s)
		ms.db = client.NewDB(testutils.MakeAmbientCtx(), factory, ms.clock)
	}
	return ms
}

func (s mockStore) Clock() *hlc.Clock             { return s.clock }
func (s mockStore) Stopper() *stop.Stopper        { return s.stopper }
func (s mockStore) DB() *client.DB                { return s.db }
func (s mockStore) GetTxnWaitKnobs() TestingKnobs { return TestingKnobs{} }
func (s mockStore) GetTxnWaitMetrics() *Metrics   { return s.metrics }

// TestMaybeWaitForQueryWithContextCancellation adds a new waiting query to the
// queue and cancels its context. It then verifies that the query was cleaned
// up. Regression test against #28849, before which the waiting query would
// leak.
func TestMaybeWaitForQueryWithContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ms := newMockStore(nil)
	defer ms.Stopper().Stop(context.Background())
	q := NewQueue(ms)
	q.Enable()

	ctx, cancel := context.WithCancel(context.Background())
	waitingRes := make(chan *roachpb.Error)
	go func() {
		req := &roachpb.QueryTxnRequest{WaitForUpdate: true}
		waitingRes <- q.MaybeWaitForQuery(ctx, mockRepl{}, req)
	}()

	cancel()
	if pErr := <-waitingRes; !testutils.IsPError(pErr, "context canceled") {
		t.Errorf("unexpected error %v", pErr)
	}
	if len(q.mu.queries) != 0 {
		t.Errorf("expected no waiting queries, found %v", q.mu.queries)
	}

	metrics := ms.GetTxnWaitMetrics()
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
	var mockSender client.SenderFunc
	ms := newMockStore(func(
		ctx context.Context, ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error) {
		return mockSender(ctx, ba)
	})
	defer ms.Stopper().Stop(context.Background())
	q := NewQueue(ms)
	q.Enable()

	// Set an extremely high transaction liveness threshold so that the pushee
	// is only queried once per pusher.
	defer TestingOverrideTxnLivenessThreshold(time.Hour)()

	// Enqueue pushee transaction in the queue.
	txn := roachpb.MakeTransaction("test", nil, 0, ms.Clock().Now(), 0)
	q.Enqueue(&txn)

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
			res, err := q.MaybeWaitForPush(ctx, mockRepl{}, &req)
			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, roachpb.ABORTED, res.PusheeTxn.Status)
		}()
	}
	wg.Wait()
}
