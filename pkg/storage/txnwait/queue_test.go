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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

type mockRepl struct{}

func (mockRepl) ContainsKey(_ roachpb.Key) bool { return true }

type mockStore struct {
	StoreInterface
	metrics *Metrics
}

func (s mockStore) GetTxnWaitMetrics() *Metrics { return s.metrics }

// TestMaybeWaitForQueryWithContextCancellation adds a new waiting query to the
// queue and cancels its context. It then verifies that the query was cleaned
// up. Regression test against #28849, before which the waiting query would
// leak.
func TestMaybeWaitForQueryWithContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	metrics := NewMetrics(time.Minute)
	q := NewQueue(mockStore{metrics: metrics})
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

	allMetricsAreZero := metrics.PusheeWaiting.Value() == 0 &&
		metrics.PusherWaiting.Value() == 0 &&
		metrics.QueryWaiting.Value() == 0 &&
		metrics.PusherSlow.Value() == 0

	if !allMetricsAreZero {
		t.Errorf("expected all metric gauges to be zero, got some that aren't")
	}
}
