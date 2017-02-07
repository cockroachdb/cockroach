// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package storage

import (
	"bytes"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

func TestShouldPushImmediately(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		typ        roachpb.PushTxnType
		pusherPri  int32
		pusheePri  int32
		shouldPush bool
	}{
		{roachpb.PUSH_ABORT, roachpb.MinTxnPriority, roachpb.MinTxnPriority, false},
		{roachpb.PUSH_ABORT, roachpb.MinTxnPriority, 1, false},
		{roachpb.PUSH_ABORT, roachpb.MinTxnPriority, roachpb.MaxTxnPriority, false},
		{roachpb.PUSH_ABORT, 1, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_ABORT, 1, 1, false},
		{roachpb.PUSH_ABORT, 1, roachpb.MaxTxnPriority, false},
		{roachpb.PUSH_ABORT, roachpb.MaxTxnPriority, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_ABORT, roachpb.MaxTxnPriority, 1, true},
		{roachpb.PUSH_ABORT, roachpb.MaxTxnPriority, roachpb.MaxTxnPriority, false},
		{roachpb.PUSH_TIMESTAMP, roachpb.MinTxnPriority, roachpb.MinTxnPriority, false},
		{roachpb.PUSH_TIMESTAMP, roachpb.MinTxnPriority, 1, false},
		{roachpb.PUSH_TIMESTAMP, roachpb.MinTxnPriority, roachpb.MaxTxnPriority, false},
		{roachpb.PUSH_TIMESTAMP, 1, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_TIMESTAMP, 1, 1, false},
		{roachpb.PUSH_TIMESTAMP, 1, roachpb.MaxTxnPriority, false},
		{roachpb.PUSH_TIMESTAMP, roachpb.MaxTxnPriority, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_TIMESTAMP, roachpb.MaxTxnPriority, 1, true},
		{roachpb.PUSH_TIMESTAMP, roachpb.MaxTxnPriority, roachpb.MaxTxnPriority, false},
		{roachpb.PUSH_TOUCH, roachpb.MinTxnPriority, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_TOUCH, roachpb.MinTxnPriority, 1, true},
		{roachpb.PUSH_TOUCH, roachpb.MinTxnPriority, roachpb.MaxTxnPriority, true},
		{roachpb.PUSH_TOUCH, 1, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_TOUCH, 1, 1, true},
		{roachpb.PUSH_TOUCH, 1, roachpb.MaxTxnPriority, true},
		{roachpb.PUSH_TOUCH, roachpb.MaxTxnPriority, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_TOUCH, roachpb.MaxTxnPriority, 1, true},
		{roachpb.PUSH_TOUCH, roachpb.MaxTxnPriority, roachpb.MaxTxnPriority, true},
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
			if shouldPush := shouldPushImmediately(&req); shouldPush != test.shouldPush {
				t.Errorf("expected %t; got %t", test.shouldPush, shouldPush)
			}
		})
	}
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
		{roachpb.PUSH_ABORT, hlc.ZeroTimestamp, roachpb.PENDING, hlc.ZeroTimestamp, false},
		{roachpb.PUSH_ABORT, hlc.ZeroTimestamp, roachpb.ABORTED, hlc.ZeroTimestamp, true},
		{roachpb.PUSH_ABORT, hlc.ZeroTimestamp, roachpb.COMMITTED, hlc.ZeroTimestamp, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, hlc.ZeroTimestamp, false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.ABORTED, hlc.ZeroTimestamp, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.COMMITTED, hlc.ZeroTimestamp, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 0), false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 1), false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 2), true},
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

func writeTxnRecord(t *testing.T, tc *testContext, txn *roachpb.Transaction) {
	key := keys.TransactionKey(txn.Key, *txn.ID)
	if err := engine.MVCCPutProto(context.Background(), tc.store.Engine(),
		nil, key, hlc.ZeroTimestamp, nil, txn); err != nil {
		t.Fatal(err)
	}
}

// createTxnForPushQueue creates a txn struct and writes a "fake"
// transaction record for it to the underlying engine.
func createTxnForPushQueue(t *testing.T, tc *testContext) *roachpb.Transaction {
	txn := newTransaction("txn", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	writeTxnRecord(t, tc, txn)
	return txn
}

func TestPushTxnQueueEnableDisable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc.Start(t, stopper)

	txn := createTxnForPushQueue(t, &tc)

	// Queue starts enabled.
	ptq := tc.repl.pushTxnQueue
	ptq.mu.Lock()
	if ptq.mu.txns == nil {
		t.Errorf("expected enable to create the txns map")
	}
	ptq.mu.Unlock()

	ptq.Enqueue(txn)
	ptq.mu.Lock()
	if pt := ptq.mu.txns[*txn.ID]; pt == nil {
		t.Errorf("expected pendingTxn to be in txns map after enqueue")
	}
	ptq.mu.Unlock()

	pusher := newTransaction("pusher", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	req := roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *pusher,
		PusheeTxn: txn.TxnMeta,
	}
	respCh := make(chan *roachpb.PushTxnResponse, 1)
	errCh := make(chan *roachpb.Error, 1)
	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), &req)
		respCh <- resp
		errCh <- pErr
	}()

	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{*pusher.ID}
		if deps := ptq.GetDependents(*txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	// Now disable the queue and make sure the waiter is returned.
	ptq.ClearAndDisable()
	ptq.mu.Lock()
	if ptq.mu.txns != nil {
		t.Errorf("expected enqueue to silently fail since queue is disabled")
	}
	ptq.mu.Unlock()

	if pErr := <-errCh; pErr != nil {
		t.Errorf("expected nil error; got %s", pErr)
	}
	if resp := <-respCh; resp != nil {
		t.Errorf("expected nil response; got %+v", resp)
	}

	if deps := ptq.GetDependents(*txn.ID); deps != nil {
		t.Errorf("expected GetDependents to return nil as queue is disabled")
	}

	ptq.Enqueue(txn)
	ptq.mu.Lock()
	if ptq.mu.txns != nil {
		t.Errorf("expected enqueue to silently fail since queue is disabled")
	}
	ptq.mu.Unlock()

	ptq.UpdateTxn(txn)
	ptq.mu.Lock()
	if ptq.mu.txns != nil {
		t.Errorf("expected update to silently fail since queue is disabled")
	}
	ptq.mu.Unlock()

	if resp, pErr := ptq.MaybeWait(context.TODO(), &req); resp != nil || pErr != nil {
		t.Errorf("expected nil resp and err as queue is disabled; got %+v, %s", resp, pErr)
	}
}

func TestPushTxnQueueCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc.Start(t, stopper)

	txn := createTxnForPushQueue(t, &tc)
	pusher := newTransaction("pusher", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	req := roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *pusher,
		PusheeTxn: txn.TxnMeta,
	}

	ptq := tc.repl.pushTxnQueue
	ptq.Enable()
	ptq.Enqueue(txn)

	ctx, cancel := context.WithCancel(context.Background())
	respCh := make(chan *roachpb.PushTxnResponse, 1)
	errCh := make(chan *roachpb.Error, 1)
	go func() {
		resp, pErr := ptq.MaybeWait(ctx, &req)
		respCh <- resp
		errCh <- pErr
	}()

	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{*pusher.ID}
		if deps := ptq.GetDependents(*txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})
	cancel()

	if pErr := <-errCh; !testutils.IsPError(pErr, "context canceled") {
		t.Errorf("expected context canceled error; got %s", pErr)
	}
	if resp := <-respCh; resp != nil {
		t.Errorf("expected nil response; got %+v", resp)
	}
}

// TestPushTxnQueueUpdateTxn creates two waiters on a txn and verifies
// both are returned when the txn is updated.
func TestPushTxnQueueUpdateTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc.Start(t, stopper)

	txn := createTxnForPushQueue(t, &tc)
	pusher1 := newTransaction("pusher1", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	pusher2 := newTransaction("pusher2", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	req1 := roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *pusher1,
		PusheeTxn: txn.TxnMeta,
	}
	req2 := req1
	req2.PusherTxn = *pusher2

	ptq := tc.repl.pushTxnQueue
	ptq.Enable()
	ptq.Enqueue(txn)

	respCh := make(chan *roachpb.PushTxnResponse, 2)
	errCh := make(chan *roachpb.Error, 2)
	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), &req1)
		respCh <- resp
		errCh <- pErr
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{*pusher1.ID}
		if deps := ptq.GetDependents(*txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), &req2)
		respCh <- resp
		errCh <- pErr
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{*pusher1.ID, *pusher2.ID}
		if deps := ptq.GetDependents(*txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	updatedTxn := *txn
	updatedTxn.Status = roachpb.COMMITTED
	ptq.UpdateTxn(&updatedTxn)

	for i := 0; i < 2; i++ {
		if pErr := <-errCh; pErr != nil {
			t.Errorf("expected nil error; got %s", pErr)
		}
		if resp := <-respCh; resp == nil || resp.PusheeTxn.Status != roachpb.COMMITTED {
			t.Errorf("expected committed txn response; got %+v", resp)
		}
	}
}

// TestPushTxnQueueUpdateNotPushedTxn verifies that no PushTxnResponse
// is returned in the event that the pushee txn only has its timestamp
// updated.
func TestPushTxnQueueUpdateNotPushedTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc.Start(t, stopper)

	txn := createTxnForPushQueue(t, &tc)
	pusher := newTransaction("pusher", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	req := roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *pusher,
		PusheeTxn: txn.TxnMeta,
	}

	ptq := tc.repl.pushTxnQueue
	ptq.Enable()
	ptq.Enqueue(txn)

	respCh := make(chan *roachpb.PushTxnResponse, 1)
	errCh := make(chan *roachpb.Error, 1)
	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), &req)
		respCh <- resp
		errCh <- pErr
	}()

	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{*pusher.ID}
		if deps := ptq.GetDependents(*txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	updatedTxn := *txn
	updatedTxn.Timestamp = txn.Timestamp.Add(1, 0)
	ptq.UpdateTxn(&updatedTxn)

	if pErr := <-errCh; pErr != nil {
		t.Errorf("expected nil error; got %s", pErr)
	}
	if resp := <-respCh; resp != nil {
		t.Errorf("on non-committed txn update, expected nil response; got %+v", resp)
	}
}

// TestPushTxnQueuePusheeExpires verifies that just one pusher is
// returned when the pushee's txn may have expired.
func TestPushTxnQueuePusheeExpires(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var queryTxnCount int32

	clock := hlc.NewClock(hlc.UnixNano, 0)
	txn := newTransaction("txn", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, clock)
	// Move the clock back so that when the PushTxn is sent, the txn
	// appears expired.
	txn.Timestamp = txn.Timestamp.Add(-2*(base.DefaultHeartbeatInterval-10*time.Millisecond).Nanoseconds(), 0)
	txn.OrigTimestamp = txn.Timestamp

	tc := testContext{}
	tsc := TestStoreConfig(clock)
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if qtReq, ok := filterArgs.Req.(*roachpb.QueryTxnRequest); ok && bytes.Equal(qtReq.Txn.Key, txn.Key) {
				atomic.AddInt32(&queryTxnCount, 1)
			}
			return nil
		}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc.StartWithStoreConfig(t, stopper, tsc)

	pusher1 := newTransaction("pusher1", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	pusher2 := newTransaction("pusher2", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	req1 := roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *pusher1,
		PusheeTxn: txn.TxnMeta,
	}
	req2 := req1
	req2.PusherTxn = *pusher2

	// Create a "fake" txn record.
	writeTxnRecord(t, &tc, txn)

	ptq := tc.repl.pushTxnQueue
	ptq.Enable()
	ptq.Enqueue(txn)

	respCh := make(chan *roachpb.PushTxnResponse, 2)
	errCh := make(chan *roachpb.Error, 2)
	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), &req1)
		respCh <- resp
		errCh <- pErr
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{*pusher1.ID}
		if deps := ptq.GetDependents(*txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), &req2)
		respCh <- resp
		errCh <- pErr
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{*pusher1.ID, *pusher2.ID}
		if deps := ptq.GetDependents(*txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	for i := 0; i < 2; i++ {
		if pErr := <-errCh; pErr != nil {
			t.Errorf("expected nil error; got %s", pErr)
		}
		if resp := <-respCh; resp != nil {
			t.Errorf("expected nil txn response; got %+v", resp)
		}
	}

	if a, e := atomic.LoadInt32(&queryTxnCount), int32(2); a != e {
		t.Errorf("expected %d query txns; got %d", e, a)
	}
}

// TestPushTxnQueuePusherUpdate verifies that the pusher's status is
// periodically updated and will notice if the pusher has been aborted.
func TestPushTxnQueuePusherUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc.Start(t, stopper)

	txn := createTxnForPushQueue(t, &tc)
	pusher := newTransaction("pusher", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	req := roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *pusher,
		PusheeTxn: txn.TxnMeta,
	}

	ptq := tc.repl.pushTxnQueue
	ptq.Enable()
	ptq.Enqueue(txn)

	respCh := make(chan *roachpb.PushTxnResponse, 1)
	errCh := make(chan *roachpb.Error, 1)
	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), &req)
		respCh <- resp
		errCh <- pErr
	}()

	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{*pusher.ID}
		if deps := ptq.GetDependents(*txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	// Update txn on disk with status ABORTED.
	pusherUpdate := *pusher
	pusherUpdate.Status = roachpb.ABORTED
	writeTxnRecord(t, &tc, &pusherUpdate)

	if pErr := <-errCh; !testutils.IsPError(pErr, "txn aborted") {
		t.Errorf("expected transaction aborted error; got %s", pErr)
	}
	if resp := <-respCh; resp != nil {
		t.Errorf("expected nil response; got %+v", resp)
	}
}

// TestPushTxnQueueDependencyCycle verifies that if txn A pushes txn B
// pushes txn C which in turn is pushing txn A, the cycle will be
// detected and broken by a higher priority pusher.
func TestPushTxnQueueDependencyCycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc.Start(t, stopper)

	txnA := createTxnForPushQueue(t, &tc)
	txnB := createTxnForPushQueue(t, &tc)
	txnC := createTxnForPushQueue(t, &tc)

	reqA := &roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *txnA,
		PusheeTxn: txnB.TxnMeta,
	}
	reqB := &roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *txnB,
		PusheeTxn: txnC.TxnMeta,
	}
	reqC := &roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *txnC,
		PusheeTxn: txnA.TxnMeta,
	}

	ptq := tc.repl.pushTxnQueue
	ptq.Enable()
	respCh := make(chan *roachpb.PushTxnResponse, 3)
	errCh := make(chan *roachpb.Error, 3)

	ctx, cancel := context.WithCancel(context.Background())
	for _, txn := range []*roachpb.Transaction{txnA, txnB, txnC} {
		ptq.Enqueue(txn)
	}
	for _, req := range []*roachpb.PushTxnRequest{reqA, reqB, reqC} {
		go func(req *roachpb.PushTxnRequest) {
			resp, pErr := ptq.MaybeWait(ctx, req)
			respCh <- resp
			errCh <- pErr
		}(req)
	}

	// Wait for first request to finish, which should break the
	// dependency cycle and have the request Force parameter set to
	// true.
	if pErr := <-errCh; pErr != nil {
		t.Errorf("expected no error; got %s", pErr)
	}
	if resp := <-respCh; resp != nil {
		t.Errorf("expected nil response; got %+v", resp)
	}

	var foundForce bool
	for _, req := range []*roachpb.PushTxnRequest{reqA, reqB, reqC} {
		if req.Force {
			foundForce = true
			break
		}
	}
	if !foundForce {
		t.Errorf("expected at least one request to have force parameter set")
	}
	cancel()
}
