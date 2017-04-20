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
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.PENDING, hlc.Timestamp{}, false},
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.ABORTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.COMMITTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, hlc.Timestamp{}, false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.ABORTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.COMMITTED, hlc.Timestamp{}, true},
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

func writeTxnRecord(ctx context.Context, tc *testContext, txn *roachpb.Transaction) error {
	key := keys.TransactionKey(txn.Key, *txn.ID)
	return engine.MVCCPutProto(ctx, tc.store.Engine(), nil, key, hlc.Timestamp{}, nil, txn)
}

// createTxnForPushQueue creates a txn struct and writes a "fake"
// transaction record for it to the underlying engine.
func createTxnForPushQueue(ctx context.Context, tc *testContext) (*roachpb.Transaction, error) {
	txn := newTransaction("txn", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	return txn, writeTxnRecord(ctx, tc, txn)
}

type RespWithErr struct {
	resp *roachpb.PushTxnResponse
	pErr *roachpb.Error
}

func TestPushTxnQueueEnableDisable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	txn, err := createTxnForPushQueue(context.Background(), &tc)
	if err != nil {
		t.Fatal(err)
	}

	// Queue starts enabled.
	ptq := tc.repl.pushTxnQueue
	if !ptq.isEnabled() {
		t.Errorf("expected push txn queue is enabled")
	}

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

	retCh := make(chan RespWithErr, 1)
	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), tc.repl, &req)
		retCh <- RespWithErr{resp, pErr}
	}()

	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{*pusher.ID}
		if deps := ptq.GetDependents(*txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	// Now disable the queue and make sure the waiter is returned.
	ptq.Clear(true /* disable */)
	if ptq.isEnabled() {
		t.Errorf("expected queue to be disabled")
	}

	respWithErr := <-retCh
	if respWithErr.resp != nil {
		t.Errorf("expected nil response; got %+v", respWithErr.resp)
	}
	if respWithErr.pErr != nil {
		t.Errorf("expected nil err; got %+v", respWithErr.pErr)
	}

	if deps := ptq.GetDependents(*txn.ID); deps != nil {
		t.Errorf("expected GetDependents to return nil as queue is disabled; got %+v", deps)
	}

	ptq.Enqueue(txn)
	if ptq.isEnabled() {
		t.Errorf("expected enqueue to silently fail since queue is disabled")
	}

	ptq.UpdateTxn(txn)
	ptq.mu.Lock()
	if ptq.mu.txns != nil {
		t.Errorf("expected update to silently fail since queue is disabled")
	}
	ptq.mu.Unlock()

	if resp, pErr := ptq.MaybeWait(context.TODO(), tc.repl, &req); resp != nil || pErr != nil {
		t.Errorf("expected nil resp and err as queue is disabled; got %+v, %s", resp, pErr)
	}
}

func TestPushTxnQueueCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	txn, err := createTxnForPushQueue(context.Background(), &tc)
	if err != nil {
		t.Fatal(err)
	}
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
	retCh := make(chan RespWithErr, 1)
	go func() {
		resp, pErr := ptq.MaybeWait(ctx, tc.repl, &req)
		retCh <- RespWithErr{resp, pErr}
	}()

	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{*pusher.ID}
		if deps := ptq.GetDependents(*txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})
	cancel()

	respWithErr := <-retCh
	if respWithErr.resp != nil {
		t.Errorf("expected nil response; got %+v", respWithErr.resp)
	}
	if !testutils.IsPError(respWithErr.pErr, context.Canceled.Error()) {
		t.Errorf("expected context canceled error; got %v", respWithErr.pErr)
	}
}

// TestPushTxnQueueUpdateTxn creates two waiters on a txn and verifies
// both are returned when the txn is updated.
func TestPushTxnQueueUpdateTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	txn, err := createTxnForPushQueue(context.Background(), &tc)
	if err != nil {
		t.Fatal(err)
	}
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

	retCh := make(chan RespWithErr, 2)
	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), tc.repl, &req1)
		retCh <- RespWithErr{resp, pErr}
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{*pusher1.ID}
		if deps := ptq.GetDependents(*txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), tc.repl, &req2)
		retCh <- RespWithErr{resp, pErr}
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
		respWithErr := <-retCh
		if respWithErr.resp == nil || respWithErr.resp.PusheeTxn.Status != roachpb.COMMITTED {
			t.Errorf("expected committed txn response; got %+v", respWithErr.resp)
		}
		if respWithErr.pErr != nil {
			t.Errorf("expected nil err; got %+v", respWithErr.pErr)
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
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	txn, err := createTxnForPushQueue(context.Background(), &tc)
	if err != nil {
		t.Fatal(err)
	}
	pusher := newTransaction("pusher", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	req := roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *pusher,
		PusheeTxn: txn.TxnMeta,
	}

	ptq := tc.repl.pushTxnQueue
	ptq.Enable()
	ptq.Enqueue(txn)

	retCh := make(chan RespWithErr, 1)
	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), tc.repl, &req)
		retCh <- RespWithErr{resp, pErr}
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

	respWithErr := <-retCh
	if respWithErr.resp != nil {
		t.Errorf("on non-committed txn update, expected nil response; got %+v", respWithErr.resp)
	}
	if respWithErr.pErr != nil {
		t.Errorf("expected nil error; got %s", respWithErr.pErr)
	}
}

// TestPushTxnQueuePusheeExpires verifies that just one pusher is
// returned when the pushee's txn may have expired.
func TestPushTxnQueuePusheeExpires(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var queryTxnCount int32

	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	txn := newTransaction("txn", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, clock)
	// Move the clock forward so that when the PushTxn is sent, the txn appears
	// expired.
	manual.Set(txnExpiration(txn).WallTime)

	tc := testContext{}
	tsc := TestStoreConfig(clock)
	tsc.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if qtReq, ok := filterArgs.Req.(*roachpb.QueryTxnRequest); ok && bytes.Equal(qtReq.Txn.Key, txn.Key) {
				atomic.AddInt32(&queryTxnCount, 1)
			}
			return nil
		}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
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
	if err := writeTxnRecord(context.Background(), &tc, txn); err != nil {
		t.Fatal(err)
	}

	ptq := tc.repl.pushTxnQueue
	ptq.Enable()
	ptq.Enqueue(txn)

	retCh := make(chan RespWithErr, 2)
	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), tc.repl, &req1)
		retCh <- RespWithErr{resp, pErr}
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{*pusher1.ID}
		if deps := ptq.GetDependents(*txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), tc.repl, &req2)
		retCh <- RespWithErr{resp, pErr}
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{*pusher1.ID, *pusher2.ID}
		if deps := ptq.GetDependents(*txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	for i := 0; i < 2; i++ {
		respWithErr := <-retCh
		if respWithErr.resp != nil {
			t.Errorf("expected nil txn response; got %+v", respWithErr.resp)
		}
		if respWithErr.pErr != nil {
			t.Errorf("expected nil error; got %s", respWithErr.pErr)
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
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	txn, err := createTxnForPushQueue(context.Background(), &tc)
	if err != nil {
		t.Fatal(err)
	}
	pusher := newTransaction("pusher", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	req := roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *pusher,
		PusheeTxn: txn.TxnMeta,
	}

	ptq := tc.repl.pushTxnQueue
	ptq.Enable()
	ptq.Enqueue(txn)

	retCh := make(chan RespWithErr, 1)
	go func() {
		resp, pErr := ptq.MaybeWait(context.Background(), tc.repl, &req)
		retCh <- RespWithErr{resp, pErr}
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
	if err := writeTxnRecord(context.Background(), &tc, &pusherUpdate); err != nil {
		t.Fatal(err)
	}

	respWithErr := <-retCh
	if respWithErr.resp != nil {
		t.Errorf("expected nil response; got %+v", respWithErr.resp)
	}
	if !testutils.IsPError(respWithErr.pErr, "txn aborted") {
		t.Errorf("expected transaction aborted error; got %v", respWithErr.pErr)
	}
}

// TestPushTxnQueueDependencyCycle verifies that if txn A pushes txn B
// pushes txn C which in turn is pushing txn A, the cycle will be
// detected and broken by a higher priority pusher.
func TestPushTxnQueueDependencyCycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	txnA, err := createTxnForPushQueue(context.Background(), &tc)
	if err != nil {
		t.Fatal(err)
	}
	txnB, err := createTxnForPushQueue(context.Background(), &tc)
	if err != nil {
		t.Fatal(err)
	}
	txnC, err := createTxnForPushQueue(context.Background(), &tc)
	if err != nil {
		t.Fatal(err)
	}

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

	ctx, cancel := context.WithCancel(context.Background())
	for _, txn := range []*roachpb.Transaction{txnA, txnB, txnC} {
		ptq.Enqueue(txn)
	}

	retCh := make(chan RespWithErr, 3)
	for _, req := range []*roachpb.PushTxnRequest{reqA, reqB, reqC} {
		go func(req *roachpb.PushTxnRequest) {
			resp, pErr := ptq.MaybeWait(ctx, tc.repl, req)
			retCh <- RespWithErr{resp, pErr}
		}(req)
	}

	// Wait for first request to finish, which should break the
	// dependency cycle by returning an errDeadlock error.
	respWithErr := <-retCh
	if respWithErr.pErr != errDeadlock {
		t.Errorf("expected errDeadlock; got %v", respWithErr.pErr)
	}
	if respWithErr.resp != nil {
		t.Errorf("expected nil response; got %+v", respWithErr.resp)
	}
	cancel()
	for i := 0; i < 2; i++ {
		<-retCh
	}
}

type ReqWithErr struct {
	req  *roachpb.PushTxnRequest
	pErr *roachpb.Error
}

// TestPushTxnQueueDependencyCycleWithPriorityInversion verifies that
// priority inversions between two dependent transactions are noticed
// and the dependency is appropriately broken.
func TestPushTxnQueueDependencyCycleWithPriorityInversion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Create txnA with a lower priority so it won't think it could push
	// txnB without updating its priority.
	txnA := newTransaction("txn", roachpb.Key("a"), -1, enginepb.SERIALIZABLE, tc.Clock())
	// However, write an "updated" txnA with higher priority, which it
	// will need to read via a QueryTxn request in order to realize it
	// can in fact break the deadlock.
	updatedTxnA := *txnA
	updatedTxnA.Priority = 3
	if err := writeTxnRecord(context.Background(), &tc, &updatedTxnA); err != nil {
		t.Fatal(err)
	}
	// Create txnB with priority=2, so txnA won't think it can push, but
	// when we set up txnB as the pusher, the request will include txnA's
	// updated priority, making txnB think it can't break a deadlock.
	txnB := newTransaction("txn", roachpb.Key("a"), -2, enginepb.SERIALIZABLE, tc.Clock())
	if err := writeTxnRecord(context.Background(), &tc, txnB); err != nil {
		t.Fatal(err)
	}

	reqA := &roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *txnA,
		PusheeTxn: txnB.TxnMeta,
	}
	reqB := &roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *txnB,
		PusheeTxn: updatedTxnA.TxnMeta,
	}

	ptq := tc.repl.pushTxnQueue
	ptq.Enable()

	ctx, cancel := context.WithCancel(context.Background())
	for _, txn := range []*roachpb.Transaction{txnA, txnB} {
		ptq.Enqueue(txn)
	}

	retCh := make(chan ReqWithErr, 2)
	for _, req := range []*roachpb.PushTxnRequest{reqA, reqB} {
		go func(req *roachpb.PushTxnRequest) {
			_, pErr := ptq.MaybeWait(ctx, tc.repl, req)
			retCh <- ReqWithErr{req, pErr}
		}(req)
	}

	// Wait for first request to finish, which should break the
	// dependency cycle by returning an errDeadlock error. The
	// returned request should be reqA.
	reqWithErr := <-retCh
	if !reflect.DeepEqual(reqA, reqWithErr.req) {
		t.Errorf("expected request %+v; got %+v", reqA, reqWithErr.req)
	}
	if reqWithErr.pErr != errDeadlock {
		t.Errorf("expected errDeadlock; got %v", reqWithErr.pErr)
	}
	cancel()
	<-retCh
}
