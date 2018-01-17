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

package storage

import (
	"bytes"
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

func writeTxnRecord(ctx context.Context, tc *testContext, txn *roachpb.Transaction) error {
	key := keys.TransactionKey(txn.Key, txn.ID)
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

func TestTxnWaitQueueEnableDisable(t *testing.T) {
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
	q := tc.repl.txnWaitQueue
	if !q.IsEnabled() {
		t.Errorf("expected push txn queue is enabled")
	}

	q.Enqueue(txn)
	if _, ok := q.TrackedTxns()[txn.ID]; !ok {
		t.Fatalf("expected pendingTxn to be in txns map after enqueue")
	}

	pusher := newTransaction("pusher", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	req := roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *pusher,
		PusheeTxn: txn.TxnMeta,
	}

	retCh := make(chan RespWithErr, 1)
	go func() {
		resp, pErr := q.MaybeWaitForPush(context.Background(), tc.repl, &req)
		retCh <- RespWithErr{resp, pErr}
	}()

	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{pusher.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	// Now disable the queue and make sure the waiter is returned.
	q.Clear(true /* disable */)
	if q.IsEnabled() {
		t.Errorf("expected queue to be disabled")
	}

	respWithErr := <-retCh
	if respWithErr.resp != nil {
		t.Errorf("expected nil response; got %+v", respWithErr.resp)
	}
	if respWithErr.pErr != nil {
		t.Errorf("expected nil err; got %+v", respWithErr.pErr)
	}

	if deps := q.GetDependents(txn.ID); deps != nil {
		t.Errorf("expected GetDependents to return nil as queue is disabled; got %+v", deps)
	}

	q.Enqueue(txn)
	if q.IsEnabled() {
		t.Errorf("expected enqueue to silently fail since queue is disabled")
	}

	q.UpdateTxn(context.Background(), txn)
	if len(q.TrackedTxns()) != 0 {
		t.Fatalf("expected update to silently fail since queue is disabled")
	}

	if resp, pErr := q.MaybeWaitForPush(context.TODO(), tc.repl, &req); resp != nil || pErr != nil {
		t.Errorf("expected nil resp and err as queue is disabled; got %+v, %s", resp, pErr)
	}
}

func TestTxnWaitQueueCancel(t *testing.T) {
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

	q := tc.repl.txnWaitQueue
	q.Enable()
	q.Enqueue(txn)

	ctx, cancel := context.WithCancel(context.Background())
	retCh := make(chan RespWithErr, 1)
	go func() {
		resp, pErr := q.MaybeWaitForPush(ctx, tc.repl, &req)
		retCh <- RespWithErr{resp, pErr}
	}()

	testutils.SucceedsSoon(t, func() error {
		select {
		case rwe := <-retCh:
			t.Fatalf("MaybeWaitForPush terminated prematurely: %+v", rwe)
		default:
		}
		expDeps := []uuid.UUID{pusher.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
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

// TestTxnWaitQueueUpdateTxn creates two waiters on a txn and verifies
// both are returned when the txn is updated.
func TestTxnWaitQueueUpdateTxn(t *testing.T) {
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

	q := tc.repl.txnWaitQueue
	q.Enable()
	q.Enqueue(txn)

	retCh := make(chan RespWithErr, 2)
	go func() {
		resp, pErr := q.MaybeWaitForPush(context.Background(), tc.repl, &req1)
		retCh <- RespWithErr{resp, pErr}
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{pusher1.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	go func() {
		resp, pErr := q.MaybeWaitForPush(context.Background(), tc.repl, &req2)
		retCh <- RespWithErr{resp, pErr}
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{pusher1.ID, pusher2.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	updatedTxn := *txn
	updatedTxn.Status = roachpb.COMMITTED
	q.UpdateTxn(context.Background(), &updatedTxn)

	for i := 0; i < 2; i++ {
		respWithErr := <-retCh
		if respWithErr.resp == nil || respWithErr.resp.PusheeTxn.Status != roachpb.COMMITTED {
			t.Errorf("expected committed txn response; got %+v, err=%v", respWithErr.resp, respWithErr.pErr)
		}
	}
}

// TestTxnWaitQueueUpdateNotPushedTxn verifies that no PushTxnResponse
// is returned in the event that the pushee txn only has its timestamp
// updated.
func TestTxnWaitQueueUpdateNotPushedTxn(t *testing.T) {
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

	q := tc.repl.txnWaitQueue
	q.Enable()
	q.Enqueue(txn)

	retCh := make(chan RespWithErr, 1)
	go func() {
		resp, pErr := q.MaybeWaitForPush(context.Background(), tc.repl, &req)
		retCh <- RespWithErr{resp, pErr}
	}()

	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{pusher.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	updatedTxn := *txn
	updatedTxn.Timestamp = txn.Timestamp.Add(1, 0)
	q.UpdateTxn(context.Background(), &updatedTxn)

	respWithErr := <-retCh
	if respWithErr.resp != nil {
		t.Errorf("on non-committed txn update, expected nil response; got %+v", respWithErr.resp)
	}
	if respWithErr.pErr != nil {
		t.Errorf("expected nil error; got %s", respWithErr.pErr)
	}
}

// TestTxnWaitQueuePusheeExpires verifies that just one pusher is
// returned when the pushee's txn may have expired.
func TestTxnWaitQueuePusheeExpires(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var queryTxnCount int32

	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	txn := newTransaction("txn", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, clock)
	// Move the clock forward so that when the PushTxn is sent, the txn appears
	// expired.
	manual.Set(txnwait.TxnExpiration(txn).WallTime)

	tc := testContext{}
	tsc := TestStoreConfig(clock)
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
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

	q := tc.repl.txnWaitQueue
	q.Enable()
	q.Enqueue(txn)

	retCh := make(chan RespWithErr, 2)
	go func() {
		resp, pErr := q.MaybeWaitForPush(context.Background(), tc.repl, &req1)
		retCh <- RespWithErr{resp, pErr}
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{pusher1.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	go func() {
		resp, pErr := q.MaybeWaitForPush(context.Background(), tc.repl, &req2)
		retCh <- RespWithErr{resp, pErr}
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{pusher1.ID, pusher2.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
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

	if a, minExpected := atomic.LoadInt32(&queryTxnCount), int32(2); a < minExpected {
		t.Errorf("expected no fewer than %d query txns; got %d", minExpected, a)
	}
}

// TestTxnWaitQueuePusherUpdate verifies that the pusher's status is
// periodically updated and will notice if the pusher has been aborted.
func TestTxnWaitQueuePusherUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "txnRecordExists", func(t *testing.T, txnRecordExists bool) {
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(context.TODO())
		tc.Start(t, stopper)

		txn, err := createTxnForPushQueue(context.Background(), &tc)
		if err != nil {
			t.Fatal(err)
		}
		var pusher *roachpb.Transaction
		if txnRecordExists {
			pusher, err = createTxnForPushQueue(context.Background(), &tc)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			pusher = newTransaction("pusher", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
		}

		req := roachpb.PushTxnRequest{
			PushType:  roachpb.PUSH_ABORT,
			PusherTxn: *pusher,
			PusheeTxn: txn.TxnMeta,
		}

		q := tc.repl.txnWaitQueue
		q.Enable()
		q.Enqueue(txn)

		retCh := make(chan RespWithErr, 1)
		go func() {
			resp, pErr := q.MaybeWaitForPush(context.Background(), tc.repl, &req)
			retCh <- RespWithErr{resp, pErr}
		}()

		testutils.SucceedsSoon(t, func() error {
			expDeps := []uuid.UUID{pusher.ID}
			if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
				return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
			}
			return nil
		})

		// If the record doesn't exist yet, give the push queue enough
		// time to query the missing record and notice.
		if !txnRecordExists {
			time.Sleep(10 * time.Millisecond)
		}

		// Update txn on disk with status ABORTED.
		pusherUpdate := *pusher
		pusherUpdate.Status = roachpb.ABORTED
		if err := writeTxnRecord(context.Background(), &tc, &pusherUpdate); err != nil {
			t.Fatal(err)
		}
		q.UpdateTxn(context.Background(), &pusherUpdate)

		respWithErr := <-retCh
		if respWithErr.resp != nil {
			t.Errorf("expected nil response; got %+v", respWithErr.resp)
		}
		if !testutils.IsPError(respWithErr.pErr, "txn aborted") {
			t.Errorf("expected transaction aborted error; got %v", respWithErr.pErr)
		}
	})
}

// TestTxnWaitQueueDependencyCycle verifies that if txn A pushes txn B
// pushes txn C which in turn is pushing txn A, the cycle will be
// detected and broken by a higher priority pusher.
func TestTxnWaitQueueDependencyCycle(t *testing.T) {
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

	q := tc.repl.txnWaitQueue
	q.Enable()

	ctx, cancel := context.WithCancel(context.Background())
	for _, txn := range []*roachpb.Transaction{txnA, txnB, txnC} {
		q.Enqueue(txn)
	}

	retCh := make(chan RespWithErr, 3)
	for _, req := range []*roachpb.PushTxnRequest{reqA, reqB, reqC} {
		go func(req *roachpb.PushTxnRequest) {
			resp, pErr := q.MaybeWaitForPush(ctx, tc.repl, req)
			retCh <- RespWithErr{resp, pErr}
		}(req)
	}

	// Wait for first request to finish, which should break the
	// dependency cycle by returning an ErrDeadlock error.
	respWithErr := <-retCh
	if respWithErr.pErr != txnwait.ErrDeadlock {
		t.Errorf("expected ErrDeadlock; got %v", respWithErr.pErr)
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

// TestTxnWaitQueueDependencyCycleWithPriorityInversion verifies that
// priority inversions between two dependent transactions are noticed
// and the dependency is appropriately broken.
func TestTxnWaitQueueDependencyCycleWithPriorityInversion(t *testing.T) {
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

	q := tc.repl.txnWaitQueue
	q.Enable()

	ctx, cancel := context.WithCancel(context.Background())
	for _, txn := range []*roachpb.Transaction{txnA, txnB} {
		q.Enqueue(txn)
	}

	retCh := make(chan ReqWithErr, 2)
	for _, req := range []*roachpb.PushTxnRequest{reqA, reqB} {
		go func(req *roachpb.PushTxnRequest) {
			_, pErr := q.MaybeWaitForPush(ctx, tc.repl, req)
			retCh <- ReqWithErr{req, pErr}
		}(req)
	}

	// Wait for first request to finish, which should break the
	// dependency cycle by returning an ErrDeadlock error. The
	// returned request should be reqA.
	reqWithErr := <-retCh
	if !reflect.DeepEqual(reqA, reqWithErr.req) {
		t.Errorf("expected request %+v; got %+v", reqA, reqWithErr.req)
	}
	if reqWithErr.pErr != txnwait.ErrDeadlock {
		t.Errorf("expected errDeadlock; got %v", reqWithErr.pErr)
	}
	cancel()
	<-retCh
}
