// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"reflect"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTxnRecord(ctx context.Context, tc *testContext, txn *roachpb.Transaction) error {
	key := keys.TransactionKey(txn.Key, txn.ID)
	return engine.MVCCPutProto(ctx, tc.store.Engine(), nil, key, hlc.Timestamp{}, nil, txn)
}

// createTxnForPushQueue creates a txn struct and writes a "fake"
// transaction record for it to the underlying engine.
func createTxnForPushQueue(ctx context.Context, tc *testContext) (*roachpb.Transaction, error) {
	txn := newTransaction("txn", roachpb.Key("a"), 1, tc.Clock())
	return txn, writeTxnRecord(ctx, tc, txn)
}

func checkAllGaugesZero(tc testContext) error {
	m := tc.store.GetTxnWaitMetrics()
	if act := m.PusheeWaiting.Value(); act != 0 {
		return errors.Errorf("expected PusheeWaiting to be 0, got %d instead", act)
	}
	if act := m.PusherWaiting.Value(); act != 0 {
		return errors.Errorf("expected PusherWaiting to be 0, got %d instead", act)
	}
	if act := m.QueryWaiting.Value(); act != 0 {
		return errors.Errorf("expected QueryWaiting to be 0, got %d instead", act)
	}
	if act := m.PusherSlow.Value(); act != 0 {
		return errors.Errorf("expected PusherSlow to be 0, got %d instead", act)
	}
	return nil
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
	if err := checkAllGaugesZero(tc); err != nil {
		t.Fatal(err.Error())
	}

	q.Enqueue(txn)
	if _, ok := q.TrackedTxns()[txn.ID]; !ok {
		t.Fatalf("expected pendingTxn to be in txns map after enqueue")
	}
	m := tc.store.GetTxnWaitMetrics()
	assert.EqualValues(tc, 1, m.PusheeWaiting.Value())

	pusher := newTransaction("pusher", roachpb.Key("a"), 1, tc.Clock())
	req := roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *pusher,
		PusheeTxn: txn.TxnMeta,
	}

	retCh := make(chan *roachpb.Error, 1)
	go func() {
		retCh <- q.MaybeWaitForPush(context.Background(), tc.repl, &req)
	}()

	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{pusher.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		if act, exp := m.PusherWaiting.Value(), int64(1); act != exp {
			return errors.Errorf("%d pushers, but want %d", act, exp)
		}
		if act, exp := m.PusheeWaiting.Value(), int64(1); act != exp {
			return errors.Errorf("%d pushees, but want %d", act, exp)
		}

		return nil
	})

	// Now disable the queue and make sure the waiter is returned.
	q.Clear(true /* disable */)
	if q.IsEnabled() {
		t.Errorf("expected queue to be disabled")
	}
	if err := checkAllGaugesZero(tc); err != nil {
		t.Fatal(err.Error())
	}

	respErr := <-retCh
	if respErr != nil {
		t.Errorf("expected nil err; got %+v", respErr)
	}

	if deps := q.GetDependents(txn.ID); deps != nil {
		t.Errorf("expected GetDependents to return nil as queue is disabled; got %+v", deps)
	}

	q.Enqueue(txn)
	if q.IsEnabled() {
		t.Errorf("expected enqueue to silently fail since queue is disabled")
	}
	if err := checkAllGaugesZero(tc); err != nil {
		t.Fatal(err.Error())
	}

	q.UpdateTxn(context.Background(), txn)
	if len(q.TrackedTxns()) != 0 {
		t.Fatalf("expected update to silently fail since queue is disabled")
	}

	if pErr := q.MaybeWaitForPush(context.TODO(), tc.repl, &req); pErr != nil {
		t.Errorf("expected nil err as queue is disabled; got %s", pErr)
	}
	if err := checkAllGaugesZero(tc); err != nil {
		t.Fatal(err.Error())
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
	pusher := newTransaction("pusher", roachpb.Key("a"), 1, tc.Clock())
	req := roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *pusher,
		PusheeTxn: txn.TxnMeta,
	}

	q := tc.repl.txnWaitQueue
	q.Enable()
	if err := checkAllGaugesZero(tc); err != nil {
		t.Fatal(err.Error())
	}
	q.Enqueue(txn)
	m := tc.store.GetTxnWaitMetrics()
	assert.EqualValues(tc, 1, m.PusheeWaiting.Value())
	assert.EqualValues(tc, 0, m.PusherWaiting.Value())

	ctx, cancel := context.WithCancel(context.Background())
	retCh := make(chan *roachpb.Error, 1)
	go func() {
		retCh <- q.MaybeWaitForPush(ctx, tc.repl, &req)
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
		if act, exp := m.PusherWaiting.Value(), int64(1); act != exp {
			return errors.Errorf("%d pushers, but want %d", act, exp)
		}
		return nil
	})
	cancel()

	respErr := <-retCh
	if !testutils.IsPError(respErr, context.Canceled.Error()) {
		t.Errorf("expected context canceled error; got %v", respErr)
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
	pusher1 := newTransaction("pusher1", roachpb.Key("a"), 1, tc.Clock())
	pusher2 := newTransaction("pusher2", roachpb.Key("a"), 1, tc.Clock())
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
	m := tc.store.GetTxnWaitMetrics()
	assert.EqualValues(tc, 1, m.PusheeWaiting.Value())

	retCh := make(chan *roachpb.Error, 2)
	go func() {
		retCh <- q.MaybeWaitForPush(context.Background(), tc.repl, &req1)
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{pusher1.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})
	testutils.SucceedsSoon(t, func() error {
		if act, exp := m.PusherWaiting.Value(), int64(1); act != exp {
			return errors.Errorf("%d pushers, but want %d", act, exp)
		}
		if act, exp := m.PusheeWaiting.Value(), int64(1); act != exp {
			return errors.Errorf("%d pushees, but want %d", act, exp)
		}
		if act, exp := m.QueryWaiting.Value(), int64(1); act != exp {
			return errors.Errorf("%d queries, but want %d", act, exp)
		}
		return nil
	})

	go func() {
		retCh <- q.MaybeWaitForPush(context.Background(), tc.repl, &req2)
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{pusher1.ID, pusher2.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		if act, exp := m.PusherWaiting.Value(), int64(2); act != exp {
			return errors.Errorf("%d pushers, but want %d", act, exp)
		}
		if act, exp := m.PusheeWaiting.Value(), int64(1); act != exp {
			return errors.Errorf("%d pushees, but want %d", act, exp)
		}
		if act, exp := m.QueryWaiting.Value(), int64(2); act != exp {
			return errors.Errorf("%d queries, but want %d", act, exp)
		}
		return nil
	})

	updatedTxn := *txn
	updatedTxn.Status = roachpb.COMMITTED
	q.UpdateTxn(context.Background(), &updatedTxn)
	testutils.SucceedsSoon(tc.TB, func() error {
		return checkAllGaugesZero(tc)
	})

	for i := 0; i < 2; i++ {
		require.Nil(t, <-retCh)
	}
}

// TestTxnWaitQueueTxnSilentlyCompletes creates a waiter on a txn and verifies
// that the waiter is eventually unblocked when the txn commits but UpdateTxn is
// not called.
//
// This simulates the following observed sequence of events. A transaction, TA,
// writes a key K. Another transaction, TB, attempts to read K. It notices the
// intent on K and sends a PushTxnRequest. The PushTxnRequest fails and returns
// a TransactionPushError. Before the replica handles the TransactionPushError,
// TA commits and the replica fully processes its EndTransactionRequest. Only
// then does the replica notice the TransactionPushError and put TB's
// PushTxnRequest into TA's wait queue. Updates to TA will never be sent via
// Queue.UpdateTxn, because Queue.UpdateTxn was already called when the
// EndTransactionRequest was processed, before TB's PushTxnRequest was in TA's
// wait queue.
//
// This sequence of events was previously mishandled when TA's transaction
// record was not immediately cleaned up, e.g. because it had non-local intents.
// The wait queue would continually poll TA's transaction record, notice it
// still existed, and continue waiting. In production, this meant that the
// PushTxnRequest would get stuck waiting out the full TxnLivenessThreshold for
// the transaction record to expire. In unit tests, where the clock might never
// be advanced, the PushTxnRequest could get stuck forever.
func TestTxnWaitQueueTxnSilentlyCompletes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)

	txn, err := createTxnForPushQueue(ctx, &tc)
	if err != nil {
		t.Fatal(err)
	}
	pusher := newTransaction("pusher", roachpb.Key("a"), 1, tc.Clock())
	req := &roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *pusher,
		PusheeTxn: txn.TxnMeta,
	}

	q := tc.repl.txnWaitQueue
	q.Enable()
	q.Enqueue(txn)

	retCh := make(chan *roachpb.Error, 2)
	go func() {
		retCh <- q.MaybeWaitForPush(context.Background(), tc.repl, req)
	}()

	m := tc.store.GetTxnWaitMetrics()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{pusher.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		if act, exp := m.PusherWaiting.Value(), int64(1); act != exp {
			return errors.Errorf("%d pushers, but want %d", act, exp)
		}
		if act, exp := m.PusheeWaiting.Value(), int64(1); act != exp {
			return errors.Errorf("%d pushees, but want %d", act, exp)
		}
		if act, exp := m.QueryWaiting.Value(), int64(1); act != exp {
			return errors.Errorf("%d queries, but want %d", act, exp)
		}
		return nil
	})

	txn.Status = roachpb.COMMITTED
	if err := writeTxnRecord(ctx, &tc, txn); err != nil {
		t.Fatal(err)
	}

	// Skip calling q.UpdateTxn to test that the wait queue periodically polls
	// txn's record and notices when it is no longer pending.

	respErr := <-retCh
	if respErr != nil {
		t.Errorf("expected nil err; got %+v", respErr)
	}
	testutils.SucceedsSoon(t, func() error {
		if act, exp := m.PusherWaiting.Value(), int64(1); act != exp {
			return errors.Errorf("%d pushers, but want %d", act, exp)
		}
		if act, exp := m.PusheeWaiting.Value(), int64(1); act != exp {
			return errors.Errorf("%d pushees, but want %d", act, exp)
		}
		if act, exp := m.QueryWaiting.Value(), int64(0); act != exp {
			return errors.Errorf("%d queries, but want %d", act, exp)
		}
		return nil
	})
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
	pusher := newTransaction("pusher", roachpb.Key("a"), 1, tc.Clock())
	req := roachpb.PushTxnRequest{
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *pusher,
		PusheeTxn: txn.TxnMeta,
	}

	q := tc.repl.txnWaitQueue
	q.Enable()
	q.Enqueue(txn)

	retCh := make(chan *roachpb.Error, 1)
	go func() {
		retCh <- q.MaybeWaitForPush(context.Background(), tc.repl, &req)
	}()

	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{pusher.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	updatedTxn := *txn
	updatedTxn.WriteTimestamp = txn.WriteTimestamp.Add(1, 0)
	q.UpdateTxn(context.Background(), &updatedTxn)

	respErr := <-retCh
	if respErr != nil {
		t.Errorf("expected nil err; got %+v", respErr)
	}
	testutils.SucceedsSoon(tc.TB, func() error {
		return checkAllGaugesZero(tc)
	})
}

// TestTxnWaitQueuePusheeExpires verifies that just one pusher is
// returned when the pushee's txn may have expired.
func TestTxnWaitQueuePusheeExpires(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var queryTxnCount int32

	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	txn := newTransaction("txn", roachpb.Key("a"), 1, clock)
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

	pusher1 := newTransaction("pusher1", roachpb.Key("a"), 1, tc.Clock())
	pusher2 := newTransaction("pusher2", roachpb.Key("a"), 1, tc.Clock())
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

	retCh := make(chan *roachpb.Error, 2)
	go func() {
		retCh <- q.MaybeWaitForPush(context.Background(), tc.repl, &req1)
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{pusher1.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	go func() {
		retCh <- q.MaybeWaitForPush(context.Background(), tc.repl, &req2)
	}()
	testutils.SucceedsSoon(t, func() error {
		expDeps := []uuid.UUID{pusher1.ID, pusher2.ID}
		if deps := q.GetDependents(txn.ID); !reflect.DeepEqual(deps, expDeps) {
			return errors.Errorf("expected GetDependents %+v; got %+v", expDeps, deps)
		}
		return nil
	})

	for i := 0; i < 2; i++ {
		respErr := <-retCh
		if respErr != nil {
			t.Errorf("expected nil error; got %s", respErr)
		}
	}

	m := tc.store.GetTxnWaitMetrics()
	testutils.SucceedsSoon(t, func() error {
		if act, exp := m.PusherWaiting.Value(), int64(2); act != exp {
			return errors.Errorf("%d pushers, but want %d", act, exp)
		}
		if act, exp := m.PusheeWaiting.Value(), int64(1); act != exp {
			return errors.Errorf("%d pushees, but want %d", act, exp)
		}
		if act, exp := m.QueryWaiting.Value(), int64(0); act != exp {
			return errors.Errorf("%d queries, but want %d", act, exp)
		}
		return nil
	})
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
			pusher = newTransaction("pusher", roachpb.Key("a"), 1, tc.Clock())
		}

		req := roachpb.PushTxnRequest{
			PushType:  roachpb.PUSH_ABORT,
			PusherTxn: *pusher,
			PusheeTxn: txn.TxnMeta,
		}

		q := tc.repl.txnWaitQueue
		q.Enable()
		q.Enqueue(txn)

		retCh := make(chan *roachpb.Error, 1)
		go func() {
			retCh <- q.MaybeWaitForPush(context.Background(), tc.repl, &req)
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

		respErr := <-retCh
		expErr := "TransactionAbortedError(ABORT_REASON_PUSHER_ABORTED)"
		if !testutils.IsPError(respErr, regexp.QuoteMeta(expErr)) {
			t.Errorf("expected %s; got %v", expErr, respErr)
		}

		m := tc.store.GetTxnWaitMetrics()
		testutils.SucceedsSoon(t, func() error {
			if act, exp := m.PusherWaiting.Value(), int64(1); act != exp {
				return errors.Errorf("%d pushers, but want %d", act, exp)
			}
			if act, exp := m.PusheeWaiting.Value(), int64(1); act != exp {
				return errors.Errorf("%d pushees, but want %d", act, exp)
			}
			if act, exp := m.QueryWaiting.Value(), int64(0); act != exp {
				return errors.Errorf("%d queries, but want %d", act, exp)
			}
			return nil
		})
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
		RequestHeader: roachpb.RequestHeader{
			Key: txnB.Key,
		},
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *txnA,
		PusheeTxn: txnB.TxnMeta,
	}
	reqB := &roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txnC.Key,
		},
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *txnB,
		PusheeTxn: txnC.TxnMeta,
	}
	reqC := &roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txnA.Key,
		},
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
	m := tc.store.GetTxnWaitMetrics()
	assert.EqualValues(tc, 0, m.DeadlocksTotal.Count())

	retCh := make(chan *roachpb.Error, 3)
	for _, req := range []*roachpb.PushTxnRequest{reqA, reqB, reqC} {
		go func(req *roachpb.PushTxnRequest) {
			retCh <- q.MaybeWaitForPush(ctx, tc.repl, req)
		}(req)
	}

	// Wait for first request to finish, which should break the
	// dependency cycle by performing a force push abort.
	respErr := <-retCh
	require.Nil(t, respErr)

	testutils.SucceedsSoon(t, func() error {
		if act, exp := m.DeadlocksTotal.Count(), int64(0); act <= exp {
			return errors.Errorf("%d deadlocks, but want more than %d", act, exp)
		}
		return nil
	})
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
	txnA := newTransaction("txn", roachpb.Key("a"), -1, tc.Clock())
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
	txnB := newTransaction("txn", roachpb.Key("a"), -2, tc.Clock())
	if err := writeTxnRecord(context.Background(), &tc, txnB); err != nil {
		t.Fatal(err)
	}

	reqA := &roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txnB.Key,
		},
		PushType:  roachpb.PUSH_ABORT,
		PusherTxn: *txnA,
		PusheeTxn: txnB.TxnMeta,
	}
	reqB := &roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txnA.Key,
		},
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
	m := tc.store.GetTxnWaitMetrics()
	assert.EqualValues(tc, 0, m.DeadlocksTotal.Count())

	retCh := make(chan ReqWithErr, 2)
	for _, req := range []*roachpb.PushTxnRequest{reqA, reqB} {
		go func(req *roachpb.PushTxnRequest) {
			pErr := q.MaybeWaitForPush(ctx, tc.repl, req)
			retCh <- ReqWithErr{req, pErr}
		}(req)
	}

	// Wait for first request to finish, which should break the
	// dependency cycle by returning an ErrDeadlock error. The
	// returned request should be reqA.
	reqWithErr := <-retCh
	require.Nil(t, reqWithErr.pErr)
	require.Equal(t, reqA, reqWithErr.req)

	testutils.SucceedsSoon(t, func() error {
		if act, exp := m.DeadlocksTotal.Count(), int64(0); act <= exp {
			return errors.Errorf("%d deadlocks, but want more than %d", act, exp)
		}
		return nil
	})
	cancel()
	<-retCh
}
