// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func makeMockTxnHeartbeater(
	txn *roachpb.Transaction,
) (th txnHeartbeater, mockSender, mockGatekeeper *mockLockedSender) {
	mockSender, mockGatekeeper = &mockLockedSender{}, &mockLockedSender{}
	manual := hlc.NewManualClock(123)
	th.init(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		stop.NewStopper(),
		hlc.NewClock(manual.UnixNano, time.Nanosecond),
		new(TxnMetrics),
		1*time.Millisecond,
		mockGatekeeper,
		new(syncutil.Mutex),
		txn,
	)
	th.setWrapped(mockSender)
	return th, mockSender, mockGatekeeper
}

func waitForHeartbeatLoopToStop(t *testing.T, th *txnHeartbeater) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		th.mu.Lock()
		defer th.mu.Unlock()
		if th.heartbeatLoopRunningLocked() {
			return errors.New("txn heartbeat loop running")
		}
		return nil
	})
}

// TestTxnHeartbeaterSetsTransactionKey tests that the txnHeartbeater sets the
// transaction key to the key of the first write that is sent through it.
func TestTxnHeartbeaterSetsTransactionKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	txn := makeTxnProto()
	txn.Key = nil // reset
	th, mockSender, _ := makeMockTxnHeartbeater(&txn)
	defer th.stopper.Stop(ctx)

	// No key is set on a read-only batch.
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: txn.Clone()}
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.Equal(t, keyA, ba.Requests[0].GetInner().Header().Key)
		require.Equal(t, keyB, ba.Requests[1].GetInner().Header().Key)

		require.Equal(t, txn.ID, ba.Txn.ID)
		require.Nil(t, ba.Txn.Key)

		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr := th.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Nil(t, txn.Key)

	// The key of the first write is set as the transaction key.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}})
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.Equal(t, keyB, ba.Requests[0].GetInner().Header().Key)
		require.Equal(t, keyA, ba.Requests[1].GetInner().Header().Key)

		require.Equal(t, txn.ID, ba.Txn.ID)
		require.Equal(t, keyB, roachpb.Key(ba.Txn.Key))

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = th.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, keyB, roachpb.Key(txn.Key))

	// The transaction key is not changed on subsequent batches.
	ba.Requests = nil
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		require.Len(t, ba.Requests, 1)
		require.Equal(t, keyA, ba.Requests[0].GetInner().Header().Key)

		require.Equal(t, txn.ID, ba.Txn.ID)
		require.Equal(t, keyB, roachpb.Key(ba.Txn.Key))

		br = ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	br, pErr = th.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
	require.Equal(t, keyB, roachpb.Key(txn.Key))
}

// TestTxnHeartbeaterLoopStartedOnFirstLock tests that the txnHeartbeater
// doesn't start its heartbeat loop until it observes the transaction issues
// a request that will acquire locks.
func TestTxnHeartbeaterLoopStartedOnFirstLock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunTrueAndFalse(t, "write", func(t *testing.T, write bool) {
		ctx := context.Background()
		txn := makeTxnProto()
		th, _, _ := makeMockTxnHeartbeater(&txn)
		defer th.stopper.Stop(ctx)

		// Read-only requests don't start the heartbeat loop.
		keyA := roachpb.Key("a")
		keyAHeader := roachpb.RequestHeader{Key: keyA}
		var ba roachpb.BatchRequest
		ba.Header = roachpb.Header{Txn: txn.Clone()}
		ba.Add(&roachpb.GetRequest{RequestHeader: keyAHeader})

		br, pErr := th.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		th.mu.Lock()
		require.False(t, th.mu.loopStarted)
		require.False(t, th.heartbeatLoopRunningLocked())
		th.mu.Unlock()

		// The heartbeat loop is started on the first locking request.
		ba.Requests = nil
		if write {
			ba.Add(&roachpb.PutRequest{RequestHeader: keyAHeader})
		} else {
			ba.Add(&roachpb.ScanRequest{RequestHeader: keyAHeader, KeyLocking: lock.Exclusive})
		}

		br, pErr = th.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		th.mu.Lock()
		require.True(t, th.mu.loopStarted)
		require.True(t, th.heartbeatLoopRunningLocked())
		th.mu.Unlock()

		// Closing the interceptor stops the heartbeat loop.
		th.mu.Lock()
		th.closeLocked()
		th.mu.Unlock()
		waitForHeartbeatLoopToStop(t, &th)
		require.True(t, th.mu.loopStarted) // still set
	})
}

// TestTxnHeartbeaterLoopNotStartedFor1PC tests that the txnHeartbeater does
// not start a heartbeat loop if it detects a 1PC transaction.
func TestTxnHeartbeaterLoopNotStartedFor1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	txn := makeTxnProto()
	th, _, _ := makeMockTxnHeartbeater(&txn)
	defer th.stopper.Stop(ctx)

	keyA := roachpb.Key("a")
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: txn.Clone()}
	ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})
	ba.Add(&roachpb.EndTxnRequest{Commit: true})

	br, pErr := th.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	th.mu.Lock()
	require.False(t, th.mu.loopStarted)
	require.False(t, th.heartbeatLoopRunningLocked())
	th.mu.Unlock()
}

// TestTxnHeartbeaterLoopRequests tests that the HeartbeatTxnRequests that the
// txnHeartbeater sends contain the correct information. It then tests that the
// heartbeat loop shuts itself down if it detects a committed transaction. This
// can occur through two different paths. A heartbeat request itself can find
// a committed transaction record or the request can race with a request sent
// from the transaction coordinator that finalizes the transaction.
func TestTxnHeartbeaterLoopRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunTrueAndFalse(t, "heartbeatObserved", func(t *testing.T, heartbeatObserved bool) {
		ctx := context.Background()
		txn := makeTxnProto()
		th, _, mockGatekeeper := makeMockTxnHeartbeater(&txn)
		defer th.stopper.Stop(ctx)

		var count int
		var lastTime hlc.Timestamp
		mockGatekeeper.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &roachpb.HeartbeatTxnRequest{}, ba.Requests[0].GetInner())

			hbReq := ba.Requests[0].GetInner().(*roachpb.HeartbeatTxnRequest)
			require.Equal(t, &txn, ba.Txn)
			require.Equal(t, roachpb.Key(txn.Key), hbReq.Key)
			require.True(t, lastTime.Less(hbReq.Now))

			count++
			lastTime = hbReq.Now

			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})

		// Kick off the heartbeat loop.
		keyA := roachpb.Key("a")
		var ba roachpb.BatchRequest
		ba.Header = roachpb.Header{Txn: txn.Clone()}
		ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

		br, pErr := th.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		// Wait for 5 heartbeat requests.
		testutils.SucceedsSoon(t, func() error {
			th.mu.Lock()
			defer th.mu.Unlock()
			require.True(t, th.mu.loopStarted)
			require.True(t, th.heartbeatLoopRunningLocked())
			if count < 5 {
				return errors.Errorf("waiting for more heartbeat requests, found %d", count)
			}
			return nil
		})

		// Mark the coordinator's transaction record as COMMITTED while a heartbeat
		// is in-flight. This should cause the heartbeat loop to shut down.
		th.mu.Lock()
		mockGatekeeper.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &roachpb.HeartbeatTxnRequest{}, ba.Requests[0].GetInner())

			br := ba.CreateReply()
			br.Txn = ba.Txn
			if heartbeatObserved {
				// Mimic a Heartbeat request that observed a committed record.
				br.Txn.Status = roachpb.COMMITTED
			} else {
				// Mimic an EndTxn that raced with the heartbeat loop.
				txn.Status = roachpb.COMMITTED
			}
			return br, nil
		})
		th.mu.Unlock()
		waitForHeartbeatLoopToStop(t, &th)

		// Depending on how the committed transaction was observed, we may or
		// may not expect the heartbeater's final observed status to be set.
		if heartbeatObserved {
			require.Equal(t, roachpb.COMMITTED, th.mu.finalObservedStatus)
		} else {
			require.Equal(t, roachpb.PENDING, th.mu.finalObservedStatus)
		}
	})
}

// TestTxnHeartbeaterAsyncAbort tests that the txnHeartbeater rolls back the
// transaction asynchronously if it detects an aborted transaction, either
// through a TransactionAbortedError or through an ABORTED transaction proto
// in the HeartbeatTxn response.
func TestTxnHeartbeaterAsyncAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunTrueAndFalse(t, "abortedErr", func(t *testing.T, abortedErr bool) {
		ctx := context.Background()
		txn := makeTxnProto()
		th, mockSender, mockGatekeeper := makeMockTxnHeartbeater(&txn)
		defer th.stopper.Stop(ctx)

		putDone, asyncAbortDone := make(chan struct{}), make(chan struct{})
		mockGatekeeper.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			// Wait for the Put to finish to avoid a data race.
			<-putDone

			require.Len(t, ba.Requests, 1)
			require.IsType(t, &roachpb.HeartbeatTxnRequest{}, ba.Requests[0].GetInner())

			if abortedErr {
				return nil, roachpb.NewErrorWithTxn(
					roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_UNKNOWN), ba.Txn,
				)
			}
			br := ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.ABORTED
			return br, nil
		})

		// Kick off the heartbeat loop.
		keyA := roachpb.Key("a")
		var ba roachpb.BatchRequest
		ba.Header = roachpb.Header{Txn: txn.Clone()}
		ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}})

		br, pErr := th.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		// Test that the transaction is rolled back.
		mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			defer close(asyncAbortDone)
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &roachpb.EndTxnRequest{}, ba.Requests[0].GetInner())

			etReq := ba.Requests[0].GetInner().(*roachpb.EndTxnRequest)
			require.Equal(t, &txn, ba.Txn)
			require.Nil(t, etReq.Key) // set in txnCommitter
			require.False(t, etReq.Commit)
			require.True(t, etReq.Poison)

			br = ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.ABORTED
			return br, nil
		})
		close(putDone)

		// The heartbeat loop should eventually close.
		waitForHeartbeatLoopToStop(t, &th)

		// Wait for the async abort to finish.
		<-asyncAbortDone

		// Regardless of which channel informed the heartbeater of the
		// transaction's aborted status, we expect the heartbeater's final
		// observed status to be set.
		require.Equal(t, roachpb.ABORTED, th.mu.finalObservedStatus)
	})
}
