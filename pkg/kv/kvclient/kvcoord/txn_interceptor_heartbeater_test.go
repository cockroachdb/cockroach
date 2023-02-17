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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func makeMockTxnHeartbeater(
	txn *roachpb.Transaction,
) (th txnHeartbeater, mockSender, mockGatekeeper *mockLockedSender) {
	mockSender, mockGatekeeper = &mockLockedSender{}, &mockLockedSender{}
	th.init(
		log.MakeTestingAmbientCtxWithNewTracer(),
		stop.NewStopper(),
		hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123))),
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	txn := makeTxnProto()
	txn.Key = nil // reset
	th, mockSender, _ := makeMockTxnHeartbeater(&txn)
	defer th.stopper.Stop(ctx)

	// No key is set on a read-only batch.
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: txn.Clone()}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
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
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyB}})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
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
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
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
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "write", func(t *testing.T, write bool) {
		ctx := context.Background()
		txn := makeTxnProto()
		th, mockSender, _ := makeMockTxnHeartbeater(&txn)
		defer th.stopper.Stop(ctx)

		// Read-only requests don't start the heartbeat loop.
		keyA := roachpb.Key("a")
		keyAHeader := kvpb.RequestHeader{Key: keyA}
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.GetRequest{RequestHeader: keyAHeader})

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
			ba.Add(&kvpb.PutRequest{RequestHeader: keyAHeader})
		} else {
			ba.Add(&kvpb.ScanRequest{RequestHeader: keyAHeader, KeyLocking: lock.Exclusive})
		}

		br, pErr = th.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		th.mu.Lock()
		require.True(t, th.mu.loopStarted)
		require.True(t, th.heartbeatLoopRunningLocked())
		th.mu.Unlock()

		// The interceptor indicates whether the heartbeat loop is
		// running on EndTxn requests.
		ba.Requests = nil
		ba.Add(&kvpb.EndTxnRequest{Commit: true})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

			br = ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.COMMITTED
			return br, nil
		})
		br, pErr = th.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		// Closing the interceptor stops the heartbeat loop.
		th.mu.Lock()
		th.closeLocked()
		th.mu.Unlock()
		waitForHeartbeatLoopToStop(t, &th)
		require.True(t, th.mu.loopStarted) // still set
	})
}

// Tests that the txnHeartbeater only starts its heartbeat loop immediately
// (upon observing a request that will acquire locks) when the transaction
// would otherwise be considered expired. Otherwise, the loop starts after a
// delay of one interval, or potentially sooner using a 200ms buffer period.
// E.g. with default heartbeat interval of 1s, and expiration at 5 intervals:
// 0-3.8s: heartbeat starts after interval
// 3.8s-4.8s: heartbeat starts by expiration-buffer (by 4.8s)
// 4.8s-onwards: heartbeat starts immediately.
func TestTxnHeartbeaterLoopStartsBeforeExpiry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Represents the time at which the heartbeat loop should begin.
	type HeartbeatLoopExpectation int
	const (
		StartImmediately HeartbeatLoopExpectation = iota
		StartBeforeInterval
		StartAfterInterval
	)

	for _, test := range []struct {
		lockingRequestDelay time.Duration
		consideredExpired   bool
		loopStarts          HeartbeatLoopExpectation
	}{
		{
			// No delay prior to first locking request. Heartbeat after loopInterval.
			consideredExpired: false,
			loopStarts:        StartAfterInterval,
		},
		{
			// First locking request happens before expiration, but more than
			// loopInterval+buffer from expiration. Heartbeat after loopInterval.
			lockingRequestDelay: 3*time.Second + 799*time.Millisecond,
			consideredExpired:   false,
			loopStarts:          StartAfterInterval,
		},
		{
			// First locking request happens before expiration, but less than
			// loopInterval+buffer from expiration. Heartbeat before loopInterval.
			lockingRequestDelay: 4*time.Second + 500*time.Millisecond,
			consideredExpired:   false,
			loopStarts:          StartBeforeInterval,
		},
		{
			// First locking request happens before expiration, but less than buffer
			// from expiration. Heartbeat immediately.
			lockingRequestDelay: 5*time.Second - 100*time.Millisecond,
			consideredExpired:   false,
			loopStarts:          StartImmediately,
		},
		{
			// First locking request happens at expiration. Heartbeat immediately.
			lockingRequestDelay: 5 * time.Second,
			consideredExpired:   true,
			loopStarts:          StartImmediately,
		},
		{
			// First locking request happens after expiration. Heartbeat immediately.
			lockingRequestDelay: 10 * time.Second,
			consideredExpired:   true,
			loopStarts:          StartImmediately,
		},
	} {
		t.Run(fmt.Sprintf("delay=%s", test.lockingRequestDelay), func(t *testing.T) {
			ctx := context.Background()
			txn := makeTxnProto()

			manualTime := timeutil.NewManualTime(timeutil.Unix(0, 123))
			clock := hlc.NewClockForTesting(manualTime)
			txn.MinTimestamp, txn.WriteTimestamp = clock.Now(), clock.Now()

			// We attempt to simulate a transaction that heartbeats every 1s, however
			// it is important to note that a transaction is considered expired when it
			// has a LastActive timestamp older than 5X the default interval of 1 second.
			heartbeatInterval := base.DefaultTxnHeartbeatInterval
			manualTime.Advance(test.lockingRequestDelay)

			var th txnHeartbeater
			mockSender, mockGatekeeper := &mockLockedSender{}, &mockLockedSender{}
			th.init(
				log.MakeTestingAmbientCtxWithNewTracer(),
				stop.NewStopper(),
				clock,
				new(TxnMetrics),
				heartbeatInterval,
				mockGatekeeper,
				new(syncutil.Mutex),
				&txn,
			)
			th.setWrapped(mockSender)
			defer th.stopper.Stop(ctx)

			th.mu.Lock()
			require.False(t, th.mu.loopStarted)
			require.False(t, th.heartbeatLoopRunningLocked())
			th.mu.Unlock()

			count := 0
			mockGatekeeper.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.IsType(t, &kvpb.HeartbeatTxnRequest{}, ba.Requests[0].GetInner())

				hbReq := ba.Requests[0].GetInner().(*kvpb.HeartbeatTxnRequest)
				require.Equal(t, &txn, ba.Txn)
				require.Equal(t, roachpb.Key(txn.Key), hbReq.Key)

				// Check that this transaction isn't already considered expired.
				if !test.consideredExpired && txnwait.IsExpired(clock.Now(), ba.Txn) {
					return nil, kvpb.NewError(errors.New("transaction expired before heartbeat"))
				}

				log.Infof(ctx, "received heartbeat request")
				count++

				br := ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			})

			// Validate that, if delayed, this transaction would be considered expired by now.
			require.Equal(t, test.consideredExpired, txnwait.IsExpired(clock.Now(), &txn))

			// The heartbeat loop is started on the first locking request, in this case
			// a GetForUpdate request.
			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: txn.Clone()}
			keyA := roachpb.Key("a")
			keyAHeader := kvpb.RequestHeader{Key: keyA}
			ba.Add(&kvpb.GetRequest{RequestHeader: keyAHeader, KeyLocking: lock.Exclusive})

			br, pErr := th.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)

			th.mu.Lock()
			require.True(t, th.mu.loopStarted)
			require.True(t, th.heartbeatLoopRunningLocked())
			if test.loopStarts == StartImmediately {
				// In the case where we'd already be considered expired, we want to
				// ensure the transaction heartbeats synchronously, before there are
				// locks for some other transaction to push (after their 50ms liveness
				// push delay).
				require.Positivef(t, count, "expected heartbeat before starting loop")
			}
			th.mu.Unlock()

			if test.loopStarts == StartBeforeInterval {
				// Ensure that we heartbeat before the full interval in the case where
				// we are within the buffer period of one full loop interval from expiry.
				testutils.SucceedsWithin(t, func() error {
					th.mu.Lock()
					defer th.mu.Unlock()
					if count < 1 {
						return errors.Errorf("waiting for more heartbeat requests, found %d", count)
					}
					return nil
				}, heartbeatInterval)
			}

			// Ensure that we get a heartbeat before we are considered expired,
			// even if starting the loop after one heartbeat interval has passed.
			if !test.consideredExpired {
				expiration := time.Duration(5) * heartbeatInterval
				testutils.SucceedsWithin(t, func() error {
					th.mu.Lock()
					defer th.mu.Unlock()
					require.True(t, th.mu.loopStarted)
					require.True(t, th.heartbeatLoopRunningLocked())
					if count < 1 {
						return errors.Errorf("waiting for more heartbeat requests, found %d", count)
					}
					return nil
				}, expiration)
			}
		})
	}
}

// TestTxnHeartbeaterLoopStartedFor1PC tests that the txnHeartbeater
// starts a heartbeat loop if it detects a 1PC transaction.
func TestTxnHeartbeaterLoopStartedFor1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	txn := makeTxnProto()
	th, mockSender, _ := makeMockTxnHeartbeater(&txn)
	defer th.stopper.Stop(ctx)

	keyA := roachpb.Key("a")
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: txn.Clone()}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})
	ba.Add(&kvpb.EndTxnRequest{Commit: true})

	mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		require.Len(t, ba.Requests, 2)
		require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())
		require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[1].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.COMMITTED
		return br, nil
	})
	br, pErr := th.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)

	th.mu.Lock()
	require.True(t, th.mu.loopStarted)
	require.True(t, th.heartbeatLoopRunningLocked())
	th.closeLocked()
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
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "heartbeatObserved", func(t *testing.T, heartbeatObserved bool) {
		ctx := context.Background()
		txn := makeTxnProto()
		th, _, mockGatekeeper := makeMockTxnHeartbeater(&txn)
		defer th.stopper.Stop(ctx)

		var count int
		var lastTime hlc.Timestamp
		mockGatekeeper.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.HeartbeatTxnRequest{}, ba.Requests[0].GetInner())

			hbReq := ba.Requests[0].GetInner().(*kvpb.HeartbeatTxnRequest)
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
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})

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
		mockGatekeeper.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.HeartbeatTxnRequest{}, ba.Requests[0].GetInner())

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
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "abortedErr", func(t *testing.T, abortedErr bool) {
		ctx := context.Background()
		txn := makeTxnProto()
		th, mockSender, mockGatekeeper := makeMockTxnHeartbeater(&txn)
		defer th.stopper.Stop(ctx)

		putDone, asyncAbortDone := make(chan struct{}), make(chan struct{})
		mockGatekeeper.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			// Wait for the Put to finish to avoid a data race.
			<-putDone

			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.HeartbeatTxnRequest{}, ba.Requests[0].GetInner())

			if abortedErr {
				return nil, kvpb.NewErrorWithTxn(
					kvpb.NewTransactionAbortedError(kvpb.ABORT_REASON_UNKNOWN), ba.Txn,
				)
			}
			br := ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.ABORTED
			return br, nil
		})

		// Kick off the heartbeat loop.
		keyA := roachpb.Key("a")
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keyA}})

		br, pErr := th.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		// Test that the transaction is rolled back.
		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			defer close(asyncAbortDone)
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

			etReq := ba.Requests[0].GetInner().(*kvpb.EndTxnRequest)
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

// TestTxnHeartbeaterAsyncAbortWaitsForInFlight tests that the txnHeartbeater
// will wait for an in-flight request to complete before sending the
// EndTxn rollback request.
func TestTxnHeartbeaterAsyncAbortWaitsForInFlight(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	txn := makeTxnProto()
	th, mockSender, mockGatekeeper := makeMockTxnHeartbeater(&txn)
	defer th.stopper.Stop(ctx)

	// Mock the heartbeat request, which should wait for an in-flight put via
	// putReady then return an aborted txn and signal hbAborted.
	putReady := make(chan struct{})
	hbAborted := make(chan struct{})
	mockGatekeeper.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		<-putReady
		defer close(hbAborted)

		require.Len(t, ba.Requests, 1)
		require.IsType(t, &kvpb.HeartbeatTxnRequest{}, ba.Requests[0].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.ABORTED
		return br, nil
	})

	putResume := make(chan struct{})
	rollbackSent := make(chan struct{})
	mockSender.ChainMockSend(
		// Mock a Put, which signals putReady and then waits for putResume
		// before returning a response.
		func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			th.mu.Unlock() // without txnLockGatekeeper, we must unlock manually
			defer th.mu.Lock()
			close(putReady)
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

			<-putResume

			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		},
		// Mock an EndTxn, which signals rollbackSent.
		func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			defer close(rollbackSent)
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

			etReq := ba.Requests[0].GetInner().(*kvpb.EndTxnRequest)
			require.Equal(t, &txn, ba.Txn)
			require.False(t, etReq.Commit)
			require.True(t, etReq.Poison)

			br := ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.ABORTED
			return br, nil
		},
	)

	// Spawn a goroutine to send the Put.
	require.NoError(t, th.stopper.RunAsyncTask(ctx, "put", func(ctx context.Context) {
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("a")}})

		th.mu.Lock() // without TxnCoordSender, we must lock manually
		defer th.mu.Unlock()
		br, pErr := th.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
	}))

	<-putReady  // wait for put
	<-hbAborted // wait for heartbeat abort
	select {
	case <-rollbackSent: // we don't expect a rollback yet
		require.Fail(t, "received unexpected EndTxn")
	case <-time.After(20 * time.Millisecond):
	}
	close(putResume) // make put return
	<-rollbackSent   // we now expect the rollback

	// The heartbeat loop should eventually close.
	waitForHeartbeatLoopToStop(t, &th)
}

// TestTxnHeartbeaterAsyncAbortCollapsesRequests tests that when the
// txnHeartbeater has an async abort rollback in flight, any client
// rollbacks will wait for the async rollback to complete and return
// its result.
func TestTxnHeartbeaterAsyncAbortCollapsesRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	txn := makeTxnProto()
	th, mockSender, mockGatekeeper := makeMockTxnHeartbeater(&txn)
	defer th.stopper.Stop(ctx)

	// Mock the heartbeat request, which simply aborts and signals hbAborted.
	hbAborted := make(chan struct{})
	mockGatekeeper.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		defer close(hbAborted)

		require.Len(t, ba.Requests, 1)
		require.IsType(t, &kvpb.HeartbeatTxnRequest{}, ba.Requests[0].GetInner())

		br := ba.CreateReply()
		br.Txn = ba.Txn
		br.Txn.Status = roachpb.ABORTED
		return br, nil
	})

	// Mock an EndTxn response, which signals rollbackReady and blocks
	// until rollbackUnblock is closed.
	rollbackReady := make(chan struct{})
	rollbackUnblock := make(chan struct{})
	mockSender.ChainMockSend(
		// The first Put request is expected and should just return.
		func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.PutRequest{}, ba.Requests[0].GetInner())

			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		},
		// The first EndTxn request from the heartbeater is expected, so block and return.
		func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			th.mu.Unlock() // manually unlock for concurrency, no txnLockGatekeeper
			defer th.mu.Lock()
			close(rollbackReady)
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &kvpb.EndTxnRequest{}, ba.Requests[0].GetInner())

			<-rollbackUnblock

			etReq := ba.Requests[0].GetInner().(*kvpb.EndTxnRequest)
			require.Equal(t, &txn, ba.Txn)
			require.False(t, etReq.Commit)
			require.True(t, etReq.Poison)

			br := ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.ABORTED
			return br, nil
		},
		// The second EndTxn request from the client is unexpected, so
		// return an error response.
		func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			return nil, kvpb.NewError(errors.Errorf("unexpected request: %v", ba))
		},
	)

	// Kick off the heartbeat loop.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: txn.Clone()}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("a")}})

	th.mu.Lock() // manually lock, there's no TxnCoordSender
	br, pErr := th.SendLocked(ctx, ba)
	th.mu.Unlock()
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// Wait for the heartbeater to abort and send an EndTxn.
	<-hbAborted
	<-rollbackReady

	// Send a rollback from the client. This should be collapsed together
	// with the heartbeat abort, and block until it returns. We spawn
	// a goroutine to unblock the rollback.
	require.NoError(t, th.stopper.RunAsyncTask(ctx, "put", func(ctx context.Context) {
		time.Sleep(100 * time.Millisecond)
		close(rollbackUnblock)
	}))

	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: txn.Clone()}
	ba.Add(&kvpb.EndTxnRequest{Commit: false})

	th.mu.Lock() // manually lock, there's no TxnCoordSender
	br, pErr = th.SendLocked(ctx, ba)
	th.mu.Unlock()
	require.Nil(t, pErr)
	require.NotNil(t, br)

	// The heartbeat loop should eventually close.
	waitForHeartbeatLoopToStop(t, &th)
}

func heartbeaterRunning(th *txnHeartbeater) bool {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.heartbeatLoopRunningLocked()
}

// TestTxnHeartbeaterCommitLoopHandling tests that interceptor cancels
// heartbeats early for rolled back transactions while keeping it untouched
// for committed ones.
func TestTxnHeartbeaterEndTxnLoopHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		transactionCommit      bool
		expectHeartbeatRunning bool
	}{
		{true, true},
		{false, false},
	} {
		t.Run(fmt.Sprintf("commit:%t", tc.transactionCommit), func(t *testing.T) {
			ctx := context.Background()
			txn := makeTxnProto()
			th, _, _ := makeMockTxnHeartbeater(&txn)
			defer th.stopper.Stop(ctx)

			// Kick off the heartbeat loop.
			key := roachpb.Key("a")
			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: txn.Clone()}
			ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: key}})

			br, pErr := th.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)
			require.True(t, heartbeaterRunning(&th), "heartbeat running")

			// End transaction to validate heartbeat state.
			ba2 := &kvpb.BatchRequest{}
			ba2.Header = kvpb.Header{Txn: txn.Clone()}
			ba2.Add(&kvpb.EndTxnRequest{RequestHeader: kvpb.RequestHeader{Key: key}, Commit: tc.transactionCommit})

			th.mu.Lock()
			br, pErr = th.SendLocked(ctx, ba2)
			th.mu.Unlock()

			require.Nil(t, pErr)
			require.NotNil(t, br)
			require.Equal(t, tc.expectHeartbeatRunning, heartbeaterRunning(&th), "heartbeat loop state")
		})
	}
}
