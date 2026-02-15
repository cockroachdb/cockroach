// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// This file contains contains integration tests that don't fit anywhere else.
// Generally its meant to test scenarios involving both "the client" and "the
// server".

// Test that waiters on transactions whose commit command is rejected see the
// transaction as Aborted. This test is a regression test for #30792 which was
// causing pushers in the txn wait queue to consider such a transaction
// committed. It is also a regression test for a similar bug [1] in which
// it was not the notification to the txn wait queue that was leaked, but the
// intents.
//
// The test sets up two ranges and lets a transaction (anchored on the left)
// write to both of them. It then starts readers for both keys written by the
// txn and waits for them to enter the txn wait queue. Next, it lets the txn
// attempt to commit but injects a forced error below Raft. The bugs would
// formerly notify the txn wait queue that the transaction had committed (not
// true) and that its external intent (i.e. the one on the right range) could
// be resolved (not true). Verify that neither occurs.
//
// [1]: https://github.com/cockroachdb/cockroach/issues/34025#issuecomment-460934278
func TestWaiterOnRejectedCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// The txn id whose commit we're going to reject. A uuid.UUID.
	var txnID atomic.Value
	// The EndTxn proposal that we want to reject. A string.
	var commitCmdID atomic.Value
	readerBlocked := make(chan struct{}, 2)
	// txnUpdate is signaled once the txn wait queue is updated for our
	// transaction. Normally it only needs a buffer length of 1, but bugs that
	// cause it to be pinged several times (e.g. #30792) might need a bigger
	// buffer to avoid the test timing out.
	txnUpdate := make(chan roachpb.TransactionStatus, 20)

	illegalLeaseIndex := true
	s, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				DisableSplitQueue: true,
				TestingProposalFilter: func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
					// We'll recognize the attempt to commit our transaction and store the
					// respective command id.
					ba := args.Req
					etReq, ok := ba.GetArg(kvpb.EndTxn)
					if !ok {
						return nil
					}
					if !etReq.(*kvpb.EndTxnRequest).Commit {
						return nil
					}
					v := txnID.Load()
					if v == nil {
						return nil
					}
					if !ba.Txn.ID.Equal(v.(uuid.UUID)) {
						return nil
					}
					t.Logf("EndTxn command ID: %v", args.CmdID)
					commitCmdID.Store(args.CmdID)
					return nil
				},
				TestingApplyCalledTwiceFilter: func(args kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
					// We'll trap the processing of the commit command and return an error
					// for it.
					v := commitCmdID.Load()
					if v == nil {
						return 0, nil
					}
					cmdID := v.(kvserverbase.CmdIDKey)
					if args.CmdID == cmdID {
						if illegalLeaseIndex {
							illegalLeaseIndex = false
							t.Logf("injecting illegal lease index for %v", cmdID)
							// NB: 1 is ProposalRejectionIllegalLeaseIndex.
							return 1, kvpb.NewErrorf("test injected err (illegal lease index)")
						}
						t.Logf("injecting permanent rejection for %v", cmdID)
						// NB: 0 is ProposalRejectionPermanent.
						return 0, kvpb.NewErrorf("test injected err")
					}
					return 0, nil
				},
				TxnWaitKnobs: txnwait.TestingKnobs{
					OnPusherBlocked: func(ctx context.Context, push *kvpb.PushTxnRequest) {
						// We'll trap a reader entering the wait queue for our txn.
						v := txnID.Load()
						if v == nil {
							return
						}
						if push.PusheeTxn.ID.Equal(v.(uuid.UUID)) {
							readerBlocked <- struct{}{}
						}
					},
					OnTxnUpdate: func(ctx context.Context, txn *roachpb.Transaction) {
						// We'll trap updates to our txn.
						v := txnID.Load()
						if v == nil {
							return
						}
						if txn.ID.Equal(v.(uuid.UUID)) {
							txnUpdate <- txn.Status
						}
					},
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	// Disable parallel commits protocol. In some rare cases, we see two different
	// command ids for the committing this transaction. The first one could be the
	// one through parallel commits protocol and the second is using the normal
	// commit protocol. There is a test race where TestingProposalFilter changes
	// the commit command id from the first one (parallel commit) to the second
	// one (normal commit). This causes the test to inject the error on the wrong
	// command id. Disabling parallel commits avoids this test race.
	_, err := sqlDB.Exec("SET CLUSTER SETTING kv.transaction.parallel_commits_enabled = 'false'")
	require.NoError(t, err)

	if _, _, err := s.SplitRange(roachpb.Key("b")); err != nil {
		t.Fatal(err)
	}

	// We'll start a transaction, write an intent on both sides of the split,
	// then separately do a read on a different goroutine and wait for that read
	// to block on the intent, then we'll attempt to commit the transaction but
	// we'll intercept the processing of the commit command and reject it. Then
	// we'll assert that the txn wait queue is told that the transaction
	// aborted, and we also check that the reader got a nil value.

	txn := kv.NewTxn(ctx, db, s.NodeID())
	keyLeft, keyRight := "a", "c"
	for _, key := range []string{keyLeft, keyRight} {
		if err := txn.Put(ctx, key, "val"); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("txn ID: %v", txn.ID())
	txnID.Store(txn.ID())

	readerDone := make(chan error, 2)

	for _, key := range []string{keyLeft, keyRight} {
		go func(key string) {
			val, err := db.Get(ctx, key)
			if err != nil {
				readerDone <- err
			} else if val.Exists() {
				readerDone <- fmt.Errorf("%s: expected value to not exist, got: %s", key, val)
			} else {
				readerDone <- nil
			}
		}(key)
	}

	// Wait for both readers to enter the txn wait queue.
	<-readerBlocked
	<-readerBlocked

	require.ErrorContains(t, txn.Commit(ctx), "test injected err", "expected injected error")
	require.NoError(t, txn.Rollback(ctx))
	// Wait for the txn wait queue to be pinged and check the status.
	if status := <-txnUpdate; status != roachpb.ABORTED {
		t.Fatalf("expected the wait queue to be updated with an Aborted txn, instead got: %s", status)
	}
	for i := 0; i < 2; i++ {
		if err := <-readerDone; err != nil {
			t.Fatal(err)
		}
	}
}

// TestRefreshingPreventsStarvation verifies that the refreshing marker in the
// timestamp cache prevents high-priority pushers from starving a serializable
// transaction during its refresh window.
//
// The scenario:
//  1. T1 reads key A, writes key B.
//  2. T2 (high priority) reads key B, pushing T1's write timestamp, then commits.
//  3. T1 commits. EndTxn detects the pushed timestamp, returns
//     RETRY_SERIALIZABLE, and adds a refreshing marker to the timestamp cache.
//  4. The txnSpanRefresher refreshes T1's read spans and retries EndTxn.
//     A request filter blocks this retry.
//  5. T3 (high priority) reads key B, encounters T1's intent, and tries to
//     push T1. Because of the refreshing marker, the push is blocked.
//  6. The retry is released, T1 commits, and T3's push resolves.
func TestRefreshingPreventsStarvation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var t1ID atomic.Value
	var endTxnCount atomic.Int32
	retryBlocked := make(chan struct{})
	retryUnblocked := make(chan struct{})
	pusherBlocked := make(chan struct{}, 1)

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(
					ctx context.Context, ba *kvpb.BatchRequest,
				) *kvpb.Error {
					etReq, ok := ba.GetArg(kvpb.EndTxn)
					if !ok || !etReq.(*kvpb.EndTxnRequest).Commit {
						return nil
					}
					v := t1ID.Load()
					if v == nil {
						return nil
					}
					if !ba.Txn.ID.Equal(v.(uuid.UUID)) {
						return nil
					}
					// The second EndTxn is the retry after a successful refresh.
					// Block it so we can verify T3's push is blocked.
					if endTxnCount.Add(1) == 2 {
						close(retryBlocked)
						<-retryUnblocked
					}
					return nil
				},
				TxnWaitKnobs: txnwait.TestingKnobs{
					OnPusherBlocked: func(
						ctx context.Context, push *kvpb.PushTxnRequest,
					) {
						v := t1ID.Load()
						if v == nil {
							return
						}
						if push.PusheeTxn.ID.Equal(v.(uuid.UUID)) {
							select {
							case pusherBlocked <- struct{}{}:
							default:
							}
						}
					},
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	kvcoord.TransactionRefreshingStatusEnabled.Override(ctx, &s.ClusterSettings().SV, true)

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")

	// T1: read key A (refresh span), write key B.
	t1 := db.NewTxn(ctx, "t1")
	_, err := t1.Get(ctx, keyA)
	require.NoError(t, err)
	require.NoError(t, t1.Put(ctx, keyB, "b-t1"))
	t1ID.Store(t1.ID())

	// T2: high priority, reads key B. This pushes T1's write timestamp
	// because T2 has higher priority.
	t2 := db.NewTxn(ctx, "t2")
	require.NoError(t, t2.SetUserPriority(roachpb.MaxUserPriority))
	_, err = t2.Get(ctx, keyB)
	require.NoError(t, err)
	require.NoError(t, t2.Commit(ctx))

	// T1 commits in the background. The first EndTxn returns
	// RETRY_SERIALIZABLE and adds a refreshing marker. The refresh
	// succeeds (key A is unchanged). The retry EndTxn is blocked by
	// the request filter.
	t1Done := make(chan error, 1)
	go func() {
		t1Done <- t1.Commit(ctx)
	}()
	<-retryBlocked

	// T3: high priority, reads key B. It encounters T1's intent and tries
	// to push, but the refreshing marker blocks the push.
	t3Done := make(chan error, 1)
	go func() {
		t3 := db.NewTxn(ctx, "t3")
		if err := t3.SetUserPriority(roachpb.MaxUserPriority); err != nil {
			t3Done <- err
			return
		}
		_, err := t3.Get(ctx, keyB)
		t3Done <- err
	}()
	<-pusherBlocked

	// Release T1's retry. T1 commits, which unblocks T3's push.
	close(retryUnblocked)
	require.NoError(t, <-t1Done)
	require.NoError(t, <-t3Done)
}

// TestRefreshingToPendingAllowsPush verifies that after a failed refresh the
// refreshing marker in the timestamp cache is canceled via HeartbeatTxn with
// UpdateTSCacheOnly, so pushers can then push the transaction normally.
//
// The scenario:
//  1. T1 reads key A, writes key B.
//  2. Key A is overwritten, invalidating T1's read span.
//  3. T2 (high priority) reads key B, pushing T1's write timestamp, then
//     commits.
//  4. T1 commits. EndTxn returns RETRY_SERIALIZABLE and adds a refreshing
//     marker to the timestamp cache. The refresh fails (key A changed).
//     The txnSpanRefresher sends HeartbeatTxn(UpdateTSCacheOnly=true) to cancel
//     the marker, then returns RETRY_SERIALIZABLE.
//  5. T3 (high priority) reads key B. It pushes T1, which succeeds because
//     the refreshing marker has been canceled.
func TestRefreshingToPendingAllowsPush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var t1ID atomic.Value
	var heartbeatSeen atomic.Bool

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(
					ctx context.Context, ba *kvpb.BatchRequest,
				) *kvpb.Error {
					hbReq, ok := ba.GetArg(kvpb.HeartbeatTxn)
					if !ok {
						return nil
					}
					v := t1ID.Load()
					if v == nil {
						return nil
					}
					if !ba.Txn.ID.Equal(v.(uuid.UUID)) {
						return nil
					}
					if hbReq.(*kvpb.HeartbeatTxnRequest).UpdateTSCacheOnly {
						heartbeatSeen.Store(true)
					}
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	kvcoord.TransactionRefreshingStatusEnabled.Override(ctx, &s.ClusterSettings().SV, true)

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")

	// T1: read key A (refresh span), write key B.
	t1 := db.NewTxn(ctx, "t1")
	_, err := t1.Get(ctx, keyA)
	require.NoError(t, err)
	require.NoError(t, t1.Put(ctx, keyB, "b-t1"))
	t1ID.Store(t1.ID())

	// Invalidate T1's read.
	require.NoError(t, db.Put(ctx, keyA, "a-overwritten"))

	// T2: Push T1 with a high-priority reader.
	t2 := db.NewTxn(ctx, "t2")
	require.NoError(t, t2.SetUserPriority(roachpb.MaxUserPriority))
	_, err = t2.Get(ctx, keyB)
	require.NoError(t, err)
	require.NoError(t, t2.Commit(ctx))

	// T1 commit attempt adds a refreshing marker to the tscache, but the
	// refresh fails (key A was overwritten). The txnSpanRefresher cancels
	// the marker via HeartbeatTxn(UpdateTSCacheOnly=true), then returns
	// RETRY_SERIALIZABLE to the client.
	require.Error(t, t1.Commit(ctx))
	require.True(t, heartbeatSeen.Load(),
		"expected HeartbeatTxn with UpdateTSCacheOnly to cancel refreshing marker")

	// T1 can now be pushed.
	t3 := db.NewTxn(ctx, "t3")
	require.NoError(t, t3.SetUserPriority(roachpb.MaxUserPriority))
	_, err = t3.Get(ctx, keyB)
	require.NoError(t, err)

	require.NoError(t, t1.Rollback(ctx))
}

// TestRefreshingServerSideRetry verifies that when a transaction has no read
// spans (CanForwardReadTimestamp=true), a pushed timestamp that would normally
// trigger a client-side refresh is instead handled via server-side retry.
// The server bumps the batch timestamp and re-evaluates, committing in a single
// EndTxn RPC.
//
// The scenario:
//  1. T1 writes key B (no reads, so CanForwardReadTimestamp is true).
//  2. T2 (high priority) reads key B, pushing T1's write timestamp, then commits.
//  3. T1 commits with UseRefreshingStatus enabled. The server detects the
//     RETRY_SERIALIZABLE error, bumps the timestamp via server-side retry, and
//     commits without a second EndTxn from the client.
func TestRefreshingServerSideRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var t1ID atomic.Value
	var endTxnCount atomic.Int32

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(
					ctx context.Context, ba *kvpb.BatchRequest,
				) *kvpb.Error {
					etReq, ok := ba.GetArg(kvpb.EndTxn)
					if !ok || !etReq.(*kvpb.EndTxnRequest).Commit {
						return nil
					}
					v := t1ID.Load()
					if v == nil {
						return nil
					}
					if !ba.Txn.ID.Equal(v.(uuid.UUID)) {
						return nil
					}
					endTxnCount.Add(1)
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	kvcoord.TransactionRefreshingStatusEnabled.Override(ctx, &s.ClusterSettings().SV, true)

	keyB := roachpb.Key("b")

	// T1: write key B only (no reads).
	t1 := db.NewTxn(ctx, "t1")
	require.NoError(t, t1.Put(ctx, keyB, "b-t1"))
	t1ID.Store(t1.ID())

	// T2: high priority, reads key B. This pushes T1's write timestamp.
	t2 := db.NewTxn(ctx, "t2")
	require.NoError(t, t2.SetUserPriority(roachpb.MaxUserPriority))
	_, err := t2.Get(ctx, keyB)
	require.NoError(t, err)
	require.NoError(t, t2.Commit(ctx))

	// T1 commits. Since T1 has no read spans, CanForwardReadTimestamp is true.
	// The server should handle the timestamp bump via server-side retry,
	// committing in a single round-trip.
	require.NoError(t, t1.Commit(ctx))

	// Exactly 1 EndTxn batch should have been sent. If the server-side retry
	// didn't work, the client would have received RETRY_SERIALIZABLE and
	// sent a second EndTxn after refreshing (which is a no-op with no read
	// spans).
	require.Equal(t, int32(1), endTxnCount.Load(),
		"expected exactly 1 EndTxn; server-side retry should have avoided a second EndTxn")
}
