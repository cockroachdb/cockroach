// Copyright 2018 The Cockroach Authors.
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

package kv_test

import (
	"context"
	"fmt"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// This file contains contains integration tests that don't fit anywhere else.
// Generally its meant to test scenarios involving both "the client" and "the
// server".

// Test the following scenario:
//
// 1) A client sends a batch with a Begin and other requests
// 2) These requests span ranges, so the DistSender splits the batch and sends
// sub-batches in parallel. Let's say the batch with the Begin is sent to range
// 1 and the rest to r2. The r2 batch executes much quicker than the r1 one, and
// leaves some intents.
// 3) Another txn runs into the intents on r2, tries to push, and succeeds
// because the txn record doesn't exist yet. It writes the txn record as
// Aborted.
// 4) If the Begin were to execute now, it'd discover the Aborted txn and return
// a retryable TxnAbortedError. But it doesn't execute now; it's still delayed
// somehow.
// 5) The heartbeat loop (which had been started on the client when the Begin
// was sent) has been waiting for 1s, which has now passed, and it sends a
// heartbeat request. This request returns the aborted txn, and so the hb loop
// tries to cleanup - it sends a EndTransaction(commit=false,poison=true). This
// rollback executes and populates the timestamp cache.
// 6) The original BeginTransaction finally executes. It runs into the ts cache
// which generates a TransactionAbortedError
// (ABORT_REASON_ALREADY_COMMITTED_OR_ROLLED_BACK_POSSIBLE_REPLAY).
//
// The point of the test is checking that the client gets a retriable error as a
// result of this convoluted scenario.
func TestDelayedBeginRetryable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Here's how this test is gonna go:
	// - We're going to send a BeginTxn+Put(a)+Put(c). The first two will be split
	// from the last one and the sub-batches will run in parallel.
	// - We're going to intercept the BeginTxn and block it.
	// - We're going to intercept the Put(c) and trigger a Pusher.
	// - We're relying on the pusher writing an Aborted txn record and the
	// heartbeat loop noticing that, at which point it will send a rollback, which
	// we intercept.
	// - When we intercept the rollback, we unblock the BeginTxn.

	ctx := context.Background()
	key := roachpb.Key("a")
	key2 := roachpb.Key("c")
	unblockBegin := make(chan struct{})
	unblockPush := make(chan struct{})
	var putCFound int64
	var rollbackFound int64
	s, _, origDB := serverutils.StartServer(t, base.TestServerArgs{
		// We're going to expect a couple of things, in sequence:
		// - a BeginTransaction, which we block.
		// - a Put("c"), at which point we trigger the pusher
		// - a rollback, at which point we unblock the Begin.
		Knobs: base.TestingKnobs{
			Store: &storage.StoreTestingKnobs{
				// We're going to perform manual splits.
				DisableMergeQueue: true,
				// We use TestingRequestFilter (as opposed to some other filter) in
				// order to catch the BeginTransaction before it enters the command
				// queue. Otherwise, it would block heartbeats and the rollback.
				TestingRequestFilter: func(ba roachpb.BatchRequest) *roachpb.Error {
					if btReq, ok := ba.GetArg(roachpb.BeginTransaction); ok {
						// We're looking for the batch with BeginTxn, but after it has been
						// split in sub-batches (so, when it no longer produces a
						// RangeKeyMismatchError).  If we find an unsplit one, it's not the
						// droid we're looking for.
						if len(ba.Requests) == 3 {
							return nil
						}
						bt := btReq.(*roachpb.BeginTransactionRequest)
						if bt.Key.Equal(key) {
							<-unblockBegin
						}
					}
					return nil
				},
				TestingResponseFilter: func(ba roachpb.BatchRequest, _ *roachpb.BatchResponse) *roachpb.Error {
					if etReq, ok := ba.GetArg(roachpb.EndTransaction); ok {
						et := etReq.(*roachpb.EndTransactionRequest)
						if !et.Commit && et.Key.Equal(key) && atomic.CompareAndSwapInt64(&rollbackFound, 0, 1) {
							close(unblockBegin)
						}
						return nil
					}
					if putReq, ok := ba.GetArg(roachpb.Put); ok {
						put := putReq.(*roachpb.PutRequest)
						if put.Key.Equal(key2) && atomic.CompareAndSwapInt64(&putCFound, 0, 1) {
							close(unblockPush)
						}
						return nil
					}
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	// Create two ranges so that the batch we're about to send gets split.
	if err := origDB.AdminSplit(ctx, "b", "b"); err != nil {
		t.Fatal(err)
	}

	// Make a db with a short heartbeat interval.
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	tsf := kv.NewTxnCoordSenderFactory(
		kv.TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			// Short heartbeat interval.
			HeartbeatInterval: time.Millisecond,
			Settings:          s.ClusterSettings(),
			Clock:             s.Clock(),
			Stopper:           s.Stopper(),
		},
		s.DistSender(),
	)
	db := client.NewDB(ambient, tsf, s.Clock())
	txn := client.NewTxn(ctx, db, 0 /* gatewayNodeID */, client.RootTxn)

	pushErr := make(chan error)
	go func() {
		<-unblockPush
		// Conflicting transaction that pushes the above transaction.
		conflictTxn := client.NewTxn(ctx, origDB, 0 /* gatewayNodeID */, client.RootTxn)
		// Push through a Put, as opposed to a Get, so that the pushee gets aborted.
		if err := conflictTxn.Put(ctx, key2, "pusher was here"); err != nil {
			pushErr <- err
			return
		}
		pushErr <- conflictTxn.CommitOrCleanup(ctx)
	}()

	put1 := roachpb.NewPut(key, roachpb.MakeValueFromString("foo")).(*roachpb.PutRequest)
	put2 := roachpb.NewPut(key2, roachpb.MakeValueFromString("foo")).(*roachpb.PutRequest)
	ba := roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: txn.Serialize()}
	ba.Add(put1, put2)
	_, pErr := txn.Send(ctx, ba)

	if err := <-pushErr; err != nil {
		t.Fatal(pushErr)
	}

	if _, ok := pErr.GetDetail().(*roachpb.HandledRetryableTxnError); !ok {
		t.Fatalf("expected HandledRetryableTxnError, got: %v", pErr)
	}
	exp := "TransactionAbortedError(ABORT_REASON_ALREADY_COMMITTED_OR_ROLLED_BACK_POSSIBLE_REPLAY)"
	if !testutils.IsPError(pErr, regexp.QuoteMeta(exp)) {
		t.Fatalf("expected %s, got: %s", exp, pErr)
	}
}

// Test that waiters on transactions whose commit command is rejected see the
// transaction as Aborted. This test is a regression test for #30792 which was
// causing pushers in the txn wait queue to consider such a transaction
// committed.
func TestWaiterOnRejectedCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// The txn id whose commit we're going to reject. A uuid.UUID.
	var txnID atomic.Value
	// The EndTransaction proposal that we want to reject. A string.
	var commitCmdID atomic.Value
	readerBlocked := make(chan struct{})
	// txnUpdate is signaled once the txn wait queue is updated for our
	// transaction. Normally it only needs a buffer length of 1, but bugs that
	// cause it to be pinged several times (e.g. #30792) might need a bigger
	// buffer to avoid the test timing out.
	txnUpdate := make(chan roachpb.TransactionStatus, 10)

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &storage.StoreTestingKnobs{
				TestingProposalFilter: func(args storagebase.ProposalFilterArgs) *roachpb.Error {
					// We'll recognize the attempt to commit our transaction and store the
					// respective command id.
					ba := args.Req
					etReq, ok := ba.GetArg(roachpb.EndTransaction)
					if !ok {
						return nil
					}
					if !etReq.(*roachpb.EndTransactionRequest).Commit {
						return nil
					}
					v := txnID.Load()
					if v == nil {
						return nil
					}
					if !ba.Txn.ID.Equal(v.(uuid.UUID)) {
						return nil
					}
					commitCmdID.Store(args.CmdID)
					return nil
				},
				TestingApplyFilter: func(args storagebase.ApplyFilterArgs) *roachpb.Error {
					// We'll trap the processing of the commit command and return an error
					// for it.
					v := commitCmdID.Load()
					if v == nil {
						return nil
					}
					cmdID := v.(storagebase.CmdIDKey)
					if args.CmdID == cmdID {
						return roachpb.NewErrorf("test injected err")
					}
					return nil
				},
				TxnWait: txnwait.TestingKnobs{
					OnPusherBlocked: func(ctx context.Context, push *roachpb.PushTxnRequest) {
						// We'll trap a reader entering the wait queue for our txn.
						v := txnID.Load()
						if v == nil {
							return
						}
						if push.PusheeTxn.ID.Equal(v.(uuid.UUID)) {
							close(readerBlocked)
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

	// We'll start a transaction, write an intent, then separately do a read on a
	// different goroutine and wait for that read to block on the intent, then
	// we'll attempt to commit the transaction but we'll intercept the processing
	// of the commit command and reject it.
	// Then we'll assert that the txn wait queue is told that the transaction
	// aborted, and we also check that the reader got a nil value.

	txn := client.NewTxn(ctx, db, s.NodeID(), client.RootTxn)
	key := "key"
	if err := txn.Put(ctx, key, "val"); err != nil {
		t.Fatal(err)
	}
	txnID.Store(txn.ID())

	readerDone := make(chan error, 1)

	go func() {
		val, err := db.Get(ctx, key)
		if err != nil {
			readerDone <- err
		}
		if val.Exists() {
			readerDone <- fmt.Errorf("expected value to not exist, got: %s", val)
		}
		close(readerDone)
	}()

	// Wait for the reader to enter the txn wait queue.
	<-readerBlocked
	if err := txn.CommitOrCleanup(ctx); !testutils.IsError(err, "test injected err") {
		t.Fatalf("expected injected err, got: %v", err)
	}
	// Wait for the txn wait queue to be pinged and check the status.
	if status := <-txnUpdate; status != roachpb.ABORTED {
		t.Fatalf("expected the wait queue to be updated with an Aborted txn, instead got: %s", status)
	}
	if err := <-readerDone; err != nil {
		t.Fatal(err)
	}
}
