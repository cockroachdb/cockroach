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
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
