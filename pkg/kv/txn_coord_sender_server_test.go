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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
)

// Test that a transaction gets cleaned up when the heartbeat loop finds out
// that it has already been aborted (by a 3rd party). That is, we don't wait for
// the client to find out before the intents are removed.
// This relies on the TxnCoordSender's heartbeat loop to notice the changed
// transaction status and do an async abort. Note that, as of June 2018,
// subsequent requests sent through the TxnCoordSender return
// TransactionAbortedErrors. On those errors, the contract is that the
// client.Txn creates a new transaction internally and switches the
// TxnCoordSender instance. The expectation is that the old transaction has been
// cleaned up by that point.
func TestHeartbeatFindsOutAboutAbortedTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, origDB := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	key := roachpb.Key("a")
	key2 := roachpb.Key("b")
	push := func(ctx context.Context, key roachpb.Key) error {
		// Conflicting transaction that pushes the above transaction.
		conflictTxn := client.NewTxn(origDB, 0 /* gatewayNodeID */, client.RootTxn)
		// We need to explicitly set a high priority for the push to happen.
		if err := conflictTxn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
			return err
		}
		// Push through a Put, as opposed to a Get, so that the pushee gets aborted.
		if err := conflictTxn.Put(ctx, key, "pusher was here"); err != nil {
			return err
		}
		return conflictTxn.CommitOrCleanup(ctx)
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
	txn := client.NewTxn(db, 0 /* gatewayNodeID */, client.RootTxn)
	if err := txn.Put(ctx, key, "val"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Put(ctx, key2, "val"); err != nil {
		t.Fatal(err)
	}

	if err := push(ctx, key); err != nil {
		t.Fatal(err)
	}

	// Now wait until the heartbeat loop notices that the transaction is aborted.
	testutils.SucceedsSoon(t, func() error {
		if txn.GetTxnCoordMeta().Txn.Status != roachpb.ABORTED {
			return fmt.Errorf("txn not aborted yet")
		}
		return nil
	})

	// Now check that the intent on key2 has been cleared. We'll do a
	// READ_UNCOMMITTED Get for that.
	testutils.SucceedsSoon(t, func() error {
		reply, err := client.SendWrappedWith(ctx, origDB.NonTransactionalSender(), roachpb.Header{
			ReadConsistency: roachpb.READ_UNCOMMITTED,
		}, roachpb.NewGet(key2))
		if err != nil {
			t.Fatal(err)
		}
		if reply.(*roachpb.GetResponse).IntentValue != nil {
			return fmt.Errorf("intent still present on key: %s", key2)
		}
		return nil
	})

	// Check that further sends through the aborted txn are rejected. The
	// TxnCoordSender is supposed to synthesize a TransactionAbortedError.
	if err := txn.CommitOrCleanup(ctx); !testutils.IsError(
		err, "HandledRetryableTxnError: TransactionAbortedError",
	) {
		t.Fatalf("expected aborted error, got: %s", err)
	}
}

// Test that, when a transaction restarts, we don't get a second heartbeat loop
// for it. This bug happened in the past.
//
// The test traces the restarting transaction and looks in it to see how many
// times a heartbeat loop was started.
func TestNoDuplicateHeartbeatLoops(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	key := roachpb.Key("a")

	tracer := tracing.NewTracer()
	sp := tracer.StartSpan("test", tracing.Recordable)
	tracing.StartRecording(sp, tracing.SingleNodeRecording)
	txnCtx := opentracing.ContextWithSpan(context.Background(), sp)

	push := func(ctx context.Context, key roachpb.Key) error {
		return db.Put(ctx, key, "push")
	}

	var attempts int
	err := db.Txn(txnCtx, func(ctx context.Context, txn *client.Txn) error {
		attempts++
		if attempts == 1 {
			if err := push(context.Background() /* keep the contexts separate */, key); err != nil {
				return err
			}
		}
		if _, err := txn.Get(ctx, key); err != nil {
			return err
		}
		return txn.Put(ctx, key, "val")
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got: %d", attempts)
	}
	sp.Finish()
	recording := tracing.GetRecording(sp)
	var foundHeartbeatLoop bool
	for _, sp := range recording {
		if strings.Contains(sp.Operation, "heartbeat loop") {
			if foundHeartbeatLoop {
				t.Fatal("second heartbeat loop found")
			}
			foundHeartbeatLoop = true
		}
	}
	if !foundHeartbeatLoop {
		t.Fatal("no heartbeat loop found. Test rotted?")
	}
}
