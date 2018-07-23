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
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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

	var cleanupSeen int64
	key := roachpb.Key("a")
	key2 := roachpb.Key("b")
	s, _, origDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &storage.StoreTestingKnobs{
				TestingProposalFilter: func(args storagebase.ProposalFilterArgs) *roachpb.Error {
					// We'll eventually expect to see an EndTransaction(commit=false) with
					// the right intents.
					if args.Req.IsSingleEndTransactionRequest() {
						et := args.Req.Requests[0].GetInner().(*roachpb.EndTransactionRequest)
						if !et.Commit && et.Key.Equal(key) &&
							reflect.DeepEqual(et.IntentSpans, []roachpb.Span{{Key: key}, {Key: key2}}) {
							atomic.StoreInt64(&cleanupSeen, 1)
						}
					}
					return nil
				},
			},
		},
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

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

	// Check that an EndTransaction(commit=false) with the right intents has been
	// sent.
	testutils.SucceedsSoon(t, func() error {
		if atomic.LoadInt64(&cleanupSeen) == 0 {
			return fmt.Errorf("no cleanup sent yet")
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
