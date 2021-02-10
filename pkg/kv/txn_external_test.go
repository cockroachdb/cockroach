// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/kvclientutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

// Test the behavior of a txn.Rollback() issued after txn.Commit() failing with
// an ambiguous error.
func TestRollbackAfterAmbiguousCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testCases := []struct {
		name string
		// The status of the transaction record at the time when we issue the
		// rollback.
		txnStatus roachpb.TransactionStatus
		// If txnStatus == COMMITTED, setting txnRecordCleanedUp will make us
		// cleanup the transaction. The txn record will be deleted.
		txnRecordCleanedUp bool
		// The error that we expect from txn.Rollback().
		expRollbackErr string
		// Is the transaction expected to be committed or not after the
		// txn.Rollback() call returns?
		expCommitted bool
	}{
		{
			name:               "txn cleaned up",
			txnStatus:          roachpb.COMMITTED,
			txnRecordCleanedUp: true,
			// The transaction will be committed, but at the same time the rollback
			// will also appear to succeed (it'll be a no-op). This behavior is
			// undesired. See #48302.
			expCommitted:   true,
			expRollbackErr: "",
		},
		{
			name:               "COMMITTED",
			txnStatus:          roachpb.COMMITTED,
			txnRecordCleanedUp: false,
			expCommitted:       true,
			expRollbackErr:     "already committed",
		},
		{
			name:      "STAGING",
			txnStatus: roachpb.STAGING,
			// The rollback succeeds. This behavior is undersired. See #48301.
			expCommitted:   false,
			expRollbackErr: "",
		},
	}
	for _, testCase := range testCases {
		if testCase.txnRecordCleanedUp {
			require.Equal(t, roachpb.COMMITTED, testCase.txnStatus)
		}
		t.Run(testCase.name, func(t *testing.T) {
			var filterSet int64
			var key roachpb.Key
			commitBlocked := make(chan struct{})
			args := base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						// We're going to block the commit of the test's txn, letting the
						// test cancel the request's ctx while the request is blocked.
						TestingResponseFilter: func(ctx context.Context, ba roachpb.BatchRequest, _ *roachpb.BatchResponse) *roachpb.Error {
							if atomic.LoadInt64(&filterSet) == 0 {
								return nil
							}
							req, ok := ba.GetArg(roachpb.EndTxn)
							if !ok {
								return nil
							}
							commit := req.(*roachpb.EndTxnRequest)
							if commit.Key.Equal(key) && commit.Commit {
								// Inform the test that the commit is blocked.
								commitBlocked <- struct{}{}
								// Block until the client interrupts the commit. The client will
								// cancel its context, at which point gRPC will return an error
								// to the client and marshall the cancelation to the server.
								<-ctx.Done()
							}
							return nil
						},
					},
				},
			}
			tci := serverutils.StartNewTestCluster(t, 2, base.TestClusterArgs{ServerArgs: args})
			tc := tci.(*testcluster.TestCluster)
			defer tc.Stopper().Stop(ctx)

			key = tc.ScratchRange(t)
			atomic.StoreInt64(&filterSet, 1)

			// This test uses a cluster with two nodes. It'll create a transaction
			// having as a coordinator the node that's *not* the leaseholder for the
			// range the txn is writing to. This is in order for the context
			// cancelation scheme that we're going to employ to work: we need a
			// non-local RPC such that canceling a requests context triggers an error
			// for the request's sender without synchronizing with the request's
			// evaluation.
			rdesc := tc.LookupRangeOrFatal(t, key)
			leaseHolder, err := tc.FindRangeLeaseHolder(rdesc, nil /* hint */)
			require.NoError(t, err)
			var db *kv.DB
			var tr *tracing.Tracer
			if leaseHolder.NodeID == 1 {
				db = tc.Servers[1].DB()
				tr = tc.Servers[1].Tracer().(*tracing.Tracer)
			} else {
				db = tc.Servers[0].DB()
				tr = tc.Servers[0].Tracer().(*tracing.Tracer)
			}

			txn := db.NewTxn(ctx, "test")
			require.NoError(t, txn.Put(ctx, key, "val"))

			// If the test wants the transaction to be committed and cleaned up, we'll
			// do a read on the key we just wrote. That will act as a pipeline stall,
			// resolving the in-flight write and eliminating the need for the STAGING
			// status.
			if testCase.txnStatus == roachpb.COMMITTED && testCase.txnRecordCleanedUp {
				v, err := txn.Get(ctx, key)
				require.NoError(t, err)
				val, err := v.Value.GetBytes()
				require.NoError(t, err)
				require.Equal(t, "val", string(val))
			}

			// Send a commit request. It's going to get blocked after being evaluated,
			// at which point we're going to cancel the request's ctx. The ctx
			// cancelation will cause gRPC to interrupt the in-flight request, and the
			// DistSender to return an ambiguous error. The transaction will be
			// committed, through.
			commitCtx, cancelCommit := context.WithCancel(ctx)
			commitCh := make(chan error)
			go func() {
				commitCh <- txn.Commit(commitCtx)
			}()

			select {
			case <-commitBlocked:
			case <-time.After(10 * time.Second):
				t.Fatalf("commit not blocked")
			}

			cancelCommit()
			commitErr := <-commitCh
			require.IsType(t, &roachpb.AmbiguousResultError{}, commitErr)
			require.Regexp(t, `result is ambiguous \(context done during DistSender.Send: context canceled\)`,
				commitErr)

			// If the test wants the upcoming rollback to find a COMMITTED record,
			// we'll perform transaction recovery. This will leave the transaction in
			// the COMMITTED state, without cleaning it up.
			if !testCase.txnRecordCleanedUp && testCase.txnStatus == roachpb.COMMITTED {
				// Sanity check - verify that the txn is STAGING.
				txnProto := txn.TestingCloneTxn()
				queryTxn := roachpb.QueryTxnRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: txnProto.Key,
					},
					Txn: txnProto.TxnMeta,
				}
				b := kv.Batch{}
				b.AddRawRequest(&queryTxn)
				require.NoError(t, db.Run(ctx, &b))
				queryTxnRes := b.RawResponse().Responses[0].GetQueryTxn()
				require.Equal(t, roachpb.STAGING, queryTxnRes.QueriedTxn.Status)

				// Perform transaction recovery.
				require.NoError(t, kvclientutils.CheckPushResult(ctx, db, tr, *txn.TestingCloneTxn(),
					kvclientutils.ExpectCommitted, kvclientutils.ExpectPusheeTxnRecovery))
			}

			// Attempt the rollback and check its result.
			rollbackErr := txn.Rollback(ctx)
			if testCase.expRollbackErr == "" {
				require.NoError(t, rollbackErr)
			} else {
				require.Regexp(t, testCase.expRollbackErr, rollbackErr)
			}

			// Check the outcome of the transaction, independently from the outcome of
			// the Rollback() above, by reading the value that it wrote.
			committed := func() bool {
				val, err := db.Get(ctx, key)
				require.NoError(t, err)
				if !val.Exists() {
					return false
				}
				v, err := val.Value.GetBytes()
				require.NoError(t, err)
				require.Equal(t, "val", string(v))
				return true
			}()
			require.Equal(t, testCase.expCommitted, committed)
		})
	}
}
