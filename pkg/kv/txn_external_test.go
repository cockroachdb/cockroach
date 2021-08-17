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
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/kvclientutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
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

// TestTxnNegotiateAndSendDoesNotBlock tests that bounded staleness read
// requests performed using (*Txn).NegotiateAndSend never block on conflicting
// intents or redirect to the leaseholder if run on follower replicas. It then
// tests that the transaction is left in the expected state after the call to
// NegotiateAndSend.
//
// The test's multiRange param dictates whether the bounded staleness read is
// performed over multiple ranges or a single range.
//
// The multiRange=false variant is the client-side sibling of
// kvserver.TestNonBlockingReadsWithServerSideBoundedStalenessNegotiation. This
// test, unlike that one, exercises client-side transaction logic in kv.Txn and
// routing logic in kvcoord.DistSender.
//
// The multiRange=true variant is currently unimplemented, as it is blocked on
// #67554.
//
// The test's strict param dictates whether strict bounded staleness reads are
// used or not. If set to true, the test is configured to never expect blocking.
// If set to false, the test is relaxed and we allow bounded staleness reads to
// block.
//
// The test's routeNearest param dictates whether bounded staleness reads are
// issued with the LEASEHOLDER routing policy to be evaluated on leaseholders,
// or the NEAREST routing policy to be evaluated on the nearest replicas to the
// client.
func TestTxnNegotiateAndSendDoesNotBlock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "multiRange", func(t *testing.T, multiRange bool) {
		testutils.RunTrueAndFalse(t, "strict", func(t *testing.T, strict bool) {
			testutils.RunTrueAndFalse(t, "routeNearest", func(t *testing.T, routeNearest bool) {
				testTxnNegotiateAndSendDoesNotBlock(t, multiRange, strict, routeNearest)
			})
		})
	})
}

func testTxnNegotiateAndSendDoesNotBlock(t *testing.T, multiRange, strict, routeNearest bool) {
	if multiRange {
		skip.IgnoreLint(t, "unimplemented, blocked on #67554.")
	}

	const testTime = 1 * time.Second
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	// Create a scratch range. Then add one voting follower and one non-voting
	// follower to the range.
	scratchKey := tc.ScratchRange(t)
	scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
	tc.AddVotersOrFatal(t, scratchKey, tc.Target(1))
	tc.AddNonVotersOrFatal(t, scratchKey, tc.Target(2))
	lh, err := tc.FindRangeLeaseHolder(scratchRange, nil)
	require.NoError(t, err)

	// Drop the closed timestamp interval far enough so that we can create
	// situations where an active intent is at a lower timestamp than the
	// range's closed timestamp - thereby being the limiting factor for the
	// range's resolved timestamp.
	sqlRunner := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlRunner.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1ms'")

	keySet := make([]roachpb.Key, 5)
	for i := range keySet {
		n := len(scratchKey)
		keySet[i] = append(scratchKey[:n:n], byte(i))
	}
	keySpan := roachpb.Span{Key: scratchKey, EndKey: scratchKey.PrefixEnd()}

	// TODO(nvanbenschoten): if multiRange, split on each key in keySet.
	// if multiRange { ... }

	var g errgroup.Group
	var done int32
	sleep := func() {
		time.Sleep(time.Duration(rand.Intn(2000)) * time.Microsecond)
	}

	// Writer goroutines: write intents and commit them.
	for _, key := range keySet {
		key := key // copy for goroutine
		g.Go(func() error {
			for ; atomic.LoadInt32(&done) == 0; sleep() {
				if err := tc.Server(0).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					if _, err := txn.Inc(ctx, key, 1); err != nil {
						return err
					}
					// Let the intent stick around for a bit.
					sleep()
					return nil
				}); err != nil {
					return err
				}
			}
			return nil
		})
	}

	// Reader goroutines: perform bounded-staleness reads that hit the server-side
	// negotiation fast-path.
	for _, s := range tc.Servers {
		store, err := s.Stores().GetStore(s.GetFirstStoreID())
		require.NoError(t, err)
		g.Go(func() error {
			// Prime range cache so that follower read attempts don't initially miss.
			if _, err := store.DB().Scan(ctx, keySpan.Key, keySpan.EndKey, 0); err != nil {
				return err
			}

			minTSBound := hlc.MinTimestamp
			var lastTxnTS hlc.Timestamp
			for ; atomic.LoadInt32(&done) == 0; sleep() {
				if err := store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					// Issue a bounded-staleness read over the keys. If using strict
					// bounded staleness, use an error wait policy so that we'll hear an
					// error (WriteIntentError) under conditions that would otherwise
					// cause us to block on an intent. Otherwise, allow the request to be
					// redirected to the leaseholder and to block on intents.
					var ba roachpb.BatchRequest
					if strict {
						ba.BoundedStaleness = &roachpb.BoundedStalenessHeader{
							MinTimestampBound:       minTSBound,
							MinTimestampBoundStrict: true,
						}
						ba.WaitPolicy = lock.WaitPolicy_Error
					} else {
						ba.BoundedStaleness = &roachpb.BoundedStalenessHeader{
							MinTimestampBound:       store.Clock().Now(),
							MinTimestampBoundStrict: false,
						}
						ba.WaitPolicy = lock.WaitPolicy_Block
					}
					ba.RoutingPolicy = roachpb.RoutingPolicy_LEASEHOLDER
					if routeNearest {
						ba.RoutingPolicy = roachpb.RoutingPolicy_NEAREST
					}
					ba.Add(&roachpb.ScanRequest{
						RequestHeader: roachpb.RequestHeaderFromSpan(keySpan),
					})

					// Trace the request so we can determine whether it was served as a
					// follower read. If running on a store with a follower replica and
					// with a NEAREST routing policy, we expect follower reads.
					ctx, collect, cancel := tracing.ContextWithRecordingSpan(
						ctx, tracing.NewTracer(), "reader")
					defer cancel()

					br, pErr := txn.NegotiateAndSend(ctx, ba)
					if pErr != nil {
						return pErr.GoError()
					}

					// Validate that the transaction timestamp is now fixed and set to the
					// request timestamp.
					if !txn.CommitTimestampFixed() {
						return errors.Errorf("transaction timestamp not fixed")
					}
					txnTS := txn.CommitTimestamp()
					if txnTS != br.Timestamp {
						return errors.Errorf("transaction timestamp (%s) does not equal request timestamp (%s)", txnTS, br.Timestamp)
					}

					// Validate that the transaction timestamp increases monotonically
					// across attempts, since we're issuing bounded staleness reads to the
					// same replica.
					if txnTS.IsEmpty() {
						return errors.Errorf("empty bounded staleness timestamp")
					}
					if txnTS.Less(lastTxnTS) {
						return errors.Errorf("bounded staleness timestamp regression: %s -> %s", lastTxnTS, txnTS)
					}
					lastTxnTS = txnTS

					// Forward the minimum timestamp bound for the next request. Once a
					// bounded staleness read has been served at a timestamp, future reads
					// should be able to be served at the same timestamp (or later) as
					// long as they are routed to the same replica.
					minTSBound = lastTxnTS

					// Determine whether the read was served by a follower replica or not
					// and confirm that this matches expectations. There are some configs
					// where it would be valid for the request to be served by a follower
					// or redirected to the leaseholder due to timing, so we make no
					// assertion.
					rec := collect()
					expFollowerRead := store.StoreID() != lh.StoreID && strict && routeNearest
					wasFollowerRead := kv.OnlyFollowerReads(rec)
					ambiguous := !strict && routeNearest
					if expFollowerRead != wasFollowerRead && !ambiguous {
						if expFollowerRead {
							return errors.Errorf("expected follower read, found leaseholder read: %s", rec)
						}
						return errors.Errorf("expected leaseholder read, found follower read: %s", rec)
					}

					return nil
				}); err != nil {
					return err
				}
			}
			return nil
		})
	}

	time.Sleep(testTime)
	atomic.StoreInt32(&done, 1)
	require.NoError(t, g.Wait())
}
