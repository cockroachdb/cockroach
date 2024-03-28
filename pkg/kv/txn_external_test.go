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
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/kvclientutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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
		// If txnStatus == STAGING, setting txnImplicitlyCommitted will make
		// the transaction qualify for the implicit commit condition. Otherwise,
		// the transaction will not qualify because the test will attach a fake
		// in-flight write to the transaction's STAGING record.
		txnImplictlyCommitted bool
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
			name:                  "STAGING, implicitly committed",
			txnStatus:             roachpb.STAGING,
			txnImplictlyCommitted: true,
			// The rollback fails after performing txn recovery and moving the
			// transaction to committed.
			expCommitted:   true,
			expRollbackErr: "already committed",
		},
		{
			name:                  "STAGING, not implicitly committed",
			txnStatus:             roachpb.STAGING,
			txnImplictlyCommitted: false,
			// The rollback succeeds after performing txn recovery and moving the
			// transaction to aborted.
			expCommitted:   false,
			expRollbackErr: "",
		},
		{
			name:      "PENDING",
			txnStatus: roachpb.PENDING,
			// The rollback succeeds.
			expCommitted:   false,
			expRollbackErr: "",
		},
	}
	for _, testCase := range testCases {
		// Sanity-check test cases.
		if testCase.txnRecordCleanedUp {
			require.Equal(t, roachpb.COMMITTED, testCase.txnStatus)
		}
		if testCase.txnImplictlyCommitted {
			require.Equal(t, roachpb.STAGING, testCase.txnStatus)
		}

		t.Run(testCase.name, func(t *testing.T) {
			var filterSet int64
			var key roachpb.Key
			commitBlocked := make(chan struct{})
			onCommitReqFilter := func(
				ba *kvpb.BatchRequest, fn func(et *kvpb.EndTxnRequest) *kvpb.Error,
			) *kvpb.Error {
				if atomic.LoadInt64(&filterSet) == 0 {
					return nil
				}
				req, ok := ba.GetArg(kvpb.EndTxn)
				if !ok {
					return nil
				}
				et := req.(*kvpb.EndTxnRequest)
				if et.Key.Equal(key) && et.Commit {
					return fn(et)
				}
				return nil
			}
			blockCommitReqFilter := func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
				return onCommitReqFilter(ba, func(et *kvpb.EndTxnRequest) *kvpb.Error {
					// Inform the test that the commit is blocked.
					commitBlocked <- struct{}{}
					// Block until the client interrupts the commit. The client will
					// cancel its context, at which point gRPC will return an error
					// to the client and marshall the cancelation to the server.
					<-ctx.Done()
					return kvpb.NewError(ctx.Err())
				})
			}
			addInFlightWriteToCommitReqFilter := func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
				return onCommitReqFilter(ba, func(et *kvpb.EndTxnRequest) *kvpb.Error {
					// Add a fake in-flight write.
					et.InFlightWrites = append(et.InFlightWrites, roachpb.SequencedWrite{
						Key: key.Next(), Sequence: et.Sequence,
					})
					// Be sure to update the EndTxn and Txn's sequence accordingly.
					et.Sequence++
					ba.Txn.Sequence++
					return nil
				})
			}
			args := base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						// We're going to block the commit of the test's txn, letting the
						// test cancel the request's ctx while the request is blocked. We
						// do this either before or after the request completes, depending
						// on the status that the test wants the txn record to be in when
						// the rollback is performed.
						TestingRequestFilter: func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
							if testCase.txnStatus == roachpb.PENDING {
								// Block and reject before the request writes the txn record.
								return blockCommitReqFilter(ctx, ba)
							}
							if testCase.txnStatus == roachpb.STAGING && !testCase.txnImplictlyCommitted {
								// If the test wants the upcoming rollback to find a STAGING
								// record for a transaction that is not implicitly committed,
								// add an in-flight write for a (key, sequence) that does not
								// exist to the EndTxn request.
								_ = addInFlightWriteToCommitReqFilter(ctx, ba)
							}
							return nil
						},
						TestingResponseFilter: func(ctx context.Context, ba *kvpb.BatchRequest, _ *kvpb.BatchResponse) *kvpb.Error {
							if testCase.txnStatus != roachpb.PENDING {
								// Block and reject after the request writes the txn record.
								return blockCommitReqFilter(ctx, ba)
							}
							return nil
						},
					},
				},
			}
			tci := serverutils.StartCluster(t, 2, base.TestClusterArgs{ServerArgs: args})
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
				tr = tc.Servers[1].TracerI().(*tracing.Tracer)
			} else {
				db = tc.Servers[0].DB()
				tr = tc.Servers[0].TracerI().(*tracing.Tracer)
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
			require.IsType(t, &kvpb.AmbiguousResultError{}, commitErr)

			// If the test wants the upcoming rollback to find a COMMITTED record,
			// we'll perform transaction recovery. This will leave the transaction in
			// the COMMITTED state, without cleaning it up.
			if testCase.txnStatus == roachpb.COMMITTED && !testCase.txnRecordCleanedUp {
				// Sanity check - verify that the txn is STAGING.
				txnProto := txn.TestingCloneTxn()
				queryTxn := kvpb.QueryTxnRequest{
					RequestHeader: kvpb.RequestHeader{
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

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					// This test is setting specific routing policies and
					// looking at the traces to determine what nodes were
					// part of the routing.
					KVClient: &kvcoord.ClientTestingKnobs{RouteToLeaseholderFirst: true},
				},
			},
		},
	)
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
		store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
		require.NoError(t, err)
		tracer := s.Tracer()
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
					ba := &kvpb.BatchRequest{}
					if strict {
						ba.BoundedStaleness = &kvpb.BoundedStalenessHeader{
							MinTimestampBound:       minTSBound,
							MinTimestampBoundStrict: true,
						}
						ba.WaitPolicy = lock.WaitPolicy_Error
					} else {
						ba.BoundedStaleness = &kvpb.BoundedStalenessHeader{
							MinTimestampBound:       store.Clock().Now(),
							MinTimestampBoundStrict: false,
						}
						ba.WaitPolicy = lock.WaitPolicy_Block
					}
					ba.RoutingPolicy = kvpb.RoutingPolicy_LEASEHOLDER
					if routeNearest {
						ba.RoutingPolicy = kvpb.RoutingPolicy_NEAREST
					}
					ba.Add(&kvpb.ScanRequest{
						RequestHeader: kvpb.RequestHeaderFromSpan(keySpan),
					})

					// Trace the request so we can determine whether it was served as a
					// follower read. If running on a store with a follower replica and
					// with a NEAREST routing policy, we expect follower reads.
					ctx, collectAndFinish := tracing.ContextWithRecordingSpan(ctx, tracer, "reader")
					defer collectAndFinish()

					br, pErr := txn.NegotiateAndSend(ctx, ba)
					if pErr != nil {
						return pErr.GoError()
					}

					// Validate that the transaction's read timestamp is now fixed and set
					// to the request timestamp.
					if !txn.ReadTimestampFixed() {
						return errors.Errorf("transaction's read timestamp not fixed")
					}
					txnTS := txn.ReadTimestamp()
					if txnTS != br.Timestamp {
						return errors.Errorf("transaction's read timestamp (%s) does not equal request timestamp (%s)", txnTS, br.Timestamp)
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
					rec := collectAndFinish()
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

// TestRevScanAndGet tests that Get and ReverseScan requests in the same batch
// can be executed correctly. See illustration below for the various
// combinations tested.
func TestRevScanAndGet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tci := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	tc := tci.(*testcluster.TestCluster)
	defer tc.Stopper().Stop(ctx)
	db := tc.Servers[0].DB()

	require.NoError(t, db.AdminSplit(ctx, "b", hlc.MaxTimestamp))
	require.NoError(t, db.AdminSplit(ctx, "h", hlc.MaxTimestamp))

	// Setup:
	// Ranges:      [keyMin-------b) [b--------h) [h------keyMax)
	// ReverseScan:                     [d-f)
	// Gets:                     *  *  *  *   *  *   *
	testCases := []struct {
		getKey string
	}{
		{
			// Get on a range to the left of the reverse scan.
			getKey: "a",
		},
		{
			// Get on the left split boundary.
			getKey: "b",
		},
		{
			// Get on the same range as the reverse scan but to the left.
			getKey: "c",
		},
		{
			// Get on the same range and enclosed by the reverse scan.
			getKey: "e",
		},
		{
			// Get on the same range as the reverse scan but to the right.
			getKey: "g",
		},
		{
			// Get on the right split boundary.
			getKey: "h",
		},
		{
			// Get on a range to the right of the reverse scan.
			getKey: "i",
		},
	}

	for _, tc := range testCases {
		txn := db.NewTxn(ctx, "test")
		b := txn.NewBatch()
		b.Get(tc.getKey)
		b.ReverseScan("d", "f")
		require.NoError(t, txn.Run(ctx, b))
	}
}

// TestGenerateForcedRetryableErrorAfterRollback poisons the txn handle and then
// rolls it back. This should fail - a retryable error should not be returned
// after a rollback.
func TestGenerateForcedRetryableErrorAfterRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	isolation.RunEachLevel(t, testGenerateForcedRetryableErrorAfterRollback)
}

func testGenerateForcedRetryableErrorAfterRollback(t *testing.T, isoLevel isolation.Level) {
	ctx := context.Background()
	ts, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(ctx)
	k, err := ts.ScratchRange()
	require.NoError(t, err)
	mkKey := func(s string) roachpb.Key {
		return encoding.EncodeStringAscending(k[:len(k):len(k)], s)
	}
	checkKey := func(t *testing.T, s string, exp int64) {
		got, err := db.Get(ctx, mkKey(s))
		require.NoError(t, err)
		require.Equal(t, exp, got.ValueInt())
	}
	var i int
	txnErr := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		require.NoError(t, txn.SetIsoLevel(isoLevel))
		i++
		require.Equal(t, 1, i)
		e1 := txn.Put(ctx, mkKey("a"), 1)
		require.NoError(t, e1)
		// Prepare an error to return after the rollback.
		retryErr := txn.GenerateForcedRetryableErr(ctx, "force retry")
		// Rolling back completes the transaction, returning an error is invalid.
		require.NoError(t, txn.Rollback(ctx))
		return retryErr
	})
	require.ErrorContains(t, txnErr, "client already committed or rolled back")
	require.True(t, errors.IsAssertionFailure(txnErr))
	checkKey(t, "a", 0)
}

// TestGenerateForcedRetryableErrorSimple verifies the retryable func can return
// a forced retryable error, and then the retry will not see writes from the
// first run, and can succeed on the second run.
func TestGenerateForcedRetryableErrorSimple(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	isolation.RunEachLevel(t, testGenerateForcedRetryableErrorSimple)
}

func testGenerateForcedRetryableErrorSimple(t *testing.T, isoLevel isolation.Level) {
	ctx := context.Background()
	ts, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(ctx)
	k, err := ts.ScratchRange()
	require.NoError(t, err)
	mkKey := func(s string) roachpb.Key {
		return encoding.EncodeStringAscending(k[:len(k):len(k)], s)
	}
	checkKey := func(t *testing.T, s string, exp int64) {
		got, err := db.Get(ctx, mkKey(s))
		require.NoError(t, err)
		require.Equal(t, exp, got.ValueInt())
	}
	var i int
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		require.NoError(t, txn.SetIsoLevel(isoLevel))
		{
			// Verify 'a' is not written yet.
			got, err := txn.Get(ctx, mkKey("a"))
			require.NoError(t, err)
			require.False(t, got.Exists())
		}
		require.NoError(t, txn.Put(ctx, mkKey("a"), 1))
		// Retry exactly once by propagating a retry error.
		if i++; i == 1 {
			return txn.GenerateForcedRetryableErr(ctx, "force retry")
		}
		require.NoError(t, txn.Put(ctx, mkKey("b"), 2))
		return nil
	}))
	checkKey(t, "a", 1)
	checkKey(t, "b", 2)
}

// TestGenerateForcedRetryableErrorByPoisoning does the same as
// TestGenerateForcedRetryableErrorSimple above, but doesn't return the error
// from the retryable func, instead, this test verifies that the txn object is
// "poisoned" (in a state where the txn object cannot be used until the error is
// cleared) and it will be reset automatically and succeed on the second try.
func TestGenerateForcedRetryableErrorByPoisoning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	isolation.RunEachLevel(t, testGenerateForcedRetryableErrorByPoisoning)
}

func testGenerateForcedRetryableErrorByPoisoning(t *testing.T, isoLevel isolation.Level) {
	ctx := context.Background()
	ts, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(ctx)
	k, err := ts.ScratchRange()
	require.NoError(t, err)
	mkKey := func(s string) roachpb.Key {
		return encoding.EncodeStringAscending(k[:len(k):len(k)], s)
	}
	checkKey := func(t *testing.T, s string, exp int64) {
		got, err := db.Get(ctx, mkKey(s))
		require.NoError(t, err)
		require.Equal(t, exp, got.ValueInt())
	}
	var i int
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		require.NoError(t, txn.SetIsoLevel(isoLevel))
		{
			// Verify 'a' is not written yet.
			got, err := txn.Get(ctx, mkKey("a"))
			require.NoError(t, err, got.ValueInt())
			require.False(t, got.Exists(), got.ValueInt())
		}
		require.NoError(t, txn.Put(ctx, mkKey("a"), 1))
		// Retry exactly once by propagating a retry error.
		if i++; i == 1 {
			// Generate an error and then return nil (not the ideal implementation but
			// should work).
			_ = txn.GenerateForcedRetryableErr(ctx, "force retry")
			return nil
		}
		require.NoError(t, txn.Put(ctx, mkKey("b"), 2))
		return nil
	}))
	checkKey(t, "a", 1)
	checkKey(t, "b", 2)
}

// TestPrepareForRetry tests that PrepareForRetry must be called after a
// retryable error is thrown by a transaction and before that transaction is
// retried. It also tests that even if a Read Committed transaction throws a
// retryable error which does not require it to start from the beginning with
// all prior writes discarded, it can choose to call PrepareForRetry and do so.
func TestPrepareForRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	isolation.RunEachLevel(t, testPrepareForRetry)
}

func testPrepareForRetry(t *testing.T, isoLevel isolation.Level) {
	ctx := context.Background()
	ts, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(ctx)
	k, err := ts.ScratchRange()
	require.NoError(t, err)
	mkKey := func(s string) roachpb.Key {
		return encoding.EncodeStringAscending(k[:len(k):len(k)], s)
	}
	keyA, keyB := mkKey("a"), mkKey("b")
	// Create a transaction that writes to keys "a" in its first epoch before
	// hitting a retryable error. Restart the transaction from the beginning and
	// verify that it its write was discarded. Then commit.
	txn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
	// Enable stepping so that Read Committed transactions can be tested.
	txn.ConfigureStepping(ctx, kv.SteppingEnabled)
	require.NoError(t, txn.SetIsoLevel(isoLevel))
	// Write to "a" in the first epoch.
	require.NoError(t, txn.Put(ctx, keyA, 1))
	require.NoError(t, txn.Step(ctx, true /* allowReadTimestampStep */))
	// Read from "b" to establish a refresh span.
	res, err := txn.Get(ctx, keyB)
	require.NoError(t, err)
	require.False(t, res.Exists())
	// Write to "b" outside the transaction to create a write-write conflict.
	require.NoError(t, db.Put(ctx, keyB, 2))
	// Write to "b" in the transaction to hit the write-write conflict, which
	// causes the transaction to retry.
	err = txn.Put(ctx, keyB, 3)
	require.Error(t, err)
	var retryErr *kvpb.TransactionRetryWithProtoRefreshError
	require.ErrorAs(t, err, &retryErr)
	require.Contains(t, retryErr.Error(), "WriteTooOldError")
	require.Equal(t, isoLevel != isolation.ReadCommitted, retryErr.TxnMustRestartFromBeginning())
	// Attempts to use the transaction again before calling PrepareForRetry are
	// rejected with the same retry error.
	_, err = txn.Get(ctx, keyB)
	require.Error(t, err)
	require.ErrorAs(t, err, &retryErr)
	require.Contains(t, retryErr.Error(), "WriteTooOldError")
	// PrepareForRetry resets the transaction and allows it to be used again.
	require.NoError(t, txn.PrepareForRetry(ctx))
	// PrepareForRetry is idempotent.
	require.NoError(t, txn.PrepareForRetry(ctx))
	// The transaction's epoch was advanced.
	require.Equal(t, enginepb.TxnEpoch(1), txn.Epoch())
	// The transaction's own write on "a" was discarded.
	res, err = txn.Get(ctx, keyA)
	require.NoError(t, err)
	require.False(t, res.Exists())
	// The non-transactional write on "b" is now visible.
	res, err = txn.Get(ctx, keyB)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.ValueInt())
	// The transaction can be committed.
	require.NoError(t, txn.Commit(ctx))
	// Validate the final state.
	checkKey := func(t *testing.T, k roachpb.Key, exp int64) {
		got, err := db.Get(ctx, k)
		require.NoError(t, err)
		require.Equal(t, exp, got.ValueInt())
	}
	checkKey(t, keyA, 0)
	checkKey(t, keyB, 2)
}

// TestPrepareForPartialRetry tests that PrepareForPartialRetry must be called
// after a retryable error is thrown by a Read Committed transaction and before
// that transaction is retried. It also tests that in these cases of "partial
// retries" in Read Committed transactions, the caller is in control of which
// writes are discarded during the retry through the use of savepoints.
func TestPrepareForPartialRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	ts, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(ctx)
	k, err := ts.ScratchRange()
	require.NoError(t, err)
	mkKey := func(s string) roachpb.Key {
		return encoding.EncodeStringAscending(k[:len(k):len(k)], s)
	}
	keyA, keyB, keyC, keyD := mkKey("a"), mkKey("b"), mkKey("c"), mkKey("d")
	// Create a read committed transaction that writes to keys "a" in its first
	// statement before hitting a retryable error during the second statement.
	// Roll back the operations performed on behalf of just the second statement
	// and retry it.
	txn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
	// Enable stepping so that we can control statement boudaries.
	txn.ConfigureStepping(ctx, kv.SteppingEnabled)
	require.NoError(t, txn.SetIsoLevel(isolation.ReadCommitted))
	// Write to "a" in the first "statement".
	stmt1, err := txn.CreateSavepoint(ctx)
	require.NoError(t, err)
	require.NoError(t, txn.Put(ctx, keyA, 1))
	require.NoError(t, txn.ReleaseSavepoint(ctx, stmt1))
	require.NoError(t, txn.Step(ctx, true /* allowReadTimestampStep */))
	// Perform a series of reads and writes in the second "statement".
	stmt2, err := txn.CreateSavepoint(ctx)
	require.NoError(t, err)
	// Read from "b" and "c" to establish refresh spans.
	res, err := txn.Get(ctx, keyB)
	require.NoError(t, err)
	require.False(t, res.Exists())
	res, err = txn.Get(ctx, keyC)
	require.NoError(t, err)
	require.False(t, res.Exists())
	// Write to "c" outside the transaction to create a write-write conflict.
	require.NoError(t, db.Put(ctx, keyC, 2))
	// Write to "b" in the transaction, without issue.
	require.NoError(t, txn.Put(ctx, keyB, 3))
	// Write to "c" in the transaction to hit the write-write conflict, which
	// causes the statement to need to retry.
	err = txn.Put(ctx, keyC, 4)
	require.Error(t, err)
	var retryErr *kvpb.TransactionRetryWithProtoRefreshError
	require.ErrorAs(t, err, &retryErr)
	require.Contains(t, retryErr.Error(), "WriteTooOldError")
	require.False(t, retryErr.TxnMustRestartFromBeginning())
	// Attempts to use the transaction again before calling
	// TestPrepareForPartialRetry are rejected with the same retry error.
	_, err = txn.Get(ctx, keyB)
	require.Error(t, err)
	require.ErrorAs(t, err, &retryErr)
	require.Contains(t, retryErr.Error(), "WriteTooOldError")
	// Roll back the second statement's writes and prepare to retry it.
	require.NoError(t, txn.RollbackToSavepoint(ctx, stmt2))
	// PrepareForPartialRetry resets the transaction and allows it to be used
	// again.
	require.NoError(t, txn.PrepareForPartialRetry(ctx))
	// PrepareForPartialRetry is idempotent.
	require.NoError(t, txn.PrepareForPartialRetry(ctx))
	// The transaction's own write on "a" from statement 1 was not discarded.
	res, err = txn.Get(ctx, keyA)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.ValueInt())
	// The transaction's own write on "b" from statement 2 was discarded.
	res, err = txn.Get(ctx, keyB)
	require.NoError(t, err)
	require.False(t, res.Exists())
	// The non-transactional write on "c" is now visible.
	res, err = txn.Get(ctx, keyC)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.ValueInt())
	// Write to "d" in the transaction during the retry.
	require.NoError(t, txn.Put(ctx, keyD, 5))
	// Commit.
	require.NoError(t, txn.ReleaseSavepoint(ctx, stmt2))
	require.NoError(t, txn.Commit(ctx))
	// Validate the final state.
	checkKey := func(t *testing.T, k roachpb.Key, exp int64) {
		got, err := db.Get(ctx, k)
		require.NoError(t, err)
		require.Equal(t, exp, got.ValueInt())
	}
	checkKey(t, keyA, 1)
	checkKey(t, keyB, 0)
	checkKey(t, keyC, 2)
	checkKey(t, keyD, 5)
}

// TestUpdateStateOnRemoteRetryableErr ensures transaction state is updated and
// a TransactionRetryWithProtoRefreshError is correctly constructed by
// UpdateStateOnRemoteRetryableError.
func TestUpdateStateOnRemoteRetryableErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	testCases := []struct {
		err         *kvpb.Error
		epochBumped bool // if we expect the epoch to be bumped
		newTxn      bool // if we expect a new transaction in the returned error; implies to an ABORT
	}{
		{
			err:         kvpb.NewError(&kvpb.ReadWithinUncertaintyIntervalError{}),
			epochBumped: true,
			newTxn:      false,
		},
		{
			err:         kvpb.NewError(&kvpb.TransactionAbortedError{}),
			epochBumped: false,
			newTxn:      true,
		},
		{
			err:         kvpb.NewError(&kvpb.TransactionPushError{}),
			epochBumped: true,
			newTxn:      false,
		},
		{
			err:         kvpb.NewError(&kvpb.TransactionRetryError{}),
			epochBumped: true,
			newTxn:      false,
		},
		{
			err:         kvpb.NewError(&kvpb.WriteTooOldError{}),
			epochBumped: true,
			newTxn:      false,
		},
	}

	for _, tc := range testCases {
		txn := db.NewTxn(ctx, "test")
		pErr := tc.err
		pErr.SetTxn(txn.Sender().TestingCloneTxn())
		epochBefore := txn.Epoch()
		txnIDBefore := txn.ID()
		err := txn.UpdateStateOnRemoteRetryableErr(ctx, pErr)
		// Ensure what we got back is a TransactionRetryWithProtoRefreshError.
		require.IsType(t, &kvpb.TransactionRetryWithProtoRefreshError{}, err)
		// Ensure the same thing is stored on the TxnCoordSender as well.
		retErr := txn.Sender().GetRetryableErr(ctx)
		require.Equal(t, retErr, err)
		if tc.epochBumped {
			require.Greater(t, txn.Epoch(), epochBefore)
			require.Equal(t, retErr.PrevTxnID, txnIDBefore) // transaction IDs should not have changed on us
		}
		if tc.newTxn {
			require.NotEqual(t, retErr.NextTransaction.ID, txnIDBefore)
			require.Equal(t, txn.Sender().TxnStatus(), roachpb.ABORTED)
		}
		// Lastly, ensure the TxnCoordSender was not swapped out, even if the
		// transaction was aborted.
		require.Equal(t, txn.Sender().TestingCloneTxn().ID, txnIDBefore)
	}
}

// TestUpdateStateOnRemoteRetryableErrErrorRanking tests that when multiple
// errors are provided to UpdateStateOnRemoteRetryableError, the error with the
// highest priority is used to update the transaction state and errors with
// lower priority are unable to change the transaction state.
func TestUpdateStateOnRemoteRetryableErrErrorRanking(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	orderedErrs := []struct {
		err         *kvpb.Error
		expNewError bool // if we expect the txn's retryable error to change
		expNewEpoch bool // if we expect the epoch to be bumped
		expAborted  bool // if we expect the txn to be aborted
	}{
		// Step 1. The txn starts out with no retryable error and restarts when
		// it hits an uncertainty error.
		{
			err:         kvpb.NewError(&kvpb.ReadWithinUncertaintyIntervalError{}),
			expNewError: true,
			expNewEpoch: true,
			expAborted:  false,
		},
		// Step 2. The txn ignores any further retryable errors from the same
		// epoch.
		{
			err:         kvpb.NewError(&kvpb.WriteTooOldError{}),
			expNewError: false,
			expNewEpoch: false,
			expAborted:  false,
		},
		// Step 3. The txn moves to an aborted state when it hits a txn aborted
		// error.
		{
			err:         kvpb.NewError(&kvpb.TransactionAbortedError{}),
			expNewError: true,
			expNewEpoch: false,
			expAborted:  true,
		},
		// Step 4. The txn ignores any further retryable errors.
		{
			err:         kvpb.NewError(&kvpb.ReadWithinUncertaintyIntervalError{}),
			expNewError: false,
			expNewEpoch: false,
			expAborted:  true,
		},
		// Step 5. The txn ignores any further txn aborted errors.
		{
			err:         kvpb.NewError(&kvpb.TransactionAbortedError{}),
			expNewError: false,
			expNewEpoch: false,
			expAborted:  true,
		},
	}

	// Unlike TestUpdateStateOnRemoteRetryableErr, use the same txn for all
	// errors and observe how it changes as each error is consumed.
	txn := db.NewTxn(ctx, "test")
	tcs := txn.Sender()
	txnProto := tcs.TestingCloneTxn()
	for _, tc := range orderedErrs {
		retryErrBefore := tcs.GetRetryableErr(ctx)
		epochBefore := tcs.Epoch()

		pErr := tc.err
		pErr.SetTxn(txnProto)
		err := txn.UpdateStateOnRemoteRetryableErr(ctx, pErr)
		// Ensure what we got back is a TransactionRetryWithProtoRefreshError.
		require.IsType(t, &kvpb.TransactionRetryWithProtoRefreshError{}, err)
		// Ensure the same thing is stored on the TxnCoordSender as well.
		retErr := tcs.GetRetryableErr(ctx)
		require.Equal(t, retErr, err)
		require.Equal(t, retErr.PrevTxnID, txnProto.ID)
		require.True(t, retErr.TxnMustRestartFromBeginning())

		// Assert that the transaction's state has changed as expected.
		if tc.expNewError {
			require.NotEqual(t, retErr, retryErrBefore)
		} else {
			require.Equal(t, retErr, retryErrBefore)
		}
		if tc.expNewEpoch {
			require.Equal(t, epochBefore+1, tcs.Epoch())
		} else {
			require.Equal(t, epochBefore, tcs.Epoch())
		}
		if tc.expAborted {
			require.Equal(t, roachpb.ABORTED, tcs.TxnStatus())
			require.True(t, retErr.PrevTxnAborted())
		} else {
			require.Equal(t, roachpb.PENDING, tcs.TxnStatus())
			require.False(t, retErr.PrevTxnAborted())
		}
	}
}

// TestUpdateRootWithLeafStateReadsBelowRefreshTimestamp ensures that if a leaf
// transaction has performed reads below the root transaction's refreshed
// timestamp an assertion error is returned. Note that the construction here
// involves concurrent use of a root and leaf txn, which is not allowed by the
// Txn API.
//
// This test codifies the desired behavior described in
// https://github.com/cockroachdb/cockroach/issues/99255.
func TestUpdateRootWithLeafFinalStateReadsBelowRefreshTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")

	performConflictingWrite := func(ctx context.Context, key roachpb.Key) (hlc.Timestamp, error) {
		conflictTxn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
		if err := conflictTxn.Put(ctx, key, "conflict"); err != nil {
			return hlc.Timestamp{}, err
		}
		if err := conflictTxn.Commit(ctx); err != nil {
			return hlc.Timestamp{}, err
		}
		return conflictTxn.CommitTimestamp()
	}
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Perform a read to set the timestamp.
		_, err := txn.Get(ctx, keyA)
		require.NoError(t, err)
		// Fork off a leaf transaction before the root is refreshed.
		leafInputState, err := txn.GetLeafTxnInputState(ctx)
		require.NoError(t, err)
		leafTxn := kv.NewLeafTxn(ctx, db, 0, leafInputState)

		writeTS, err := performConflictingWrite(ctx, keyB)
		require.NoError(t, err)
		require.True(t, leafTxn.TestingCloneTxn().ReadTimestamp.Less(writeTS))

		// Write to keyB using the root transaction. This should hit a write-write
		// conflict and cause the root transaction to be immediately refreshed.
		err = txn.Put(ctx, keyB, "garbage")
		require.NoError(t, err)

		// Finally, try and update the root with the leaf transaction's state.
		finalState, err := leafTxn.GetLeafTxnFinalState(ctx)
		require.NoError(t, err)
		err = txn.UpdateRootWithLeafFinalState(ctx, finalState)
		require.Error(t, err)
		require.True(t, errors.IsAssertionFailure(err), "%+v", err)
		require.Regexp(
			t, "attempting to append refresh spans after the tracked timestamp has moved forward", err,
		)
		return nil
	})
	require.NoError(t, err)
}
