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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/kvclientutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
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
			if leaseHolder.NodeID == 1 {
				db = tc.Servers[1].DB()
			} else {
				db = tc.Servers[0].DB()
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
				require.NoError(t, kvclientutils.CheckPushResult(ctx, db, *txn.TestingCloneTxn(),
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

// TestChildTransactionMetadataPropagates is a very simple test to ensure
// that requests issued in a child transaction carry the TxnMeta of the
// parent.
func TestChildTransactionMetadataPropagates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var state = struct {
		syncutil.Mutex
		childTxnUUID  uuid.UUID
		parentTxnUUID uuid.UUID
		called        bool
	}{}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter: func(ctx context.Context, request roachpb.BatchRequest) *roachpb.Error {
						if request.Txn == nil {
							return nil
						}
						state.Lock()
						defer state.Unlock()
						if state.childTxnUUID.Equal(uuid.UUID{}) {
							return nil
						}
						if request.Txn.ID.Equal(state.childTxnUUID) {
							if assert.NotNil(t, request.Txn.Parent) {
								assert.Equal(t, state.parentTxnUUID, request.Txn.Parent.ID)
								state.called = true
							}
						}
						return nil
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	scratch := tc.ScratchRange(t)
	// We want to create a transaction, then use it to create a child, then
	// observe a child request carrying the parent's TxnMeta.
	db := tc.Server(0).DB()
	txn := db.NewTxn(ctx, "testing")
	require.NoError(t, txn.ChildTxn(ctx, func(ctx context.Context, child *kv.Txn) error {
		func() {
			state.Lock()
			defer state.Unlock()
			state.parentTxnUUID = txn.ID()
			state.childTxnUUID = child.ID()
		}()

		return child.Put(ctx, scratch, "foo")
	}))
	state.Lock()
	defer state.Unlock()
	require.True(t, state.called)
}

// TestChildTransactionPushesParentToCommitTimestamp ensures that the child
// transaction pushes the parent to its commit timestamp.
func TestChildTransactionPushesParentToCommitTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()

	txn := kv.NewTxn(ctx, db, tc.Server(0).NodeID() /* gatewayNodeID */)
	before := txn.ProvisionalCommitTimestamp()
	toPushTo := before.Next().Next()
	require.NoError(t, txn.ChildTxn(ctx, func(ctx context.Context, childTxn *kv.Txn) error {
		childTxn.SetFixedTimestamp(ctx, toPushTo)
		return nil
	}))
	require.Equal(t, toPushTo, txn.ProvisionalCommitTimestamp())
}

// TestChildTransactionDeadlockDetection tests that deadlock cycles involving
// child transactions and parents are properly detected and broken.
func TestChildTransactionDeadlockDetection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()
	var wg sync.WaitGroup

	// runTxn will run a transaction which first performs an initPut to
	// (parentKey, parentVal), then closes afterWrite (if not closed), then
	// waits on beforeChild, then, in a ChildTxn, performs an initPut to
	// (childKey, childVal). It will drive the below deadlock scenario.
	runTxn := func(
		parentKey, childKey roachpb.Key,
		parentVal, childVal string,
		afterWrite, beforeChild chan struct{},
		res *error,
	) {
		defer wg.Done()
		*res = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			const failOnTombstones = false
			if err := txn.InitPut(ctx, parentKey, parentVal, failOnTombstones); err != nil {
				return err
			}
			select {
			case <-afterWrite:
			default:
				close(afterWrite)
			}
			<-beforeChild
			return txn.ChildTxn(ctx, func(ctx context.Context, child *kv.Txn) error {
				return child.InitPut(ctx, childKey, childVal, failOnTombstones)
			})
		})
	}

	// We're going to create the below scenario:
	//
	//          .-------------------------------------------------------.
	//          |                                                       |
	//          v                                                       |
	//    [txnA record]     [txnA2 record] --> [txnB record]     [txnB2 record]
	//
	// We'll ensure that this cycle is broken because the children query parents.
	//
	//          .-------------------------------------------------------.
	//          | .......................................               |
	//          | .  .................................  .               |
	//          v .  v                               .  v               |
	//    [txnA record]     [txnA2 record] --> [txnB record]     [txnB2 record]
	//     deps:                                deps:
	//     - txnB2                               - txnA2
	scratch := tc.ScratchRange(t)
	keyA := append(scratch[:len(scratch):len(scratch)], 'a')
	keyB := append(scratch[:len(scratch):len(scratch)], 'b')
	const (
		aValA, aValB = "foo", "bar"
		bValB, bValA = "baz", "bax"
	)
	var errA, errB error
	aChan, bChan := make(chan struct{}), make(chan struct{})
	wg.Add(2)
	go runTxn(keyA, keyB, aValA, aValB, aChan, bChan, &errA)
	go runTxn(keyB, keyA, bValB, bValA, bChan, aChan, &errB)
	wg.Wait()
	// The invariants are that:
	//  * This always makes progress.
	//  * Both transactions cannot commit fully.
	//
	// Without any transaction timeouts, the two possible outcomes should be
	// either that A and its child success or that B and its child success.
	//
	// There are some extreme scenarios which can happen under transaction
	// expiration whereby A ends up getting aborted because of a timeout and A1
	// does not notice. Similarly that can happen for B1. Lastly, in theory, that
	// could happen for both of them. We do not anticipate or allow these
	// scenarios.
	if (errA == nil && errB == nil) || (errA != nil && errB != nil) {
		t.Errorf("expected one of %v and %v to be nil", errA, errB)
	}
	getKeyAsString := func(key roachpb.Key) string {
		t.Helper()
		got, err := db.Get(ctx, key)
		require.NoError(t, err)
		if !got.Exists() {
			return ""
		}
		gotBytes, err := got.Value.GetBytes()
		require.NoError(t, err)
		return string(gotBytes)
	}
	strA := getKeyAsString(keyA)
	strB := getKeyAsString(keyB)
	switch {
	case strA == aValA && strB == aValB:
	case strA == bValA && strB == bValB:
	default:
		t.Fatalf("unexpected outcome: a=%s, b=%s", strA, strB)
	}
}

// TestChildTxnSelfInteractions is an integration-style test of child
// transaction interactions with parent state.
func TestChildTxnSelfInteractions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	type testCase struct {
		name string
		f    func(t *testing.T, db *kv.DB)
	}
	run := func(test testCase) {
		t.Run(test.name, func(t *testing.T) {
			tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
			defer tc.Stopper().Stop(ctx)
			db := tc.Server(0).DB()
			test.f(t, db)
		})
	}
	testCases := []testCase{
		{
			"child reads parent's write",
			func(t *testing.T, db *kv.DB) {
				require.NoError(t, db.Txn(ctx, func(
					ctx context.Context, txn *kv.Txn,
				) error {
					require.NoError(t, txn.Put(ctx, "foo", "bar"))
					return txn.ChildTxn(ctx, func(
						ctx context.Context, childTxn *kv.Txn,
					) error {
						got, err := childTxn.Get(ctx, "foo")
						require.NoError(t, err)
						gotBytes, err := got.Value.GetBytes()
						require.NoError(t, err)
						require.Equal(t, string(gotBytes), "bar")
						return nil
					})
				}))
			},
		},
		{
			"child write-write conflict gets an error",
			func(t *testing.T, db *kv.DB) {
				require.Regexp(t,
					`child transaction \w+ attempted to write over parent \w+ at key "foo"`,
					db.Txn(ctx, func(
						ctx context.Context, txn *kv.Txn,
					) error {
						require.NoError(t, txn.Put(ctx, "foo", "bar"))

						// This attempt to write over the parent's intent will fail.
						return txn.ChildTxn(ctx, func(
							ctx context.Context, childTxn *kv.Txn,
						) error {
							err := childTxn.Put(ctx, "foo", "baz")
							return err
						})
					}))
			},
		},
		{

			// In this case the child should pass through the read lock of the parent,
			// write successfully, and then it should push the parent which will then
			// be forced to refresh.
			"child read-write conflict forces parent to get an error (locking)",
			func(t *testing.T, db *kv.DB) {
				k := roachpb.Key("foo")
				require.NoError(t, db.Put(ctx, k, "bar"))
				require.Regexp(t,
					`cannot forward provisional commit timestamp due to overlapping write`,
					db.Txn(ctx, func(
						ctx context.Context, txn *kv.Txn,
					) error {
						scan := &roachpb.ScanRequest{
							KeyLocking: lock.Exclusive,
						}
						scan.Key = k
						scan.EndKey = k.PrefixEnd()
						b := txn.NewBatch()
						b.AddRawRequest(scan)
						require.NoError(t, txn.Run(ctx, b))

						// The below write will succeed but the forwarding of the parent above
						// the write's timestamp will fail as it detects that the parent's
						// read has been invalidated.
						return txn.ChildTxn(ctx, func(
							ctx context.Context, childTxn *kv.Txn,
						) error {
							err := childTxn.Put(ctx, k, "baz")
							require.NoError(t, err)
							return nil
						})
					}))
			},
		},
	}
	for _, tc := range testCases {
		run(tc)
	}
}
