// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var defaultClusterArgs = base.TestClusterArgs{
	ServerArgs: base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: aggressiveResolvedTimestampPushKnobs(),
		},
	},
}

func TestClosedTimestampCanServe(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if util.RaceEnabled {
		// Limiting how long transactions can run does not work
		// well with race unless we're extremely lenient, which
		// drives up the test duration.
		t.Skip("skipping under race")
	}

	ctx := context.Background()
	tc, db0, desc, repls := setupClusterForClosedTimestampTesting(ctx, t, testingTargetDuration, defaultClusterArgs)
	defer tc.Stopper().Stop(ctx)

	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}

	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	baRead := makeReadBatchRequestForDesc(desc, ts)
	testutils.SucceedsSoon(t, func() error {
		return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(1))
	})

	// We just served a follower read. As a sanity check, make sure that we can't write at
	// that same timestamp.
	{
		var baWrite roachpb.BatchRequest
		r := &roachpb.DeleteRequest{}
		r.Key = desc.StartKey.AsRawKey()
		txn := roachpb.MakeTransaction("testwrite", r.Key, roachpb.NormalUserPriority, ts, 100)
		baWrite.Txn = &txn
		baWrite.Add(r)
		baWrite.RangeID = repls[0].RangeID
		if err := baWrite.SetActiveTimestamp(tc.Server(0).Clock().Now); err != nil {
			t.Fatal(err)
		}

		var found bool
		for _, repl := range repls {
			resp, pErr := repl.Send(ctx, baWrite)
			if errors.HasType(pErr.GoError(), (*roachpb.NotLeaseHolderError)(nil)) {
				continue
			} else if pErr != nil {
				t.Fatal(pErr)
			}
			found = true
			if resp.Txn.WriteTimestamp.LessEq(ts) || resp.Txn.ReadTimestamp == resp.Txn.WriteTimestamp {
				t.Fatal("timestamp did not get bumped")
			}
			break
		}
		if !found {
			t.Fatal("unable to send to any replica")
		}
	}
}

// TestClosedTimestampCanServerThroughoutLeaseTransfer verifies that lease
// transfers does not prevent reading a value from a follower that was
// previously readable.
func TestClosedTimestampCanServeThroughoutLeaseTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if util.RaceEnabled {
		// Limiting how long transactions can run does not work
		// well with race unless we're extremely lenient, which
		// drives up the test duration.
		t.Skip("skipping under race")
	}
	ctx := context.Background()
	tc, db0, desc, repls := setupClusterForClosedTimestampTesting(ctx, t, testingTargetDuration, defaultClusterArgs)
	defer tc.Stopper().Stop(ctx)

	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	baRead := makeReadBatchRequestForDesc(desc, ts)
	testutils.SucceedsSoon(t, func() error {
		return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(1))
	})

	// Once we know that we can read safely at this timestamp, we want to ensure
	// that we can always read from this timestamp from all replicas even while
	// lease transfers are ongoing. The test launches a goroutine to randomly
	// trigger transfers at random intervals up to 50ms and ensures that there
	// are no errors reading the same value from any replica throughout the
	// duration of the test (testTime).
	const testTime = 500 * time.Millisecond
	const maxTransferWait = 50 * time.Millisecond
	deadline := timeutil.Now().Add(testTime)
	g, gCtx := errgroup.WithContext(ctx)

	transferLeasesRandomlyUntilDeadline := func() error {
		for timeutil.Now().Before(deadline) {
			lh := getCurrentLeaseholder(t, tc, desc)
			target := pickRandomTarget(tc, lh, desc)
			if err := tc.TransferRangeLease(desc, target); err != nil {
				return err
			}
			time.Sleep(time.Duration(rand.Intn(int(maxTransferWait))))
		}
		return nil
	}
	g.Go(transferLeasesRandomlyUntilDeadline)

	// Attempt to send read requests to a replica in a tight loop until deadline
	// is reached. If an error is seen on any replica then it is returned to the
	// errgroup.
	baRead = makeReadBatchRequestForDesc(desc, ts)
	ensureCanReadFromReplicaUntilDeadline := func(r *kvserver.Replica) {
		g.Go(func() error {
			for timeutil.Now().Before(deadline) {
				resp, pErr := r.Send(gCtx, baRead)
				if pErr != nil {
					return errors.Wrapf(pErr.GoError(), "on %s", r)
				}
				rows := resp.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
				// Should see the write.
				if len(rows) != 1 {
					return fmt.Errorf("expected one row, but got %d", len(rows))
				}
			}
			return nil
		})
	}
	for _, r := range repls {
		ensureCanReadFromReplicaUntilDeadline(r)
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
}

// TestClosedTimestampCanServeWithConflictingIntent validates that a read served
// from a follower replica will wait on conflicting intents and ensure that they
// are cleaned up if necessary to allow the read to proceed.
func TestClosedTimestampCanServeWithConflictingIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc, _, desc, repls := setupClusterForClosedTimestampTesting(ctx, t, testingTargetDuration, defaultClusterArgs)
	defer tc.Stopper().Stop(ctx)
	ds := tc.Server(0).DistSenderI().(*kvcoord.DistSender)

	// Write N different intents for the same transaction, where N is the number
	// of replicas in the testing range. Each intent will be read and eventually
	// resolved by a read on a different replica.
	txnKey := desc.StartKey.AsRawKey()
	txnKey = txnKey[:len(txnKey):len(txnKey)] // avoid aliasing
	txn := roachpb.MakeTransaction("txn", txnKey, 0, tc.Server(0).Clock().Now(), 0)
	var keys []roachpb.Key
	for i := range repls {
		key := append(txnKey, []byte(strconv.Itoa(i))...)
		keys = append(keys, key)
		put := putArgs(key, []byte("val"))
		resp, err := kv.SendWrappedWith(ctx, ds, roachpb.Header{Txn: &txn}, put)
		if err != nil {
			t.Fatal(err)
		}
		txn.Update(resp.Header().Txn)
	}

	// Read a different intent on each replica. All should begin waiting on the
	// intents by pushing the transaction that wrote them. None should complete.
	ts := txn.WriteTimestamp
	respCh := make(chan struct{}, len(keys))
	for i, key := range keys {
		go func(repl *kvserver.Replica, key roachpb.Key) {
			var baRead roachpb.BatchRequest
			r := &roachpb.ScanRequest{}
			r.Key = key
			r.EndKey = key.Next()
			baRead.Add(r)
			baRead.Timestamp = ts
			baRead.RangeID = desc.RangeID

			testutils.SucceedsSoon(t, func() error {
				// Expect 0 rows, because the intents will be aborted.
				_, err := expectRows(0)(repl.Send(ctx, baRead))
				return err
			})
			respCh <- struct{}{}
		}(repls[i], key)
	}

	select {
	case <-respCh:
		t.Fatal("request unexpectedly succeeded, should block")
	case <-time.After(20 * time.Millisecond):
	}

	// Abort the transaction. All pushes should succeed and all intents should
	// be resolved, allowing all reads (on the leaseholder and on followers) to
	// proceed and finish.
	endTxn := &roachpb.EndTxnRequest{
		RequestHeader: roachpb.RequestHeader{Key: txn.Key},
		Commit:        false,
	}
	if _, err := kv.SendWrappedWith(ctx, ds, roachpb.Header{Txn: &txn}, endTxn); err != nil {
		t.Fatal(err)
	}
	for range keys {
		<-respCh
	}
}

// TestClosedTimestampCanServeAfterSplitsAndMerges validates the invariant that
// if a timestamp is safe for reading on both the left side and right side of a
// a merge then it will be safe after the merge and that if a timestamp is safe
// for reading before the beginning of a split it will be safe on both sides of
// of the split.
func TestClosedTimestampCanServeAfterSplitAndMerges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if util.RaceEnabled {
		// Limiting how long transactions can run does not work
		// well with race unless we're extremely lenient, which
		// drives up the test duration.
		t.Skip("skipping under race")
	}
	ctx := context.Background()
	tc, db0, desc, repls := setupClusterForClosedTimestampTesting(ctx, t, testingTargetDuration, defaultClusterArgs)
	// Disable the automatic merging.
	if _, err := db0.Exec("SET CLUSTER SETTING kv.range_merge.queue_enabled = false"); err != nil {
		t.Fatal(err)
	}

	defer tc.Stopper().Stop(ctx)

	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}
	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(3, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}
	// Start by ensuring that the values can be read from all replicas at ts.
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	baRead := makeReadBatchRequestForDesc(desc, ts)
	testutils.SucceedsSoon(t, func() error {
		return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(2))
	})
	// Manually split the table to have easier access to descriptors.
	tableID, err := getTableID(db0, "cttest", "kv")
	if err != nil {
		t.Fatalf("failed to lookup ids: %+v", err)
	}
	// Split the table at key 2.
	idxPrefix := keys.SystemSQLCodec.IndexPrefix(uint32(tableID), 1)
	k, err := sqlbase.EncodeTableKey(idxPrefix, tree.NewDInt(2), encoding.Ascending)
	if err != nil {
		t.Fatalf("failed to encode key: %+v", err)
	}
	lr, rr, err := tc.Server(0).SplitRange(k)
	if err != nil {
		t.Fatalf("failed to split range at key %v: %+v", roachpb.Key(k), err)
	}

	// Ensure that we can perform follower reads from all replicas.
	lRepls := replsForRange(ctx, t, tc, lr)
	rRepls := replsForRange(ctx, t, tc, rr)
	// Now immediately query both the ranges and there's 1 value per range.
	// We need to tolerate RangeNotFound as the split range may not have been
	// created yet.
	baReadL := makeReadBatchRequestForDesc(lr, ts)
	require.Nil(t, verifyCanReadFromAllRepls(ctx, t, baReadL, lRepls,
		respFuncs(retryOnRangeNotFound, expectRows(1))))
	baReadR := makeReadBatchRequestForDesc(rr, ts)
	require.Nil(t, verifyCanReadFromAllRepls(ctx, t, baReadR, rRepls,
		respFuncs(retryOnRangeNotFound, expectRows(1))))

	// Now merge the ranges back together and ensure that there's two values in
	// the merged range.
	merged, err := tc.Server(0).MergeRanges(lr.StartKey.AsRawKey())
	require.Nil(t, err)
	mergedRepls := replsForRange(ctx, t, tc, merged)
	// The hazard here is that a follower is not yet aware of the merge and will
	// return an error. We'll accept that because a client wouldn't see that error
	// from distsender.
	baReadMerged := makeReadBatchRequestForDesc(merged, ts)
	require.Nil(t, verifyCanReadFromAllRepls(ctx, t, baReadMerged, mergedRepls,
		respFuncs(retryOnRangeKeyMismatch, expectRows(2))))
}

func getTableID(db *gosql.DB, dbName, tableName string) (tableID sqlbase.ID, err error) {
	err = db.QueryRow(`SELECT table_id FROM crdb_internal.tables WHERE database_name = $1 AND name = $2`,
		dbName, tableName).Scan(&tableID)
	return
}

func TestClosedTimestampCantServeBasedOnMaxTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if util.RaceEnabled {
		// Limiting how long transactions can run does not work
		// well with race unless we're extremely lenient, which
		// drives up the test duration.
		t.Skip("skipping under race")
	}

	ctx := context.Background()
	// Set up the target duration to be very long and rely on lease transfers to
	// drive MaxClosed.
	tc, db0, desc, repls := setupClusterForClosedTimestampTesting(ctx, t, time.Hour, defaultClusterArgs)
	defer tc.Stopper().Stop(ctx)

	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}

	// Grab a timestamp before initiating a lease transfer, transfer the lease,
	// then ensure that reads	at that timestamp can occur from all the replicas.
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	lh := getCurrentLeaseholder(t, tc, desc)
	target := pickRandomTarget(tc, lh, desc)
	require.Nil(t, tc.TransferRangeLease(desc, target))
	baRead := makeReadBatchRequestForDesc(desc, ts)
	testutils.SucceedsSoon(t, func() error {
		return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(1))
	})
	// Make a non-writing transaction that has a MaxTimestamp after the lease
	// transfer but a timestamp before.
	roTxn := roachpb.MakeTransaction("test", nil, roachpb.NormalUserPriority, ts,
		timeutil.Now().UnixNano()-ts.WallTime)
	baRead.Header.Txn = &roTxn
	// Send the request to all three replicas. One should succeed and
	// the other two should return NotLeaseHolderErrors.
	verifyNotLeaseHolderErrors(t, baRead, repls, 2)
}

func TestClosedTimestampCantServeForWritingTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if util.RaceEnabled {
		// Limiting how long transactions can run does not work
		// well with race unless we're extremely lenient, which
		// drives up the test duration.
		t.Skip("skipping under race")
	}

	ctx := context.Background()
	tc, db0, desc, repls := setupClusterForClosedTimestampTesting(ctx, t, testingTargetDuration, defaultClusterArgs)
	defer tc.Stopper().Stop(ctx)

	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}

	// Verify that we can serve a follower read at a timestamp. Wait if necessary.
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	baRead := makeReadBatchRequestForDesc(desc, ts)
	testutils.SucceedsSoon(t, func() error {
		return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(1))
	})

	// Create a read-only batch and attach a read-write transaction.
	rwTxn := roachpb.MakeTransaction("test", []byte("key"), roachpb.NormalUserPriority, ts, 0)
	baRead.Txn = &rwTxn

	// Send the request to all three replicas. One should succeed and
	// the other two should return NotLeaseHolderErrors.
	verifyNotLeaseHolderErrors(t, baRead, repls, 2)
}

func TestClosedTimestampCantServeForNonTransactionalReadRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if util.RaceEnabled {
		// Limiting how long transactions can run does not work
		// well with race unless we're extremely lenient, which
		// drives up the test duration.
		t.Skip("skipping under race")
	}

	ctx := context.Background()
	tc, db0, desc, repls := setupClusterForClosedTimestampTesting(ctx, t, testingTargetDuration, defaultClusterArgs)
	defer tc.Stopper().Stop(ctx)

	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}

	// Verify that we can serve a follower read at a timestamp. Wait if necessary
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	baRead := makeReadBatchRequestForDesc(desc, ts)
	testutils.SucceedsSoon(t, func() error {
		return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(1))
	})

	// Create a "nontransactional" read-only batch.
	var baQueryTxn roachpb.BatchRequest
	baQueryTxn.Header.RangeID = desc.RangeID
	r := &roachpb.QueryTxnRequest{}
	r.Key = desc.StartKey.AsRawKey()
	r.Txn.Key = r.Key
	r.Txn.MinTimestamp = ts
	baQueryTxn.Add(r)
	baQueryTxn.Timestamp = ts

	// Send the request to all three replicas. One should succeed and
	// the other two should return NotLeaseHolderErrors.
	verifyNotLeaseHolderErrors(t, baQueryTxn, repls, 2)
}

// TestClosedTimestampInactiveAfterSubsumption verifies that, during a merge,
// replicas of the subsumed range (RHS) cannot serve follower reads for
// timestamps after the subsumption time.
func TestClosedTimestampInactiveAfterSubsumption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("without lease transfer", func(t *testing.T) {
		ctx := context.Background()
		blockMergeTrigger := make(chan struct{}, 10) // headroom in case the merge txn retries
		finishMergeTxn := make(chan struct{})
		clusterArgs := defaultClusterArgs
		clusterArgs.ServerArgs.Knobs.Store.(*kvserver.StoreTestingKnobs).TestingRequestFilter =
			suspendMergeTrigger(blockMergeTrigger, finishMergeTxn)
		clusterArgs.ServerArgs.Knobs.Store.(*kvserver.StoreTestingKnobs).DisableMergeQueue = true

		tc, err, leftDesc, rightDesc := initClusterWithSplitRanges(ctx, t, clusterArgs)
		if err != nil {
			t.Fatal(err)
		}
		defer tc.Stopper().Stop(ctx)

		g, ctx := errgroup.WithContext(ctx)
		// Merge the ranges back together. The LHS leaseholder should block right
		// before the merge trigger request is sent.
		g.Go(beginMergeTxn(tc, leftDesc))
		defer func() {
			// Unblock the leaseholder so it can finally commit the merge.
			close(finishMergeTxn)
			if err := g.Wait(); err != nil {
				t.Error(err)
			}
		}()

		// We now have the RHS in its subsumed state.
		<-blockMergeTrigger
		leaseholder := getCurrentLeaseholder(t, tc, rightDesc)
		// We want to issue a read request with a timestamp one logical tick after
		// the current maximum closed timestamp of the RHS so we know that this
		// timestamp cannot be activated for follower reads until there is a future
		// closed timestamp update that contains an MLAI entry for the RHS range.
		postSubsumptionTimestamp := getCurrentMaxClosed(t, tc, leaseholder, rightDesc).ClosedTimestamp.Next()
		// Poll the store until we see a closed timestamp entry from RHS that is
		// greater than or equal to `postSubsumptionTimestamp`
		closedTimestampCh := make(chan ctpb.Entry)
		g.Go(func() error {
			pollForGreaterClosedTimestamp(t, tc, leaseholder, rightDesc, postSubsumptionTimestamp,
				closedTimestampCh)
			return nil
		})
		select {
		case <-closedTimestampCh:
		case <-time.After(testingTargetDuration):
			t.Fatal("did not observe a closed timestamp update for the RHS range after subsumption")
		}

		baReadRHSAfterSubsume := makeReadBatchRequestForDesc(rightDesc, postSubsumptionTimestamp)
		rightReplFollowers := getFollowerReplicas(ctx, t, tc, rightDesc, leaseholder)
		verifyNotLeaseHolderErrors(t, baReadRHSAfterSubsume, rightReplFollowers, 2 /* expectedNLEs */)
	})

	// This subtest ensures that follower reads are inactive on timestamps after
	// the subsumption time even in the presence of lease transfers on the
	// subsumed range.
	t.Run("with intervening lease transfer", func(t *testing.T) {
		ctx := context.Background()
		blockMergeTrigger := make(chan struct{}, 10) // headroom in case the merge txn retries
		finishMergeTxn := make(chan struct{})
		var leaseAcquisitionTrap atomic.Value
		clusterArgs := defaultClusterArgs
		clusterArgs.ServerArgs.Knobs.Store.(*kvserver.StoreTestingKnobs).TestingRequestFilter =
			suspendMergeTrigger(blockMergeTrigger, finishMergeTxn)
		clusterArgs.ServerArgs.Knobs.Store.(*kvserver.StoreTestingKnobs).LeaseRequestEvent =
			func(ts hlc.Timestamp, storeID roachpb.StoreID, rangeID roachpb.RangeID) {
				val := leaseAcquisitionTrap.Load()
				if val == nil {
					return
				}
				leaseAcquisitionCallback := val.(func(storeID roachpb.StoreID, rangeID roachpb.RangeID))
				if leaseAcquisitionCallback != nil {
					leaseAcquisitionCallback(storeID, rangeID)
				}
			}
		clusterArgs.ServerArgs.Knobs.Store.(*kvserver.StoreTestingKnobs).DisableMergeQueue = true

		tc, err, leftDesc, rightDesc := initClusterWithSplitRanges(ctx, t, clusterArgs)
		if err != nil {
			t.Fatal(err)
		}
		defer tc.Stopper().Stop(ctx)

		g, ctx := errgroup.WithContext(ctx)
		// Merge the ranges back together. The LHS leaseholder should block right
		// before the merge trigger request is sent.
		g.Go(beginMergeTxn(tc, leftDesc))
		defer func() {
			// Unblock the leaseholder so it can finally commit the merge.
			close(finishMergeTxn)
			if err := g.Wait(); err != nil {
				t.Error(err)
			}
		}()

		// We now have the RHS in its subsumed state.
		<-blockMergeTrigger
		leaseholder := getCurrentLeaseholder(t, tc, rightDesc)
		store := getTargetStoreOrFatal(t, tc, leaseholder)
		postSubsumptionTimestamp := store.Clock().Now()

		// We pause the node liveness heartbeats until a lease transfer
		// occurs.
		oldLease, _ := store.LookupReplica(rightDesc.StartKey).GetLease()
		require.True(t, oldLease.Replica.StoreID == store.StoreID())
		// Instantiate the lease acquisition callback right before we pause the node
		// liveness heartbeats. We do this here because leases may be requested at
		// any time for any reason, even before we pause the heartbeats.
		leaseAcquisitionCh := make(chan struct{})
		var once sync.Once
		leaseAcquisitionTrap.Store(func(storeID roachpb.StoreID, rangeID roachpb.RangeID) {
			if rangeID == rightDesc.RangeID {
				once.Do(func() {
					log.Infof(ctx, "received lease request from store %v for RHS range %v",
						storeID, rangeID)
					close(leaseAcquisitionCh)
				})
			}
		})
		restartHeartbeats := store.NodeLiveness().DisableAllHeartbeatsForTest()
		log.Infof(ctx, "paused RHS leaseholder's liveness heartbeats")
		time.Sleep(9 * time.Second)

		// Send a read request from one of the followers of RHS so that it notices
		// that the current leaseholder has stopped heartbeating. This will prompt
		// it to acquire the range lease for itself.
		g.Go(func() error {
			newLeaseholder := getFollowerReplicas(ctx, t, tc, rightDesc, leaseholder)[0]
			leaseAcquisitionRequest := makeReadBatchRequestForDesc(rightDesc, postSubsumptionTimestamp)
			log.Info(ctx,
				"sending a read request from a follower of RHS in order to trigger lease acquisition")
			_, pErr := newLeaseholder.Send(ctx, leaseAcquisitionRequest)
			// After the merge commits, the RHS will cease to exist. Thus, we expect
			// all pending queries on RHS to return RangeNotFoundErrors.
			require.IsType(t, &roachpb.RangeNotFoundError{}, pErr.GetDetail())
			return nil
		})
		select {
		case <-leaseAcquisitionCh:
			restartHeartbeats()
		case <-time.After(time.Second):
			t.Fatal("lease transfer did not occur as expected")
		}
		var leaseStart hlc.Timestamp
		testutils.SucceedsSoon(t, func() error {
			newLease, _ := store.LookupReplica(rightDesc.StartKey).GetLease()
			if newLease.Sequence == oldLease.Sequence {
				return errors.New("RHS lease not updated")
			}
			leaseholder = roachpb.ReplicationTarget{
				NodeID:  newLease.Replica.NodeID,
				StoreID: newLease.Replica.StoreID,
			}
			leaseStart = newLease.Start
			return nil
		})

		baReadAfterLeaseTransfer := makeReadBatchRequestForDesc(rightDesc, leaseStart.Next())
		// Poll the store to check if there are any closed timestamp updates for the RHS
		// after a new lease has started.
		closedTimestampCh := make(chan ctpb.Entry, 1)
		g.Go(func() (e error) {
			pollForGreaterClosedTimestamp(t, tc, leaseholder, rightDesc, leaseStart, closedTimestampCh)
			return
		})
		select {
		case <-closedTimestampCh:
			// After a lease transfer, we expect that the new leaseholder for the RHS
			// will detect that a merge is in progress and will simply block all
			// incoming requests. This means that we should not see any closed
			// timestamp updates containing an MLAI for the subsumed RHS range until
			// the merge is complete.
			t.Fatal("saw a closed timestamp update for the subsumed RHS range after a lease transfer")
		case <-time.After(testingTargetDuration):
		}
		rightReplFollowers := getFollowerReplicas(ctx, t, tc, rightDesc, leaseholder)
		log.Infof(ctx, "sending read requests from followers at a timestamp after the start of the new lease")
		verifyNotLeaseHolderErrors(t, baReadAfterLeaseTransfer, rightReplFollowers, 2 /* expectedNLEs */)
	})
}

func suspendMergeTrigger(
	suspendMergeTrigger chan<- struct{}, finishMergeTxn <-chan struct{},
) func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
	return func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		for _, req := range ba.Requests {
			if et := req.GetEndTxn(); et != nil && et.InternalCommitTrigger.GetMergeTrigger() != nil {
				// We block the LHS leaseholder from applying the merge trigger. Note
				// that RHS followers will have already caught up to the leaseholder
				// well before this point.
				suspendMergeTrigger <- struct{}{}
				<-finishMergeTxn
			}
		}
		return nil
	}
}

func beginMergeTxn(
	tc serverutils.TestClusterInterface, leftDesc roachpb.RangeDescriptor,
) func() error {
	return func() error {
		if _, err := tc.Server(0).MergeRanges(leftDesc.StartKey.AsRawKey()); err != nil {
			return err
		}
		return nil
	}
}

func initClusterWithSplitRanges(
	ctx context.Context, t *testing.T, clusterArgs base.TestClusterArgs,
) (serverutils.TestClusterInterface, error, roachpb.RangeDescriptor, roachpb.RangeDescriptor) {
	tc, db0, desc, repls := setupClusterForClosedTimestampTesting(ctx, t, testingTargetDuration, clusterArgs)

	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}
	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(3, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}
	// Start by ensuring that the values can be read from all replicas at ts.
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	baRead := makeReadBatchRequestForDesc(desc, ts)
	testutils.SucceedsSoon(t, func() error {
		return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(2))
	})
	// Manually split the table to have easier access to descriptors.
	tableID, err := getTableID(db0, "cttest", "kv")
	if err != nil {
		t.Fatalf("failed to lookup ids: %+v", err)
	}

	idxPrefix := keys.SystemSQLCodec.IndexPrefix(uint32(tableID), 1)
	k, err := sqlbase.EncodeTableKey(idxPrefix, tree.NewDInt(2), encoding.Ascending)
	if err != nil {
		t.Fatalf("failed to encode split key: %+v", err)
	}
	tcImpl := tc.(*testcluster.TestCluster)
	leftDesc, rightDesc := tcImpl.SplitRangeOrFatal(t, k)
	if err := tcImpl.WaitForFullReplication(); err != nil {
		t.Fatal(err)
	}
	return tc, err, leftDesc, rightDesc
}

func getCurrentMaxClosed(
	t *testing.T,
	tc serverutils.TestClusterInterface,
	target roachpb.ReplicationTarget,
	desc roachpb.RangeDescriptor,
) ctpb.Entry {
	deadline := time.Now().Add(testingTargetDuration)
	store := getTargetStoreOrFatal(t, tc, target)
	var maxClosed ctpb.Entry
	attempts := 0
	for attempts == 0 || timeutil.Now().Before(deadline) {
		attempts++
		store.ClosedTimestamp().Storage.VisitDescending(target.NodeID, func(entry ctpb.Entry) (done bool) {
			if _, ok := entry.MLAI[desc.RangeID]; ok {
				maxClosed = entry
				return true
			}
			return false
		})
		if _, ok := maxClosed.MLAI[desc.RangeID]; !ok {
			// We ran out of closed timestamps to visit without finding one that
			// corresponds to rightDesc. It is likely that no closed timestamps have
			// been broadcast for desc yet, try again.
			continue
		}
		return maxClosed
	}
	t.Fatal("Could not find a non-empty closed timestamp update for RHS range.")
	return ctpb.Entry{}
}

func pollForGreaterClosedTimestamp(
	t *testing.T,
	tc serverutils.TestClusterInterface,
	target roachpb.ReplicationTarget,
	desc roachpb.RangeDescriptor,
	lowerBound hlc.Timestamp,
	returnCh chan<- ctpb.Entry,
) {
	for {
		if t.Failed() {
			return
		}
		maxClosed := getCurrentMaxClosed(t, tc, target, desc)
		if _, ok := maxClosed.MLAI[desc.RangeID]; ok && lowerBound.LessEq(maxClosed.ClosedTimestamp) {
			returnCh <- maxClosed
			return
		}
	}
}

func getFollowerReplicas(
	ctx context.Context,
	t *testing.T,
	tc serverutils.TestClusterInterface,
	rangeDesc roachpb.RangeDescriptor,
	leaseholder roachpb.ReplicationTarget,
) []*kvserver.Replica {
	repls := replsForRange(ctx, t, tc, rangeDesc)
	followers := make([]*kvserver.Replica, 0)
	for _, repl := range repls {
		if repl.StoreID() == leaseholder.StoreID && repl.NodeID() == leaseholder.NodeID {
			continue
		}
		followers = append(followers, repl)
	}
	return followers
}

func getTargetStoreOrFatal(
	t *testing.T, tc serverutils.TestClusterInterface, target roachpb.ReplicationTarget,
) (store *kvserver.Store) {
	for i := 0; i < tc.NumServers(); i++ {
		if server := tc.Server(i); server.NodeID() == target.NodeID &&
			server.GetStores().(*kvserver.Stores).HasStore(target.StoreID) {
			store, err := server.GetStores().(*kvserver.Stores).GetStore(target.StoreID)
			if err != nil {
				t.Fatal(err)
			}
			return store
		}
	}
	t.Fatalf("Could not find store for replication target %+v\n", target)
	return nil
}

func verifyNotLeaseHolderErrors(
	t *testing.T, ba roachpb.BatchRequest, repls []*kvserver.Replica, expectedNLEs int,
) {
	notLeaseholderErrs, err := countNotLeaseHolderErrors(ba, repls)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := notLeaseholderErrs, int64(expectedNLEs); a != e {
		t.Fatalf("expected %d NotLeaseHolderError; found %d", e, a)
	}
}

func countNotLeaseHolderErrors(ba roachpb.BatchRequest, repls []*kvserver.Replica) (int64, error) {
	g, ctx := errgroup.WithContext(context.Background())
	var notLeaseholderErrs int64
	for i := range repls {
		repl := repls[i]
		g.Go(func() (err error) {
			if _, pErr := repl.Send(ctx, ba); pErr != nil {
				if _, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); ok {
					atomic.AddInt64(&notLeaseholderErrs, 1)
					return nil
				}
				return pErr.GetDetail()
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}
	return notLeaseholderErrs, nil
}

// Every 0.1s=100ms, try close out a timestamp ~300ms in the past.
// We don't want to be more aggressive than that since it's also
// a limit on how long transactions can run.
const testingTargetDuration = 300 * time.Millisecond
const closeFraction = 0.333
const numNodes = 3

func replsForRange(
	ctx context.Context,
	t *testing.T,
	tc serverutils.TestClusterInterface,
	desc roachpb.RangeDescriptor,
) (repls []*kvserver.Replica) {
	testutils.SucceedsSoon(t, func() error {
		repls = nil
		for i := 0; i < numNodes; i++ {
			repl, _, err := tc.Server(i).GetStores().(*kvserver.Stores).GetReplicaForRangeID(desc.RangeID)
			if err != nil {
				return err
			}
			if repl != nil {
				repls = append(repls, repl)
			}
		}
		return nil
	})
	return repls
}

func getCurrentLeaseholder(
	t *testing.T, tc serverutils.TestClusterInterface, desc roachpb.RangeDescriptor,
) (lh roachpb.ReplicationTarget) {
	testutils.SucceedsSoon(t, func() error {
		var err error
		lh, err = tc.FindRangeLeaseHolder(desc, nil)
		return err
	})
	return lh
}

func pickRandomTarget(
	tc serverutils.TestClusterInterface, lh roachpb.ReplicationTarget, desc roachpb.RangeDescriptor,
) (t roachpb.ReplicationTarget) {
	for {
		if t = tc.Target(rand.Intn(len(desc.InternalReplicas))); t != lh {
			return t
		}
	}
}

// aggressiveResolvedTimestampPushKnobs returns store testing knobs short
// rangefeed push age and interval.
func aggressiveResolvedTimestampPushKnobs() *kvserver.StoreTestingKnobs {
	if !util.RaceEnabled {
		return &kvserver.StoreTestingKnobs{
			RangeFeedPushTxnsInterval: 10 * time.Millisecond,
			RangeFeedPushTxnsAge:      20 * time.Millisecond,
		}
	} else {
		// Under race (particularly on an overloaded machine) it's easy to get
		// transactions to retry continuously with these settings too low because,
		// by the time a transaction finishes a refresh, it gets pushed again (and
		// thus forced to refresh again).
		return &kvserver.StoreTestingKnobs{
			RangeFeedPushTxnsInterval: 500 * time.Millisecond,
			RangeFeedPushTxnsAge:      time.Second,
		}
	}
}

// setupClusterForClosedTimestampTesting creates a test cluster that is prepared
// to exercise follower reads. The returned test cluster has follower reads
// enabled using the given targetDuration and above closeFraction. In addition
// to the newly minted test cluster, this function returns a db handle to node
// 0, a range descriptor for the range used by the table `cttest.kv` and the
// replica objects corresponding to the replicas for the range. It is the
// caller's responsibility to Stop the Stopper on the returned test cluster when
// done.
func setupClusterForClosedTimestampTesting(
	ctx context.Context, t *testing.T, targetDuration time.Duration, clusterArgs base.TestClusterArgs,
) (
	tc serverutils.TestClusterInterface,
	db0 *gosql.DB,
	kvTableDesc roachpb.RangeDescriptor,
	repls []*kvserver.Replica,
) {
	tc = serverutils.StartTestCluster(t, numNodes, clusterArgs)
	db0 = tc.ServerConn(0)

	if _, err := db0.Exec(fmt.Sprintf(`
-- Set a timeout to get nicer test failures from these statements. Because of
-- the aggressiveResolvedTimestampPushKnobs() these statements can restart
-- forever under high load (testrace under high concurrency).
SET statement_timeout='30s';
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '%s';
SET CLUSTER SETTING kv.closed_timestamp.close_fraction = %.3f;
SET CLUSTER SETTING kv.closed_timestamp.follower_reads_enabled = true;
CREATE DATABASE cttest;
CREATE TABLE cttest.kv (id INT PRIMARY KEY, value STRING);
-- Reset the timeout set above.
RESET statement_timeout;
`, targetDuration, closeFraction)); err != nil {
		t.Fatal(err)
	}

	var rangeID roachpb.RangeID
	var startKey roachpb.Key
	var numReplicas int
	testutils.SucceedsSoon(t, func() error {
		if err := db0.QueryRow(
			`SELECT range_id, start_key, array_length(replicas, 1) FROM crdb_internal.ranges WHERE table_name = 'kv' AND database_name = 'cttest'`,
		).Scan(&rangeID, &startKey, &numReplicas); err != nil {
			return err
		}
		if numReplicas != 3 {
			return errors.New("not fully replicated yet")
		}
		return nil
	})

	desc, err := tc.LookupRange(startKey)
	require.Nil(t, err)

	// First, we perform an arbitrary lease transfer because that will turn the
	// lease into an epoch based one (the initial lease is likely expiration based
	// since the range just split off from the very first range which is expiration
	// based).
	var lh roachpb.ReplicationTarget
	testutils.SucceedsSoon(t, func() error {
		var err error
		lh, err = tc.FindRangeLeaseHolder(desc, nil)
		return err
	})

	for i := 0; i < numNodes; i++ {
		target := tc.Target(i)
		if target != lh {
			if err := tc.TransferRangeLease(desc, target); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	repls = replsForRange(ctx, t, tc, desc)
	require.Equal(t, numReplicas, len(repls))
	// Wait until we see an epoch based lease on our chosen range. This should
	// happen fairly quickly since we just transferred a lease (as a means to make
	// it epoch based). If the lease transfer fails, we'll be sitting out the lease
	// expiration, which is on the order of seconds. Not great, but good enough since
	// the transfer basically always works.
	for ok := false; !ok; time.Sleep(10 * time.Millisecond) {
		for _, repl := range repls {
			lease, _ := repl.GetLease()
			if lease.Epoch != 0 {
				ok = true
				break
			}
		}
	}
	return tc, db0, desc, repls
}

type respFunc func(*roachpb.BatchResponse, *roachpb.Error) (shouldRetry bool, err error)

// respFuncs returns a respFunc which is passes its arguments to each passed
// func until one returns shouldRetry or a non-nil error.
func respFuncs(funcs ...respFunc) respFunc {
	return func(resp *roachpb.BatchResponse, pErr *roachpb.Error) (shouldRetry bool, err error) {
		for _, f := range funcs {
			shouldRetry, err = f(resp, pErr)
			if err != nil || shouldRetry {
				break
			}
		}
		return shouldRetry, err
	}
}

func retryOnError(f func(*roachpb.Error) bool) respFunc {
	return func(resp *roachpb.BatchResponse, pErr *roachpb.Error) (shouldRetry bool, err error) {
		if pErr != nil && f(pErr) {
			return true, nil
		}
		return false, pErr.GoError()
	}
}

var retryOnRangeKeyMismatch = retryOnError(func(pErr *roachpb.Error) bool {
	_, isRangeKeyMismatch := pErr.Detail.Value.(*roachpb.ErrorDetail_RangeKeyMismatch)
	return isRangeKeyMismatch
})

var retryOnRangeNotFound = retryOnError(func(pErr *roachpb.Error) bool {
	_, isRangeNotFound := pErr.Detail.Value.(*roachpb.ErrorDetail_RangeNotFound)
	return isRangeNotFound
})

func expectRows(expectedRows int) respFunc {
	return func(resp *roachpb.BatchResponse, pErr *roachpb.Error) (shouldRetry bool, err error) {
		if pErr != nil {
			return false, pErr.GoError()
		}
		rows := resp.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
		// Should see the write.
		if len(rows) != expectedRows {
			return false, fmt.Errorf("expected %d rows, but got %d", expectedRows, len(rows))
		}
		return false, nil
	}
}

func verifyCanReadFromAllRepls(
	ctx context.Context,
	t *testing.T,
	baRead roachpb.BatchRequest,
	repls []*kvserver.Replica,
	f respFunc,
) error {
	t.Helper()
	retryOptions := retry.Options{
		InitialBackoff: 500 * time.Microsecond,
		MaxBackoff:     5 * time.Millisecond,
		MaxRetries:     100,
	}
	// The read should succeed once enough time (~300ms, but it's difficult to
	// assert on that) has passed - on all replicas!
	g, ctx := errgroup.WithContext(ctx)
	for i := range repls {
		repl := repls[i]
		g.Go(func() (err error) {
			var shouldRetry bool
			for r := retry.StartWithCtx(ctx, retryOptions); r.Next(); <-r.NextCh() {
				if shouldRetry, err = f(repl.Send(ctx, baRead)); !shouldRetry {
					return err
				}
			}
			return err
		})
	}
	return g.Wait()
}

func makeReadBatchRequestForDesc(
	desc roachpb.RangeDescriptor, ts hlc.Timestamp,
) roachpb.BatchRequest {
	var baRead roachpb.BatchRequest
	baRead.Header.RangeID = desc.RangeID
	r := &roachpb.ScanRequest{}
	r.Key = desc.StartKey.AsRawKey()
	r.EndKey = desc.EndKey.AsRawKey()
	baRead.Add(r)
	baRead.Timestamp = ts
	return baRead
}
