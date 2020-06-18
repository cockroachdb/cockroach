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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestClosedTimestampCanServe(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if util.RaceEnabled {
		// Limiting how long transactions can run does not work
		// well with race unless we're extremely lenient, which
		// drives up the test duration.
		t.Skip("skipping under race")
	}

	ctx := context.Background()
	tc, db0, desc, repls := setupTestClusterForClosedTimestampTesting(ctx, t, testingTargetDuration)
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
	tc, db0, desc, repls := setupTestClusterForClosedTimestampTesting(ctx, t, testingTargetDuration)
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
	tc, _, desc, repls := setupTestClusterForClosedTimestampTesting(ctx, t, testingTargetDuration)
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
	tc, db0, desc, repls := setupTestClusterForClosedTimestampTesting(ctx, t, testingTargetDuration)
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
	tc, db0, desc, repls := setupTestClusterForClosedTimestampTesting(ctx, t, time.Hour)
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
	tc, db0, desc, repls := setupTestClusterForClosedTimestampTesting(ctx, t, testingTargetDuration)
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
	tc, db0, desc, repls := setupTestClusterForClosedTimestampTesting(ctx, t, testingTargetDuration)
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

func verifyNotLeaseHolderErrors(
	t *testing.T, ba roachpb.BatchRequest, repls []*kvserver.Replica, expectedNLEs int,
) {
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
		t.Fatal(err)
	}
	if a, e := notLeaseholderErrs, int64(expectedNLEs); a != e {
		t.Fatalf("expected %d NotLeaseHolderError; found %d", e, a)
	}
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

// This function creates a test cluster that is prepared to exercise follower
// reads. The returned test cluster has follower reads enabled using the above
// targetDuration and closeFraction. In addition to the newly minted test
// cluster, this function returns a db handle to node 0, a range descriptor for
// the range used by the table `cttest.kv` and the replica objects corresponding
// to the replicas for the range. It is the caller's responsibility to Stop the
// Stopper on the returned test cluster when done.
func setupTestClusterForClosedTimestampTesting(
	ctx context.Context, t *testing.T, targetDuration time.Duration,
) (
	tc serverutils.TestClusterInterface,
	db0 *gosql.DB,
	kvTableDesc roachpb.RangeDescriptor,
	repls []*kvserver.Replica,
) {
	tc = serverutils.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: aggressiveResolvedTimestampPushKnobs(),
			},
		},
	})
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
