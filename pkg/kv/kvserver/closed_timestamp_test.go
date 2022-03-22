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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var aggressiveResolvedTimestampClusterArgs = base.TestClusterArgs{
	ServerArgs: base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: aggressiveResolvedTimestampPushKnobs(),
		},
	},
}

func TestClosedTimestampCanServe(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Limiting how long transactions can run does not work
	// well with race unless we're extremely lenient, which
	// drives up the test duration.
	skip.UnderRace(t)

	testutils.RunTrueAndFalse(t, "withNonVoters", func(t *testing.T, withNonVoters bool) {
		ctx := context.Background()
		dbName, tableName := "cttest", "kv"
		clusterArgs := aggressiveResolvedTimestampClusterArgs
		// Disable the replicateQueue so that it doesn't interfere with replica
		// membership ranges.
		clusterArgs.ReplicationMode = base.ReplicationManual
		tc, db0, desc := setupClusterForClosedTSTesting(ctx, t, testingTargetDuration, clusterArgs, dbName, tableName)
		defer tc.Stopper().Stop(ctx)

		if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
			t.Fatal(err)
		}

		if withNonVoters {
			desc = tc.AddNonVotersOrFatal(t, desc.StartKey.AsRawKey(), tc.Target(1),
				tc.Target(2))
		} else {
			desc = tc.AddVotersOrFatal(t, desc.StartKey.AsRawKey(), tc.Target(1),
				tc.Target(2))
		}

		repls := replsForRange(ctx, t, tc, desc, numNodes)
		ts := tc.Server(0).Clock().Now()
		baRead := makeTxnReadBatchForDesc(desc, ts)
		testutils.SucceedsSoon(t, func() error {
			return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(1))
		})

		// We just served a follower read. As a sanity check, make sure that we can't write at
		// that same timestamp.
		{
			var baWrite roachpb.BatchRequest
			r := &roachpb.DeleteRequest{}
			r.Key = desc.StartKey.AsRawKey()
			txn := roachpb.MakeTransaction("testwrite", r.Key, roachpb.NormalUserPriority, ts, 100, int32(tc.Server(0).SQLInstanceID()))
			baWrite.Txn = &txn
			baWrite.Add(r)
			baWrite.RangeID = repls[0].RangeID
			if err := baWrite.SetActiveTimestamp(tc.Server(0).Clock()); err != nil {
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
	})
}

func TestClosedTimestampCanServeOnVoterIncoming(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Limiting how long transactions can run does not work well with race unless
	// we're extremely lenient, which drives up the test duration.
	skip.UnderRace(t)

	ctx := context.Background()
	dbName, tableName := "cttest", "kv"
	clusterArgs := aggressiveResolvedTimestampClusterArgs
	clusterArgs.ReplicationMode = base.ReplicationManual
	knobs, ltk := makeReplicationTestKnobs()
	clusterArgs.ServerArgs.Knobs = knobs
	tc, db0, desc := setupClusterForClosedTSTesting(ctx, t, testingTargetDuration, clusterArgs, dbName, tableName)
	defer tc.Stopper().Stop(ctx)

	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}
	// Add a new voting replica, which should get the range into a joint config.
	// It will stay in that state because of the `VoterAddStopAfterJointConfig`
	// testing knob `makeReplicationTestKnobs()`.
	ltk.withStopAfterJointConfig(func() {
		tc.AddVotersOrFatal(t, desc.StartKey.AsRawKey(), tc.Target(1), tc.Target(2))
	})

	reqTS := tc.Server(0).Clock().Now()
	// Sleep for a sufficiently long time so that reqTS can be closed.
	time.Sleep(3 * testingTargetDuration)
	baRead := makeTxnReadBatchForDesc(desc, reqTS)
	repls := replsForRange(ctx, t, tc, desc, numNodes)
	testutils.SucceedsSoon(t, func() error {
		return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(1))
	})
}

// TestClosedTimestampCanServerThroughoutLeaseTransfer verifies that lease
// transfers does not prevent reading a value from a follower that was
// previously readable.
func TestClosedTimestampCanServeThroughoutLeaseTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Limiting how long transactions can run does not work
	// well with race unless we're extremely lenient, which
	// drives up the test duration.
	skip.UnderRace(t)

	ctx := context.Background()
	tc, db0, desc := setupClusterForClosedTSTesting(ctx, t, testingTargetDuration, aggressiveResolvedTimestampClusterArgs, "cttest", "kv")
	defer tc.Stopper().Stop(ctx)
	repls := replsForRange(ctx, t, tc, desc, numNodes)

	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}
	ts := tc.Server(0).Clock().Now()
	baRead := makeTxnReadBatchForDesc(desc, ts)
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
	baRead = makeTxnReadBatchForDesc(desc, ts)
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

// TestClosedTimestampCantServeWithConflictingIntent validates that a read
// served from a follower replica will redirect to the leaseholder if it
// encounters a conflicting intent below the closed timestamp.
func TestClosedTimestampCantServeWithConflictingIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, _, desc := setupClusterForClosedTSTesting(ctx, t, testingTargetDuration, aggressiveResolvedTimestampClusterArgs, "cttest", "kv")
	defer tc.Stopper().Stop(ctx)
	repls := replsForRange(ctx, t, tc, desc, numNodes)
	ds := tc.Server(0).DistSenderI().(*kvcoord.DistSender)

	// Write N different intents for the same transaction, where N is the number
	// of replicas in the testing range. Each intent will be read on a different
	// replica.
	txnKey := desc.StartKey.AsRawKey()
	txnKey = txnKey[:len(txnKey):len(txnKey)] // avoid aliasing
	txn := roachpb.MakeTransaction("txn", txnKey, 0, tc.Server(0).Clock().Now(), 0, int32(tc.Server(0).SQLInstanceID()))
	var keys []roachpb.Key
	for i := range repls {
		key := append(txnKey, []byte(strconv.Itoa(i))...)
		keys = append(keys, key)
		put := putArgs(key, []byte("val"))
		resp, err := kv.SendWrappedWith(ctx, ds, roachpb.Header{Txn: &txn}, put)
		require.Nil(t, err)
		txn.Update(resp.Header().Txn)
	}

	// Set a long txn liveness threshold so that the txn cannot be aborted.
	defer txnwait.TestingOverrideTxnLivenessThreshold(time.Hour)()

	// runFollowerReads attempts to perform a follower read on a different key on
	// each replica, using the provided timestamp as the request timestamp.
	runFollowerReads := func(ts hlc.Timestamp, retryUntilSuccessful bool) chan error {
		respCh := make(chan error, len(repls))
		for i := range repls {
			go func(repl *kvserver.Replica, key roachpb.Key) {
				baRead := makeTxnReadBatchForDesc(desc, ts)
				baRead.Requests[0].GetScan().SetSpan(roachpb.Span{
					Key:    key,
					EndKey: key.Next(),
				})
				var err error
				if retryUntilSuccessful {
					err = testutils.SucceedsSoonError(func() error {
						// Expect 0 rows, because the intents are never committed.
						_, err := expectRows(0)(repl.Send(ctx, baRead))
						return err
					})
				} else {
					_, pErr := repl.Send(ctx, baRead)
					err = pErr.GoError()
				}
				respCh <- err
			}(repls[i], keys[i])
		}
		return respCh
	}

	// Follower reads should be possible up to just below the intents' timestamp.
	// We use MinTimestamp instead of WriteTimestamp because the WriteTimestamp
	// may have been bumped after the txn wrote some intents.
	respCh1 := runFollowerReads(txn.MinTimestamp.Prev(), true)
	for i := 0; i < len(repls); i++ {
		require.NoError(t, <-respCh1)
	}

	// At the intents' timestamp, reads on the leaseholder should block and reads
	// on the followers should be redirected to the leaseholder, even though the
	// read timestamp is below the closed timestamp.
	respCh2 := runFollowerReads(txn.WriteTimestamp, false)
	for i := 0; i < len(repls)-1; i++ {
		err := <-respCh2
		require.Error(t, err)
		var lErr *roachpb.NotLeaseHolderError
		require.True(t, errors.As(err, &lErr))
	}
	select {
	case err := <-respCh2:
		t.Fatalf("request unexpectedly returned, should block; err: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	// Abort the transaction. All intents should be rolled back.
	endTxn := &roachpb.EndTxnRequest{
		RequestHeader: roachpb.RequestHeader{Key: txn.Key},
		Commit:        false,
		LockSpans:     []roachpb.Span{desc.KeySpan().AsRawSpanWithNoLocals()},
	}
	_, err := kv.SendWrappedWith(ctx, ds, roachpb.Header{Txn: &txn}, endTxn)
	require.Nil(t, err)

	// The blocked read on the leaseholder should succeed.
	require.NoError(t, <-respCh2)

	// Follower reads should now be possible at the intents' timestamp.
	respCh3 := runFollowerReads(txn.WriteTimestamp, true)
	for i := 0; i < len(repls); i++ {
		require.NoError(t, <-respCh3)
	}
}

// TestClosedTimestampCanServeAfterSplitsAndMerges validates the invariant that
// if a timestamp is safe for reading on both the left side and right side of a
// merge then it will be safe after the merge and that if a timestamp is safe
// for reading before the beginning of a split it will be safe on both sides of
// the split.
func TestClosedTimestampCanServeAfterSplitAndMerges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Limiting how long transactions can run does not work
	// well with race unless we're extremely lenient, which
	// drives up the test duration.
	skip.UnderRace(t)

	ctx := context.Background()
	tc, db0, desc := setupClusterForClosedTSTesting(ctx, t, testingTargetDuration, aggressiveResolvedTimestampClusterArgs, "cttest", "kv")
	repls := replsForRange(ctx, t, tc, desc, numNodes)
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
	ts := tc.Server(0).Clock().Now()
	baRead := makeTxnReadBatchForDesc(desc, ts)
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
	k, err := keyside.Encode(idxPrefix, tree.NewDInt(2), encoding.Ascending)
	if err != nil {
		t.Fatalf("failed to encode key: %+v", err)
	}
	lr, rr, err := tc.Server(0).SplitRange(k)
	if err != nil {
		t.Fatalf("failed to split range at key %v: %+v", roachpb.Key(k), err)
	}

	// Ensure that we can perform follower reads from all replicas.
	lRepls := replsForRange(ctx, t, tc, lr, numNodes)
	rRepls := replsForRange(ctx, t, tc, rr, numNodes)
	// Now immediately query both the ranges and there's 1 value per range.
	// We need to tolerate RangeNotFound as the split range may not have been
	// created yet.
	baReadL := makeTxnReadBatchForDesc(lr, ts)
	require.Nil(t, verifyCanReadFromAllRepls(ctx, t, baReadL, lRepls,
		respFuncs(retryOnRangeNotFound, expectRows(1))))
	baReadR := makeTxnReadBatchForDesc(rr, ts)
	require.Nil(t, verifyCanReadFromAllRepls(ctx, t, baReadR, rRepls,
		respFuncs(retryOnRangeNotFound, expectRows(1))))

	// Now merge the ranges back together and ensure that there's two values in
	// the merged range.
	merged, err := tc.Server(0).MergeRanges(lr.StartKey.AsRawKey())
	require.Nil(t, err)
	mergedRepls := replsForRange(ctx, t, tc, merged, numNodes)
	// The hazard here is that a follower is not yet aware of the merge and will
	// return an error. We'll accept that because a client wouldn't see that error
	// from distsender.
	baReadMerged := makeTxnReadBatchForDesc(merged, ts)
	require.Nil(t, verifyCanReadFromAllRepls(ctx, t, baReadMerged, mergedRepls,
		respFuncs(retryOnRangeKeyMismatch, expectRows(2))))
}

func getTableID(db *gosql.DB, dbName, tableName string) (tableID descpb.ID, err error) {
	err = db.QueryRow(`SELECT table_id FROM crdb_internal.tables WHERE database_name = $1 AND name = $2`,
		dbName, tableName).Scan(&tableID)
	return
}

func TestClosedTimestampCantServeBasedOnUncertaintyLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Limiting how long transactions can run does not work
	// well with race unless we're extremely lenient, which
	// drives up the test duration.
	skip.UnderRace(t)

	ctx := context.Background()
	// Set up the target duration to be very long and rely on lease transfers to
	// drive MaxClosed.
	tc, db0, desc := setupClusterForClosedTSTesting(ctx, t, testingTargetDuration, aggressiveResolvedTimestampClusterArgs, "cttest", "kv")
	defer tc.Stopper().Stop(ctx)
	repls := replsForRange(ctx, t, tc, desc, numNodes)

	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}

	// Verify that we can serve a follower read at a timestamp. Wait if necessary.
	ts := tc.Server(0).Clock().Now()
	baRead := makeTxnReadBatchForDesc(desc, ts)
	testutils.SucceedsSoon(t, func() error {
		return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(1))
	})

	// Update the batch to simulate a transaction that has a global uncertainty
	// limit after the current clock time. Keep its read timestamp the same.
	baRead.Txn.GlobalUncertaintyLimit = tc.Server(0).Clock().Now().Add(time.Second.Nanoseconds(), 0)
	// Send the request to all three replicas. One should succeed and
	// the other two should return NotLeaseHolderErrors.
	verifyNotLeaseHolderErrors(t, baRead, repls, 2)
}

func TestClosedTimestampCanServeForWritingTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Limiting how long transactions can run does not work
	// well with race unless we're extremely lenient, which
	// drives up the test duration.
	skip.UnderRace(t)

	ctx := context.Background()
	tc, db0, desc := setupClusterForClosedTSTesting(ctx, t, testingTargetDuration, aggressiveResolvedTimestampClusterArgs, "cttest", "kv")
	defer tc.Stopper().Stop(ctx)
	repls := replsForRange(ctx, t, tc, desc, numNodes)

	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}

	// Verify that we can serve a follower read at a timestamp. Wait if necessary.
	ts := tc.Server(0).Clock().Now()
	baRead := makeTxnReadBatchForDesc(desc, ts)
	testutils.SucceedsSoon(t, func() error {
		return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(1))
	})

	// Update the batch to simulate a transaction that has written an intent.
	baRead.Txn.Key = []byte("key")
	baRead.Txn.Sequence++

	// The write timestamp of the transaction is still closed, so a read-only
	// request by the transaction should be servicable by followers. This is
	// because the writing transaction can still read its writes on the
	// followers.
	testutils.SucceedsSoon(t, func() error {
		return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(1))
	})

	// Update the batch to simulate a transaction that has written past its read
	// timestamp and past the expected closed timestamp. This should prevent its
	// reads from being served by followers.
	baRead.Txn.WriteTimestamp = tc.Server(0).Clock().Now().Add(time.Second.Nanoseconds(), 0)

	// Send the request to all three replicas. One should succeed and the other
	// two should return NotLeaseHolderErrors.
	verifyNotLeaseHolderErrors(t, baRead, repls, 2)
}

func TestClosedTimestampCantServeForNonTransactionalReadRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Limiting how long transactions can run does not work
	// well with race unless we're extremely lenient, which
	// drives up the test duration.
	skip.UnderRace(t)

	ctx := context.Background()
	tc, db0, desc := setupClusterForClosedTSTesting(ctx, t, testingTargetDuration, aggressiveResolvedTimestampClusterArgs, "cttest", "kv")
	defer tc.Stopper().Stop(ctx)
	repls := replsForRange(ctx, t, tc, desc, numNodes)

	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}

	// Verify that we can serve a follower read at a timestamp. Wait if
	// necessary.
	ts := tc.Server(0).Clock().Now()
	baRead := makeTxnReadBatchForDesc(desc, ts)
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

	// Send the request to all three replicas. One should succeed and the other
	// two should return NotLeaseHolderErrors.
	verifyNotLeaseHolderErrors(t, baQueryTxn, repls, 2)
}

func TestClosedTimestampCantServeForNonTransactionalBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Limiting how long transactions can run does not work
	// well with race unless we're extremely lenient, which
	// drives up the test duration.
	skip.UnderRace(t)

	testutils.RunTrueAndFalse(t, "tsFromServer", func(t *testing.T, tsFromServer bool) {
		ctx := context.Background()
		tc, db0, desc := setupClusterForClosedTSTesting(ctx, t, testingTargetDuration, aggressiveResolvedTimestampClusterArgs, "cttest", "kv")
		defer tc.Stopper().Stop(ctx)
		repls := replsForRange(ctx, t, tc, desc, numNodes)

		if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
			t.Fatal(err)
		}

		// Verify that we can serve a follower read at a timestamp with a
		// transactional batch. Wait if necessary.
		ts := tc.Server(0).Clock().Now()
		baRead := makeTxnReadBatchForDesc(desc, ts)
		testutils.SucceedsSoon(t, func() error {
			return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(1))
		})

		// Remove the transaction and send the request to all three replicas. If the
		// batch indicates that the timestamp was set from the server's clock, then
		// one should succeed and the other two should return NotLeaseHolderErrors.
		// Otherwise, all three should succeed.
		baRead.Txn = nil
		if tsFromServer {
			baRead.TimestampFromServerClock = (*hlc.ClockTimestamp)(&ts)
			verifyNotLeaseHolderErrors(t, baRead, repls, 2)
		} else {
			testutils.SucceedsSoon(t, func() error {
				return verifyCanReadFromAllRepls(ctx, t, baRead, repls, expectRows(1))
			})
		}
	})
}

// Test that, during a merge, the closed timestamp of the subsumed RHS doesn't
// go above the subsumption time. It'd be bad if it did, since this advanced
// closed timestamp would be lost when the merge finalizes.
func TestClosedTimestampFrozenAfterSubsumption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)

	for _, test := range []struct {
		name string
		// transferLease, if set, will be called while the RHS is subsumed in order
		// to perform a RHS lease transfer.
		transferLease func(
			ctx context.Context,
			t *testing.T,
			tc serverutils.TestClusterInterface,
			rhsDesc roachpb.RangeDescriptor,
			rhsLeaseholder roachpb.ReplicationTarget,
			clock *hlc.HybridManualClock,
		) (newLeaseholder roachpb.ReplicationTarget, leaseStart hlc.Timestamp)
	}{
		{
			name:          "basic",
			transferLease: nil,
		},
		{
			name: "rhs lease transfer while subsumed",
			transferLease: func(
				ctx context.Context,
				t *testing.T,
				tc serverutils.TestClusterInterface,
				rhsDesc roachpb.RangeDescriptor,
				rhsLeaseholder roachpb.ReplicationTarget,
				clock *hlc.HybridManualClock,
			) (roachpb.ReplicationTarget, hlc.Timestamp) {
				oldLeaseholderStore := getTargetStoreOrFatal(t, tc, rhsLeaseholder)
				oldLease, _ := oldLeaseholderStore.LookupReplica(rhsDesc.StartKey).GetLease()
				require.True(t, oldLease.Replica.StoreID == oldLeaseholderStore.StoreID())
				newLeaseholder := getFollowerReplicas(ctx, t, tc, rhsDesc, rhsLeaseholder)[0]
				target := roachpb.ReplicationTarget{
					NodeID:  newLeaseholder.NodeID(),
					StoreID: newLeaseholder.StoreID(),
				}
				newLease, err := tc.MoveRangeLeaseNonCooperatively(ctx, rhsDesc, target, clock)
				require.NoError(t, err)
				return target, newLease.Start.ToTimestamp()
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			// Set a long txn liveness threshold; we'll bump the clock to cause a
			// lease to expire and we don't want that to cause transactions to be
			// aborted (in particular, the merge txn that will be in progress when we
			// bump the clock).
			defer txnwait.TestingOverrideTxnLivenessThreshold(time.Hour)()

			// Range merges can be internally retried by the coordinating node (the
			// leaseholder of the left hand side range). If this happens, the right hand
			// side can get re-subsumed. However, in the current implementation, even if
			// the merge txn gets retried, the follower replicas should not be able to
			// activate any closed timestamp updates succeeding the timestamp the RHS
			// was subsumed _for the first time_.
			st := mergeFilter{}
			manual := hlc.NewHybridManualClock()
			pinnedLeases := kvserver.NewPinnedLeases()
			clusterArgs := base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					RaftConfig: base.RaftConfig{
						// We set the raft election timeout to a small duration. This should
						// result in the node liveness duration being ~3.6 seconds. Note that
						// if we set this too low, the test may flake due to the test
						// cluster's nodes frequently missing their liveness heartbeats.
						RaftHeartbeatIntervalTicks: 5,
						RaftElectionTimeoutTicks:   6,
					},
					Knobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							ClockSource: manual.UnixNano,
						},
						Store: &kvserver.StoreTestingKnobs{
							// This test suspends the merge txn right before it can apply the
							// commit trigger and can lead to the merge txn taking longer than
							// the defaults specified in aggressiveResolvedTimestampPushKnobs().
							// We use really high values here in order to avoid the merge txn
							// being pushed due to resolved timestamps.
							RangeFeedPushTxnsInterval: 5 * time.Second,
							RangeFeedPushTxnsAge:      60 * time.Second,
							TestingRequestFilter:      st.SuspendMergeTrigger,
							DisableMergeQueue:         true,
							// A subtest wants to force a lease change by stopping the liveness
							// heartbeats on the old leaseholder and sending a request to
							// another replica. If we didn't use this knob, we'd have to
							// architect a Raft leadership change too in order to let the
							// replica get the lease.
							AllowLeaseRequestProposalsWhenNotLeader: true,
							PinnedLeases:                            pinnedLeases,
						},
					},
				},
			}

			// Set up the closed timestamp timing such that, when we block a merge and
			// transfer the RHS lease, the closed timestamp advances over the LHS
			// lease but not over the RHS lease.
			tc, _ := setupTestClusterWithDummyRange(t, clusterArgs, "cttest" /* dbName */, "kv" /* tableName */, numNodes)
			defer tc.Stopper().Stop(ctx)
			_, err := tc.ServerConn(0).Exec(fmt.Sprintf(`
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '%s';
SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '%s';
SET CLUSTER SETTING kv.closed_timestamp.follower_reads_enabled = true;
`, 5*time.Second, 100*time.Millisecond))
			require.NoError(t, err)
			leftDesc, rightDesc := splitDummyRangeInTestCluster(t, tc, "cttest", "kv", hlc.Timestamp{} /* splitExpirationTime */)

			leftLeaseholder := getCurrentLeaseholder(t, tc, leftDesc)
			rightLeaseholder := getCurrentLeaseholder(t, tc, rightDesc)
			// Pin the lhs lease where it already is. We're going to bump the clock to
			// expire the rhs lease, and we don't want the lhs lease to move to a
			// different node.
			pinnedLeases.PinLease(leftDesc.RangeID, leftLeaseholder.StoreID)

			g := ctxgroup.WithContext(ctx)
			// Merge the ranges back together. The LHS leaseholder should block right
			// before the merge trigger request is sent.
			leftLeaseholderStore := getTargetStoreOrFatal(t, tc, leftLeaseholder)
			mergeBlocker := st.BlockNextMerge()
			mergeErrCh := make(chan error, 1)
			g.Go(func() error {
				err := mergeWithRightNeighbor(ctx, leftLeaseholderStore, leftDesc)
				mergeErrCh <- err
				return err
			})
			defer func() {
				// Unblock the merge.
				if mergeBlocker.Unblock() {
					assert.NoError(t, g.Wait())
				}
			}()

			var freezeStartTimestamp hlc.Timestamp
			// Wait for the RHS to enter the subsumed state.
			select {
			case freezeStartTimestamp = <-mergeBlocker.WaitCh():
				log.Infof(ctx, "test: merge blocked. Freeze time: %s", freezeStartTimestamp)
			case err := <-mergeErrCh:
				t.Fatal(err)
			case <-time.After(45 * time.Second):
				t.Fatal("did not receive merge commit trigger as expected")
			}

			var rhsLeaseStart hlc.Timestamp
			if test.transferLease != nil {
				// Transfer the RHS lease while the RHS is subsumed.
				log.Infof(ctx, "test: transferring RHS lease...")
				rightLeaseholder, rhsLeaseStart = test.transferLease(ctx, t, tc, rightDesc, rightLeaseholder, manual)
				// Sanity check.
				require.True(t, freezeStartTimestamp.Less(rhsLeaseStart))
				log.Infof(ctx, "test: transferring RHS lease... done")
			}

			// Sleep a bit and assert that the closed timestamp has not advanced while
			// we were sleeping. We need to sleep sufficiently to give the side
			// transport a chance to publish updates.
			log.Infof(ctx, "test: sleeping...")
			time.Sleep(5 * closedts.SideTransportCloseInterval.Get(&tc.Server(0).ClusterSettings().SV))
			log.Infof(ctx, "test: sleeping... done")

			store, err := getTargetStore(tc, rightLeaseholder)
			require.NoError(t, err)
			r, err := store.GetReplica(rightDesc.RangeID)
			require.NoError(t, err)
			maxClosed := r.GetClosedTimestamp(ctx)
			// Note that maxClosed would not necessarily be below the freeze start if
			// this was a LEAD_FOR_GLOBAL_READS range.
			assert.True(t, maxClosed.LessEq(freezeStartTimestamp),
				"expected closed %s to be <= freeze %s", maxClosed, freezeStartTimestamp)

			// Sanity check that follower reads are not served by the RHS at
			// timestamps above the freeze (and also above the closed timestamp that
			// we verified above).
			scanTime := freezeStartTimestamp.Next()
			scanReq := makeTxnReadBatchForDesc(rightDesc, scanTime)
			follower := getFollowerReplicas(ctx, t, tc, rightDesc, roachpb.ReplicationTarget{
				NodeID:  r.NodeID(),
				StoreID: r.StoreID(),
			})[0]
			_, pErr := follower.Send(ctx, scanReq)
			require.NotNil(t, pErr)
			require.Regexp(t, "NotLeaseHolderError", pErr.String())

			log.Infof(ctx, "test: unblocking merge")
			mergeBlocker.Unblock()
			require.NoError(t, g.Wait())

			// Sanity check for the case where we've performed a lease transfer: make
			// sure that we can write at a timestamp *lower* than that lease's start
			// time. This shows that the test is not fooling itself and orchestrates
			// the merge scenario that it wants; in this scenario the lease start time
			// doesn't matter since the RHS is merged into its neighbor, which has a
			// lower lease start time. If the closed timestamp would advance past the
			// subsumption time (which we've asserted above that it doesn't), this
			// write would be a violation of that closed timestamp.
			if !rhsLeaseStart.IsEmpty() {
				mergedLeaseholder, err := leftLeaseholderStore.GetReplica(leftDesc.RangeID)
				require.NoError(t, err)
				writeTime := rhsLeaseStart.Prev()
				require.True(t, mergedLeaseholder.GetClosedTimestamp(ctx).Less(writeTime))
				var baWrite roachpb.BatchRequest
				baWrite.Header.RangeID = leftDesc.RangeID
				baWrite.Header.Timestamp = writeTime
				put := &roachpb.PutRequest{}
				put.Key = rightDesc.StartKey.AsRawKey()
				baWrite.Add(put)
				resp, pErr := mergedLeaseholder.Send(ctx, baWrite)
				require.Nil(t, pErr)
				require.Equal(t, writeTime, resp.Timestamp,
					"response time %s different from request time %s", resp.Timestamp, writeTime)
			}
		})
	}
}

// mergeFilter provides a method (SuspendMergeTrigger) that can be installed as
// a TestingRequestFilter, blocking commits with the MergeTrigger set.
type mergeFilter struct {
	mu struct {
		syncutil.Mutex
		// blocker is set when the next merge commit is to be trapped.
		blocker *mergeBlocker
	}
}

// mergeBlocker represents the blocker that the mergeFilter installs. The
// blocker encapsulates the communication of a blocked merge to tests, and the
// unblocking of that merge by the test.
type mergeBlocker struct {
	mu struct {
		syncutil.Mutex
		// mergeCh is the channel on which the merge is signaled. If nil, means that
		// the reader is not interested in receiving the notification any more.
		mergeCh   chan hlc.Timestamp
		unblockCh chan struct{}
	}
}

func newMergeBlocker() *mergeBlocker {
	m := &mergeBlocker{}
	m.mu.unblockCh = make(chan struct{})
	return m
}

// WaitCh returns the channel on which the blocked merge will be signaled. The
// channel will carry the freeze start timestamp for the RHS.
func (mb *mergeBlocker) WaitCh() <-chan hlc.Timestamp {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.mu.mergeCh
}

// Unblock unblocks the blocked merge, if any. It's legal to call this even if
// no merge is currently blocked, in which case the next merge trigger will no
// longer block. Unblock can be called multiple times; the first call returns
// true, subsequent ones return false and are no-ops.
//
// Calls to Unblock() need to be synchronized with reading from the channel
// returned by WaitCh().
func (mb *mergeBlocker) Unblock() bool {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.mu.unblockCh == nil {
		// Unblock was already called.
		return false
	}

	close(mb.mu.unblockCh)
	mb.mu.mergeCh = nil
	mb.mu.unblockCh = nil
	return true
}

// signal sends a freezeTs to someone waiting for a blocked merge. Returns the
// channel to wait on for the merge to be unblocked.
func (mb *mergeBlocker) signal(freezeTs hlc.Timestamp) chan struct{} {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	ch := mb.mu.mergeCh
	if ch == nil {
		// Nobody's waiting on this merge any more.
		return nil
	}
	ch <- freezeTs
	return mb.mu.unblockCh
}

// BlockNextMerge arms the merge filter state, installing a blocker for the next
// merge commit trigger it sees. The blocker is returned, to be be used for waiting
// on the upcoming merge.
//
// After calling BlockNextMerge(), the next merge will be merge blocked and, at
// that point, the filter will be automatically disarmed again. Once the next
// merge has been trapped, BlockNextMerge() can be called again.
func (filter *mergeFilter) BlockNextMerge() *mergeBlocker {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	if filter.mu.blocker != nil {
		panic("next merge already blocked")
	}
	filter.mu.blocker = newMergeBlocker()
	// This channel is buffered because we don't force the caller to read from it;
	// the caller can call mergeBlocker.Unblock() instead.
	filter.mu.blocker.mu.mergeCh = make(chan hlc.Timestamp, 1)
	return filter.mu.blocker
}

// resetBlocker disarms the filter. If the filter had been armed, it returns the
// blocker that had been installed by BlockNextMerge(), if any. If a blocker had
// been installed, it is returned and the bool retval is true.
func (filter *mergeFilter) resetBlocker() (*mergeBlocker, bool) {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	blocker := filter.mu.blocker
	filter.mu.blocker = nil
	return blocker, blocker != nil
}

// SuspendMergeTrigger is a request filter that can block merge transactions.
// This is intended to get the RHS range suspended in its subsumed state.
// Communication with actors interested in blocked merges is done through
// BlockNextMerge().
func (filter *mergeFilter) SuspendMergeTrigger(
	ctx context.Context, ba roachpb.BatchRequest,
) *roachpb.Error {
	for _, req := range ba.Requests {
		if et := req.GetEndTxn(); et != nil && et.Commit &&
			et.InternalCommitTrigger.GetMergeTrigger() != nil {

			// Disarm the mergeFilterState because we do _not_ want to block any other
			// merges in the system, or the future retries of this merge txn.
			blocker, ok := filter.resetBlocker()
			if !ok {
				continue
			}

			freezeStart := et.InternalCommitTrigger.MergeTrigger.FreezeStart
			log.Infof(ctx, "suspending the merge txn with FreezeStart: %s", freezeStart)

			// We block the LHS leaseholder from applying the merge trigger. Note
			// that RHS followers will have already caught up to the leaseholder
			// well before this point.
			unblockCh := blocker.signal(freezeStart.ToTimestamp())
			// Wait for the merge to be unblocked.
			if unblockCh != nil {
				<-unblockCh
			}
		}
	}
	return nil
}

func mergeWithRightNeighbor(
	ctx context.Context, store *kvserver.Store, leftDesc roachpb.RangeDescriptor,
) error {
	mergeArgs := adminMergeArgs(leftDesc.StartKey.AsRawKey())
	_, err := kv.SendWrapped(ctx, store.TestSender(), mergeArgs)
	return err.GoError()
}

func getEncodedKeyForTable(
	t *testing.T, db *gosql.DB, dbName, tableName string, val tree.Datum,
) roachpb.Key {
	tableID, err := getTableID(db, dbName, tableName)
	if err != nil {
		t.Fatalf("failed to lookup ids: %+v", err)
	}
	idxPrefix := keys.SystemSQLCodec.IndexPrefix(uint32(tableID), 1)
	k, err := keyside.Encode(idxPrefix, val, encoding.Ascending)
	if err != nil {
		t.Fatalf("failed to encode split key: %+v", err)
	}
	return k
}

// splitDummyRangeInTestCluster is supposed to be used in conjunction with the
// dummy table created in setupTestClusterWithDummyRange. It adds two rows to
// the given table and performs splits on the table's range such that the 2
// resulting ranges contain exactly one of the rows each.
func splitDummyRangeInTestCluster(
	t *testing.T,
	tc serverutils.TestClusterInterface,
	dbName, tableName string,
	splitExpirationTime hlc.Timestamp,
) (roachpb.RangeDescriptor, roachpb.RangeDescriptor) {
	db0 := tc.ServerConn(0)
	if _, err := db0.Exec(fmt.Sprintf(`INSERT INTO %s.%s VALUES(1, '%s')`,
		dbName, tableName, "foo")); err != nil {
		t.Fatal(err)
	}
	if _, err := db0.Exec(fmt.Sprintf(`INSERT INTO %s.%s VALUES(3, '%s')`,
		dbName, tableName, "foo")); err != nil {
		t.Fatal(err)
	}
	// Manually split the table to have easier access to descriptors.
	k := getEncodedKeyForTable(t, db0, dbName, tableName, tree.NewDInt(1))
	tcImpl := tc.(*testcluster.TestCluster)
	// Split at `1` and `2` so that the table has exactly two ranges: [1,2) and
	// [2, Max). This first split will never be merged by the merge queue so the
	// expiration time doesn't matter here.
	tcImpl.SplitRangeOrFatal(t, k)

	k = getEncodedKeyForTable(t, db0, dbName, tableName, tree.NewDInt(2))
	leftDesc, rightDesc, err := tcImpl.SplitRangeWithExpiration(k, splitExpirationTime)
	require.NoError(t, err)

	if tc.ReplicationMode() != base.ReplicationManual {
		if err := tcImpl.WaitForFullReplication(); err != nil {
			t.Fatal(err)
		}
	}
	return leftDesc, rightDesc
}

func getFollowerReplicas(
	ctx context.Context,
	t *testing.T,
	tc serverutils.TestClusterInterface,
	rangeDesc roachpb.RangeDescriptor,
	leaseholder roachpb.ReplicationTarget,
) []*kvserver.Replica {
	repls := replsForRange(ctx, t, tc, rangeDesc, numNodes)
	followers := make([]*kvserver.Replica, 0, len(repls)-1)
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
) *kvserver.Store {
	s, err := getTargetStore(tc, target)
	require.NoError(t, err)
	return s
}

func getTargetStore(
	tc serverutils.TestClusterInterface, target roachpb.ReplicationTarget,
) (_ *kvserver.Store, err error) {
	for i := 0; i < tc.NumServers(); i++ {
		if server := tc.Server(i); server.NodeID() == target.NodeID {
			return server.GetStores().(*kvserver.Stores).GetStore(target.StoreID)
		}
	}
	return nil, errors.Errorf("could not find node for replication target %+v\n", target)
}

func verifyNotLeaseHolderErrors(
	t *testing.T, ba roachpb.BatchRequest, repls []*kvserver.Replica, expectedNLEs int,
) {
	t.Helper()
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
				return pErr.GoError()
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

const testingSideTransportInterval = 100 * time.Millisecond
const numNodes = 3

func replsForRange(
	ctx context.Context,
	t *testing.T,
	tc serverutils.TestClusterInterface,
	desc roachpb.RangeDescriptor,
	numNodes int,
) (repls []*kvserver.Replica) {
	testutils.SucceedsSoon(t, func() error {
		repls = nil
		for i := 0; i < numNodes; i++ {
			repl, _, err := tc.Server(i).GetStores().(*kvserver.Stores).GetReplicaForRangeID(ctx, desc.RangeID)
			if err != nil {
				return err
			}
			if repl != nil {
				if !repl.IsInitialized() {
					return errors.Errorf("%s not initialized", repl)
				}
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

// setupClusterForClosedTSTesting creates a test cluster that is prepared to
// exercise follower reads. The returned test cluster has follower reads enabled
// using the given targetDuration and testingCloseFraction. In addition to the
// newly minted test cluster, this function returns a db handle to node 0, a
// range descriptor for the range used by the table `{dbName}.{tableName}`. It
// is the caller's responsibility to Stop the Stopper on the returned test
// cluster when done.
func setupClusterForClosedTSTesting(
	ctx context.Context,
	t *testing.T,
	targetDuration time.Duration,
	clusterArgs base.TestClusterArgs,
	dbName, tableName string,
) (tc serverutils.TestClusterInterface, db0 *gosql.DB, kvTableDesc roachpb.RangeDescriptor) {
	tc, desc := setupTestClusterWithDummyRange(t, clusterArgs, dbName, tableName, numNodes)
	_, err := tc.ServerConn(0).Exec(fmt.Sprintf(`
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '%s';
SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '%s';
SET CLUSTER SETTING kv.closed_timestamp.follower_reads_enabled = true;
`, targetDuration, targetDuration/4))
	require.NoError(t, err)

	return tc, tc.ServerConn(0), desc
}

// setupTestClusterWithDummyRange creates a TestCluster with an empty table. It
// returns a handle to the range descriptor corresponding to this table.
func setupTestClusterWithDummyRange(
	t *testing.T, clusterArgs base.TestClusterArgs, dbName, tableName string, numNodes int,
) (serverutils.TestClusterInterface, roachpb.RangeDescriptor) {
	tc := serverutils.StartNewTestCluster(t, numNodes, clusterArgs)
	db0 := tc.ServerConn(0)

	if _, err := db0.Exec(fmt.Sprintf(`
-- Set a timeout to get nicer test failures from these statements. Because of
-- the aggressiveResolvedTimestampPushKnobs() these statements can restart
-- forever under high load (testrace under high concurrency).
SET statement_timeout='30s';
CREATE DATABASE %[1]s;
CREATE TABLE %[1]s.%[2]s (id INT PRIMARY KEY CHECK (id >= 0), value STRING);
-- Reset the timeout set above.
RESET statement_timeout;
`, dbName, tableName)); err != nil {
		t.Fatal(err)
	}

	var numReplicas int
	var err error
	var desc roachpb.RangeDescriptor
	// If replicate queue is not disabled, wait until the table's range is fully
	// replicated.
	if clusterArgs.ReplicationMode != base.ReplicationManual {
		testutils.SucceedsSoon(t, func() error {
			if err := db0.QueryRow(
				fmt.Sprintf(
					`SELECT array_length(replicas, 1) FROM crdb_internal.ranges
WHERE table_name = '%s' AND database_name = '%s'`, tableName, dbName),
			).Scan(&numReplicas); err != nil {
				return err
			}
			if numReplicas != numNodes {
				return errors.New("not fully replicated yet")
			}
			require.Nil(t, err)
			return nil
		})
	}
	startKey := getEncodedKeyForTable(t, tc.ServerConn(0), dbName, tableName, tree.NewDInt(0))
	_, desc, err = tc.Server(0).SplitRange(startKey)
	require.NoError(t, err)
	return tc, desc
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
	_, isRangeKeyMismatch := pErr.GetDetail().(*roachpb.RangeKeyMismatchError)
	return isRangeKeyMismatch
})

var retryOnRangeNotFound = retryOnError(func(pErr *roachpb.Error) bool {
	_, isRangeNotFound := pErr.GetDetail().(*roachpb.RangeNotFoundError)
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

func makeTxnReadBatchForDesc(desc roachpb.RangeDescriptor, ts hlc.Timestamp) roachpb.BatchRequest {
	txn := roachpb.MakeTransaction("txn", nil, 0, ts, 0, 0)

	var baRead roachpb.BatchRequest
	baRead.Header.RangeID = desc.RangeID
	baRead.Header.Timestamp = ts
	baRead.Header.Txn = &txn
	r := &roachpb.ScanRequest{}
	r.Key = desc.StartKey.AsRawKey()
	r.EndKey = desc.EndKey.AsRawKey()
	baRead.Add(r)
	return baRead
}
