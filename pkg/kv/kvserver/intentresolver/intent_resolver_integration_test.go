// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package intentresolver_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func intentCountForTable(ctx context.Context, t *testing.T, db *gosql.DB, table string) int {
	q := fmt.Sprintf(`
		select sum((crdb_internal.range_stats(raw_start_key)->>'intent_count')::int)
		from [show ranges from table %s with keys]`,
		table)
	var count int
	err := db.QueryRowContext(ctx, q).Scan(&count)
	require.NoError(t, err)
	return count
}

func lockCountForTable(ctx context.Context, t *testing.T, db *gosql.DB, table string) int {
	q := fmt.Sprintf(
		`select lock_key_pretty
		from crdb_internal.cluster_locks
		where table_name = '%s'`,
		table)
	rows, err := db.QueryContext(ctx, q)
	require.NoError(t, err)
	defer rows.Close()
	var count int
	for rows.Next() {
		var lockKeyPretty string
		require.NoError(t, rows.Scan(&lockKeyPretty))
		t.Logf("lock at key: %s", lockKeyPretty)
		count++
	}
	return count
}

// TestAsyncIntentResolution runs a transaction that adds an unreplicated lock
// on each range and then writes an intent on each range. Intent resolution for
// the intents/locks on the first range will be synchronous and on the second
// range will be asynchronous.
func TestAsyncIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testAsyncIntentResolution(t, func(db *gosql.DB) {
		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec("SELECT * FROM t WHERE i IN (1, 3) FOR UPDATE")
		require.NoError(t, err)
		_, err = tx.Exec("INSERT INTO t (i) VALUES (2), (4)")
		require.NoError(t, err)
		err = tx.Commit()
		require.NoError(t, err)
	}, false /* exp1PC */)
}

// TestAsyncIntentResolution_1PC runs a transaction that writes to a single
// range and hits the 1-phase commit fast-path.
func TestAsyncIntentResolution_1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testAsyncIntentResolution(t, func(db *gosql.DB) {
		_, err := db.Exec("INSERT INTO t (i) VALUES (2)")
		require.NoError(t, err)
	}, true /* exp1PC */)
}

// TestAsyncIntentResolution_1PCUnreplicatedLocks runs a transaction that adds
// an unreplicated lock on each range but does not perform any writes. The
// transaction will hit the 1-phase commit fast-path. Resolution for the locks
// on the first range will be synchronous and on the second range will be
// asynchronous.
func TestAsyncIntentResolution_1PCUnreplicatedLocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testAsyncIntentResolution(t, func(db *gosql.DB) {
		_, err := db.Exec("SELECT * FROM t WHERE i IN (1, 3) FOR UPDATE")
		require.NoError(t, err)
	}, true /* exp1PC */)
}

// testAsyncIntentResolution runs a test function against a table 't' that is
// split into two ranges. It then asserts that all intents and locks across the
// table's ranges are resolved. If the exp1PC flag is true, the function also
// asserts that the transaction executed by fn was performed using the one-phase
// commit fast-path.
func testAsyncIntentResolution(t *testing.T, fn func(*gosql.DB), exp1PC bool) {
	// Start test cluster.
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	metrics := tc.Server(0).DB().GetFactory().(*kvcoord.TxnCoordSenderFactory).Metrics()

	// Create table t and split into two ranges, with a boundary at key 3. Add a
	// key on key 1 and key 3 that can be locked using SELECT FOR UPDATE.
	_, err := db.Exec("CREATE TABLE t (i INT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = db.Exec("ALTER TABLE t SPLIT AT VALUES (3)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t (i) VALUES (1), (3)")
	require.NoError(t, err)

	// Run the test's user txn function.
	onePCBefore := metrics.Commits1PC.Count()
	fn(db)
	onePCAfter := metrics.Commits1PC.Count()

	// Check that all intents and locks have been resolved to ensure async
	// intent resolution completed.
	testutils.SucceedsSoon(t, func() error {
		if c := intentCountForTable(ctx, t, db, "t"); c != 0 {
			return errors.Errorf("%d intents still unresolved", c)
		}
		if c := lockCountForTable(ctx, t, db, "t"); c != 0 {
			return errors.Errorf("%d locks still unresolved", c)
		}
		return nil
	})

	// If the test expects the transaction to hit the one-phase commit fast-path,
	// verify that it did.
	if exp1PC {
		require.Less(t, onePCBefore, onePCAfter)
	}
}

// TestAsyncIntentResolution_ByteSizePagination tests that async intent
// resolution through the IntentResolver has byte size pagination. This is done
// by creating a transaction that first writes to a range (transaction record)
// and then in another range: writes such that the total bytes of the write
// values exceeds the max raft command size and updating the transaction
// timestamp to ensure the key values are written to the raft command during
// intent resolution. The latter intents will be resolved asynchronously in the
// IntentResolver, but the write batch size from intent resolution will exceed
// the max raft command size resulting in an error and not all intents will be
// resolved, unless byte size pagination is implemented.
func TestAsyncIntentResolution_ByteSizePagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Start test cluster.
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	const numIntents = 7

	// Create table t and split into two ranges, the first range consists of
	// primary key < numIntents and second range consists of primary key >=
	// numIntents.
	{
		_, err := db.Exec("CREATE TABLE t (i INT PRIMARY KEY, j STRING)")
		require.NoError(t, err)
		_, err = db.Exec(fmt.Sprintf("ALTER TABLE t SPLIT AT VALUES (%d)", numIntents))
		require.NoError(t, err)
	}

	// Set the max raft command size to 5MB.
	st := tc.Servers[0].ClusterSettings()
	st.Manual.Store(true)
	kvserverbase.MaxCommandSize.Override(ctx, &st.SV, 5<<20)

	{
		// Create the first transaction.
		tx, err := db.Begin()
		require.NoError(t, err)

		// Create transaction record on the second range of t to ensure intents
		// for t on the first range are resolved asynchronously.
		_, err = tx.Exec(fmt.Sprintf("INSERT INTO t (i, j) VALUES (%d, '0')", numIntents))
		require.NoError(t, err)

		// Insert kv pairs whose values exceed max raft command size = 5MB in
		// total. This will be inserted into t on the first range.
		for i := 0; i < numIntents-1; i++ {
			_, err = tx.Exec(fmt.Sprintf("INSERT INTO t(i, j) VALUES (%d, '%01000000d')", i, i))
			require.NoError(t, err)
		}

		// Create a later transaction that writes to key numIntents-1. This
		// will be inserted into t on the first range.
		{
			tx2, err := db.Begin()
			require.NoError(t, err)
			_, err = tx2.Exec(fmt.Sprintf("INSERT INTO t (i, j) VALUES (%d, '0')", numIntents-1))
			require.NoError(t, err)
			err = tx2.Commit()
			require.NoError(t, err)
		}

		// Have the first transaction write to key numIntents-1, which will
		// force the transaction to update its timestamp.
		_, err = tx.Exec(fmt.Sprintf("UPDATE t SET j = '1' WHERE i = %d", numIntents-1))
		require.NoError(t, err)

		// Commit, which will asynchronously resolve the intents for t, and the
		// write batch size from intent resolution will exceed the max raft
		// command size resulting in an error and not all intents will be
		// resolved, unless byte size pagination is implemented. Below, we
		// check that all intents have been resolved.
		err = tx.Commit()
		require.NoError(t, err)
	}

	// Check that all intents have been resolved to ensure async intent
	// resolution did not exceed the max raft command size, which can only
	// happen if byte size pagination was implemented.
	testutils.SucceedsSoon(t, func() error {
		if c := intentCountForTable(ctx, t, db, "t"); c != 0 {
			return errors.Errorf("%d intents still unresolved", c)
		}
		return nil
	})
}

// TestSyncIntentResolution_ByteSizePagination tests that EndTxn has byte size
// pagination. This is done by creating a transaction where the total bytes of
// the write values exceeds the max raft command size and updating the
// transaction timestamp to ensure the key values are written to the raft
// command during intent resolution. EndTxn will synchronously resolve the
// intents and the write batch size from intent resolution will exceed the max
// raft command size resulting in an error and no intents will be resolved,
// unless byte size pagination is implemented.
func TestSyncIntentResolution_ByteSizePagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Start test cluster.
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
						DisableAsyncIntentResolution: true,
					},
				},
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	const numIntents = 7

	{
		_, err := db.Exec("CREATE TABLE t (i INT PRIMARY KEY, j STRING)")
		require.NoError(t, err)
	}

	// Set the max raft command size to 5MB.
	st := tc.Servers[0].ClusterSettings()
	st.Manual.Store(true)
	kvserverbase.MaxCommandSize.Override(ctx, &st.SV, 5<<20)

	{
		// Insert kv pairs whose values exceed max raft command size = 5MB in
		// total.
		tx, err := db.Begin()
		require.NoError(t, err)
		for i := 0; i < numIntents-1; i++ {
			_, err = tx.Exec(fmt.Sprintf("INSERT INTO t (i, j) VALUES (%d, '%01000000d')", i, i))
			require.NoError(t, err)
		}

		// Create a later transaction that writes to key numIntents-1.
		{
			tx2, err := db.Begin()
			require.NoError(t, err)
			_, err = tx2.Exec(fmt.Sprintf("INSERT INTO t (i, j) VALUES (%d, '0')", numIntents-1))
			require.NoError(t, err)
			err = tx2.Commit()
			require.NoError(t, err)
		}

		// Have the first transaction write to key numIntents-1, which will force
		// the transaction to update its timestamp.
		_, err = tx.Exec(fmt.Sprintf("UPDATE t SET j = '1' WHERE i = %d", numIntents-1))
		require.NoError(t, err)

		// Commit, which will call EndTxn and synchronously resolve the intents,
		// and the write batch size from intent resolution will exceed the max raft
		// command size resulting in an error and no intents will be resolved,
		// unless byte size pagination is implemented. Below, we check that at
		// least 1 intent has been resolved.
		err = tx.Commit()
		require.NoError(t, err)
	}

	// Check that at least 1 intent has been resolved to ensure synchronous
	// intent resolution did not exceed the max raft command size, which can only
	// happen if byte size pagination was implemented.
	testutils.SucceedsSoon(t, func() error {
		if c := intentCountForTable(ctx, t, db, "t"); c == numIntents {
			return errors.Errorf("expected fewer than %d unresolved intents, got %d", numIntents, c)
		}
		return nil
	})
}

func forceScanOnAllReplicationQueues(tc *testcluster.TestCluster) (err error) {
	for _, s := range tc.Servers {
		err = s.GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
			return store.ForceReplicationScanAndProcess()
		})
	}
	return err
}

// TestIntentResolutionUnavailableRange tests that InFlightBackpressureLimit
// resolve intent batches for an unavailable range does not stall indefinitely
// and times out, allowing other resolve intent requests and queries for
// available ranges to be unblocked, run, and finish. This test does this by:
// 1. Setting InFlightBackpressureLimit to 1
// 2. Configure so that intent resolution on t2 cannot finish successfully and
// blocks, relying on the timeout to unblock (to simulate t2 being
// unavailable).
// 3. Have a transaction update t1 (transaction record) and t2.
// The intent resolver / request batcher on the store containing t1 will run
// async intent resolution for t2, which will block and clog up intent
// resolution on the store containing t1.
//
// 4. Have a transaction update t3 (transaction record) and t1, but disable
// async intent resolution on t3
// 5. Read from the updated row on t1 and test this step finishes
// Step 4 will create an intent for t1 that will not be resolved since async
// intent resolution is disabled. Step 5 cannot finish as we need to resolve
// the intent for t1 and intent resolution is clogged up on the store
// containing t1, unless the intent resolution for the "unavailable" t2 times
// out.
//
// TODO(sumeer): this test clogs up batched intent resolution via an inflight
// backpressure limit, which by default in no longer limited. But an inflight
// backpressure limit does exist for GC of txn records. This test should
// continue to exist until we have production experience with no inflight
// backpressure for intent resolution. And after that we should create an
// equivalent test for inflight backpressure for GC of txn records and remove
// this test.
func TestIntentResolutionUnavailableRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test sometimes times out trying to replicate 3 tables t1, t2, t3 with
	// replication factor 1 located on different servers under stress.
	skip.UnderStress(t)

	ctx := context.Background()

	// Set server args. We set InFlightBackpressureLimit to 1 so that we only
	// need one intent resolution batch to clog up the intent resolver. For store
	// containing t1 (node 0), we set TestingAsyncIntentResolution to block async
	// intent resolution until range containing t2 becomes unavailable. For store
	// containing t3 (node 2), we set DisableAsyncIntentResolution to true so
	// that async intent resolution is disabled for store containing t3.
	const numNodes = 3
	const inFlightBackpressureLimit = 1
	const intentResolutionSendBatchTimeout = 1 * time.Second
	serverArgs := make(map[int]base.TestServerArgs)
	waitForIntentResolutionForT2 := make(chan struct{})
	var t2RangeID atomic.Int32
	storeTestingKnobs := []kvserver.StoreTestingKnobs{
		{
			IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
				InFlightBackpressureLimit:           inFlightBackpressureLimit,
				MaxIntentResolutionSendBatchTimeout: intentResolutionSendBatchTimeout,
			},
		},
		{
			TestingRequestFilter: func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
				// Configure so that intent resolution on t2 cannot finish
				// successfully and blocks, relying on the timeout to unblock.
				for _, req := range ba.Requests {
					switch req.GetInner().(type) {
					case *kvpb.ResolveIntentRequest:
						if ba.RangeID == roachpb.RangeID(t2RangeID.Load()) {
							close(waitForIntentResolutionForT2)
							// Block until the request is cancelled.
							<-ctx.Done()
							return kvpb.NewError(ctx.Err())
						}
					}
				}
				return nil
			},
		},
		{
			IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
				DisableAsyncIntentResolution: true,
			},
		},
	}
	for i := 1; i <= numNodes; i++ {
		serverArgs[i-1] = base.TestServerArgs{
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key: "rack", Value: strconv.Itoa(i),
					},
				},
			},
			Knobs: base.TestingKnobs{
				Store: &storeTestingKnobs[i-1],
			},
		}
	}

	// Start test cluster.
	clusterArgs := base.TestClusterArgs{
		ReplicationMode:   base.ReplicationAuto,
		ServerArgsPerNode: serverArgs,
	}
	tc := testcluster.StartTestCluster(t, numNodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)

	// Create 3 tables t1, t2, t3 with replication factor 1 located on different
	// servers and wait for replication and rebalancing to finish.
	const numTables = 3
	for i := 1; i <= numTables; i++ {
		_, err := db.Exec(fmt.Sprintf("CREATE TABLE t%d (i INT PRIMARY KEY, j INT)", i))
		require.NoError(t, err)
		_, err = db.Exec(fmt.Sprintf("ALTER TABLE t%d CONFIGURE ZONE USING num_replicas = 1, constraints = '{\"+rack=%d\": 1}'", i, i))
		require.NoError(t, err)
	}
	testutils.SucceedsSoon(t, func() error {
		if err := forceScanOnAllReplicationQueues(tc); err != nil {
			return err
		}
		for i := 1; i <= numTables; i++ {
			r := db.QueryRow(fmt.Sprintf("select replicas from [show ranges from table t%d]", i))
			var repl string
			if err := r.Scan(&repl); err != nil {
				return err
			}
			if repl != fmt.Sprintf("{%d}", i) {
				return errors.Newf("Expected replicas {%d} for table t%d, got %s", i, i, repl)
			}
		}
		return nil
	})

	{
		// Get the range ID for t2.
		var tmpT2RangeID int32
		err := db.QueryRow("select range_id from [show ranges from table t2] limit 1").Scan(&tmpT2RangeID)
		require.NoError(t, err)
		t2RangeID.Store(tmpT2RangeID)
	}

	{
		// Execute a transaction for t1 (transaction record) and t2 so that the
		// intent resolver / request batcher of the store containing t1 will run
		// async intent resolution for t2. Intent resolution on t2 is configured to
		// not finish successfully and block, relying on the timeout to unblock
		// (see the TestingRequestFilter for server 1). Thus, async intent
		// resolution for t2 will be stuck and clog up intent resolution on the
		// store containing t1, only unclogging after timeout.
		tx, err := db.Begin()
		require.NoError(t, err)
		for _, i := range []int{1, 2} {
			_, err = tx.Exec(fmt.Sprintf("INSERT INTO t%d (i, j) VALUES (0, 0)", i))
			require.NoError(t, err)
		}
		err = tx.Commit()
		require.NoError(t, err)
	}

	// Wait for intent resolution for t2 to start to ensure that intent
	// resolution on the store containing t1 is clogged before executing the
	// below transactions.
	<-waitForIntentResolutionForT2

	{
		// One transaction updates t3 (transaction record) and t1, which would
		// create an intent on t1 that will not get resolved as intent resolution
		// is disabled.
		tx, err := db.Begin()
		require.NoError(t, err)
		for _, i := range []int{3, 1} {
			_, err = tx.Exec(fmt.Sprintf("INSERT INTO t%d (i, j) VALUES (1, 0)", i))
			require.NoError(t, err)
		}
		err = tx.Commit()
		require.NoError(t, err)
	}

	{
		// Read from t1 and try to resolve the intent, which will be initially
		// blocked since intent resolution on the store containing t1 is clogged
		// up. We test this read finishes, which can only happen if the intent
		// resolution on the unavailable t2 times out and finishes.
		_, err := db.Exec("SELECT * FROM t1")
		require.NoError(t, err)
	}
}
