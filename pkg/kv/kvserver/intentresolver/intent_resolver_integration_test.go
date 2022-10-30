// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package intentresolver_test

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func forceScanOnAllReplicationQueues(tc *testcluster.TestCluster) (err error) {
	for _, s := range tc.Servers {
		err = s.Stores().VisitStores(func(store *kvserver.Store) error {
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
// 2. Have a transaction update t1 (transaction record) and t2, but block async
// intent resolution until after step 3
// 3. Make range containing t2 unavailable
// This should clog up the intent resolver on the store containing t1.
//
// 4. Have a transaction update t3 (transaction record) and t1, but block async
// intent resolution until after step 5
// 5. Read from the updated row on t1 and test this step finishes
// Step 4 will create an intent for t1 that cannot be resolved since async
// intent resolution is blocked. Step 5 cannot finish as we need to resolve the
// intent for t1 and intent resolution is clogged up on the store containing
// t1, unless the previous intent resolution for the unavailable t2 times out.
func TestIntentResolutionUnavailableRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Set server args. We set InFlightBackpressureLimit to 1 so that we only
	// need one intent resolution batch to clog up the intent resolver.
	const numNodes = 3
	const inFlightBackpressureLimit = 1
	const intentResolutionSendBatchTimeout = 1 * time.Second
	serverArgs := make(map[int]base.TestServerArgs)
	blockAsyncIntentResolution := syncutil.Mutex{}
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
				Store: &kvserver.StoreTestingKnobs{
					IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
						InFlightBackpressureLimit:           inFlightBackpressureLimit,
						MaxIntentResolutionSendBatchTimeout: intentResolutionSendBatchTimeout,
						EnableBlockingAsyncIntentResolution: true,
						BlockAsyncIntentResolution:          &blockAsyncIntentResolution,
					},
				},
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

	// Block async intent resolution until after range for t2 becomes unavailable
	// to ensure that intent resolution of t2 occurs after it is unavailable in
	// order to clog up intent resolution.
	blockAsyncIntentResolution.Lock()

	{
		// Queue up async intent resolution (currently blocked) of t2 on the store
		// containing t1.
		tx, err := db.Begin()
		require.NoError(t, err)
		for _, i := range []int{1, 2} {
			_, err = tx.Exec(fmt.Sprintf("INSERT INTO t%d (i, j) VALUES (0, 0)", i))
			require.NoError(t, err)
		}
		err = tx.Commit()
		require.NoError(t, err)
	}

	// Make the range for t2 unavailable so intent resolution in the above
	// transaction cannot finish successfully, clogging up intent resolution on
	// the store containing t1. Unclogging intent resolution will rely on the
	// timeout. Sleep to allow async intent resolution for the above transaction
	// to start.
	tc.Servers[1].Stopper().Stop(ctx)
	blockAsyncIntentResolution.Unlock()
	time.Sleep(500 * time.Millisecond)

	{
		// Block async intent resolution to ensure intent for t1 is not resolved in
		// the below transaction.
		blockAsyncIntentResolution.Lock()

		// One transaction updates t3 (transaction record) and t1, which would
		// create an intent on t1 that will not get resolved as intent resolution
		// is blocked.
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
		// A later transaction reads from t1 and tries to resolve the intent, which
		// will be initially blocked since intent resolution on the store
		// containing t1 is clogged up. We test this read finishes, which can only
		// happen if the intent resolution on the unavailable t2 times out and
		// finishes.
		var done int32
		go func() {
			_, err := db.Exec("SELECT * FROM t1")
			require.NoError(t, err)
			atomic.StoreInt32(&done, 1)
		}()
		testutils.SucceedsSoon(t, func() error {
			if atomic.LoadInt32(&done) != 1 {
				return errors.New("SELECT * FROM t1 did not complete because intent resolution has been clogged up")
			}
			return nil
		})
	}
	blockAsyncIntentResolution.Unlock()
}
