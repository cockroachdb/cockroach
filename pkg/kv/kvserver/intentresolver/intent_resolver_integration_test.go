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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
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
	var t2RangeID roachpb.RangeID
	storeTestingKnobs := []kvserver.StoreTestingKnobs{
		{
			IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
				InFlightBackpressureLimit:           inFlightBackpressureLimit,
				MaxIntentResolutionSendBatchTimeout: intentResolutionSendBatchTimeout,
			},
		},
		{
			TestingRequestFilter: func(ctx context.Context, ba *roachpb.BatchRequest) *roachpb.Error {
				// Configure so that intent resolution on t2 cannot finish
				// successfully and blocks, relying on the timeout to unblock.
				for _, req := range ba.Requests {
					switch req.GetInner().(type) {
					case *roachpb.ResolveIntentRequest:
						if ba.RangeID == t2RangeID {
							close(waitForIntentResolutionForT2)
							// Block until the request is cancelled.
							<-ctx.Done()
							return roachpb.NewError(ctx.Err())
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
		err := db.QueryRow("select range_id from [show ranges from table t2] limit 1").Scan(&t2RangeID)
		require.NoError(t, err)
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
