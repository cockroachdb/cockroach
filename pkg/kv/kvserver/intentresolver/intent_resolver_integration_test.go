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
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
// available ranges to be unblocked, run, and finish. This test does this by
// creating InFlightBackpressureLimit intent resolution batches containing
// range R, making range R unavailable and clogging up the intent resolver,
// having a transaction read a write intent to trigger intent resolution, and
// testing this read finishes (which can only happen if the previous intent
// resolution batches on the unavailable range R timed out).
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

	// Create 2 tables with replication factor 1 located on different servers and
	// wait for replication and rebalancing to finish.
	numTables := 2
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
		// Insert a row into two tables in a transaction.
		tx, err := db.Begin()
		require.NoError(t, err)
		for i := 1; i <= numTables; i++ {
			_, err = tx.Exec(fmt.Sprintf("INSERT INTO t%d (i, j) VALUES (0, 0)", i))
			require.NoError(t, err)
		}
		err = tx.Commit()
		require.NoError(t, err)
	}

	// Immediately make the range for t2 unavailable so intent resolution in the
	// above transaction cannot finish successfully and must rely on the timout
	// to finish. Sleep one second to ensure async intent cleanup as started.
	tc.Servers[1].Stopper().Stop(ctx)
	time.Sleep(400 * time.Millisecond)

	{
		// One transaction updates a row in the first table, but does not commit
		// immediately, ensuring a later read encountering this write intent
		// (uncommitted value) triggers intent resolution.
		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec("UPDATE t1 SET j = 1 WHERE i = 0")
		require.NoError(t, err)

		go func() {
			time.Sleep(400 * time.Millisecond)
			err = tx.Commit()
			require.NoError(t, err)
		}()
	}

	{
		// A later transaction reads from the above row and tries to resolve the
		// intent. We test this read finishes, which can only happen if the intent
		// resolution on the unavailable range timed out and finished.
		db2 := tc.ServerConn(2)
		var done int32
		go func() {
			_, _ = db2.Exec("SELECT * FROM t1")
			atomic.StoreInt32(&done, 1)
		}()
		testutils.SucceedsSoon(t, func() error {
			if atomic.LoadInt32(&done) != 1 {
				return errors.New("SELECT * FROM t1 did not complete because intent resolution has been clogged up")
			}
			return nil
		})
	}
}

// TestIntentResolutionByteSizePagination tests that intent resolution has byte
// size pagination. This is done by resolving intents where the total byte size
// of the intents in the batch exceeds the max raft command size actually
// cleans up the intents.
func TestIntentResolutionByteSizePagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const numNodes = 2
	serverArgs := make(map[int]base.TestServerArgs)
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
						TargetBytesPerBatchReq: 2900,
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

	// Create 2 tables t1 and t2 and wait for t1 to be replicated on store 1 and
	// t2 to be replicated on store 2.
	numTables := 2
	for i := 1; i <= numTables; i++ {
		_, err := db.Exec(fmt.Sprintf("CREATE TABLE t%d (i STRING PRIMARY KEY)", i))
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

	// Set the max raft command size to 4900.
	for _, server := range tc.Servers {
		st := server.ClusterSettings()
		st.Manual.Store(true)
		kvserverbase.MaxCommandSize.Override(ctx, &st.SV, 4900)
	}

	bytes := []byte{'a', 'b', 'c', 'd', 'e'}
	testKeys := make([][]byte, len(bytes))
	for i, b := range bytes {
		testKeys[i] = make([]byte, 1000)
		for j := range testKeys[i] {
			testKeys[i][j] = b
		}
	}

	// Insert keys and values and commit to generate 5 intents whose keys are
	// 1000 bytes, which should be batched together and a resolve intent request
	// will be invoked on this batch.
	for i, key := range testKeys {
		_, err := db.Exec("BEGIN")
		require.NoError(t, err)
		_, err = db.Exec(fmt.Sprintf("INSERT INTO t1 (i) VALUES ('%s')", []byte{bytes[i]}))
		require.NoError(t, err)

		_, err = db.Exec(fmt.Sprintf("INSERT INTO t2 (i) VALUES ('%s')", key))
		require.NoError(t, err)
		_, err = db.Exec("COMMIT")
		require.NoError(t, err)
	}

	// Get the store, start key, and end key of the range containing table t2
	// which contains the 5 intents whose keys are 1000 bytes.
	var rangeID roachpb.RangeID
	var startKey roachpb.Key
	var endKey roachpb.Key
	var store *kvserver.Store
	err := db.QueryRow("select range_id from [show ranges from table t2] limit 1").Scan(&rangeID)
	require.NoError(t, err)
	for _, server := range tc.Servers {
		require.NoError(t, server.Stores().VisitStores(func(s *kvserver.Store) error {
			if replica, err := s.GetReplica(rangeID); err == nil && replica.OwnsValidLease(ctx, replica.Clock().NowAsClockTimestamp()) {
				desc := replica.Desc()
				startKey = desc.StartKey.AsRawKey()
				endKey = desc.EndKey.AsRawKey()
				store = s
			}
			return nil
		}))
	}

	// Check that the 5 intents whose keys are 1000 bytes are eventually
	// resolved. Note that if byte-size pagination for intent resolution did not
	// exist, then this batch of intents would not be cleaned up since the batch
	// contains 5 intents each of size > 1000 bytes, meaning the write batch
	// resulting from intent resolution would be > 5000 bytes i.e. exceeding the
	// max raft command size i.e. resolving these intents would fail.
	testutils.SucceedsSoon(t, func() error {
		if err != nil {
			return err
		}
		result, err := storage.MVCCScanToBytes(ctx, store.Engine(), startKey, endKey,
			hlc.MaxTimestamp, storage.MVCCScanOptions{Inconsistent: true})
		if err != nil {
			return err
		}
		if intentCount := len(result.Intents); intentCount != 0 {
			return errors.Errorf("%d intents not cleaned up due to intent resolution batch exceeding max raft command size", intentCount)
		}
		return nil
	})
}
