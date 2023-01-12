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
	gosql "database/sql"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func getRangeInfoForTable(
	ctx context.Context, t *testing.T, db *gosql.DB, servers []*server.TestServer, tableName string,
) (startKey, endKey roachpb.Key, store *kvserver.Store) {
	var rangeID roachpb.RangeID
	err := db.QueryRow(fmt.Sprintf("select range_id from [show ranges from table %s] limit 1", tableName)).Scan(&rangeID)
	require.NoError(t, err)
	for _, server := range servers {
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
	return startKey, endKey, store
}

func forceScanOnAllReplicationQueues(tc *testcluster.TestCluster) (err error) {
	for _, s := range tc.Servers {
		err = s.Stores().VisitStores(func(store *kvserver.Store) error {
			return store.ForceReplicationScanAndProcess()
		})
	}
	return err
}

// TestAsyncIntentResolutionByteSizePagination tests that async intent
// resolution through the IntentResolver has byte size pagination. This is done
// by creating a transaction that first writes to a range (transaction record)
// and then in another range: writes such that the total bytes of the write
// values exceeds the max raft command size and updating the transaction
// timestamp to ensure the key values are written to the raft command during
// intent resolution. The latter intents will be resolved asynchronously in the
// IntentResolver, but the write batch size from intent resolution will exceed
// the max raft command size resulting in an error and not all intents will be
// resolved, unless byte size pagination is implemented.
func TestAsyncIntentResolutionByteSizePagination(t *testing.T) {
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
		_, err := db.Exec(fmt.Sprintf("CREATE TABLE t%d (i INT PRIMARY KEY, j STRING)", i))
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

	// Set the max raft command size to 5MB.
	for _, server := range tc.Servers {
		st := server.ClusterSettings()
		st.Manual.Store(true)
		kvserverbase.MaxCommandSize.Override(ctx, &st.SV, 5<<20)
	}

	const numIntents = 7
	{
		tx, err := db.Begin()
		require.NoError(t, err)

		// Create transaction record on a different range from t1 to ensure intents
		// for t1 are resolved asynchronously.
		_, err = tx.Exec("INSERT INTO t2 (i, j) VALUES (0, '0')")
		require.NoError(t, err)

		// Insert kv pairs whose values exceed max raft command size = 5MB in
		// total.
		for i := 0; i < numIntents-1; i++ {
			_, err = tx.Exec(fmt.Sprintf("INSERT INTO t1 (i, j) VALUES (%d, '%01000000d')", i, i))
			require.NoError(t, err)
		}

		// Create a later transaction that writes to key numIntents-1.
		{
			tx2, err := db.Begin()
			require.NoError(t, err)
			_, err = tx2.Exec(fmt.Sprintf("INSERT INTO t1 (i, j) VALUES (%d, '0')", numIntents-1))
			require.NoError(t, err)
			err = tx2.Commit()
			require.NoError(t, err)
		}

		// Have the first transaction write to key numIntents-1, which will force
		// the transaction to update its timestamp.
		_, err = tx.Exec(fmt.Sprintf("UPDATE t1 SET j = '1' WHERE i = %d", numIntents-1))
		require.NoError(t, err)

		// Commit, which will asynchronously resolve the intents for t1, and the
		// write batch size from intent resolution will exceed the max raft command
		// size resulting in an error and not all intents will be resolved, unless
		// byte size pagination is implemented. Below, we check that all intents
		// have been resolved.
		err = tx.Commit()
		require.NoError(t, err)
	}

	// Get the store, start key, and end key of the range containing table t1.
	startKey, endKey, store := getRangeInfoForTable(ctx, t, db, tc.Servers, "t1")

	// Check that all intents have been resolved to ensure async intent
	//resolution did not exceed the max raft command size, which can only happen
	// if byte size pagination was implemented.
	testutils.SucceedsSoon(t, func() error {
		result, err := storage.MVCCScanToBytes(ctx, store.Engine(), startKey, endKey,
			hlc.MaxTimestamp, storage.MVCCScanOptions{Inconsistent: true})
		if err != nil {
			return err
		}
		if intentCount := len(result.Intents); intentCount != 0 {
			return errors.Errorf("%d intents still unresolved", intentCount)
		}
		return nil
	})
}
