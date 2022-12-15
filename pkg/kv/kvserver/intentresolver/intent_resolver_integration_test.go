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

// TestEndTxnByteSizePagination tests that EndTxn has byte size pagination.
// This is done by creating a transaction where the total bytes of the write
// values exceeds the max raft command size and updating the transaction
// timestamp to ensure the key values are written to the raft command during
// intent resolution. EndTxn will synchronously resolve the intents and the
// write batch size from intent resolution will exceed the max raft command
// size resulting in an error and no intents will be resolved, unless byte size
// pagination is implemented.
func TestEndTxnByteSizePagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const numNodes = 1
	serverArgs := make(map[int]base.TestServerArgs)
	serverArgs[0] = base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
					DisableAsyncIntentResolution: true,
				},
			},
		},
	}

	// Start test cluster.
	clusterArgs := base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: serverArgs,
	}
	tc := testcluster.StartTestCluster(t, numNodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)

	{
		_, err := db.Exec("CREATE TABLE t1 (i INT PRIMARY KEY, j STRING)")
		require.NoError(t, err)
	}

	// Set the max raft command size to 5MB.
	for _, server := range tc.Servers {
		st := server.ClusterSettings()
		st.Manual.Store(true)
		kvserverbase.MaxCommandSize.Override(ctx, &st.SV, 5<<20)
	}

	const numIntents = 7
	{
		// Insert kv pairs whose values exceed max raft command size = 5MB in
		// total.
		tx, err := db.Begin()
		require.NoError(t, err)
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

		// Commit, which will call EndTxn and synchronously resolve the intents,
		// and the write batch size from intent resolution will exceed the max raft
		// command size resulting in an error and no intents will be resolved,
		// unless byte size pagination is implemented. Below, we check that at
		// least 1 intent has been resolved.
		err = tx.Commit()
		require.NoError(t, err)
	}

	// Get the store, start key, and end key of the range containing table t1.
	startKey, endKey, store := getRangeInfoForTable(ctx, t, db, tc.Servers, "t1")

	// Check that at least 1 intent has been resolved to ensure synchronous
	// intent resolution did not exceed the max raft command size, which can only
	// happen if byte size pagination was implemented.
	testutils.SucceedsSoon(t, func() error {
		result, err := storage.MVCCScanToBytes(ctx, store.Engine(), startKey, endKey,
			hlc.MaxTimestamp, storage.MVCCScanOptions{Inconsistent: true})
		if err != nil {
			return err
		}
		if intentCount := len(result.Intents); intentCount == numIntents {
			return errors.Errorf("Expected fewer than %d unresolved intents, got %d", numIntents, intentCount)
		}
		return nil
	})
}
