// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

// TestEnsureLocalReadsOnGlobalTables ensures that all present time reads on
// GLOBAL tables don't incur a network hop.
func TestEnsureLocalReadsOnGlobalTables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// ensureOnlyLocalReads looks at a trace to ensure that reads were served
	// locally. It returns true if the read was served as a follower read.
	ensureOnlyLocalReads := func(t *testing.T, rec tracing.Recording) (servedUsingFollowerReads bool) {
		for _, sp := range rec {
			if sp.Operation == "dist sender send" {
				require.True(t, tracing.LogsContainMsg(sp, kvbase.RoutingRequestLocallyMsg),
					"query was not served locally: %s", rec)

				// Check the child span to find out if the query was served using a
				// follower read.
				for _, span := range rec {
					if span.ParentSpanID == sp.SpanID {
						if tracing.LogsContainMsg(span, kvbase.FollowerReadServingMsg) {
							servedUsingFollowerReads = true
						}
					}
				}
			}
		}
		return servedUsingFollowerReads
	}

	presentTimeRead := `SELECT * FROM t.test_table WHERE k=2`
	recCh := make(chan tracing.Recording, 1)

	knobs := base.TestingKnobs{
		SQLExecutor: &sql.ExecutorTestingKnobs{
			WithStatementTrace: func(trace tracing.Recording, stmt string) {
				if stmt == presentTimeRead {
					recCh <- trace
				}
			},
		},
	}

	numServers := 3
	tc, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, numServers, knobs, multiregionccltestutils.WithReplicationMode(base.ReplicationManual),
	)
	defer cleanup()

	_, err := sqlDB.Exec(`CREATE DATABASE t PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3"`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`CREATE TABLE t.test_table (k INT PRIMARY KEY) LOCALITY GLOBAL`)
	require.NoError(t, err)

	// Set up some write traffic in the background.
	errCh := make(chan error)
	stopWritesCh := make(chan struct{})
	go func() {
		i := 0
		for {
			select {
			case <-stopWritesCh:
				errCh <- nil
				return
			case <-time.After(10 * time.Millisecond):
				_, err := sqlDB.Exec(`INSERT INTO t.test_table VALUES($1)`, i)
				i++
				if err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	var tableID uint32
	err = sqlDB.QueryRow(`SELECT id from system.namespace WHERE name='test_table'`).Scan(&tableID)
	require.NoError(t, err)
	tablePrefix := keys.MustAddr(keys.SystemSQLCodec.TablePrefix(tableID))
	// Split the range at the start of the table and add a voter to all nodes in
	// the cluster.
	tc.SplitRangeOrFatal(t, tablePrefix.AsRawKey())
	tc.AddVotersOrFatal(t, tablePrefix.AsRawKey(), tc.Target(1), tc.Target(2))

	for i := 0; i < numServers; i++ {
		// Run a query to populate its cache.
		conn := tc.ServerConn(i)
		_, err = conn.Exec("SELECT * from t.test_table WHERE k=1")
		require.NoError(t, err)

		// Check that the cache was indeed populated.
		cache := tc.Server(i).DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache()
		entry := cache.GetCached(context.Background(), tablePrefix, false /* inverted */)
		require.NotNil(t, entry.Lease().Empty())
		require.NotNil(t, entry)
		require.Equal(t, roachpb.LEAD_FOR_GLOBAL_READS, entry.ClosedTimestampPolicy())
		isLeaseHolder := entry.Lease().Replica.NodeID == tc.Server(i).NodeID()

		// Run the query to ensure local read.
		_, err = conn.Exec(presentTimeRead)
		require.NoError(t, err)

		rec := <-recCh
		followerRead := ensureOnlyLocalReads(t, rec)

		// Expect every non-leaseholder to serve a (local) follower read. The
		// leaseholder on the other hand won't serve a follower read.
		require.Equal(t, !isLeaseHolder, followerRead)
	}

	close(stopWritesCh)
	writeErr := <-errCh
	require.NoError(t, writeErr)
}
