// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestEnsureLocalReadsOnGlobalTables ensures that all present time reads on
// GLOBAL tables don't incur a network hop.
func TestEnsureLocalReadsOnGlobalTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "https://github.com/cockroachdb/cockroach/issues/102798#issuecomment-1543852311")

	// ensureOnlyLocalReads looks at a trace to ensure that reads were served
	// locally. It returns true if the read was served as a follower read.
	ensureOnlyLocalReads := func(t *testing.T, rec tracingpb.Recording) (servedUsingFollowerReads bool) {
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
	recCh := make(chan tracingpb.Recording, 1)

	knobs := base.TestingKnobs{
		SQLExecutor: &sql.ExecutorTestingKnobs{
			WithStatementTrace: func(trace tracingpb.Recording, stmt string) {
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
		conn := tc.ServerConn(i)
		isLeaseHolder := false
		testutils.SucceedsSoon(t, func() error {
			// Run a query to populate its cache.
			_, err = conn.Exec("SELECT * from t.test_table WHERE k=1")
			require.NoError(t, err)

			// Check that the cache was indeed populated.
			cache := tc.Server(i).DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache()
			entry, err := cache.TestingGetCached(
				context.Background(), tablePrefix, false /* inverted */, roachpb.LAG_BY_CLUSTER_SETTING,
			)
			require.NoError(t, err)
			require.False(t, entry.Lease.Empty())

			if expected, got := roachpb.LEAD_FOR_GLOBAL_READS, entry.ClosedTimestampPolicy; got != expected {
				return errors.Newf("expected closedts policy %s, got %s", expected, got)
			}

			isLeaseHolder = entry.Lease.Replica.NodeID == tc.Server(i).NodeID()
			return nil
		})

		// Run the query to ensure local read.
		_, err = conn.Exec(presentTimeRead)
		require.NoError(t, err)

		rec := <-recCh
		followerRead := ensureOnlyLocalReads(t, rec)

		// Expect every non-leaseholder to serve a (local) follower read. The
		// leaseholder on the other hand won't serve a follower read.
		require.Equal(t, !isLeaseHolder, followerRead, "%v", rec)
	}

	close(stopWritesCh)
	writeErr := <-errCh
	require.NoError(t, writeErr)
}

// TestEnsureLocalReadsOnGlobalTablesWithDelay tests that
// kv.closed_timestamp.lead_for_global_reads_auto_tune.enabled correctly
// auto-tunes closed timestamp updates to dynamically adjust the lead time for
// global reads based on the observed network latencies.
//
// This test ensures that all present time reads on GLOBAL tables don't incur a
// network hop even when there is a delay in the network latencies that exceed
// the previously hardcoded maxNetworkRTT (150ms). Without
// kv.closed_timestamp.lead_for_global_reads_auto_tune.enabled, this test should
// fail as the default hardcoded lead time for global tables will be too short
// for the 500ms network latencies. Follower reads would fail to serve reads
// locally and incur a network hop.
func TestEnsureLocalReadsOnGlobalTablesWithDelay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "too slow, this test use long simulated network latencies")

	// ensureOnlyLocalReads looks at a trace to ensure that reads were served
	// locally. It returns true if the read was served as a follower read.
	ensureOnlyLocalReads := func(t *testing.T, rec tracingpb.Recording) (servedUsingFollowerReads bool) {
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
	recCh := make(chan tracingpb.Recording, 1)

	knobs := base.TestingKnobs{
		SQLExecutor: &sql.ExecutorTestingKnobs{
			WithStatementTrace: func(trace tracingpb.Recording, stmt string) {
				if stmt == presentTimeRead {
					recCh <- trace
				}
			},
		},
	}

	numServers := 3

	// Start the server with a 500ms injected network latency. The injected
	// latencies are not applied until enableLatency() is called below.
	tc, sqlDB, cleanup, enableLatency := multiregionccltestutils.TestingCreateMultiRegionClusterWithDelay(
		t, numServers, knobs, 500*time.Millisecond, multiregionccltestutils.WithReplicationMode(base.ReplicationManual))

	defer cleanup()

	// Most of cluster settings are set based on TestColdStartLatency which also
	// use simulated network latencies in order to make the test less flaky.
	_, _ = sqlDB.Exec(`SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '50 ms'`)

	// Enable the lead for global reads auto-tuning. Disabling it fails test since
	// the default hardcoded lead time for global tables will be too short for the
	// 500ms network latencies.
	_, _ = sqlDB.Exec(`SET CLUSTER SETTING kv.closed_timestamp.lead_for_global_reads_auto_tune.enabled = true`)
	_, _ = sqlDB.Exec(`CREATE DATABASE t PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3"`)
	_, _ = sqlDB.Exec(`CREATE TABLE t.test_table (k INT PRIMARY KEY) LOCALITY GLOBAL`)
	_, _ = sqlDB.Exec("SET CLUSTER SETTING kv.allocator.load_based_rebalancing = off")
	_, _ = sqlDB.Exec("SET CLUSTER SETTING kv.allocator.min_lease_transfer_interval = '10ms'")
	_, _ = sqlDB.Exec(`SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '200ms'`)
	_, _ = sqlDB.Exec(`ALTER TENANT ALL SET CLUSTER SETTING spanconfig.reconciliation_job.checkpoint_interval = '500ms'`)

	var tableID uint32
	err := sqlDB.QueryRow(`SELECT id from system.namespace WHERE name='test_table'`).Scan(&tableID)
	require.NoError(t, err)
	tablePrefix := keys.MustAddr(keys.SystemSQLCodec.TablePrefix(tableID))

	// Split the range at the start of the table and add a voter to all nodes in
	// the cluster.
	tc.SplitRangeOrFatal(t, tablePrefix.AsRawKey())
	tc.AddVotersOrFatal(t, tablePrefix.AsRawKey(), tc.Target(1), tc.Target(2))

	// Enable simulated latencies late in the process to minimize startup time.
	enableLatency()

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

	populateClosedTsPolicy := func(i int) bool {
		conn := tc.ServerConn(i)
		isLeaseHolder := false
		testutils.SucceedsWithin(t, func() error {
			// Run a query to populate its cache.
			_, err = conn.Exec("SELECT * from t.test_table WHERE k=1")
			require.NoError(t, err)

			// Check that the cache was indeed populated.
			cache := tc.Server(i).DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache()
			entry, err := cache.TestingGetCached(
				context.Background(), tablePrefix, false /* inverted */, roachpb.LAG_BY_CLUSTER_SETTING,
			)
			require.NoError(t, err)
			require.False(t, entry.Lease.Empty())

			if expected, got := roachpb.LEAD_FOR_GLOBAL_READS, entry.ClosedTimestampPolicy; got != expected {
				return errors.Newf("expected closedts policy %s, got %s", expected, got)
			}
			isLeaseHolder = entry.Lease.Replica.NodeID == tc.Server(i).NodeID()
			t.Logf("suceeded at populating closed ts policy for: %d", tc.Server(i).NodeID())
			return nil
		}, 5*time.Minute)
		return isLeaseHolder
	}

	// Without this population first, the test is flaky since it takes sometime
	// for the latencies to be observed by sideTransport.Sender and replicas.
	for i := 0; i < numServers; i++ {
		populateClosedTsPolicy(i)
	}

	for i := 0; i < numServers; i++ {
		isLeaseHolder := populateClosedTsPolicy(i)
		conn := tc.ServerConn(i)
		// Run the query to ensure local read.
		_, err = conn.Exec(presentTimeRead)
		require.NoError(t, err)

		rec := <-recCh
		followerRead := ensureOnlyLocalReads(t, rec)

		// Expect every non-leaseholder to serve a (local) follower read. The
		// leaseholder on the other hand won't serve a follower read.
		require.Equal(t, !isLeaseHolder, followerRead, "%v", rec)
	}

	close(stopWritesCh)
	writeErr := <-errCh
	require.NoError(t, writeErr)
}
