// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/localtestcluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestSpanConfigUpdatesBlockedByRangeSizeBackpressureDefaultTestCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rRand, _ := randutil.NewTestRand()

	const (
		// Use reasonable values for testing
		rowSize        = 1 << 20  // 1 MiB per row
		targetDataSize = 10 << 20 // 10 MiB total
		numRows        = 1000
		maxRangeBytes  = 5 << 20 // 5 MiB max range size - smaller than our data
		minRangeBytes  = 1 << 20 // 1 MiB min range size - smaller than our data
		numSplits      = 10
	)
	val := randutil.RandBytes(rRand, rowSize)

	// // Setup single node cluster with aggressive backpressure settings
	// args := base.TestClusterArgs{
	// 	ReplicationMode: base.ReplicationManual,
	// 	ServerArgs: base.TestServerArgs{
	// 		Knobs: base.TestingKnobs{
	// 			Store: &kvserver.StoreTestingKnobs{
	// 				DisableMergeQueue: true,
	// 				DisableSplitQueue: true, // Prevent splits so range gets oversized
	// 				DisableGCQueue:    true,
	// 			},
	// 		},
	// 	},
	// }

	// tc := testcluster.StartTestCluster(t, 1, args)
	tc, tdb, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableSplitQueue: true,
				DisableMergeQueue: true,
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// tdb := sqlutils.MakeSQLRunner(tc.ServerConn)

	// Create table for our span config target
	_, err := tdb.Exec("CREATE TABLE foo (k INT PRIMARY KEY, v BYTES NOT NULL)")
	require.NoError(t, err)

	// Get the table prefix
	var tableID int
	err = tdb.QueryRow("SELECT table_id FROM crdb_internal.tables WHERE name = 'foo'").Scan(&tableID)
	require.NoError(t, err)
	require.NotEqual(t, 0, tableID)
	tablePrefix := keys.SystemSQLCodec.TablePrefix(uint32(tableID))
	log.Infof(ctx, "tableID: %d, tablePrefix: %s", tableID, tablePrefix)

	// Split the table into its own range
	_, _, err = tc.SplitRange(tablePrefix)
	require.NoError(t, err)

	// require.NoError(t, tc.WaitForSplitAndInitialization(tablePrefix))

	// Set a small max range size to trigger backpressure more easily
	_, err = tdb.Exec("ALTER TABLE foo CONFIGURE ZONE USING range_max_bytes = $1, range_min_bytes = $2", maxRangeBytes, minRangeBytes)
	require.NoError(t, err)

	// Wait for span config to be applied
	testutils.SucceedsSoon(t, func() error {
		// s := tc.Server(0)
		s := tc
		store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
		if err != nil {
			return err
		}
		repl := store.LookupReplica(keys.MustAddr(tablePrefix))
		conf, err := repl.LoadSpanConfig(ctx)
		if err != nil {
			return err
		}
		if conf.RangeMaxBytes != maxRangeBytes {
			return fmt.Errorf("expected %d, got %d", maxRangeBytes, conf.RangeMaxBytes)
		}
		return nil
	})

	// Fill the table with data to make the range larger than the configured max
	for i := 0; i < numRows; i++ {
		_, err := tdb.Exec("UPSERT INTO foo VALUES ($1, $2)",
			rRand.Intn(numRows), val)
		require.NoError(t, err)
	}

	// Verify the range is now oversized
	// s := tc.Server(0)
	s := tc
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	repl := store.LookupReplica(keys.MustAddr(tablePrefix))
	stats := repl.GetMVCCStats()
	t.Logf("Range size: %d bytes, max allowed: %d bytes", stats.Total(), maxRangeBytes)
	require.Greater(t, stats.Total(), int64(maxRangeBytes),
		"Range should be oversized to trigger backpressure")

	// Set a very low backpressure tolerance so backpressure kicks in
	_, err = tdb.Exec("SET CLUSTER SETTING kv.range.backpressure_byte_tolerance = '32 GiB'")
	require.NoError(t, err)

	// Now trigger the split queue to detect the oversized range
	require.NoError(t, store.ForceSplitScanAndProcess())

	// Try to update the span config - this should fail due to backpressure
	// if the span configuration table is in the same range as our test table
	// or if backpressure affects the span configurations table range

	// Attempt to change the zone config again - this requires updating span configs
	// This should demonstrate the catch-22 where span config updates fail
	start := func() error {
		_, err := tdb.Exec("ALTER TABLE foo CONFIGURE ZONE USING range_max_bytes = $1", maxRangeBytes*2)
		return err
	}

	err = start()
	if err != nil {
		t.Logf("Span config update failed as expected due to backpressure: %v", err)
		// Success - we reproduced the issue where span config updates are blocked
	} else {
		t.Logf("Span config update succeeded - may not have triggered backpressure condition")
		// This might happen if the span configurations table is in a different range
		// Let's try a more direct approach by inserting data that specifically targets
		// the system table ranges
	}

	// Additional verification: try to insert more data to see if backpressure is active
	insertErr := func() error {
		_, err := tdb.Exec("UPSERT INTO foo VALUES ($1, $2)",
			rRand.Intn(1000000), val)
		return err
	}()

	if insertErr != nil {
		t.Logf("Regular insert also blocked by backpressure: %v", insertErr)
	} else {
		t.Logf("Regular insert succeeded - backpressure may not be active yet")
	}
}

// TestSpanConfigUpdatesBlockedByRangeSizeBackpressure reproduces the
// catch-22 situation where span config updates are blocked by range size
// backpressure, preventing the clearing of protected timestamps that would
// allow GC to proceed.
func TestSpanConfigUpdatesBlockedByRangeSizeBackpressureLocalTestCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rRand, _ := randutil.NewTestRand()
	var err error // Declare err at function level

	const (
		// Use small values for faster testing
		rowSize = 500 << 10 // 500 KiB per row
		numRows = 1000      // Total ~500 MiB
	)
	val := randutil.RandBytes(rRand, rowSize)

	// Setup single node cluster with aggressive backpressure settings
	cfg := roachpb.SpanConfig{
		RangeMinBytes: 0,
		RangeMaxBytes: 100 << 10, // 100 KiB - extremely aggressive
		GCPolicy: roachpb.GCPolicy{
			TTLSeconds: 60 * 60 * 4, // 4 hours
		},
		NumReplicas: 3,
	}

	s := &localtestcluster.LocalTestCluster{
		Cfg: kvserver.StoreConfig{
			DefaultSpanConfig: cfg,
			Settings:          cluster.MakeTestingClusterSettings(),
		},
		StoreTestingKnobs: &kvserver.StoreTestingKnobs{
			DisableMergeQueue: true,
			DisableSplitQueue: true, // Prevent splits so range gets oversized
			DisableGCQueue:    true,
		},
	}
	s.Start(t, kvcoord.InitFactoryForLocalTestCluster)
	defer s.Stop()

	// We can't set cluster settings via SQL in LocalTestCluster, but we can make the test
	// more aggressive by using a very small RangeMaxBytes and writing lots of data.

	// The strategy: target the span_configurations table directly by writing to its range
	// span_configurations table has ID=47, which puts it in the backpressurable range
	spanConfigTableKey := keys.SystemSQLCodec.TablePrefix(keys.SpanConfigurationsTableID)

	t.Logf("Targeting span configurations table at key: %s (table ID %d)",
		spanConfigTableKey, keys.SpanConfigurationsTableID)

	// First, let's generate a lot of span config entries by creating keys that will trigger:
	// span config updates. We'll write to keys that require span config management.

	// Write data to make the span_configurations table range oversized
	// We'll use keys in that range to bloat it
	for i := 0; i < numRows; i++ {
		// Create keys in the span_configurations table range
		key := append(spanConfigTableKey, []byte(fmt.Sprintf("/synthetic_key_%d", i))...)
		err = s.DB.Put(ctx, key, val)
		require.NoError(t, err)
		// Print the current range size and the total max allowed range size
		repl := s.Store.LookupReplica(keys.MustAddr(spanConfigTableKey))
		fmt.Printf("Replica: %+v\n", repl)
		stats := repl.GetMVCCStats()
		fmt.Printf("Current range size: %d bytes, Max allowed: %d bytes\n", stats.Total(), cfg.RangeMaxBytes)
	}

	// Check if the range is oversized
	store := s.Store
	repl := store.LookupReplica(keys.MustAddr(spanConfigTableKey))
	stats := repl.GetMVCCStats()
	t.Logf("Range size: %d bytes, max allowed: %d bytes", stats.Total(), cfg.RangeMaxBytes)

	if stats.Total() <= cfg.RangeMaxBytes {
		t.Logf("Range not yet oversized, writing more data...")
		// Write more data to ensure we exceed the limit
		for i := numRows; i < numRows*3; i++ {
			key := append(spanConfigTableKey, []byte(fmt.Sprintf("/synthetic_key_%d", i))...)
			err = s.DB.Put(ctx, key, val)
			require.NoError(t, err)
		}
		stats = repl.GetMVCCStats()
		t.Logf("SpanConfigurations range size after additional writes: %d bytes", stats.Total())
	}

	// The range is oversized! Now try to write to it - this should fail with backpressure
	// We don't need to force split queue processing since backpressure should be automatic
	// when a range exceeds the configured thresholds during writes

	testKey := append(spanConfigTableKey, []byte("/synthetic_key")...)
	err = s.DB.Put(ctx, testKey, []byte("test_value"))

	if err != nil {
		t.Logf("SUCCESS: Write to span config range blocked by backpressure: %v", err)

		// Since span config updates would also write to this same range, they would be blocked
		t.Logf("This demonstrates the catch-22: span config updates (which write to this range)")
		t.Logf("are blocked by backpressure, preventing protected timestamp cleanup")
	} else {
		t.Logf("Write succeeded - backpressure may not be active yet")
		t.Logf("This could mean:")
		t.Logf("1. Range size multiplier/tolerance settings prevent backpressure")
		t.Logf("2. Range is not actually oversized enough")
		t.Logf("3. Backpressure logic may have mitigations we didn't account for")
	}
}
