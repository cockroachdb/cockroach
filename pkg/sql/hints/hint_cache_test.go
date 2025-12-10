// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hints_test

import (
	"context"
	"hash/fnv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/hintpb"
	"github.com/cockroachdb/cockroach/pkg/sql/hints"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestHintCacheBasic tests the basic functionality of the hint cache, including
// hinted hashes tracking, hint insertion/deletion, and query functionality.
func TestHintCacheBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()
	r := sqlutils.MakeSQLRunner(db)
	setTestDefaults(t, srv)

	st := cluster.MakeTestingClusterSettings()
	fingerprintFlags := tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&st.SV))

	hc := ts.ExecutorConfig().(sql.ExecutorConfig).StatementHintsCache
	require.Equal(t, 0, hc.TestingHashCount())

	// Insert a hint for a statement. The cache should soon contain the hash.
	fingerprint1 := "SELECT a FROM t WHERE b = $1"
	insertStatementHint(t, r, fingerprint1)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, fingerprintFlags, 1)
	require.Equal(t, 1, hc.TestingHashCount())

	// Insert another hint with the same fingerprint (same hash). The count for
	// that hash should increase.
	insertStatementHint(t, r, fingerprint1)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, fingerprintFlags, 2)
	require.Equal(t, 1, hc.TestingHashCount())

	// Query for hints on a statement that has no hints.
	nonHintedFingerprint := "SELECT x FROM t WHERE y = $1"
	require.False(t, hc.TestingHashHasHints(computeHash(t, nonHintedFingerprint)))
	requireHintsCount(t, hc, ctx, nonHintedFingerprint, fingerprintFlags, 0)

	// Add a hint for another statement.
	fingerprint2 := "SELECT c FROM t WHERE d = $1"
	insertStatementHint(t, r, fingerprint2)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint2, fingerprintFlags, 1)
	require.Equal(t, 2, hc.TestingHashCount())

	// Delete one hint from the first statement. The count should decrease to 1.
	deleteStatementHints(t, r, fingerprint1, 1)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, fingerprintFlags, 1)
	require.Equal(t, 2, hc.TestingHashCount())

	// Delete all remaining hints from the first statement. The hash should be
	// removed from the map entirely.
	deleteStatementHints(t, r, fingerprint1, 0)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, fingerprintFlags, 0)
	require.Equal(t, 1, hc.TestingHashCount())
}

// TestHintCacheLRU tests the LRU functionality of the hintCache, ensuring
// that entries are evicted according to the LRU policy when the cache is full.
func TestHintCacheLRU(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()
	r := sqlutils.MakeSQLRunner(db)
	setTestDefaults(t, srv)

	st := cluster.MakeTestingClusterSettings()
	fingerprintFlags := tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&st.SV))

	// Set cache size to 2 for testing eviction.
	r.Exec(t, "SET CLUSTER SETTING sql.hints.statement_hints_cache_size = 2")

	hc := ts.ExecutorConfig().(sql.ExecutorConfig).StatementHintsCache
	require.Equal(t, 0, hc.TestingHashCount())

	// Create test data: 3 different fingerprints.
	fingerprints := []string{
		"SELECT a FROM t WHERE b = $1",
		"SELECT c FROM t WHERE d = $1",
		"SELECT e FROM t WHERE f = $1",
	}
	hashes := make([]int64, len(fingerprints))

	// Insert hints for all fingerprints.
	for i, fp := range fingerprints {
		hashes[i] = computeHash(t, fp)
		insertStatementHint(t, r, fp)
	}

	// Wait for all hashes to appear in hintedHashes.
	testutils.SucceedsSoon(t, func() error {
		if count := hc.TestingHashCount(); count != len(fingerprints) {
			return errors.Errorf("expected %d hinted hashes, got %d", len(fingerprints), count)
		}
		return nil
	})

	// Before the initial scan is complete, all queries unconditionally check
	// the hint cache and might perform DB reads, so we need to ignore all reads
	// that happened already.
	ignoredReads := hc.TestingNumTableReads()

	// Access the first two fingerprints to populate the cache.
	// This should result in 2 table reads.
	requireHintsCount(t, hc, ctx, fingerprints[0], fingerprintFlags, 1)
	require.Equal(t, ignoredReads+1, hc.TestingNumTableReads())

	requireHintsCount(t, hc, ctx, fingerprints[1], fingerprintFlags, 1)
	require.Equal(t, ignoredReads+2, hc.TestingNumTableReads())

	// Access the same fingerprints again - should be served from cache with no
	// additional reads.
	requireHintsCount(t, hc, ctx, fingerprints[0], fingerprintFlags, 1)
	requireHintsCount(t, hc, ctx, fingerprints[1], fingerprintFlags, 1)
	require.Equal(t, ignoredReads+2, hc.TestingNumTableReads())

	// Access the third fingerprint. This should evict the first (LRU) due to
	// cache size limit of 2, resulting in one more table read.
	requireHintsCount(t, hc, ctx, fingerprints[2], fingerprintFlags, 1)
	require.Equal(t, ignoredReads+3, hc.TestingNumTableReads())

	// Access the first fingerprint again. Since it was evicted, this should
	// result in another table read on the first access.
	requireHintsCount(t, hc, ctx, fingerprints[0], fingerprintFlags, 1)
	require.Equal(t, ignoredReads+4, hc.TestingNumTableReads())

	// Access the second fingerprint. It should have been evicted by now, so
	// another table read on the first access.
	requireHintsCount(t, hc, ctx, fingerprints[1], fingerprintFlags, 1)
	require.Equal(t, ignoredReads+5, hc.TestingNumTableReads())

	// The first and second fingerprint should now be cached, so accessing them
	// again should not increase table reads.
	requireHintsCount(t, hc, ctx, fingerprints[0], fingerprintFlags, 1)
	requireHintsCount(t, hc, ctx, fingerprints[1], fingerprintFlags, 1)
	require.Equal(t, ignoredReads+5, hc.TestingNumTableReads())

	// Access the third fingerprint again - should have been evicted, so the first
	// access should cause a table read.
	requireHintsCount(t, hc, ctx, fingerprints[2], fingerprintFlags, 1)
	requireHintsCount(t, hc, ctx, fingerprints[2], fingerprintFlags, 1)
	require.Equal(t, ignoredReads+6, hc.TestingNumTableReads())
}

// TestHintCacheInitialScan tests that a new cache correctly populates from
// existing hints in the system table during its initial scan.
func TestHintCacheInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	listenerReg := listenerutil.NewListenerRegistry()
	defer listenerReg.Close()
	stickyVFSRegistry := fs.NewStickyRegistry()

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					// Sticky vfs is needed for cluster restart.
					StickyVFSRegistry: stickyVFSRegistry,
				},
			},
		},
		// A listener is required for cluster restart.
		ReusableListenerReg: listenerReg,
	})
	defer tc.Stopper().Stop(ctx)
	ts := tc.ApplicationLayer(0)
	r := sqlutils.MakeSQLRunner(ts.SQLConn(t))
	setTestDefaults(t, tc.Server(0))

	st := cluster.MakeTestingClusterSettings()
	fingerprintFlags := tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&st.SV))

	// Insert multiple hints into the system table.
	fingerprints := []string{
		"SELECT a FROM t WHERE b = $1",
		"SELECT c FROM t WHERE d = $1",
		"SELECT e FROM t WHERE f = $1",
	}
	hashes := make([]int64, len(fingerprints))

	for i, fp := range fingerprints {
		hashes[i] = computeHash(t, fp)
		insertStatementHint(t, r, fp)
	}
	// Insert multiple hints for the first fingerprint.
	insertStatementHint(t, r, fingerprints[0])

	// Restart the cluster to trigger the initial scan for the hints cache.
	require.NoError(t, tc.Restart())
	ts = tc.ApplicationLayer(0)
	r = sqlutils.MakeSQLRunner(ts.SQLConn(t))

	hc := ts.ExecutorConfig().(sql.ExecutorConfig).StatementHintsCache

	// The cache should have all the hashes once the initial scan completes.
	testutils.SucceedsSoon(t, func() error {
		if count := hc.TestingHashCount(); count != len(fingerprints) {
			return errors.Errorf("expected %d hinted hashes, got %d", len(fingerprints), count)
		}
		return nil
	})
	require.True(t, hc.TestingHashHasHints(hashes[0]))
	require.True(t, hc.TestingHashHasHints(hashes[1]))
	require.True(t, hc.TestingHashHasHints(hashes[2]))
	requireHintsCount(t, hc, ctx, fingerprints[0], fingerprintFlags, 2)
	requireHintsCount(t, hc, ctx, fingerprints[1], fingerprintFlags, 1)
	requireHintsCount(t, hc, ctx, fingerprints[2], fingerprintFlags, 1)

	// Query for a fingerprint with no hints.
	nonHintedFingerprint := "SELECT x FROM t WHERE y = $1"
	nonHintedHash := computeHash(t, nonHintedFingerprint)
	require.False(t, hc.TestingHashHasHints(nonHintedHash))
	requireHintsCount(t, hc, ctx, nonHintedFingerprint, fingerprintFlags, 0)

	// After the initial scan, new hints should still be detected via rangefeed.
	fingerprint4 := "SELECT g FROM t WHERE h = $1"
	insertStatementHint(t, r, fingerprint4)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint4, fingerprintFlags, 1)
	require.Equal(t, 4, hc.TestingHashCount())

	// Delete one of the initial hints.
	deleteStatementHints(t, r, fingerprints[0], 1)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprints[0], fingerprintFlags, 1)
	require.Equal(t, 4, hc.TestingHashCount())
}

// TestHintCacheMultiNode tests that the cache automatically refreshes when new
// hints are added and removed from different nodes, and that concurrent access
// works correctly.
func TestHintCacheMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Skip secondary tenants when running under race to avoid flakes.
	var serverArgs base.TestServerArgs
	if util.RaceEnabled {
		serverArgs.DefaultTestTenant = base.TestSkipSecondaryTenantsUnderDuress
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{ServerArgs: serverArgs})
	defer tc.Stopper().Stop(ctx)

	// Use node 0 as the primary node for the cache.
	ts := tc.Server(0).ApplicationLayer()
	r1 := sqlutils.MakeSQLRunner(tc.ServerConn(1))
	r2 := sqlutils.MakeSQLRunner(tc.ServerConn(2))
	setTestDefaults(t, tc.Server(0))

	st := cluster.MakeTestingClusterSettings()
	fingerprintFlags := tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&st.SV))

	// Use the hints cache from node 0.
	hc := ts.ExecutorConfig().(sql.ExecutorConfig).StatementHintsCache
	require.Equal(t, 0, hc.TestingHashCount())

	// Insert hints from node 1.
	fingerprint1 := "SELECT a FROM t WHERE b = $1"
	insertStatementHint(t, r1, fingerprint1)
	insertStatementHint(t, r1, fingerprint1)

	// Insert another hint from node 2.
	fingerprint2 := "SELECT c FROM t WHERE d = $1"
	insertStatementHint(t, r2, fingerprint2)

	// Both hints should be available via auto-refresh.
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, fingerprintFlags, 2)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint2, fingerprintFlags, 1)
	require.Equal(t, 2, hc.TestingHashCount())

	// Delete one hint for fingerprint1 from node 2.
	deleteStatementHints(t, r2, fingerprint1, 1)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, fingerprintFlags, 1)
	require.Equal(t, 2, hc.TestingHashCount())

	// Delete all hints for fingerprint2 from node 1.
	deleteStatementHints(t, r1, fingerprint2, 0)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint2, fingerprintFlags, 0)
	require.Equal(t, 1, hc.TestingHashCount())

	// Verify that fingerprint1 still has hints but fingerprint2 doesn't.
	requireHintsCount(t, hc, ctx, fingerprint1, fingerprintFlags, 1)
	requireHintsCount(t, hc, ctx, fingerprint2, fingerprintFlags, 0)
}

// TestHintCacheMultiTenant tests that hint caches for different tenants are
// correctly populated and independent of one another.
func TestHintCacheMultiTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer srv.Stopper().Stop(ctx)
	setTestDefaults(t, srv)

	st := cluster.MakeTestingClusterSettings()
	fingerprintFlags := tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&st.SV))

	// Create two separate tenants.
	tenantID1 := serverutils.TestTenantID()
	tenantID2 := serverutils.TestTenantID2()
	tenant1, tenantConn1 := serverutils.StartTenant(t, srv, base.TestTenantArgs{
		TenantID: tenantID1,
	})
	defer tenantConn1.Close()
	tenant2, tenantConn2 := serverutils.StartTenant(t, srv, base.TestTenantArgs{
		TenantID: tenantID2,
	})
	defer tenantConn2.Close()

	r1 := sqlutils.MakeSQLRunner(tenantConn1)
	r2 := sqlutils.MakeSQLRunner(tenantConn2)

	hc1 := tenant1.ExecutorConfig().(sql.ExecutorConfig).StatementHintsCache
	hc2 := tenant2.ExecutorConfig().(sql.ExecutorConfig).StatementHintsCache
	require.Equal(t, 0, hc1.TestingHashCount())
	require.Equal(t, 0, hc2.TestingHashCount())

	// Insert a hint for tenant1 only.
	fingerprint1 := "SELECT a FROM t WHERE b = $1"
	insertStatementHint(t, r1, fingerprint1)
	waitForUpdateOnFingerprintHash(t, ctx, hc1, fingerprint1, fingerprintFlags, 1)
	require.Equal(t, 1, hc1.TestingHashCount())
	requireHintsCount(t, hc1, ctx, fingerprint1, fingerprintFlags, 1)

	// Tenant2's cache should remain empty.
	require.Equal(t, 0, hc2.TestingHashCount())
	require.False(t, hc2.TestingHashHasHints(computeHash(t, fingerprint1)))
	requireHintsCount(t, hc2, ctx, fingerprint1, fingerprintFlags, 0)

	// Insert a different hint for tenant2.
	fingerprint2 := "SELECT c FROM t WHERE d = $1"
	insertStatementHint(t, r2, fingerprint2)
	waitForUpdateOnFingerprintHash(t, ctx, hc2, fingerprint2, fingerprintFlags, 1)
	require.Equal(t, 1, hc2.TestingHashCount())
	requireHintsCount(t, hc2, ctx, fingerprint2, fingerprintFlags, 1)

	// Tenant1's cache should still only have its original hint.
	require.Equal(t, 1, hc1.TestingHashCount())
	require.False(t, hc1.TestingHashHasHints(computeHash(t, fingerprint2)))
	requireHintsCount(t, hc1, ctx, fingerprint2, fingerprintFlags, 0)

	// Insert the same fingerprint in both tenants - they should be independent.
	fingerprintShared := "SELECT x FROM t WHERE y = $1"
	insertStatementHint(t, r1, fingerprintShared)
	insertStatementHint(t, r1, fingerprintShared) // Two hints for tenant1.
	insertStatementHint(t, r2, fingerprintShared) // One hint for tenant2.

	waitForUpdateOnFingerprintHash(t, ctx, hc1, fingerprintShared, fingerprintFlags, 2)
	waitForUpdateOnFingerprintHash(t, ctx, hc2, fingerprintShared, fingerprintFlags, 1)

	// Both tenants should now have 2 hinted hashes.
	require.Equal(t, 2, hc1.TestingHashCount())
	require.Equal(t, 2, hc2.TestingHashCount())

	// Verify that the shared fingerprint has different counts in each tenant.
	require.True(t, hc1.TestingHashHasHints(computeHash(t, fingerprintShared)))
	require.True(t, hc2.TestingHashHasHints(computeHash(t, fingerprintShared)))
	requireHintsCount(t, hc1, ctx, fingerprintShared, fingerprintFlags, 2)
	requireHintsCount(t, hc2, ctx, fingerprintShared, fingerprintFlags, 1)

	// Delete one hint from tenant1's shared fingerprint.
	deleteStatementHints(t, r1, fingerprintShared, 1)
	waitForUpdateOnFingerprintHash(t, ctx, hc1, fingerprintShared, fingerprintFlags, 1)

	// Tenant2's count for the shared fingerprint should remain unchanged.
	require.True(t, hc1.TestingHashHasHints(computeHash(t, fingerprintShared)))
	require.True(t, hc2.TestingHashHasHints(computeHash(t, fingerprintShared)))

	// Delete all hints for the shared fingerprint from tenant2.
	deleteStatementHints(t, r2, fingerprintShared, 0)
	waitForUpdateOnFingerprintHash(t, ctx, hc2, fingerprintShared, fingerprintFlags, 0)

	// Tenant1 should still have the hint, but tenant2 should not.
	require.True(t, hc1.TestingHashHasHints(computeHash(t, fingerprintShared)))
	require.False(t, hc2.TestingHashHasHints(computeHash(t, fingerprintShared)))
	requireHintsCount(t, hc1, ctx, fingerprintShared, fingerprintFlags, 1)
	requireHintsCount(t, hc2, ctx, fingerprintShared, fingerprintFlags, 0)
}

// TestHintCacheGeneration tests that the cache generation is incremented
// correctly when hints are added and removed, and that the generation is not
// incremented when there are no updates.
func TestHintCacheGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rnd, _ := randutil.NewTestRand()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()
	r := sqlutils.MakeSQLRunner(db)
	setTestDefaults(t, srv)

	st := cluster.MakeTestingClusterSettings()
	fingerprintFlags := tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&st.SV))

	hc := ts.ExecutorConfig().(sql.ExecutorConfig).StatementHintsCache

	// Helper that retrieves the generation and verifies that it doesn't change
	// over a short period.
	getGenerationAssertNoChange := func() int64 {
		t.Helper()
		startGeneration := hc.GetGeneration()
		time.Sleep(time.Duration(rnd.Intn(int(time.Millisecond))))
		require.Equal(t, startGeneration, hc.GetGeneration())
		return startGeneration
	}
	// Helper that waits until the generation increments from the given start
	// point.
	waitForGenerationInc := func(prevGeneration int64) {
		t.Helper()
		testutils.SucceedsSoon(t, func() error {
			t.Helper()
			if gen := hc.GetGeneration(); gen <= prevGeneration {
				return errors.Errorf("expected generation >= %d, got %d", prevGeneration, gen)
			}
			return nil
		})
	}

	// The initial scan should increment the generation.
	waitForGenerationInc(1)
	generationAfterInitialScan := getGenerationAssertNoChange()

	// Insert a hint - generation should increment.
	fingerprint1 := "SELECT a FROM t WHERE b = $1"
	insertStatementHint(t, r, fingerprint1)
	waitForGenerationInc(generationAfterInitialScan)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, fingerprintFlags, 1)
	generationAfterInsert := getGenerationAssertNoChange()

	// Insert another hint for the same fingerprint - generation should increment
	// again.
	insertStatementHint(t, r, fingerprint1)
	waitForGenerationInc(generationAfterInsert)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, fingerprintFlags, 2)
	generationAfterSecondInsert := getGenerationAssertNoChange()

	// Add a hint for a different fingerprint - generation should increment.
	fingerprint2 := "SELECT c FROM t WHERE d = $1"
	insertStatementHint(t, r, fingerprint2)
	waitForGenerationInc(generationAfterSecondInsert)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint2, fingerprintFlags, 1)
	generationAfterDifferentFingerprint := getGenerationAssertNoChange()

	// Delete one hint - generation should increment.
	deleteStatementHints(t, r, fingerprint1, 1)
	waitForGenerationInc(generationAfterDifferentFingerprint)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, fingerprintFlags, 1)
	generationAfterDelete := getGenerationAssertNoChange()

	// Delete all remaining hints for fingerprint1 - generation should increment.
	deleteStatementHints(t, r, fingerprint1, 0)
	waitForGenerationInc(generationAfterDelete)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, fingerprintFlags, 0)
	generationAfterDeleteAll := getGenerationAssertNoChange()

	// Query for hints (cache access) should NOT increment generation.
	hc.MaybeGetStatementHints(ctx, fingerprint2, fingerprintFlags)
	getGenerationAssertNoChange()

	// Accessing a non-existent fingerprint should also NOT increment generation.
	hc.MaybeGetStatementHints(ctx, "SELECT nonexistent FROM t", fingerprintFlags)
	getGenerationAssertNoChange()

	// Delete all remaining hints.
	deleteStatementHints(t, r, fingerprint2, 0)
	waitForGenerationInc(generationAfterDeleteAll)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint2, fingerprintFlags, 0)
	getGenerationAssertNoChange()
}

func setTestDefaults(t *testing.T, srv serverutils.TestServerInterface) {
	// These settings can only be set on the system tenant.
	r := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	r.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
	r.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10ms'")
	r.Exec(t, "SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '10ms'")
}

// waitForUpdateOnFingerprintHash waits for the cache to automatically refresh
// to reflect a hint insertion or deletion.
func waitForUpdateOnFingerprintHash(
	t *testing.T,
	ctx context.Context,
	hc *hints.StatementHintsCache,
	fingerprint string,
	fingerprintFlags tree.FmtFlags,
	expected int,
) {
	t.Helper()
	hash := computeHash(t, fingerprint)
	testutils.SucceedsSoon(t, func() error {
		t.Helper()
		if hasHints := hc.TestingHashHasHints(hash); (expected > 0) != hasHints {
			return errors.Errorf("expected hash %d with hasHints=%t, got hasHints=%t", hash, expected > 0, hasHints)
		}
		hints, ids := hc.MaybeGetStatementHints(ctx, fingerprint, fingerprintFlags)
		if len(hints) != expected {
			return errors.Errorf("expected %d hints for fingerprint %q, got %d", expected, fingerprint, len(hints))
		}
		checkIDOrder(t, ids)
		return nil
	})
}

// requireHintsCount verifies that MaybeGetStatementHints returns the expected number of hints and IDs.
func requireHintsCount(
	t *testing.T,
	hc *hints.StatementHintsCache,
	ctx context.Context,
	fingerprint string,
	fingerprintFlags tree.FmtFlags,
	expectedCount int,
) {
	hints, ids := hc.MaybeGetStatementHints(ctx, fingerprint, fingerprintFlags)
	require.Len(t, hints, expectedCount)
	require.Len(t, ids, expectedCount)
	checkIDOrder(t, ids)
}

// checkIDOrder verifies that the row IDs are in descending order.
func checkIDOrder(t *testing.T, ids []int64) {
	t.Helper()
	for i := 1; i < len(ids); i++ {
		if ids[i] >= ids[i-1] {
			t.Fatalf("expected IDs to be in descending order, got %v", ids)
		}
	}
}

// insertStatementHint inserts a random statement hint into the
// system.statement_hints table.
func insertStatementHint(t *testing.T, r *sqlutils.SQLRunner, fingerprint string) {
	// TODO(drewk,michae2): randomly choose the hint type once we support others.
	var hint hintpb.StatementHintUnion
	hint.SetValue(&hintpb.InjectHints{})
	hintBytes, err := hintpb.ToBytes(hint)
	require.NoError(t, err)
	const insertStmt = `INSERT INTO system.statement_hints ("fingerprint", "hint", "hint_type") VALUES ($1, $2, $3)`
	r.Exec(t, insertStmt, fingerprint, hintBytes, hint.HintType())
}

// deleteStatementHints deletes statement hints from the system.statement_hints
// table. If limit is 0, deletes all hints for the fingerprint. If limit > 0,
// deletes up to that many hints.
func deleteStatementHints(t *testing.T, r *sqlutils.SQLRunner, fingerprint string, limit int) {
	if limit > 0 {
		const deleteStmt = `DELETE FROM system.statement_hints WHERE fingerprint = $1 ORDER BY row_id LIMIT $2`
		r.Exec(t, deleteStmt, fingerprint, limit)
	} else {
		const deleteStmt = `DELETE FROM system.statement_hints WHERE fingerprint = $1`
		r.Exec(t, deleteStmt, fingerprint)
	}
}

// computeHash computes the same hash that the cache uses for fingerprints.
func computeHash(t *testing.T, fingerprint string) int64 {
	hash := fnv.New64()
	_, err := hash.Write([]byte(fingerprint))
	require.NoError(t, err)
	return int64(hash.Sum64())
}
