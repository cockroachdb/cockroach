// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hints_test

import (
	"context"
	"hash/fnv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/hints"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	// Create a hints cache.
	hc := createHintsCache(t, ctx, ts)
	require.Equal(t, 0, hc.TestingHashCount())

	// Insert a hint for a statement. The cache should soon contain the hash.
	fingerprint1 := "SELECT a FROM t WHERE b = $1"
	insertStatementHint(t, r, fingerprint1)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, 1)
	require.Equal(t, 1, hc.TestingHashCount())

	// Insert another hint with the same fingerprint (same hash). The count for
	// that hash should increase.
	insertStatementHint(t, r, fingerprint1)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, 2)
	require.Equal(t, 1, hc.TestingHashCount())

	// Query for hints on a statement that has no hints.
	nonHintedFingerprint := "SELECT x FROM t WHERE y = $1"
	require.Equal(t, 0, hc.TestingLookupHash(computeHash(t, nonHintedFingerprint)))
	require.Nil(t, hc.MaybeGetStatementHints(ctx, nonHintedFingerprint))

	// Add a hint for another statement.
	fingerprint2 := "SELECT c FROM t WHERE d = $1"
	insertStatementHint(t, r, fingerprint2)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint2, 1)
	require.Equal(t, 2, hc.TestingHashCount())

	// Delete one hint from the first statement. The count should decrease to 1.
	deleteStatementHints(t, r, fingerprint1, 1)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, 1)
	require.Equal(t, 2, hc.TestingHashCount())

	// Delete all remaining hints from the first statement. The hash should be
	// removed from the map entirely.
	deleteStatementHints(t, r, fingerprint1, 0)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, 0)
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

	// Set cache size to 2 for testing eviction.
	r.Exec(t, "SET CLUSTER SETTING sql.hints.statement_hints_cache_size = 2")

	hc := createHintsCache(t, ctx, ts)
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

	// Access the first two fingerprints to populate the cache.
	// This should result in 2 table reads.
	require.NotNil(t, hc.MaybeGetStatementHints(ctx, fingerprints[0]))
	require.Equal(t, 1, hc.TestingNumTableReads())

	require.NotNil(t, hc.MaybeGetStatementHints(ctx, fingerprints[1]))
	require.Equal(t, 2, hc.TestingNumTableReads())

	// Access the same fingerprints again - should be served from cache with no
	// additional reads.
	require.NotNil(t, hc.MaybeGetStatementHints(ctx, fingerprints[0]))
	require.NotNil(t, hc.MaybeGetStatementHints(ctx, fingerprints[1]))
	require.Equal(t, 2, hc.TestingNumTableReads())

	// Access the third fingerprint. This should evict the first (LRU) due to
	// cache size limit of 2, resulting in one more table read.
	require.NotNil(t, hc.MaybeGetStatementHints(ctx, fingerprints[2]))
	require.Equal(t, 3, hc.TestingNumTableReads())

	// Access the first fingerprint again. Since it was evicted, this should
	// result in another table read on the first access.
	require.NotNil(t, hc.MaybeGetStatementHints(ctx, fingerprints[0]))
	require.Equal(t, 4, hc.TestingNumTableReads())

	// Access the second fingerprint. It should have been evicted by now, so
	// another table read on the first access.
	require.NotNil(t, hc.MaybeGetStatementHints(ctx, fingerprints[1]))
	require.Equal(t, 5, hc.TestingNumTableReads())

	// The first and second fingerprint should now be cached, so accessing them
	// again should not increase table reads.
	require.NotNil(t, hc.MaybeGetStatementHints(ctx, fingerprints[0]))
	require.NotNil(t, hc.MaybeGetStatementHints(ctx, fingerprints[1]))
	require.Equal(t, 5, hc.TestingNumTableReads())

	// Access the third fingerprint again - should have been evicted, so the first
	// access should cause a table read.
	require.NotNil(t, hc.MaybeGetStatementHints(ctx, fingerprints[2]))
	require.NotNil(t, hc.MaybeGetStatementHints(ctx, fingerprints[2]))
	require.Equal(t, 6, hc.TestingNumTableReads())
}

// TestHintCacheInitialScan tests that a new cache correctly populates from
// existing hints in the system table during its initial scan.
func TestHintCacheInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()
	r := sqlutils.MakeSQLRunner(db)
	setTestDefaults(t, srv)

	// Insert multiple hints into the system table BEFORE creating the cache.
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

	// Now create a hints cache - it should populate from existing hints during
	// initial scan.
	hc := createHintsCache(t, ctx, ts)

	// The cache should have all the hashes from the initial scan. No retry
	// necessary since initial scan is blocking.
	require.Equal(t, 3, hc.TestingHashCount())
	require.Equal(t, 2, hc.TestingLookupHash(hashes[0]))
	require.Equal(t, 1, hc.TestingLookupHash(hashes[1]))
	require.Equal(t, 1, hc.TestingLookupHash(hashes[2]))
	require.Len(t, hc.MaybeGetStatementHints(ctx, fingerprints[0]), 2)
	require.Len(t, hc.MaybeGetStatementHints(ctx, fingerprints[1]), 1)
	require.Len(t, hc.MaybeGetStatementHints(ctx, fingerprints[2]), 1)

	// Query for a fingerprint with no hints.
	nonHintedFingerprint := "SELECT x FROM t WHERE y = $1"
	nonHintedHash := computeHash(t, nonHintedFingerprint)
	require.Equal(t, 0, hc.TestingLookupHash(nonHintedHash))
	require.Nil(t, hc.MaybeGetStatementHints(ctx, nonHintedFingerprint))

	// After the initial scan, new hints should still be detected via rangefeed.
	fingerprint4 := "SELECT g FROM t WHERE h = $1"
	insertStatementHint(t, r, fingerprint4)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint4, 1)
	require.Equal(t, 4, hc.TestingHashCount())

	// Delete one of the initial hints.
	deleteStatementHints(t, r, fingerprints[0], 1)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprints[0], 1)
	require.Equal(t, 4, hc.TestingHashCount())
}

// TestHintCacheMultiNode tests that the cache automatically refreshes when new
// hints are added and removed from different nodes, and that concurrent access
// works correctly.
func TestHintCacheMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{},
	})
	defer tc.Stopper().Stop(ctx)

	// Use node 0 as the primary node for the cache.
	ts := tc.Server(0).ApplicationLayer()
	r1 := sqlutils.MakeSQLRunner(tc.ServerConn(1))
	r2 := sqlutils.MakeSQLRunner(tc.ServerConn(2))
	setTestDefaults(t, tc.Server(0))

	// Create a hints cache on node 0.
	hc := createHintsCache(t, ctx, ts)
	require.Equal(t, 0, hc.TestingHashCount())

	// Insert hints from node 1.
	fingerprint1 := "SELECT a FROM t WHERE b = $1"
	insertStatementHint(t, r1, fingerprint1)
	insertStatementHint(t, r1, fingerprint1)

	// Insert another hint from node 2.
	fingerprint2 := "SELECT c FROM t WHERE d = $1"
	insertStatementHint(t, r2, fingerprint2)

	// Both hints should be available via auto-refresh.
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, 2)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint2, 1)
	require.Equal(t, 2, hc.TestingHashCount())

	// Delete one hint for fingerprint1 from node 2.
	deleteStatementHints(t, r2, fingerprint1, 1)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint1, 1)
	require.Equal(t, 2, hc.TestingHashCount())

	// Delete all hints for fingerprint2 from node 1.
	deleteStatementHints(t, r1, fingerprint2, 0)
	waitForUpdateOnFingerprintHash(t, ctx, hc, fingerprint2, 0)
	require.Equal(t, 1, hc.TestingHashCount())

	// Verify that fingerprint1 still has hints but fingerprint2 doesn't.
	require.Len(t, hc.MaybeGetStatementHints(ctx, fingerprint1), 1)
	require.Nil(t, hc.MaybeGetStatementHints(ctx, fingerprint2))
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

	// Create hints caches for both tenants.
	hc1 := createHintsCache(t, ctx, tenant1)
	hc2 := createHintsCache(t, ctx, tenant2)
	require.Equal(t, 0, hc1.TestingHashCount())
	require.Equal(t, 0, hc2.TestingHashCount())

	// Insert a hint for tenant1 only.
	fingerprint1 := "SELECT a FROM t WHERE b = $1"
	insertStatementHint(t, r1, fingerprint1)
	waitForUpdateOnFingerprintHash(t, ctx, hc1, fingerprint1, 1)
	require.Equal(t, 1, hc1.TestingHashCount())
	require.Len(t, hc1.MaybeGetStatementHints(ctx, fingerprint1), 1)

	// Tenant2's cache should remain empty.
	require.Equal(t, 0, hc2.TestingHashCount())
	require.Equal(t, 0, hc2.TestingLookupHash(computeHash(t, fingerprint1)))
	require.Nil(t, hc2.MaybeGetStatementHints(ctx, fingerprint1))

	// Insert a different hint for tenant2.
	fingerprint2 := "SELECT c FROM t WHERE d = $1"
	insertStatementHint(t, r2, fingerprint2)
	waitForUpdateOnFingerprintHash(t, ctx, hc2, fingerprint2, 1)
	require.Equal(t, 1, hc2.TestingHashCount())
	require.Len(t, hc2.MaybeGetStatementHints(ctx, fingerprint2), 1)

	// Tenant1's cache should still only have its original hint.
	require.Equal(t, 1, hc1.TestingHashCount())
	require.Equal(t, 0, hc1.TestingLookupHash(computeHash(t, fingerprint2)))
	require.Nil(t, hc1.MaybeGetStatementHints(ctx, fingerprint2))

	// Insert the same fingerprint in both tenants - they should be independent.
	fingerprintShared := "SELECT x FROM t WHERE y = $1"
	insertStatementHint(t, r1, fingerprintShared)
	insertStatementHint(t, r1, fingerprintShared) // Two hints for tenant1.
	insertStatementHint(t, r2, fingerprintShared) // One hint for tenant2.

	waitForUpdateOnFingerprintHash(t, ctx, hc1, fingerprintShared, 2)
	waitForUpdateOnFingerprintHash(t, ctx, hc2, fingerprintShared, 1)

	// Both tenants should now have 2 hinted hashes.
	require.Equal(t, 2, hc1.TestingHashCount())
	require.Equal(t, 2, hc2.TestingHashCount())

	// Verify that the shared fingerprint has different counts in each tenant.
	require.Equal(t, 2, hc1.TestingLookupHash(computeHash(t, fingerprintShared)))
	require.Equal(t, 1, hc2.TestingLookupHash(computeHash(t, fingerprintShared)))
	require.Len(t, hc1.MaybeGetStatementHints(ctx, fingerprintShared), 2)
	require.Len(t, hc2.MaybeGetStatementHints(ctx, fingerprintShared), 1)

	// Delete one hint from tenant1's shared fingerprint.
	deleteStatementHints(t, r1, fingerprintShared, 1)
	waitForUpdateOnFingerprintHash(t, ctx, hc1, fingerprintShared, 1)

	// Tenant2's count for the shared fingerprint should remain unchanged.
	require.Equal(t, 1, hc1.TestingLookupHash(computeHash(t, fingerprintShared)))
	require.Equal(t, 1, hc2.TestingLookupHash(computeHash(t, fingerprintShared)))

	// Delete all hints for the shared fingerprint from tenant2.
	deleteStatementHints(t, r2, fingerprintShared, 0)
	waitForUpdateOnFingerprintHash(t, ctx, hc2, fingerprintShared, 0)

	// Tenant1 should still have the hint, but tenant2 should not.
	require.Equal(t, 1, hc1.TestingLookupHash(computeHash(t, fingerprintShared)))
	require.Equal(t, 0, hc2.TestingLookupHash(computeHash(t, fingerprintShared)))
	require.Len(t, hc1.MaybeGetStatementHints(ctx, fingerprintShared), 1)
	require.Nil(t, hc2.MaybeGetStatementHints(ctx, fingerprintShared))
}

func setTestDefaults(t *testing.T, srv serverutils.TestServerInterface) {
	// These settings can only be set on the system tenant.
	r := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	r.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
	r.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10ms'")
	r.Exec(t, "SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '10ms'")
}

func createHintsCache(
	t *testing.T, ctx context.Context, ts serverutils.ApplicationLayerInterface,
) *hints.StatementHintsCache {
	hc := hints.NewStatementHintsCache(
		ts.Clock(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		ts.AppStopper(),
		ts.Codec(),
		ts.InternalDB().(descs.DB),
		ts.ClusterSettings(),
	)
	require.NoError(t, hc.Start(ctx, ts.SystemTableIDResolver().(catalog.SystemTableIDResolver)))
	return hc
}

// waitForUpdateOnFingerprintHash waits for the cache to automatically refresh
// to reflect a hint insertion or deletion.
func waitForUpdateOnFingerprintHash(
	t *testing.T,
	ctx context.Context,
	hc *hints.StatementHintsCache,
	fingerprint string,
	expected int,
) {
	hash := computeHash(t, fingerprint)
	testutils.SucceedsSoon(t, func() error {
		if count := hc.TestingLookupHash(hash); count != expected {
			return errors.Errorf("expected hash %d with count %d, got count %d", hash, expected, count)
		}
		return nil
	})
	require.Len(t, hc.MaybeGetStatementHints(ctx, fingerprint), expected)
}

// insertStatementHint inserts an empty statement hint into the
// system.statement_hints table.
func insertStatementHint(t *testing.T, r *sqlutils.SQLRunner, fingerprint string) {
	emptyHint := &hints.StatementHintUnion{}
	hintBytes, err := emptyHint.ToBytes()
	require.NoError(t, err)
	const insertStmt = `INSERT INTO system.statement_hints ("fingerprint", "hint") VALUES ($1, $2)`
	r.Exec(t, insertStmt, fingerprint, hintBytes)
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
