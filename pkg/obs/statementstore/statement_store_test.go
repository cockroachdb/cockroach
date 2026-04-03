// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package statementstore_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs/statementstore"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

// TestPutStatementDeduplicates verifies that PutStatement only caches
// a fingerprint once thanks to the in-memory cache.
func TestPutStatementDeduplicates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	store := statementstore.NewStatementStore(settings, nil)
	db, ok := srv.ApplicationLayer().InternalDB().(isql.DB)
	require.True(t, ok, "expected isql.DB")
	m := mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name: mon.MakeName("test-statement-store"),
	})
	store.SetInternalExecutor(db, m)
	store.Start(ctx, srv.Stopper())

	info := statementstore.StatementInfo{
		FingerprintID: appstatspb.StmtFingerprintID(42),
		Fingerprint:   "SELECT _",
		Database:      "defaultdb",
		Summary:       "SELECT",
		ImplicitTxn:   true,
	}

	// First call should cache and buffer for async persistence.
	store.PutStatement(ctx, info)
	require.True(t, store.TestingIsCached(info.FingerprintID))

	// Second call with same fingerprint should be a cache hit (no-op).
	store.PutStatement(ctx, info)
	require.True(t, store.TestingIsCached(info.FingerprintID))
}

// TestPutStatementDifferentFingerprints verifies that distinct fingerprints
// each get cached.
func TestPutStatementDifferentFingerprints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	store := statementstore.NewStatementStore(settings, nil)
	db, ok := srv.ApplicationLayer().InternalDB().(isql.DB)
	require.True(t, ok, "expected isql.DB")
	m := mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name: mon.MakeName("test-statement-store"),
	})
	store.SetInternalExecutor(db, m)
	store.Start(ctx, srv.Stopper())

	for i := 0; i < 5; i++ {
		store.PutStatement(ctx, statementstore.StatementInfo{
			FingerprintID: appstatspb.StmtFingerprintID(i),
			Fingerprint:   fmt.Sprintf("SELECT %d", i),
			Database:      "defaultdb",
			Summary:       "SELECT",
		})
	}

	for i := 0; i < 5; i++ {
		require.True(t, store.TestingIsCached(appstatspb.StmtFingerprintID(i)),
			"expected fingerprint %d to be cached", i)
	}
}

// TestStatementStoreIntegration verifies the end-to-end path: executing
// SQL statements causes their fingerprints to be persisted in
// system.statements.
func TestStatementStoreIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Execute some distinct statements.
	sqlDB.Exec(t, "SELECT 1")
	sqlDB.Exec(t, "SELECT 1, 2")
	sqlDB.Exec(t, "SELECT 1, 2, 3")

	// Wait for the statement store to persist to system.statements and
	// verify a specific fingerprint was stored correctly.
	testutils.SucceedsSoon(t, func() error {
		var fingerprint, db string
		row := sqlDB.DB.QueryRowContext(ctx,
			"SELECT fingerprint, db FROM system.statements WHERE fingerprint = $1",
			"SELECT _, _",
		)
		if err := row.Scan(&fingerprint, &db); err != nil {
			return err
		}
		if fingerprint != "SELECT _, _" {
			return fmt.Errorf("expected fingerprint 'SELECT _, _', got %q", fingerprint)
		}
		if db != "defaultdb" {
			return fmt.Errorf("expected db 'defaultdb', got %q", db)
		}
		return nil
	})
}

// TestStatementStoreDisabledSetting verifies that PutStatement is a
// no-op when the obs.statements.store.enabled setting is false.
func TestStatementStoreDisabledSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	store := statementstore.NewStatementStore(settings, nil)
	db, ok := srv.ApplicationLayer().InternalDB().(isql.DB)
	require.True(t, ok, "expected isql.DB")
	m := mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name: mon.MakeName("test-statement-store"),
	})
	store.SetInternalExecutor(db, m)
	store.Start(ctx, srv.Stopper())

	// Disable the store.
	statementstore.StatementStoreEnabled.Override(ctx, &settings.SV, false)

	info := statementstore.StatementInfo{
		FingerprintID: appstatspb.StmtFingerprintID(99),
		Fingerprint:   "SELECT _",
		Database:      "defaultdb",
		Summary:       "SELECT",
	}
	store.PutStatement(ctx, info)

	// The fingerprint should not have been cached.
	require.False(t, store.TestingIsCached(info.FingerprintID),
		"fingerprint should not be cached when store is disabled")
	require.Equal(t, 0, store.TestingPendingCount(),
		"pending buffer should be empty when store is disabled")
}

// TestStatementStoreBatchChunking verifies that entries are correctly
// split across multiple INSERT batches when batch_size is small.
func TestStatementStoreBatchChunking(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	// Set batch size to 2 so 5 entries require 3 batches.
	statementstore.BatchSize.Override(ctx, &settings.SV, 2)

	store := statementstore.NewStatementStore(settings, nil)
	db, ok := srv.ApplicationLayer().InternalDB().(isql.DB)
	require.True(t, ok, "expected isql.DB")
	m := mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name: mon.MakeName("test-statement-store"),
	})
	store.SetInternalExecutor(db, m)
	store.Start(ctx, srv.Stopper())

	// Enqueue 5 fingerprints.
	for i := 1; i <= 5; i++ {
		store.PutStatement(ctx, statementstore.StatementInfo{
			FingerprintID: appstatspb.StmtFingerprintID(i),
			Fingerprint:   fmt.Sprintf("SELECT %d", i),
			Database:      "defaultdb",
			Summary:       "SELECT",
			ImplicitTxn:   true,
		})
	}
	require.Equal(t, 5, store.TestingPendingCount())

	// Synchronously flush — this exercises the chunking path.
	store.TestingFlushPending(ctx)

	// All 5 should have been persisted across 3 batches.
	testutils.SucceedsSoon(t, func() error {
		row, err := db.Executor().QueryRowEx(ctx,
			"count-persisted", nil,
			sessiondata.NoSessionDataOverride,
			"SELECT count(*) FROM system.statements WHERE fingerprint LIKE 'SELECT %'",
		)
		if err != nil {
			return err
		}
		if row == nil {
			return fmt.Errorf("no rows returned")
		}
		count := int(tree.MustBeDInt(row[0]))
		if count < 5 {
			return fmt.Errorf("expected 5 persisted fingerprints, got %d", count)
		}
		return nil
	})
}

// TestStatementStoreFlushFailureEvictsCache verifies that when a batch
// INSERT fails, the affected entries are evicted from the cache so they
// can be retried on the next occurrence.
func TestStatementStoreFlushFailureEvictsCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	store := statementstore.NewStatementStore(settings, &statementstore.TestingKnobs{
		BatchInsertOverride: func(
			_ context.Context, _ []statementstore.StatementInfo,
		) error {
			return errors.New("injected flush failure")
		},
	})
	db, ok := srv.ApplicationLayer().InternalDB().(isql.DB)
	require.True(t, ok, "expected isql.DB")
	m := mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name: mon.MakeName("test-statement-store"),
	})
	store.SetInternalExecutor(db, m)
	store.Start(ctx, srv.Stopper())

	info := statementstore.StatementInfo{
		FingerprintID: appstatspb.StmtFingerprintID(123),
		Fingerprint:   "UPDATE _ SET _ = _",
		Database:      "defaultdb",
		Summary:       "UPDATE",
		ImplicitTxn:   true,
	}

	// Enqueue and verify cached.
	store.PutStatement(ctx, info)
	require.True(t, store.TestingIsCached(info.FingerprintID))

	// Flush — the injected error should cause eviction from cache.
	store.TestingFlushPending(ctx)

	require.False(t, store.TestingIsCached(info.FingerprintID),
		"fingerprint should be evicted from cache after flush failure")
}
