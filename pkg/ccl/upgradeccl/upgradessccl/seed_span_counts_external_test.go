// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package upgradessccl_test

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestPreSeedSpanCountTable tests that incremental schema changes after
// PreSeedSpanCountTable is enabled get tracked as such. It also tests that once
// SeedSpanCountTable is reached, the span count is updated to capture the most
// up-to-date view of all schema objects. Specifically, we're not
// double-counting the incremental update we tracked in the
// PreSeedSpanCountTable state.
func TestPreSeedSpanCountTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		v0 = clusterversion.ByKey(clusterversion.SpanCountTable)
		v1 = clusterversion.ByKey(clusterversion.PreSeedSpanCountTable)
		v2 = clusterversion.ByKey(clusterversion.SeedSpanCountTable)
	)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(v2, v0, false /* initializeVersion */)
	require.NoError(t, clusterversion.Initialize(ctx, v0, &settings.SV))

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          v0,
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)
	hostDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	tenantID := roachpb.MakeTenantID(10)
	tenantSettings := cluster.MakeTestingClusterSettingsWithVersions(v2, v0, false /* initializeVersion */)
	require.NoError(t, clusterversion.Initialize(ctx, v0, &tenantSettings.SV))
	tenant, err := ts.StartTenant(ctx, base.TestTenantArgs{
		TenantID:     tenantID,
		TestingKnobs: base.TestingKnobs{},
		Settings:     tenantSettings,
	})
	require.NoError(t, err)

	pgURL, cleanupPGUrl := sqlutils.PGUrl(t, tenant.SQLAddr(), "Tenant", url.User(username.RootUser))
	defer cleanupPGUrl()

	tenantSQLDB, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer func() { require.NoError(t, tenantSQLDB.Close()) }()

	// Upgrade the host cluster all the way.
	hostDB.Exec(t, "SET CLUSTER SETTING version = $1", v2.String())

	var spanCount, numRows int
	tenantDB := sqlutils.MakeSQLRunner(tenantSQLDB)

	tenantDB.CheckQueryResults(t, "SHOW CLUSTER SETTING version", [][]string{{v0.String()}})
	tenantDB.Exec(t, `CREATE TABLE t(k INT PRIMARY KEY)`)
	tenantDB.QueryRow(t, `SELECT count(*) FROM system.span_count`).Scan(&numRows)
	require.Equal(t, 0, numRows)

	tenantDB.Exec(t, "SET CLUSTER SETTING version = $1", v1.String())
	tenantDB.CheckQueryResults(t, "SHOW CLUSTER SETTING version", [][]string{{v1.String()}})
	tenantDB.Exec(t, `CREATE INDEX idx ON t (k)`)
	tenantDB.QueryRow(t, `SELECT span_count FROM system.span_count LIMIT 1`).Scan(&spanCount)
	require.Equal(t, 2, spanCount)

	tenantDB.Exec(t, "SET CLUSTER SETTING version = $1", v2.String())
	tenantDB.CheckQueryResults(t, "SHOW CLUSTER SETTING version", [][]string{{v2.String()}})
	tenantDB.QueryRow(t, `SELECT span_count FROM system.span_count LIMIT 1`).Scan(&spanCount)
	require.Equal(t, 5, spanCount)
}

// TestSeedSpanCountTable tests that the upgrade seeds system.span_count
// correctly for secondary tenants.
func TestSeedSpanCountTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		v0 = clusterversion.ByKey(clusterversion.SpanCountTable)
		v2 = clusterversion.ByKey(clusterversion.SeedSpanCountTable)
	)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(v2, v0, false /* initializeVersion */)
	require.NoError(t, clusterversion.Initialize(ctx, v0, &settings.SV))

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          v0,
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)
	hostDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	tenantID := roachpb.MakeTenantID(10)
	tenantSettings := cluster.MakeTestingClusterSettingsWithVersions(v2, v0, false /* initializeVersion */)
	require.NoError(t, clusterversion.Initialize(ctx, v0, &tenantSettings.SV))
	tenant, err := ts.StartTenant(ctx, base.TestTenantArgs{
		TenantID:     tenantID,
		TestingKnobs: base.TestingKnobs{},
		Settings:     tenantSettings,
	})
	require.NoError(t, err)

	pgURL, cleanupPGUrl := sqlutils.PGUrl(t, tenant.SQLAddr(), "Tenant", url.User(username.RootUser))
	defer cleanupPGUrl()

	tenantSQLDB, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer func() { require.NoError(t, tenantSQLDB.Close()) }()

	tenantDB := sqlutils.MakeSQLRunner(tenantSQLDB)
	tenantDB.CheckQueryResults(t, "SHOW CLUSTER SETTING version", [][]string{{v0.String()}})

	// Upgrade the host cluster.
	hostDB.Exec(t, "SET CLUSTER SETTING version = $1", v2.String())
	tenantDB.CheckQueryResults(t, "SHOW CLUSTER SETTING version", [][]string{{v0.String()}})

	tenantDB.Exec(t, `CREATE TABLE t(k INT PRIMARY KEY)`)

	var spanCount, numRows int
	tenantDB.QueryRow(t, `SELECT count(*) FROM system.span_count`).Scan(&numRows)
	require.Equal(t, 0, numRows)

	tenantDB.Exec(t, "SET CLUSTER SETTING version = $1", v2.String())
	tenantDB.QueryRow(t, `SELECT span_count FROM system.span_count LIMIT 1`).Scan(&spanCount)
	require.Equal(t, 3, spanCount)
}

// TestSeedSpanCountTableOverLimit tests that the upgrade seeds
// system.span_count correctly for secondary tenants, even if over the
// proscribed limit. In these cases the tenant goes into debt -- all subsequent
// schema changes that add schema elements will be rejected. Attempts to free up
// spans however will be accepted.
func TestSeedSpanCountTableOverLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		v0 = clusterversion.ByKey(clusterversion.SpanCountTable)
		v2 = clusterversion.ByKey(clusterversion.SeedSpanCountTable)
	)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(v2, v0, false /* initializeVersion */)
	require.NoError(t, clusterversion.Initialize(ctx, v0, &settings.SV))

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          v0,
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)
	hostDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	const limit = 1
	tenantID := roachpb.MakeTenantID(10)
	tenantSettings := cluster.MakeTestingClusterSettingsWithVersions(v2, v0, false /* initializeVersion */)
	require.NoError(t, clusterversion.Initialize(ctx, v0, &tenantSettings.SV))
	tenant, err := ts.StartTenant(ctx, base.TestTenantArgs{
		TenantID: tenantID,
		TestingKnobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				ExcludeDroppedDescriptorsFromLookup: true,
				LimiterLimitOverride: func() int64 {
					return limit
				},
			},
		},
		Settings: tenantSettings,
	})
	require.NoError(t, err)

	pgURL, cleanupPGUrl := sqlutils.PGUrl(t, tenant.SQLAddr(), "Tenant", url.User(username.RootUser))
	defer cleanupPGUrl()

	tenantSQLDB, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer func() { require.NoError(t, tenantSQLDB.Close()) }()

	// Upgrade the host cluster.
	hostDB.Exec(t, "SET CLUSTER SETTING version = $1", v2.String())

	tenantDB := sqlutils.MakeSQLRunner(tenantSQLDB)
	tenantDB.Exec(t, `CREATE TABLE t1(k INT PRIMARY KEY)`)
	tenantDB.Exec(t, `CREATE TABLE t2(k INT PRIMARY KEY)`)
	tenantDB.Exec(t, `CREATE TABLE t3(k INT PRIMARY KEY)`)

	var spanCount int
	tenantDB.Exec(t, "SET CLUSTER SETTING version = $1", v2.String())
	tenantDB.QueryRow(t, `SELECT span_count FROM system.span_count LIMIT 1`).Scan(&spanCount)
	require.Equal(t, 9, spanCount)

	_, err = tenantDB.DB.ExecContext(ctx, `CREATE TABLE t4(k INT PRIMARY KEY)`)
	require.True(t, testutils.IsError(err, "exceeded limit for number of table spans"))

	tenantDB.Exec(t, `DROP TABLE t3`)
	tenantDB.QueryRow(t, `SELECT span_count FROM system.span_count LIMIT 1`).Scan(&spanCount)
	require.Equal(t, 6, spanCount)
}
