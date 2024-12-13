// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	apd "github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMrSystemDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "runs too slow")

	ctx := context.Background()

	// Enable settings required for configuring a tenant's system database as multi-region.
	makeSettings := func() *cluster.Settings {
		cs := cluster.MakeTestingClusterSettings()
		instancestorage.ReclaimLoopInterval.Override(ctx, &cs.SV, 150*time.Millisecond)
		return cs
	}

	cluster, systemSQL, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(t, 3,
		base.TestingKnobs{},
		multiregionccltestutils.WithSettings(makeSettings()))
	defer cleanup()

	id, err := roachpb.MakeTenantID(11)
	require.NoError(t, err)

	tenantArgs := base.TestTenantArgs{
		Settings: makeSettings(),
		TenantID: id,
		Locality: cluster.Servers[0].Locality(),
	}
	ts, tenantSQL := serverutils.StartTenant(t, cluster.Servers[0], tenantArgs)

	tDB := sqlutils.MakeSQLRunner(tenantSQL)

	// Generate stats for system.sqlinstances. See the "QueryByEnum" test for
	// details.
	tDB.Exec(t, `ANALYZE system.sqlliveness;`)

	tDB.Exec(t, `ALTER DATABASE system SET PRIMARY REGION "us-east1"`)
	tDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east2"`)
	tDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east3"`)

	tDB.CheckQueryResults(t, "SELECT create_statement FROM [SHOW CREATE DATABASE system]", [][]string{
		{"CREATE DATABASE system PRIMARY REGION \"us-east1\" REGIONS = \"us-east1\", \"us-east2\", \"us-east3\" SURVIVE REGION FAILURE"},
	})

	// Run schema validations to ensure the manual descriptor modifications are
	// okay.
	tDB.CheckQueryResults(t, `SELECT * FROM crdb_internal.invalid_objects`, [][]string{})

	sDB := sqlutils.MakeSQLRunner(systemSQL)

	sDB.Exec(t, `ANALYZE system.sqlliveness;`)
	sDB.Exec(t, `SET CLUSTER SETTING sql.multiregion.system_database_multiregion.enabled = true`)
	sDB.Exec(t, `ALTER DATABASE system SET PRIMARY REGION "us-east1"`)
	sDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east2"`)
	sDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east3"`)

	testCases := []struct {
		name     string
		database *sqlutils.SQLRunner
	}{
		{
			name:     "system database",
			database: sDB,
		},
		{
			name:     "tenant database",
			database: tDB,
		},
	}
	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("Sqlliveness %s", testCase.name), func(t *testing.T) {
			// When optimizing the system database the ALTER DATABASE command will
			// delete stats, but these are refreshed in memory using a range feed.
			// Since there can be a delay in the new stats being picked up its possible
			// for this query to fail with:
			// "unsupported comparison: bytes to crdb_internal_region"
			// querying table statistics. This is a transient condition that will
			// clear up once the range feed catches up.
			testutils.SucceedsSoon(t, func() error {
				row := testCase.database.DB.QueryRowContext(ctx, `SELECT crdb_region, session_id, expiration FROM system.sqlliveness LIMIT 1`)
				var sessionID string
				var crdbRegion string
				var rawExpiration apd.Decimal
				err := row.Scan(&crdbRegion, &sessionID, &rawExpiration)
				if err != nil {
					return err
				}
				if crdbRegion != "us-east1" {
					return errors.AssertionFailedf("unexpected region, got: %q expected: %q",
						crdbRegion, "us-east1")
				}
				return nil
			})
		})

		t.Run(fmt.Sprintf("Sqlinstances %s", testCase.name), func(t *testing.T) {
			t.Run("InUse", func(t *testing.T) {
				query := `
                SELECT id, addr, session_id, locality, crdb_region
                FROM system.sql_instances
                WHERE session_id IS NOT NULL
            `
				rows := testCase.database.Query(t, query)
				require.True(t, rows.Next())
				for {
					var id base.SQLInstanceID
					var addr, locality string
					var crdb_region string
					var session sqlliveness.SessionID

					require.NoError(t, rows.Scan(&id, &addr, &session, &locality, &crdb_region))

					require.True(t, 0 < id)
					require.NotEmpty(t, addr)
					require.NotEmpty(t, locality)
					require.NotEmpty(t, session)
					require.NotEmpty(t, crdb_region)

					require.Equal(t, "us-east1", crdb_region)

					if !rows.Next() {
						break
					}
				}
				require.NoError(t, rows.Close())
			})

			t.Run(fmt.Sprintf("Preallocated %s", testCase.name), func(t *testing.T) {
				query := `
                SELECT id, addr, session_id, locality, crdb_region
                FROM system.sql_instances
                WHERE session_id IS NULL
            `
				rows := testCase.database.Query(t, query)
				require.True(t, rows.Next())
				for {
					var id base.SQLInstanceID
					var addr, locality, session gosql.NullString
					var crdb_region string

					require.NoError(t, rows.Scan(&id, &addr, &session, &locality, &crdb_region))

					require.True(t, 0 < id)
					require.False(t, addr.Valid)
					require.False(t, locality.Valid)
					require.False(t, session.Valid)
					require.NotEmpty(t, crdb_region)

					if !rows.Next() {
						break
					}
				}
				require.NoError(t, rows.Close())

				query = `
				SELECT count(id), crdb_region
				FROM system.sql_instances
				WHERE session_id IS NULL GROUP BY crdb_region
			`
				preallocatedCount := instancestorage.PreallocatedCount.Get(&ts.ClusterSettings().SV)
				testutils.SucceedsSoon(t, func() error {
					rows := testCase.database.Query(t, query)
					require.True(t, rows.Next())

					countMap := map[string]int{}
					for {
						var count int
						var crdb_region string

						require.NoError(t, rows.Scan(&count, &crdb_region))
						countMap[crdb_region] = count

						if !rows.Next() {
							break
						}
					}
					require.NoError(t, rows.Close())
					if len(countMap) != 3 {
						return errors.New("some regions have not been preallocated")
					}
					for _, r := range []string{"us-east1", "us-east2", "us-east3"} {
						c, ok := countMap[r]
						require.True(t, ok)
						if c != int(preallocatedCount) {
							return errors.Newf("require %d, but got %d", preallocatedCount, c)
						}
					}
					return nil
				})
			})

			t.Run(fmt.Sprintf("Reclaim %s", testCase.name), func(t *testing.T) {
				id := uuid.MakeV4()
				s1, err := slstorage.MakeSessionID(make([]byte, 100), id)
				require.NoError(t, err)
				s2, err := slstorage.MakeSessionID(make([]byte, 200), id)
				require.NoError(t, err)

				// Insert expired entries into sql_instances.
				testCase.database.Exec(t, `INSERT INTO system.sql_instances (id, addr, session_id, locality, crdb_region) VALUES
		   		(100, NULL, $1, NULL, 'us-east2'),
		   		(200, NULL, $2, NULL, 'us-east3')`, s1.UnsafeBytes(), s2.UnsafeBytes())

				query := `SELECT count(*) FROM system.sql_instances WHERE id = 42`

				// Wait until expired entries get removed.
				testutils.SucceedsSoon(t, func() error {
					var rowCount int
					testCase.database.QueryRow(t, query).Scan(&rowCount)
					if rowCount != 0 {
						return errors.New("some regions have not been reclaimed")
					}
					return nil
				})
			})
		})

		t.Run(fmt.Sprintf("GlobalTables %s", testCase.name), func(t *testing.T) {
			query := `
		    SELECT target
			FROM [SHOW ALL ZONE CONFIGURATIONS]
			WHERE target LIKE 'TABLE system.public.%'
			    AND raw_config_sql LIKE '%global_reads = true%'
			ORDER BY target;
		`
			testCase.database.CheckQueryResults(t, query, [][]string{
				{"TABLE system.public.comments"},
				{"TABLE system.public.database_role_settings"},
				{"TABLE system.public.descriptor"},
				{"TABLE system.public.namespace"},
				{"TABLE system.public.privileges"},
				{"TABLE system.public.region_liveness"},
				{"TABLE system.public.role_members"},
				{"TABLE system.public.role_options"},
				{"TABLE system.public.settings"},
				{"TABLE system.public.table_statistics"},
				{"TABLE system.public.users"},
				{"TABLE system.public.web_sessions"},
				{"TABLE system.public.zones"},
			})
		})

		t.Run("RegionTables", func(t *testing.T) {
			query := `
		    SELECT target
			FROM [SHOW ALL ZONE CONFIGURATIONS]
			WHERE target LIKE 'TABLE system.public.%'
			    AND raw_config_sql NOT LIKE '%global_reads = true%'
					AND target = 'TABLE system.public.locations'
			ORDER BY target;
		`
			tDB.CheckQueryResults(t, query, [][]string{
				{"TABLE system.public.locations"},
			})

			sDB.CheckQueryResults(t, query, [][]string{
				{"TABLE system.public.locations"},
			})
		})

		t.Run(fmt.Sprintf("QueryByEnum %s", testCase.name), func(t *testing.T) {
			// This is a regression test for a bug triggered by setting up the system
			// database. If the operation to configure the does not clear table
			// statistics, this query will fail in the optimizer, because the stats will
			// have the wrong type for the crdb_region column. Since stats are generated
			// asynchronously, we poll for the results until they are correct.
			testutils.SucceedsSoon(t, func() error {
				var sessionID string
				var crdbRegion string
				var rawExpiration apd.Decimal
				err := tenantSQL.QueryRow(`
				SELECT crdb_region, session_id, expiration 
				FROM system.sqlliveness 
				WHERE crdb_region = 'us-east1'
				LIMIT 1;`).Scan(&crdbRegion, &sessionID, &rawExpiration)
				if err != nil {
					return err
				}
				if crdbRegion != "us-east1" {
					return errors.Newf("expected region to be us-east1; got %s", crdbRegion)
				}
				return nil
			})

		})
	}
}

// TestMultiRegionTenantRegions tests the behavior of region-related
// commands in the context of a multi-region tenant (a tenant with a
// multi-region system database).
func TestMultiRegionTenantRegions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /*numServers*/, base.TestingKnobs{},
	)
	defer cleanup()

	ctx := context.Background()
	ten, tSQL := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID: serverutils.TestTenantID(),
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
			},
		},
	})
	defer ten.AppStopper().Stop(ctx)
	defer tSQL.Close()
	tenSQLDB := sqlutils.MakeSQLRunner(tSQL)

	// Update system database with regions.
	checkRegions := func(t *testing.T, regions ...string) {
		var res [][]string
		for _, r := range regions {
			res = append(res, []string{r})
		}
		tenSQLDB.CheckQueryResults(t, "SELECT region FROM [SHOW REGIONS] ORDER BY region ASC", res)
	}

	// Note that before we've made this a multi-region tenant, because we've
	// enabled the cluster setting, we can see all the host cluster regions,
	// and we can create databases using them.
	checkRegions(t, "us-east1", "us-east2", "us-east3")
	tenSQLDB.Exec(t, `CREATE DATABASE db PRIMARY REGION "us-east2"`)
	tenSQLDB.Exec(t, `ALTER DATABASE db ADD REGION "us-east1"`)
	tenSQLDB.Exec(t, `DROP DATABASE db`)

	// Convert the tenant to a multi-region tenant by adding a primary region
	// to the system database.  Ensure that the regions show up as they are added.
	tenSQLDB.Exec(t, `ALTER DATABASE system SET PRIMARY REGION "us-east1"`)
	checkRegions(t, "us-east1")

	// Check that regions which are not part of the database cannot be used
	// until they are added to the system database.
	tenSQLDB.ExpectErr(t, `region "us-east2" does not exist`,
		`CREATE DATABASE db PRIMARY REGION "us-east2"`)
	tenSQLDB.Exec(t, `CREATE DATABASE db PRIMARY REGION "us-east1"`)

	tenSQLDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east2"`)
	checkRegions(t, "us-east1", "us-east2")
	tenSQLDB.ExpectErr(t, `region "us-east3" does not exist`,
		`CREATE DATABASE db2 PRIMARY REGION "us-east3"`)
	tenSQLDB.ExpectErr(t, `region "us-east3" does not exist`,
		`ALTER DATABASE db ADD REGION "us-east3"`)
	tenSQLDB.Exec(t, `ALTER DATABASE db ADD REGION "us-east2"`)

	// Check that a region cannot be dropped from the system database while
	// it is in use in any database in that tenant.
	tenSQLDB.ExpectErr(t, `(?s)cannot drop region "us-east2" from the system `+
		`database while that region is still in use\s+HINT: region is in use by `+
		`databases: db`,
		`ALTER DATABASE system DROP REGION "us-east2"`)
	tenSQLDB.Exec(t, `ALTER DATABASE db DROP REGION "us-east2"`)
	tenSQLDB.Exec(t, `ALTER DATABASE system DROP REGION "us-east2"`)

	tenSQLDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east3"`)
	checkRegions(t, "us-east1", "us-east3")
	tenSQLDB.Exec(t, `ALTER DATABASE db ADD REGION "us-east3"`)
}

func TestTenantStartupWithMultiRegionEnum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /*numServers*/, base.TestingKnobs{},
	)
	defer cleanup()

	tenID := roachpb.MustMakeTenantID(10)
	ten, tSQL := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID: tenID,
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
			},
		},
	})
	defer tSQL.Close()
	tenSQLDB := sqlutils.MakeSQLRunner(tSQL)

	// Update system database with regions.
	tenSQLDB.Exec(t, `ALTER DATABASE system SET PRIMARY REGION "us-east1"`)
	tenSQLDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east2"`)
	tenSQLDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east3"`)

	ten2, tSQL2 := serverutils.StartTenant(t, tc.Server(2), base.TestTenantArgs{
		TenantID: tenID,
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east3"},
			},
		},
	})
	defer tSQL2.Close()
	tenSQLDB2 := sqlutils.MakeSQLRunner(tSQL2)

	// The sqlliveness entry created by the first SQL server has enum.One as the
	// region as the system database hasn't been updated when it first started.
	var sessionID string
	tenSQLDB2.QueryRow(t, `SELECT session_id FROM system.sql_instances WHERE id = $1`,
		ten.SQLInstanceID()).Scan(&sessionID)
	region, _, err := slstorage.UnsafeDecodeSessionID(sqlliveness.SessionID(sessionID))
	require.NoError(t, err)
	require.Equal(t, enum.One, region)
	ten1SessionID := sessionID

	// Ensure that the sqlliveness entry created by the second SQL server has
	// the right region and session UUID.
	tenSQLDB2.QueryRow(t, `SELECT session_id FROM system.sql_instances WHERE id = $1`,
		ten2.SQLInstanceID()).Scan(&sessionID)
	region, _, err = slstorage.UnsafeDecodeSessionID(sqlliveness.SessionID(sessionID))
	require.NoError(t, err)
	require.NotEqual(t, enum.One, region)

	rows := tenSQLDB2.Query(t, `SELECT crdb_region, session_id FROM system.sqlliveness`)
	defer rows.Close()
	livenessMap := map[string]string{}
	for rows.Next() {
		var region, ID string
		require.NoError(t, rows.Scan(&region, &ID))
		livenessMap[ID] = region
	}
	require.NoError(t, rows.Err())
	{
		r, ok := livenessMap[sessionID]
		require.True(t, ok)
		require.Equal(t, r, "us-east3")
	}
	{
		r, ok := livenessMap[ten1SessionID]
		require.True(t, ok)
		require.Equal(t, r, "us-east1")
	}

	// Validate that the zone configuration contains the appropriate constraints.q
	tenSQLDB.CheckQueryResults(t, "SELECT raw_config_sql FROM [SHOW ZONE CONFIGURATIONS] WHERE target LIKE 'PARTITION %lease%' ORDER BY target;", [][]string{
		{"ALTER PARTITION \"us-east1\" OF INDEX system.public.lease@primary CONFIGURE ZONE USING\n\tnum_voters = 3,\n\tvoter_constraints = '[+region=us-east1]',\n\tlease_preferences = '[[+region=us-east1]]'"},
		{"ALTER PARTITION \"us-east2\" OF INDEX system.public.lease@primary CONFIGURE ZONE USING\n\tnum_voters = 3,\n\tvoter_constraints = '[+region=us-east2]',\n\tlease_preferences = '[[+region=us-east2]]'"},
		{"ALTER PARTITION \"us-east3\" OF INDEX system.public.lease@primary CONFIGURE ZONE USING\n\tnum_voters = 3,\n\tvoter_constraints = '[+region=us-east3]',\n\tlease_preferences = '[[+region=us-east3]]'"},
	})
}

func TestMrSystemDatabaseUpgrade(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Enable settings required for configuring a tenant's system database as multi-region.
	makeSettings := func() *cluster.Settings {
		cs := cluster.MakeTestingClusterSettingsWithVersions(clusterversion.Latest.Version(),
			clusterversion.MinSupported.Version(),
			false)
		instancestorage.ReclaimLoopInterval.Override(ctx, &cs.SV, 150*time.Millisecond)
		return cs
	}

	cluster, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(t, 3,
		base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         clusterversion.MinSupported.Version(),
			},
		},
		multiregionccltestutils.WithSettings(makeSettings()))
	defer cleanup()
	id, err := roachpb.MakeTenantID(11)
	require.NoError(t, err)

	// Disable license enforcement for this test.
	for _, s := range cluster.Servers {
		s.ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer.Disable(ctx)
	}

	tenantArgs := base.TestTenantArgs{
		Settings: makeSettings(),
		TestingKnobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         clusterversion.MinSupported.Version(),
			},
		},
		TenantID: id,
		Locality: cluster.Servers[0].Locality(),
	}
	appLayer, tenantSQL := serverutils.StartTenant(t, cluster.Servers[0], tenantArgs)
	appLayer.ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer.Disable(ctx)

	tDB := sqlutils.MakeSQLRunner(tenantSQL)

	tDB.Exec(t, `ALTER DATABASE system SET PRIMARY REGION "us-east1"`)
	tDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east2"`)
	tDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east3"`)
	tDB.Exec(t, `ALTER DATABASE defaultdb SET PRIMARY REGION "us-east1"`)
	tDB.Exec(t, `ALTER DATABASE defaultdb ADD REGION "us-east2"`)
	tDB.Exec(t, `ALTER DATABASE defaultdb ADD REGION "us-east3"`)
	tDB.Exec(t, "ALTER DATABASE defaultdb SURVIVE REGION FAILURE")

	tDB.CheckQueryResults(t, "SELECT create_statement FROM [SHOW CREATE DATABASE system]", [][]string{
		{"CREATE DATABASE system PRIMARY REGION \"us-east1\" REGIONS = \"us-east1\", \"us-east2\", \"us-east3\" SURVIVE REGION FAILURE"},
	})

	_, err = cluster.Conns[0].Exec("SET CLUSTER SETTING version = crdb_internal.node_executable_version();")
	require.NoError(t, err)
	tDB.Exec(t, "SET CLUSTER SETTING version = crdb_internal.node_executable_version();")

	tDB.CheckQueryResults(t, "SELECT create_statement FROM [SHOW CREATE DATABASE system]", [][]string{
		{"CREATE DATABASE system PRIMARY REGION \"us-east1\" REGIONS = \"us-east1\", \"us-east2\", \"us-east3\" SURVIVE REGION FAILURE"},
	})
	tDB.CheckQueryResults(t, "SELECT raw_config_sql FROM [SHOW ZONE CONFIGURATIONS] WHERE target LIKE 'PARTITION %lease%' ORDER BY target;", [][]string{
		{"ALTER PARTITION \"us-east1\" OF INDEX system.public.lease@primary CONFIGURE ZONE USING\n\tnum_voters = 3,\n\tvoter_constraints = '[+region=us-east1]',\n\tlease_preferences = '[[+region=us-east1]]'"},
		{"ALTER PARTITION \"us-east2\" OF INDEX system.public.lease@primary CONFIGURE ZONE USING\n\tnum_voters = 3,\n\tvoter_constraints = '[+region=us-east2]',\n\tlease_preferences = '[[+region=us-east2]]'"},
		{"ALTER PARTITION \"us-east3\" OF INDEX system.public.lease@primary CONFIGURE ZONE USING\n\tnum_voters = 3,\n\tvoter_constraints = '[+region=us-east3]',\n\tlease_preferences = '[[+region=us-east3]]'"},
	})
}

func TestMrSystemDatabaseDropRegion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Enable settings required for configuring a tenant's system database as multi-region.
	makeSettings := func() *cluster.Settings {
		cs := cluster.MakeTestingClusterSettingsWithVersions(clusterversion.Latest.Version(),
			clusterversion.MinSupported.Version(),
			false)
		instancestorage.ReclaimLoopInterval.Override(ctx, &cs.SV, 150*time.Millisecond)
		return cs
	}

	cluster, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(t, 3,
		base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         clusterversion.MinSupported.Version(),
			},
		},
		multiregionccltestutils.WithSettings(makeSettings()))
	defer cleanup()
	id, err := roachpb.MakeTenantID(11)
	require.NoError(t, err)

	// Disable license enforcement for this test.
	for _, s := range cluster.Servers {
		s.ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer.Disable(ctx)
	}

	tenantArgs := base.TestTenantArgs{
		Settings: makeSettings(),
		TestingKnobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         clusterversion.MinSupported.Version(),
			},
		},
		TenantID: id,
		Locality: cluster.Servers[0].Locality(),
	}
	appLayer, tenantSQL := serverutils.StartTenant(t, cluster.Servers[0], tenantArgs)
	appLayer.ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer.Disable(ctx)

	tDB := sqlutils.MakeSQLRunner(tenantSQL)

	tDB.Exec(t, `ALTER DATABASE system SET PRIMARY REGION "us-east1"`)
	tDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east2"`)
	tDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east3"`)
	tDB.Exec(t, `ALTER DATABASE defaultdb SET PRIMARY REGION "us-east1"`)
	tDB.Exec(t, `ALTER DATABASE defaultdb ADD REGION "us-east2"`)
	tDB.Exec(t, `ALTER DATABASE defaultdb ADD REGION "us-east3"`)

	tDB.CheckQueryResults(t, "SELECT create_statement FROM [SHOW CREATE DATABASE system]", [][]string{
		{"CREATE DATABASE system PRIMARY REGION \"us-east1\" REGIONS = \"us-east1\", \"us-east2\", \"us-east3\" SURVIVE REGION FAILURE"},
	})

	tDB.ExpectErr(t, "region is still in use", `ALTER DATABASE system DROP REGION "us-east3"`)
	tDB.Exec(t, `ALTER DATABASE defaultdb DROP REGION "us-east3"`)
	tDB.Exec(t, `ALTER DATABASE system DROP REGION "us-east3"`)

	tDB.CheckQueryResults(t, `SELECT count(*) FROM system.sql_instances WHERE crdb_region != 'us-east1'::system.public.crdb_internal_region AND crdb_region != 'us-east2'::system.public.crdb_internal_region`, [][]string{
		{"0"},
	})
	tDB.CheckQueryResults(t, `SELECT count(*) FROM system.sqlliveness WHERE crdb_region != 'us-east1'::system.public.crdb_internal_region AND crdb_region != 'us-east2'::system.public.crdb_internal_region`, [][]string{
		{"0"},
	})
	tDB.CheckQueryResults(t, `SELECT count(*) FROM system.region_liveness WHERE crdb_region != 'us-east1'::system.public.crdb_internal_region AND crdb_region != 'us-east2'::system.public.crdb_internal_region`, [][]string{
		{"0"},
	})
	tDB.CheckQueryResults(t, `SELECT count(*) FROM system.lease WHERE crdb_region != 'us-east1'::system.public.crdb_internal_region AND crdb_region != 'us-east2'::system.public.crdb_internal_region`, [][]string{
		{"0"},
	})
}
