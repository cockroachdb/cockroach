package changefeedccl

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestChangefeedTenants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	kvServer, kvSQLdb, cleanup := startTestFullServer(t, makeOptions(t, withArgsFn(func(args *base.TestServerArgs) {
		args.ExternalIODirConfig.DisableOutbound = true
	})))
	defer cleanup()

	tenantArgs := base.TestTenantArgs{
		// crdb_internal.create_tenant called by StartTenant
		TenantID: serverutils.TestTenantID(),
		// Non-enterprise changefeeds are currently only
		// disabled by setting DisableOutbound true
		// everywhere.
		ExternalIODirConfig: base.ExternalIODirConfig{
			DisableOutbound: true,
		},
		UseDatabase: `d`,
	}

	tenantServer, tenantDB := serverutils.StartTenant(t, kvServer, tenantArgs)
	tenantSQL := sqlutils.MakeSQLRunner(tenantDB)
	tenantSQL.ExecMultiple(t, strings.Split(tenantSetupStatements, ";")...)
	tenantSQL.Exec(t, `CREATE TABLE foo_in_tenant (pk INT PRIMARY KEY)`)
	t.Run("changefeed on non-tenant table fails", func(t *testing.T) {
		kvSQL := sqlutils.MakeSQLRunner(kvSQLdb)
		kvSQL.Exec(t, `CREATE TABLE d.foo (pk INT PRIMARY KEY)`)

		tenantSQL.ExpectErr(t, `table "foo" does not exist`,
			`CREATE CHANGEFEED FOR foo`,
		)
	})
	t.Run("sinkful changefeed fails", func(t *testing.T) {
		tenantSQL.ExpectErr(t, "Outbound IO is disabled by configuration, cannot create changefeed into kafka",
			`CREATE CHANGEFEED FOR foo_in_tenant INTO 'kafka://does-not-matter'`,
		)
	})
	t.Run("sinkless changefeed works", func(t *testing.T) {
		coreFeedFactory, cleanup := makeFeedFactory(t, "sinkless", tenantServer, tenantDB)
		defer cleanup()
		tenantSQL.Exec(t, `INSERT INTO foo_in_tenant VALUES (1)`)
		feed := feed(t, coreFeedFactory, `CREATE CHANGEFEED FOR foo_in_tenant`)
		assertPayloads(t, feed, []string{
			`foo_in_tenant: [1]->{"after": {"pk": 1}}`,
		})
	})
}

func TestChangefeedTenantsExternalIOEnabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, cleanup := makeTenantServer(t, withArgsFn(func(args *base.TestServerArgs) {
		args.ExternalIODirConfig.DisableOutbound = true
	}))
	defer cleanup()

	tenantSQL := sqlutils.MakeSQLRunner(s.DB)
	tenantSQL.Exec(t, `CREATE TABLE foo_in_tenant (pk INT PRIMARY KEY)`)

	t.Run("sinkful changefeed works", func(t *testing.T) {
		f, cleanup := makeFeedFactory(t, "kafka", s.Server, s.DB)
		defer cleanup()
		tenantSQL.Exec(t, `INSERT INTO foo_in_tenant VALUES (1)`)
		feed := feed(t, f, `CREATE CHANGEFEED FOR foo_in_tenant`)
		defer closeFeed(t, feed)
		assertPayloads(t, feed, []string{
			`foo_in_tenant: [1]->{"after": {"pk": 1}}`,
		})
	})
}

func TestChangefeedRestartMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cluster, db, cleanup := startTestCluster(t)
	defer cleanup()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE test_tab (a INT PRIMARY KEY, b INT UNIQUE NOT NULL) WITH (schema_locked = false)`)
	sqlDB.Exec(t, `INSERT INTO test_tab VALUES (0, 0)`)

	row := sqlDB.QueryRow(t, `SELECT range_id, lease_holder FROM [SHOW RANGES FROM TABLE test_tab WITH DETAILS] LIMIT 1`)
	var rangeID, leaseHolder int
	row.Scan(&rangeID, &leaseHolder)

	// Start the changefeed on a node other than the leaseholder
	// so that it is likely that the changeAggregator and
	// changeFrontier are on different nodes.
	feedServerID := ((leaseHolder - 1) + 1) % 3
	t.Logf("Range %d is on lease holder %d, running rangefeed on server %d (store id: %d)", rangeID, leaseHolder, feedServerID, cluster.Server(feedServerID).GetFirstStoreID())
	db = cluster.ServerConn(feedServerID)
	sqlDB = sqlutils.MakeSQLRunner(db)

	f := makeKafkaFeedFactory(t, cluster, db)
	feed := feed(t, f, "CREATE CHANGEFEED FOR test_tab WITH updated")
	defer closeFeed(t, feed)
	assertPayloadsStripTs(t, feed, []string{
		`test_tab: [0]->{"after": {"a": 0, "b": 0}}`,
	})

	waitForSchemaChange(t, sqlDB, `ALTER TABLE test_tab ALTER PRIMARY KEY USING COLUMNS (b)`)
	sqlDB.Exec(t, `INSERT INTO test_tab VALUES (1, 11)`)
	// No backfill, but we should see the newly insert value
	assertPayloadsStripTs(t, feed, []string{
		`test_tab: [11]->{"after": {"a": 1, "b": 11}}`,
	})

	waitForSchemaChange(t, sqlDB, `ALTER TABLE test_tab SET LOCALITY REGIONAL BY ROW`)
	// schema-changer backfill for the ADD COLUMN
	assertPayloadsStripTs(t, feed, []string{
		`test_tab: [0]->{"after": {"a": 0, "b": 0}}`,
		`test_tab: [11]->{"after": {"a": 1, "b": 11}}`,
	})
	// changefeed backfill for the ADD COLUMN
	assertPayloadsStripTs(t, feed, []string{
		`test_tab: ["us-east1", 0]->{"after": {"a": 0, "b": 0, "crdb_region": "us-east1"}}`,
		`test_tab: ["us-east1", 11]->{"after": {"a": 1, "b": 11, "crdb_region": "us-east1"}}`,
	})

	sqlDB.Exec(t, `INSERT INTO test_tab VALUES (2, 22)`)
	// Newly inserted data works
	assertPayloadsStripTs(t, feed, []string{
		`test_tab: ["us-east1", 22]->{"after": {"a": 2, "b": 22, "crdb_region": "us-east1"}}`,
	})
}

func TestChangefeedStopPolicyMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)

	cluster, db, cleanup := startTestCluster(t)
	defer cleanup()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE test_tab (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO test_tab VALUES (0)`)

	row := sqlDB.QueryRow(t, `SELECT range_id, lease_holder FROM [SHOW RANGES FROM TABLE test_tab WITH DETAILS] LIMIT 1`)
	var rangeID, leaseHolder int
	row.Scan(&rangeID, &leaseHolder)

	// Start the changefeed on a node other than the leaseholder
	// so that it is likely that the changeAggregator and
	// changeFrontier are on different nodes.
	feedServerID := ((leaseHolder - 1) + 1) % 3
	t.Logf("Range %d is on lease holder %d, running rangefeed on server %d (store id: %d)", rangeID, leaseHolder, feedServerID, cluster.Server(feedServerID).GetFirstStoreID())
	db = cluster.ServerConn(feedServerID)
	sqlDB = sqlutils.MakeSQLRunner(db)

	f := makeKafkaFeedFactory(t, cluster, db)
	feed := feed(t, f, "CREATE CHANGEFEED FOR test_tab WITH schema_change_policy='stop'")
	defer closeFeed(t, feed)
	sqlDB.Exec(t, `INSERT INTO test_tab VALUES (1)`)
	assertPayloads(t, feed, []string{
		`test_tab: [0]->{"after": {"a": 0}}`,
		`test_tab: [1]->{"after": {"a": 1}}`,
	})
	sqlDB.Exec(t, `ALTER TABLE test_tab ADD COLUMN b INT NOT NULL DEFAULT 0`)

	waitForSchemaChangeError := func(t *testing.T, f cdctest.TestFeed) {
		t.Helper()
		for {
			if _, err := f.Next(); err != nil {
				require.Contains(t, err.Error(), "schema change occurred at")
				break
			}
		}
	}
	waitForSchemaChangeError(t, feed)
}

func TestChangefeedMultiPodTenantPlanning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "may time out due to multiple servers")

	// Record the number of aggregators in planning
	aggregatorCount := 0

	// Create 2 connections of the same tenant on a cluster to have 2 pods
	tc, _, cleanupDB := startTestCluster(t)
	defer cleanupDB()

	tenantKnobs := base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{Changefeed: &TestingKnobs{
			OnDistflowSpec: func(aggregatorSpecs []*execinfrapb.ChangeAggregatorSpec, _ *execinfrapb.ChangeFrontierSpec) {
				aggregatorCount = len(aggregatorSpecs)
			},
		}},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		Server:           &server.TestingKnobs{},
	}
	tenant1Args := base.TestTenantArgs{
		TenantID:     serverutils.TestTenantID(),
		TestingKnobs: tenantKnobs,
		UseDatabase:  `d`,
	}
	tenant1Server, tenant1DB := serverutils.StartTenant(t, tc.Server(0), tenant1Args)
	tenantRunner := sqlutils.MakeSQLRunner(tenant1DB)
	tenantRunner.ExecMultiple(t, strings.Split(tenantSetupStatements, ";")...)
	sql1 := sqlutils.MakeSQLRunner(tenant1DB)
	defer tenant1DB.Close()

	tenant2Args := tenant1Args
	tenant2Args.DisableCreateTenant = true
	_, db2 := serverutils.StartTenant(t, tc.Server(1), tenant2Args)
	defer db2.Close()

	// Ensure both pods can be assigned work
	waitForTenantPodsActive(t, tenant1Server, 2)

	feedFactory, cleanupSink := makeFeedFactory(t, randomSinkType(t, feedTestEnterpriseSinks), tenant1Server, tenant1DB)
	defer cleanupSink()

	// Run a changefeed across two tables to guarantee multiple spans that can be spread across the aggregators
	sql1.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY)")
	sql1.Exec(t, "INSERT INTO foo VALUES (1), (2)")
	sql1.Exec(t, "CREATE TABLE bar (b INT PRIMARY KEY)")
	sql1.Exec(t, "INSERT INTO bar VALUES (1), (2)")

	foo := feed(t, feedFactory, "CREATE CHANGEFEED FOR foo, bar")
	defer closeFeed(t, foo)

	assertPayloads(t, foo, []string{
		`foo: [1]->{"after": {"a": 1}}`,
		`foo: [2]->{"after": {"a": 2}}`,
		`bar: [1]->{"after": {"b": 1}}`,
		`bar: [2]->{"after": {"b": 2}}`,
	})

	require.Equal(t, 2, aggregatorCount)
}
