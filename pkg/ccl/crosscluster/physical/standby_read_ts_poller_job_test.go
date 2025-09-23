// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStandbyReadTSPollerJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	beginTS := timeutil.Now()
	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.EnableReaderTenant = true
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	c.SrcTenantSQL.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY)`)
	c.SrcTenantSQL.Exec(t, `CREATE TABLE bar (i INT PRIMARY KEY)`)

	offset, offsetChecksInReaderTenant := maybeOffsetReaderTenantSystemTables(t, c)

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	t.Logf("test setup took %s", timeutil.Since(beginTS))

	readerTenantName := fmt.Sprintf("%s-readonly", args.DestTenantName)

	// Ensures the reader tenant can spin up even if the system tenant overrode
	// the diagnostics setting, set during a permanent migration.
	c.DestSysSQL.Exec(t, fmt.Sprintf("ALTER TENANT '%s' SET CLUSTER SETTING diagnostics.reporting.enabled = true", readerTenantName))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)
	readerTenantID := stats.IngestionDetails.ReadTenantID
	require.NotNil(t, readerTenantID)

	c.ConnectToReaderTenant(ctx, readerTenantID, readerTenantName)

	defaultDBQuery := `
USE defaultdb;
CREATE TABLE a (i INT PRIMARY KEY);
INSERT INTO a VALUES (1);
`

	c.SrcTenantSQL.Exec(t, defaultDBQuery)
	waitForPollerJobToStartDest(t, c, ingestionJobID)
	observeValueInReaderTenant(t, c.ReaderTenantSQL)

	var idWithOffsetCount int
	c.ReaderTenantSQL.QueryRow(t, fmt.Sprintf("SELECT count(*) FROM system.namespace where id = %d", 50+offset)).Scan(&idWithOffsetCount)
	require.Equal(t, 1, idWithOffsetCount, "expected to find namespace entry for table a with offset applied")
	offsetChecksInReaderTenant(c.ReaderTenantSQL)

	// Failback and setup stanby reader tenant on the og source.
	{
		c.Cutover(ctx, producerJobID, ingestionJobID, srcTime.GoTime(), false)
		defer c.StartDestTenant(ctx, nil, 0)()

		destPgURL, cleanupSinkCert := sqlutils.PGUrl(t, c.DestSysServer.AdvSQLAddr(), t.Name(), url.User(username.RootUser))
		defer cleanupSinkCert()

		c.SrcSysSQL.Exec(t, fmt.Sprintf("ALTER VIRTUAL CLUSTER '%s' STOP SERVICE", c.Args.SrcTenantName))
		waitUntilTenantServerStopped(t, c.SrcSysServer, string(c.Args.SrcTenantName))

		c.SrcSysSQL.Exec(c.T,
			`ALTER TENANT $1 START REPLICATION OF $2 ON $3 WITH READ VIRTUAL CLUSTER`,
			c.Args.SrcTenantName, c.Args.DestTenantName, destPgURL.String())

		_, failbackJobID := replicationtestutils.GetStreamJobIds(t, ctx, c.SrcSysSQL, c.Args.SrcTenantName)
		now := c.SrcCluster.Servers[0].Clock().Now()
		replicationtestutils.WaitUntilReplicatedTime(c.T, now, c.SrcSysSQL, jobspb.JobID(failbackJobID))
		stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.SrcSysSQL, failbackJobID)
		srcReaderTenantID := stats.IngestionDetails.ReadTenantID
		require.NotNil(t, srcReaderTenantID)

		// We cutover the dest tenant to a time bfore we created table a, so lets
		// created it again and observe it in the new reader tenant.
		c.DestTenantSQL.Exec(t, defaultDBQuery)

		srcReaderTenantName := fmt.Sprintf("%s-readonly", c.Args.SrcTenantName)
		srcReaderSQL := sqlutils.MakeSQLRunner(replicationtestutils.SetupReaderTenant(ctx, t, srcReaderTenantID, srcReaderTenantName, c.SrcCluster, c.SrcSysSQL))
		waitForPollerJobToStart(t, srcReaderSQL)

		var numTables int
		srcReaderSQL.QueryRow(t, `SELECT count(*) FROM [SHOW TABLES]`).Scan(&numTables)
		observeValueInReaderTenant(t, srcReaderSQL)
		offsetChecksInReaderTenant(srcReaderSQL)
	}
}

func maybeOffsetReaderTenantSystemTables(
	t *testing.T, c *replicationtestutils.TenantStreamingClusters,
) (int, func(sql *sqlutils.SQLRunner)) {
	if c.Rng.Intn(2) == 0 {
		return 0, func(sql *sqlutils.SQLRunner) {}
	}
	offset := 100000
	c.DestSysSQL.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING physical_cluster_replication.reader_system_table_id_offset = %d`, offset))
	// Set on source to ensure failback works well too.
	c.SrcSysSQL.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING physical_cluster_replication.reader_system_table_id_offset = %d`, offset))

	// swap a system table ID and a user table ID to simulate a cluster that has interleaving user and system table ids.
	scaryTableIDRemapFunc := `
	CREATE OR REPLACE FUNCTION renumber_desc(oldID INT, newID INT) RETURNS BOOL AS
$$
BEGIN
-- Rewrite the ID within the descriptor
SELECT crdb_internal.unsafe_upsert_descriptor(
        newid,
        crdb_internal.json_to_pb(
            'cockroach.sql.sqlbase.Descriptor',
            d
        ),
        true
       )
  FROM (
        SELECT id,
               json_set(
                json_set(
                    crdb_internal.pb_to_json(
                        'cockroach.sql.sqlbase.Descriptor',
                        descriptor,
                        false
                    ),
                    ARRAY['table', 'id'],
                    newid::STRING::JSONB
                ),
                ARRAY['table', 'modificationTime'],
                json_build_object(
                    'wallTime',
                    (
                        (
                            extract('epoch', now())
                            * 1000000
                        )::INT8
                        * 1000
                    )::STRING
                )
               ) AS d
          FROM system.descriptor
         WHERE id IN (oldid,)
       );
-- Update the namespace entry and delete the old descriptor.
	SELECT crdb_internal.unsafe_upsert_namespace_entry("parentID", "parentSchemaID", name, newID, true) FROM (SELECT "parentID", "parentSchemaID", name, id FROM system.namespace where id =oldID) UNION ALL
	SELECT crdb_internal.unsafe_delete_descriptor(oldID, true);

	RETURN true;

END
$$ LANGUAGE PLpgSQL;`

	c.SrcTenantSQL.Exec(t, scaryTableIDRemapFunc)
	var txnInsightsID, privilegesID int
	c.SrcTenantSQL.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'transaction_execution_insights'`).Scan(&txnInsightsID)
	c.SrcTenantSQL.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'privileges'`).Scan(&privilegesID)
	require.NotEqual(t, 0, txnInsightsID)
	require.NotEqual(t, 0, privilegesID)

	// renumber these two priv tables to be out of the way
	txnInsightIDRemapedID := txnInsightsID + 1000
	privilegesIDRemapedID := privilegesID + 1000
	c.SrcTenantSQL.Exec(t, `SELECT renumber_desc($1, $2)`, txnInsightsID, txnInsightIDRemapedID)
	c.SrcTenantSQL.Exec(t, `SELECT renumber_desc($1, $2)`, privilegesID, privilegesIDRemapedID)

	// create two user tables on the source and interleave them with system table ids
	var fooID, barID int
	c.SrcTenantSQL.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'foo'`).Scan(&fooID)
	c.SrcTenantSQL.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'bar'`).Scan(&barID)
	require.NotEqual(t, 0, fooID)
	require.NotEqual(t, 0, barID)

	c.SrcTenantSQL.Exec(t, `SELECT renumber_desc($1, $2)`, fooID, txnInsightsID)
	c.SrcTenantSQL.Exec(t, `SELECT renumber_desc($1, $2)`, barID, privilegesID)

	// Drop the function, to avoid hitting 152978
	c.SrcTenantSQL.Exec(t, `DROP FUNCTION renumber_desc`)

	offsetChecksInReaderTenant := func(sql *sqlutils.SQLRunner) {
		// Check that txn execution insights table is not at the same id as source as it's not replicated.
		sql.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'transaction_execution_insights'`).Scan(&txnInsightsID)
		require.NotEqual(t, txnInsightIDRemapedID, txnInsightsID)

		// On 25.3, the privs table is not replicated so the ids should differ.
		sql.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'privileges'`).Scan(&privilegesID)
		require.NotEqual(t, privilegesIDRemapedID, privilegesID)
		var count int
		sql.QueryRow(t, `SELECT count(*) FROM system.namespace WHERE name = 'privileges'`).Scan(&count)
		require.Equal(t, 1, count)
	}

	return offset, offsetChecksInReaderTenant
}

func observeValueInReaderTenant(t *testing.T, readerSQL *sqlutils.SQLRunner) {
	now := timeutil.Now()
	// Verify that updates have been replicated to reader tenant. This may take a
	// second as the historical timestamp these AOST queries run needs to advance.
	testutils.SucceedsSoon(t, func() error {
		var numTables int
		readerSQL.QueryRow(t, `SELECT count(*) FROM [SHOW TABLES]`).Scan(&numTables)

		if numTables != 3 {
			return errors.Errorf("expected 3 tables to be present in reader tenant, but got %d instead", numTables)
		}

		var actualQueryResult int
		readerSQL.QueryRow(t, `SELECT * FROM a`).Scan(&actualQueryResult)
		if actualQueryResult != 1 {
			return errors.Newf("expected %d to be replicated to table {a} in reader tenant, received %d instead",
				1, actualQueryResult)
		}
		return nil
	})
	t.Logf("waited for %s for updates to reflect in reader tenant", timeutil.Since(now))
}

func waitForPollerJobToStartDest(
	t *testing.T, c *replicationtestutils.TenantStreamingClusters, ingestionJobID int,
) {
	// TODO(annezhu): we really should be waiting for the AOST timestamp on the
	// reader tenant to advance to point where the reader tenant's defaultdb
	// exists, but we don't have a way to track that. The historical ts really
	// should be stored in the reader tenant job progress.
	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	waitForPollerJobToStart(t, c.ReaderTenantSQL)
}

func waitForPollerJobToStart(t *testing.T, readerSQL *sqlutils.SQLRunner) {
	testutils.SucceedsSoon(t, func() error {
		var numJobs int
		readerSQL.QueryRow(t, `
SELECT count(*)
FROM crdb_internal.jobs 
WHERE job_type = 'STANDBY READ TS POLLER';
`).Scan(&numJobs)

		if numJobs != 1 {
			return errors.Errorf("expected 1 standby read ts poller job, but got %d instead", numJobs)
		}
		return nil
	})
	var jobID jobspb.JobID
	readerSQL.QueryRow(t, `
SELECT job_id
FROM crdb_internal.jobs 
WHERE job_type = 'STANDBY READ TS POLLER'
`).Scan(&jobID)
	jobutils.WaitForJobToRun(t, readerSQL, jobID)
}

func TestReaderTenantCutover(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "cutoverToLatest", func(t *testing.T, cutoverToLatest bool) {
		ctx := context.Background()
		args := replicationtestutils.DefaultTenantStreamingClustersArgs
		args.EnableReaderTenant = true
		c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
		defer cleanup()

		c.SrcTenantSQL.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY)`)
		c.SrcTenantSQL.Exec(t, `CREATE TABLE bar (i INT PRIMARY KEY)`)

		producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

		jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
		jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

		srcTime := c.SrcCluster.Server(0).Clock().Now()
		c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

		stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)
		readerTenantID := stats.IngestionDetails.ReadTenantID
		require.NotNil(t, readerTenantID)

		readerTenantName := fmt.Sprintf("%s-readonly", args.DestTenantName)
		c.ConnectToReaderTenant(ctx, readerTenantID, readerTenantName)

		c.SrcTenantSQL.Exec(t, `
USE defaultdb;
CREATE TABLE a (i INT PRIMARY KEY);
INSERT INTO a VALUES (1);
`)

		waitForPollerJobToStartDest(t, c, ingestionJobID)
		if cutoverToLatest {
			observeValueInReaderTenant(t, c.ReaderTenantSQL)
			c.Cutover(ctx, producerJobID, ingestionJobID, time.Time{}, false)
			jobutils.WaitForJobToSucceed(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
			observeValueInReaderTenant(t, c.ReaderTenantSQL)
		} else {
			c.Cutover(ctx, producerJobID, ingestionJobID, c.SrcCluster.Server(0).Clock().Now().GoTime(), false)
			waitToRemoveTenant(t, c.DestSysSQL, readerTenantName)
			jobutils.WaitForJobToSucceed(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
		}
	})
}

func waitToRemoveTenant(t testing.TB, db *sqlutils.SQLRunner, tenantName string) {
	testutils.SucceedsSoon(t, func() error {
		var count int
		db.QueryRow(t, `SELECT count(*) FROM system.tenants where name = $1`, tenantName).Scan(&count)
		if count != 0 {
			return errors.Newf("expected tenant %s to be removed, but it still exists", tenantName)
		}
		return nil
	})
}
