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

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	t.Logf("test setup took %s", timeutil.Since(beginTS))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)
	readerTenantID := stats.IngestionDetails.ReadTenantID
	require.NotNil(t, readerTenantID)

	readerTenantName := fmt.Sprintf("%s-readonly", args.DestTenantName)
	c.ConnectToReaderTenant(ctx, readerTenantID, readerTenantName)

	defaultDBQuery := `
USE defaultdb;
CREATE TABLE a (i INT PRIMARY KEY);
INSERT INTO a VALUES (1);
`

	c.SrcTenantSQL.Exec(t, defaultDBQuery)
	waitForPollerJobToStartDest(t, c, ingestionJobID)
	observeValueInReaderTenant(t, c.ReaderTenantSQL)

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
	}
}

func observeValueInReaderTenant(t *testing.T, readerSQL *sqlutils.SQLRunner) {
	now := timeutil.Now()
	// Verify that updates have been replicated to reader tenant. This may take a
	// second as the historical timestamp these AOST queries run needs to advance.
	testutils.SucceedsSoon(t, func() error {
		var numTables int
		readerSQL.QueryRow(t, `SELECT count(*) FROM [SHOW TABLES]`).Scan(&numTables)

		if numTables != 1 {
			return errors.Errorf("expected 1 table to be present in reader tenant, but got %d instead", numTables)
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
