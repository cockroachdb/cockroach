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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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

	stats := replicationtestutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)
	readerTenantID := stats.IngestionDetails.ReadTenantID
	require.NotNil(t, readerTenantID)

	readerTenantName := fmt.Sprintf("%s-readonly", args.DestTenantName)
	c.ConnectToReaderTenant(ctx, readerTenantID, readerTenantName, 0)

	c.SrcTenantSQL.Exec(t, `
USE defaultdb;
CREATE TABLE a (i INT PRIMARY KEY);
INSERT INTO a VALUES (1);
`)
	waitForPollerJobToStart(t, c, ingestionJobID)
	observeValueInReaderTenant(t, c)
}

func observeValueInReaderTenant(t *testing.T, c *replicationtestutils.TenantStreamingClusters) {
	now := timeutil.Now()
	// Verify that updates have been replicated to reader tenant. This may take a
	// second as the historical timestamp these AOST queries run needs to advance.
	testutils.SucceedsSoon(t, func() error {
		var numTables int
		c.ReaderTenantSQL.QueryRow(t, `SELECT count(*) FROM [SHOW TABLES]`).Scan(&numTables)

		if numTables != 1 {
			return errors.Errorf("expected 1 table to be present in reader tenant, but got %d instead", numTables)
		}

		var actualQueryResult int
		c.ReaderTenantSQL.QueryRow(t, `SELECT * FROM a`).Scan(&actualQueryResult)
		if actualQueryResult != 1 {
			return errors.Newf("expected %d to be replicated to table {a} in reader tenant, received %d instead",
				1, actualQueryResult)
		}
		return nil
	})
	t.Logf("waited for %s for updates to reflect in reader tenant", timeutil.Since(now))
}

func waitForPollerJobToStart(
	t *testing.T, c *replicationtestutils.TenantStreamingClusters, ingestionJobID int,
) {
	// TODO(annezhu): we really should be waiting for the AOST timestamp on the
	// reader tenant to advance to point where the reader tenant's defaultdb
	// exists, but we don't have a way to track that. The historical ts really
	// should be stored in the reader tenant job progress.
	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	testutils.SucceedsSoon(t, func() error {
		var numJobs int
		c.ReaderTenantSQL.QueryRow(t, `
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
	c.ReaderTenantSQL.QueryRow(t, `
SELECT job_id
FROM crdb_internal.jobs 
WHERE job_type = 'STANDBY READ TS POLLER'
`).Scan(&jobID)
	jobutils.WaitForJobToRun(t, c.ReaderTenantSQL, jobID)
}

func TestFastFailbackWithReaderTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	skip.UnderRace(t, "test takes ~5 minutes under race")

	serverA, aDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer serverA.Stopper().Stop(ctx)
	serverB, bDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer serverB.Stopper().Stop(ctx)

	sqlA := sqlutils.MakeSQLRunner(aDB)
	sqlB := sqlutils.MakeSQLRunner(bDB)

	serverAURL, cleanupURLA := pgurlutils.PGUrl(t, serverA.SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanupURLA()
	serverBURL, cleanupURLB := pgurlutils.PGUrl(t, serverB.SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanupURLB()

	for _, s := range []string{
		"SET CLUSTER SETTING kv.rangefeed.enabled = true",
		"SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '200ms'",
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'",
		"SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '50ms'",

		"SET CLUSTER SETTING physical_replication.consumer.heartbeat_frequency = '1s'",
		"SET CLUSTER SETTING physical_replication.consumer.job_checkpoint_frequency = '100ms'",
		"SET CLUSTER SETTING physical_replication.consumer.minimum_flush_interval = '10ms'",
		"SET CLUSTER SETTING physical_replication.consumer.failover_signal_poll_interval = '100ms'",
		"SET CLUSTER SETTING spanconfig.reconciliation_job.checkpoint_interval = '100ms'",
	} {
		sqlA.Exec(t, s)
		sqlB.Exec(t, s)
	}

	t.Logf("creating tenant f")
	sqlA.Exec(t, "CREATE VIRTUAL CLUSTER f")
	sqlA.Exec(t, "ALTER VIRTUAL CLUSTER f START SERVICE SHARED")

	t.Logf("starting replication f->g")
	sqlB.Exec(t, "CREATE VIRTUAL CLUSTER g FROM REPLICATION OF f ON $1 WITH READ VIRTUAL CLUSTER", serverAURL.String())

	// Verify that reader tenant has been created for g
	waitForReaderTenant(t, sqlB, "g-readonly")

	// FAILOVER
	_, consumerGJobID := replicationtestutils.GetStreamJobIds(t, ctx, sqlB, roachpb.TenantName("g"))
	var ts1 string
	sqlA.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&ts1)

	rng, _ := randutil.NewPseudoRand()
	if rng.Intn(2) == 0 {
		t.Logf("waiting for g@%s", ts1)
		replicationtestutils.WaitUntilReplicatedTime(t,
			replicationtestutils.DecimalTimeToHLC(t, ts1),
			sqlB,
			jobspb.JobID(consumerGJobID))

		t.Logf("completing replication on g@%s", ts1)
		sqlB.Exec(t, fmt.Sprintf("ALTER VIRTUAL CLUSTER g COMPLETE REPLICATION TO SYSTEM TIME '%s'", ts1))
	} else {
		t.Log("waiting for initial scan on g")
		replicationtestutils.WaitUntilStartTimeReached(t, sqlB, jobspb.JobID(consumerGJobID))
		t.Log("completing replication on g to latest")
		sqlB.Exec(t, "ALTER VIRTUAL CLUSTER g COMPLETE REPLICATION TO LATEST")
	}
	jobutils.WaitForJobToSucceed(t, sqlB, jobspb.JobID(consumerGJobID))

	sqlB.Exec(t, "ALTER VIRTUAL CLUSTER g START SERVICE SHARED")
	var ts2 string
	sqlA.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&ts2)

	sqlA.Exec(t, "ALTER VIRTUAL CLUSTER f STOP SERVICE")
	waitUntilTenantServerStopped(t, serverA.SystemLayer(), "f")
	t.Logf("starting replication g->f")
	sqlA.Exec(t, "ALTER VIRTUAL CLUSTER f START REPLICATION OF g ON $1 WITH READ VIRTUAL CLUSTER", serverBURL.String())
	_, consumerFJobID := replicationtestutils.GetStreamJobIds(t, ctx, sqlA, roachpb.TenantName("f"))
	t.Logf("waiting for f@%s", ts2)
	replicationtestutils.WaitUntilReplicatedTime(t,
		replicationtestutils.DecimalTimeToHLC(t, ts2),
		sqlA,
		jobspb.JobID(consumerFJobID))

	// Verify that reader tenant has been created for f
	waitForReaderTenant(t, sqlA, "f-readonly")
}

func waitForReaderTenant(t *testing.T, db *sqlutils.SQLRunner, tenantName string) {
	testutils.SucceedsSoon(t, func() error {
		var numTenants int
		db.QueryRow(t, `
SELECT count(*)
FROM system.tenants
WHERE name = $1
`, tenantName).Scan(&numTenants)

		if numTenants != 1 {
			return errors.Errorf("expected 1 tenant, got %d", numTenants)
		}
		return nil
	})
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

		stats := replicationtestutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)
		readerTenantID := stats.IngestionDetails.ReadTenantID
		require.NotNil(t, readerTenantID)

		readerTenantName := fmt.Sprintf("%s-readonly", args.DestTenantName)
		c.ConnectToReaderTenant(ctx, readerTenantID, readerTenantName, 0)

		c.SrcTenantSQL.Exec(t, `
USE defaultdb;
CREATE TABLE a (i INT PRIMARY KEY);
INSERT INTO a VALUES (1);
`)

		waitForPollerJobToStart(t, c, ingestionJobID)
		if cutoverToLatest {
			observeValueInReaderTenant(t, c)
			c.Cutover(ctx, producerJobID, ingestionJobID, time.Time{}, false)
			jobutils.WaitForJobToSucceed(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
			observeValueInReaderTenant(t, c)
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
