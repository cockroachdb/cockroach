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
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStandbyReadTSPollerJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 131611)

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
	c.ConnectToReaderTenant(ctx, readerTenantID, readerTenantName, 0)

	pollInterval := time.Microsecond
	serverutils.SetClusterSetting(t, c.DestCluster, "bulkio.stream_ingestion.standby_read_ts_poll_interval", pollInterval)

	var jobID jobspb.JobID
	c.ReaderTenantSQL.QueryRow(t, `
SELECT job_id 
FROM crdb_internal.jobs 
WHERE job_type = 'STANDBY READ TS POLLER'
`).Scan(&jobID)
	jobutils.WaitForJobToRun(t, c.ReaderTenantSQL, jobID)

	c.SrcTenantSQL.Exec(t, `
USE defaultdb;
CREATE TABLE a (i INT PRIMARY KEY);
INSERT INTO a VALUES (1);
`)

	// Verify that updates have been replicated to reader tenant
	testutils.SucceedsWithin(t, func() error {
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
	}, time.Minute)
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

	serverAURL, cleanupURLA := sqlutils.PGUrl(t, serverA.SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanupURLA()
	serverBURL, cleanupURLB := sqlutils.PGUrl(t, serverB.SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanupURLB()

	for _, s := range []string{
		"SET CLUSTER SETTING kv.rangefeed.enabled = true",
		"SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '200ms'",
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'",
		"SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '50ms'",

		"SET CLUSTER SETTING physical_replication.consumer.heartbeat_frequency = '1s'",
		"SET CLUSTER SETTING physical_replication.consumer.job_checkpoint_frequency = '100ms'",
		"SET CLUSTER SETTING physical_replication.consumer.minimum_flush_interval = '10ms'",
		"SET CLUSTER SETTING physical_replication.consumer.cutover_signal_poll_interval = '100ms'",
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
