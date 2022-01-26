// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func startTestClusterWithTenant(
	ctx context.Context,
	t *testing.T,
	serverArgs base.TestServerArgs,
	tenantID roachpb.TenantID,
	numNodes int,
) (serverutils.TestClusterInterface, *gosql.DB, *gosql.DB, func()) {
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	c := testcluster.StartTestCluster(t, numNodes, params)
	// TODO(casper): support adding splits when we have multiple nodes.
	_, tenantConn := serverutils.StartTenant(t, c.Server(0), base.TestTenantArgs{TenantID: tenantID})
	return c, c.ServerConn(0), tenantConn, func() {
		tenantConn.Close()
		c.Stopper().Stop(ctx)
	}
}

func compareResult(t *testing.T, src *sqlutils.SQLRunner, dest *sqlutils.SQLRunner, query string) {
	sourceData := src.QueryStr(t, query)
	destData := dest.QueryStr(t, query)
	require.Equal(t, sourceData, destData)
}

func TestPartitionedTenantStreamingEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "slow under race")
	skip.UnderStress(t, "slow under stress")

	ctx := context.Background()
	args := base.TestServerArgs{Knobs: base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	}

	// Start the source cluster.
	tenantID := serverutils.TestTenantID()
	sc, sourceSysDB, sourceTenantDB, srcCleanup := startTestClusterWithTenant(ctx, t, args, tenantID, 3)
	defer srcCleanup()
	sourceSysSQL, sourceTenantSQL := sqlutils.MakeSQLRunner(sourceSysDB), sqlutils.MakeSQLRunner(sourceTenantDB)

	sourceSysSQL.Exec(t, `
	SET CLUSTER SETTING kv.rangefeed.enabled = true;
	SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s';
	SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms';
  SET CLUSTER SETTING stream_replication.job_liveness_timeout = '3s';
  SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '2s';
  SET CLUSTER SETTING stream_replication.min_checkpoint_frequency = '1s';
  `)

	// Start the destination cluster.
	_, destSysDB, destTenantDB, destCleanup := startTestClusterWithTenant(ctx, t, args, tenantID, 2)
	defer destCleanup()
	destSysSQL, destTenantSQL := sqlutils.MakeSQLRunner(destSysDB), sqlutils.MakeSQLRunner(destTenantDB)

	destSysSQL.Exec(t, `
	SET CLUSTER SETTING stream_replication.consumer_heartbeat_frequency = '100ms';
	SET CLUSTER SETTING bulkio.stream_ingestion.minimum_flush_interval = '10ms';
	SET CLUSTER SETTING bulkio.stream_ingestion.cutover_signal_poll_interval = '100ms';
	SET enable_experimental_stream_replication = true;
	`)

	pgURL, cleanupSinkCert := sqlutils.PGUrl(t, sc.Server(0).ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupSinkCert()

	var startTime string
	sourceSysSQL.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&startTime)
	var ingestionJobID, streamProducerJobID int
	destSysSQL.QueryRow(t,
		`RESTORE TENANT `+tenantID.String()+` FROM REPLICATION STREAM FROM $1 AS OF SYSTEM TIME `+startTime,
		pgURL.String(),
	).Scan(&ingestionJobID)
	sourceSysSQL.CheckQueryResultsRetry(t,
		"SELECT count(*) FROM [SHOW JOBS] WHERE job_type = 'STREAM REPLICATION'", [][]string{{"1"}})
	sourceSysSQL.QueryRow(t, "SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'STREAM REPLICATION'").
		Scan(&streamProducerJobID)

	sourceTenantSQL.Exec(t, `
	CREATE DATABASE d;
	CREATE TABLE d.t1(i int primary key, a string, b string);
	CREATE TABLE d.t2(i int primary key);
	INSERT INTO d.t1 (i) VALUES (42);
	INSERT INTO d.t2 VALUES (2);
	UPDATE d.t1 SET b = 'world' WHERE i = 42;
	`)

	sourceTenantSQL.Exec(t, `
	ALTER TABLE d.t1 DROP COLUMN b;
	`)

	// Pick a cutover time, then wait for the job to reach that time.
	cutoverTime := timeutil.Now().Round(time.Microsecond)
	testutils.SucceedsSoon(t, func() error {
		progress := jobutils.GetJobProgress(t, destSysSQL, jobspb.JobID(ingestionJobID))
		if progress.GetHighWater() == nil {
			return errors.Newf("stream ingestion has not recorded any progress yet, waiting to advance pos %s",
				cutoverTime.String())
		}
		highwater := timeutil.Unix(0, progress.GetHighWater().WallTime)
		if highwater.Before(cutoverTime) {
			return errors.Newf("waiting for stream ingestion job progress %s to advance beyond %s",
				highwater.String(), cutoverTime.String())
		}
		return nil
	})

	compareResult(t, sourceTenantSQL, destTenantSQL, "SELECT * FROM d.t1")
	compareResult(t, sourceTenantSQL, destTenantSQL, "SELECT * FROM d.t2")

	// Cut over the ingestion job and the job will stop eventually.
	destSysSQL.Exec(t, `SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`, ingestionJobID, cutoverTime)
	jobutils.WaitForJob(t, destSysSQL, jobspb.JobID(ingestionJobID))
	// TODO(casper): Make producer job exit normally in the cutover scenario.
	sourceSysSQL.CheckQueryResultsRetry(t,
		fmt.Sprintf("SELECT status, error FROM [SHOW JOBS] WHERE job_id = %d", streamProducerJobID),
		[][]string{{"failed", fmt.Sprintf("replication stream %d timed out", streamProducerJobID)}})

	// After cutover, changes to source won't be streamed into destination cluster.
	sourceTenantSQL.Exec(t, `
	INSERT INTO d.t2 VALUES (3);
	`)
	require.Equal(t, [][]string{{"2"}}, destTenantSQL.QueryStr(t, "SELECT * FROM d.t2"))
}
