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
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type execFunc func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner)

type tenantStreamingClustersArgs struct {
	srcTenantID roachpb.TenantID
	srcInitFunc execFunc
	srcNumNodes int

	destTenantID roachpb.TenantID
	destInitFunc execFunc
	destNumNodes int
}

type tenantStreamingClusters struct {
	t            *testing.T
	args         tenantStreamingClustersArgs
	srcSysSQL    *sqlutils.SQLRunner
	srcTenantSQL *sqlutils.SQLRunner
	srcURL       url.URL

	destSysSQL    *sqlutils.SQLRunner
	destTenantSQL *sqlutils.SQLRunner
}

func (c *tenantStreamingClusters) compareResult(query string) {
	sourceData := c.srcTenantSQL.QueryStr(c.t, query)
	destData := c.destTenantSQL.QueryStr(c.t, query)
	require.Equal(c.t, sourceData, destData)
}

func (c *tenantStreamingClusters) cutover(
	producerJobID, ingestionJobID int, cutoverTime time.Time,
) {
	// Wait for the job high watermark to reach the given cutover time.
	testutils.SucceedsSoon(c.t, func() error {
		progress := jobutils.GetJobProgress(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
		if progress.GetHighWater() == nil {
			return errors.Newf("stream ingestion has not recorded any progress yet, waiting to advance pos %s",
				cutoverTime.String())
		}
		highwater := *progress.GetHighWater()
		if highwater.GoTime().Before(cutoverTime) {
			return errors.Newf("waiting for stream ingestion job progress %s to advance beyond %s",
				highwater.String(), cutoverTime.String())
		}
		return nil
	})

	// Cut over the ingestion job and the job will stop eventually.
	c.destSysSQL.Exec(c.t, `SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`, ingestionJobID, cutoverTime)
	jobutils.WaitForJobToSucceed(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	c.srcSysSQL.CheckQueryResultsRetry(c.t,
		fmt.Sprintf("SELECT status FROM [SHOW JOBS] WHERE job_id = %d", producerJobID), [][]string{{"succeeded"}})
}

// Returns producer job ID and ingestion job ID.
func (c *tenantStreamingClusters) startStreamReplication(startTime string) (int, int) {
	var ingestionJobID, streamProducerJobID int
	streamReplStmt := fmt.Sprintf("RESTORE TENANT %s FROM REPLICATION STREAM FROM '%s'",
		c.args.srcTenantID, c.srcURL.String())
	if startTime != "" {
		streamReplStmt = streamReplStmt + fmt.Sprintf(" AS OF SYSTEM TIME %s", startTime)
	}
	streamReplStmt = streamReplStmt + fmt.Sprintf("AS TENANT %s", c.args.destTenantID)
	c.destSysSQL.QueryRow(c.t, streamReplStmt).Scan(&ingestionJobID)
	c.srcSysSQL.CheckQueryResultsRetry(c.t,
		"SELECT count(*) FROM [SHOW JOBS] WHERE job_type = 'STREAM REPLICATION'", [][]string{{"1"}})
	c.srcSysSQL.QueryRow(c.t, "SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'STREAM REPLICATION'").
		Scan(&streamProducerJobID)
	return streamProducerJobID, ingestionJobID
}

func createTenantStreamingClusters(
	ctx context.Context, t *testing.T, args tenantStreamingClustersArgs,
) (*tenantStreamingClusters, func()) {
	serverArgs := base.TestServerArgs{Knobs: base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	}

	startTestClusterWithTenant := func(
		ctx context.Context,
		t *testing.T,
		serverArgs base.TestServerArgs,
		tenantID roachpb.TenantID,
		numNodes int,
	) (*sqlutils.SQLRunner, *sqlutils.SQLRunner, url.URL, func()) {
		params := base.TestClusterArgs{ServerArgs: serverArgs}
		c := testcluster.StartTestCluster(t, numNodes, params)
		// TODO(casper): support adding splits when we have multiple nodes.
		_, tenantConn := serverutils.StartTenant(t, c.Server(0), base.TestTenantArgs{TenantID: tenantID})
		pgURL, cleanupSinkCert := sqlutils.PGUrl(t, c.Server(0).ServingSQLAddr(), t.Name(), url.User(username.RootUser))
		return sqlutils.MakeSQLRunner(c.ServerConn(0)), sqlutils.MakeSQLRunner(tenantConn), pgURL, func() {
			require.NoError(t, tenantConn.Close())
			c.Stopper().Stop(ctx)
			cleanupSinkCert()
		}
	}

	// Start the source cluster.
	sourceSysSQL, sourceTenantSQL, srcURL, srcCleanup := startTestClusterWithTenant(ctx, t, serverArgs, args.srcTenantID, args.srcNumNodes)
	// Start the destination cluster.
	destSysSQL, destTenantSQL, _, destCleanup := startTestClusterWithTenant(ctx, t, serverArgs, args.destTenantID, args.destNumNodes)

	args.srcInitFunc(t, sourceSysSQL, sourceTenantSQL)
	args.destInitFunc(t, destSysSQL, destTenantSQL)
	return &tenantStreamingClusters{
			t:             t,
			args:          args,
			srcSysSQL:     sourceSysSQL,
			srcTenantSQL:  sourceTenantSQL,
			srcURL:        srcURL,
			destSysSQL:    destSysSQL,
			destTenantSQL: destTenantSQL,
		}, func() {
			destCleanup()
			srcCleanup()
		}
}

func (c *tenantStreamingClusters) srcExec(exec execFunc) {
	exec(c.t, c.srcSysSQL, c.srcTenantSQL)
}

var srcClusterSetting = `
	SET CLUSTER SETTING kv.rangefeed.enabled = true;
	SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s';
	SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms';
  SET CLUSTER SETTING stream_replication.job_liveness_timeout = '20s';
  SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '2s';
  SET CLUSTER SETTING stream_replication.min_checkpoint_frequency = '1s';
  SET CLUSTER SETTING kv.bulk_io_write.small_write_size = '1';
`

var destClusterSetting = `
	SET enable_experimental_stream_replication = true;
	SET CLUSTER SETTING stream_replication.consumer_heartbeat_frequency = '100ms';
	SET CLUSTER SETTING bulkio.stream_ingestion.minimum_flush_interval = '10ms';
	SET CLUSTER SETTING bulkio.stream_ingestion.cutover_signal_poll_interval = '100ms';
`

func TestPartitionedTenantStreamingEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "slow under race")
	skip.UnderStress(t, "slow under stress")

	dataSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			if _, err := w.Write([]byte("42,42\n43,43\n")); err != nil {
				t.Logf("failed to write: %s", err.Error())
			}
		}
	}))
	defer dataSrv.Close()

	ctx := context.Background()
	testTenantStreaming := func(t *testing.T, withInitialScan bool) {
		// 'startTime' is a timestamp before we insert any data into the source cluster.
		var startTime string
		c, cleanup := createTenantStreamingClusters(ctx, t, tenantStreamingClustersArgs{
			srcTenantID: roachpb.MakeTenantID(10),
			srcInitFunc: func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
				sysSQL.ExecMultiple(t, strings.Split(srcClusterSetting, ";")...)
				if !withInitialScan {
					sysSQL.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&startTime)
				}
				tenantSQL.Exec(t, `
	CREATE DATABASE d;
	CREATE TABLE d.t1(i int primary key, a string, b string);
	CREATE TABLE d.t2(i int primary key);
	INSERT INTO d.t1 (i) VALUES (42);
	INSERT INTO d.t2 VALUES (2);
	UPDATE d.t1 SET b = 'world' WHERE i = 42;
	`)
				tenantSQL.Exec(t, `
	ALTER TABLE d.t1 DROP COLUMN b;
	`)
			},
			srcNumNodes:  1,
			destTenantID: roachpb.MakeTenantID(20),
			destInitFunc: func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
				sysSQL.ExecMultiple(t, strings.Split(destClusterSetting, ";")...)
			},
			destNumNodes: 1,
		})
		defer cleanup()

		producerJobID, ingestionJobID := c.startStreamReplication(startTime)
		c.srcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
			tenantSQL.Exec(t, "CREATE TABLE d.x (id INT PRIMARY KEY, n INT)")
			tenantSQL.Exec(t, "IMPORT INTO d.x CSV DATA ($1)", dataSrv.URL)
		})

		var cutoverTime time.Time
		c.srcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
			sysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)
		})

		c.cutover(producerJobID, ingestionJobID, cutoverTime)
		c.compareResult("SELECT * FROM d.t1")
		c.compareResult("SELECT * FROM d.t2")
		c.compareResult("SELECT * FROM d.x")
		// After cutover, changes to source won't be streamed into destination cluster.
		c.srcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
			tenantSQL.Exec(t, `INSERT INTO d.t2 VALUES (3);`)
		})
		require.Equal(t, [][]string{{"2"}}, c.destTenantSQL.QueryStr(t, "SELECT * FROM d.t2"))
	}

	t.Run("initial-scan", func(t *testing.T) {
		testTenantStreaming(t, true /* withInitialScan */)
	})

	t.Run("no-initial-scan", func(t *testing.T) {
		testTenantStreaming(t, false /* withInitialScan */)
	})
}
