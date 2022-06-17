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

// Waits for the ingestion job high watermark to reach the given high watermark.
func (c *tenantStreamingClusters) waitUntilHighWatermark(
	highWatermark time.Time, ingestionJobID jobspb.JobID,
) {
	testutils.SucceedsSoon(c.t, func() error {
		progress := jobutils.GetJobProgress(c.t, c.destSysSQL, ingestionJobID)
		if progress.GetHighWater() == nil {
			return errors.Newf("stream ingestion has not recorded any progress yet, waiting to advance pos %s",
				highWatermark.String())
		}
		highwater := *progress.GetHighWater()
		if highwater.GoTime().Before(highWatermark) {
			return errors.Newf("waiting for stream ingestion job progress %s to advance beyond %s",
				highwater.String(), highWatermark.String())
		}
		return nil
	})
}

func (c *tenantStreamingClusters) cutover(
	producerJobID, ingestionJobID int, cutoverTime time.Time,
) {
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
	serverArgs := base.TestServerArgs{
		// Test fails because it tries to set a cluster setting only accessible
		// to system tenants. Tracked with #76378.
		DisableDefaultTestTenant: true,
		Knobs: base.TestingKnobs{
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
	// Enable stream replication on dest by default.
	destSysSQL.Exec(t, `SET enable_experimental_stream_replication = true;`)
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

var srcClusterSetting = map[string]string{
	`kv.rangefeed.enabled`:                `true`,
	`kv.closed_timestamp.target_duration`: `'1s'`,
	// Large timeout makes test to not fail with unexpected timeout failures.
	`stream_replication.job_liveness_timeout`:            `'20s'`,
	`stream_replication.stream_liveness_track_frequency`: `'2s'`,
	`stream_replication.min_checkpoint_frequency`:        `'1s'`,
	// Make all AddSSTable operation to trigger AddSSTable events.
	`kv.bulk_io_write.small_write_size`: `'1'`,
	`jobs.registry.interval.adopt`:      `'1s'`,
}

var destClusterSetting = map[string]string{
	`stream_replication.consumer_heartbeat_frequency`:      `'1s'`,
	`bulkio.stream_ingestion.minimum_flush_interval`:       `'10ms'`,
	`bulkio.stream_ingestion.cutover_signal_poll_interval`: `'100ms'`,
	`jobs.registry.interval.adopt`:                         `'1s'`,
}

func configureClusterSettings(setting map[string]string) []string {
	res := make([]string, len(setting))
	for key, val := range setting {
		res = append(res, fmt.Sprintf("SET CLUSTER SETTING %s = %s;", key, val))
	}
	return res
}

func TestTenantStreamingSuccessfulIngestion(t *testing.T) {
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
				sysSQL.ExecMultiple(t, configureClusterSettings(srcClusterSetting)...)
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
				sysSQL.ExecMultiple(t, configureClusterSettings(destClusterSetting)...)
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
		// Check the dst cluster didn't receive the change after a while.
		<-time.NewTimer(3 * time.Second).C
		require.Equal(t, [][]string{{"2"}}, c.destTenantSQL.QueryStr(t, "SELECT * FROM d.t2"))
	}

	t.Run("initial-scan", func(t *testing.T) {
		testTenantStreaming(t, true /* withInitialScan */)
	})

	t.Run("no-initial-scan", func(t *testing.T) {
		testTenantStreaming(t, false /* withInitialScan */)
	})
}

func TestTenantStreamingProducerJobTimedOut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srcClusterSetting[`stream_replication.job_liveness_timeout`] = `'3m'`
	c, cleanup := createTenantStreamingClusters(ctx, t, tenantStreamingClustersArgs{
		srcTenantID: roachpb.MakeTenantID(10),
		srcInitFunc: func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
			sysSQL.ExecMultiple(t, configureClusterSettings(srcClusterSetting)...)
			tenantSQL.Exec(t, `
	CREATE DATABASE d;
	CREATE TABLE d.t1(i int primary key, a string, b string);
	CREATE TABLE d.t2(i int primary key);
	INSERT INTO d.t1 (i) VALUES (42);
	INSERT INTO d.t2 VALUES (2);
	UPDATE d.t1 SET b = 'world' WHERE i = 42;
	`)
		},
		srcNumNodes:  1,
		destTenantID: roachpb.MakeTenantID(20),
		destInitFunc: func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
			sysSQL.ExecMultiple(t, configureClusterSettings(destClusterSetting)...)
		},
		destNumNodes: 1,
	})
	defer cleanup()

	// initial scan
	producerJobID, ingestionJobID := c.startStreamReplication("" /* startTime */)

	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	var srcTime time.Time
	c.srcSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&srcTime)
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))

	c.compareResult("SELECT * FROM d.t1")
	c.compareResult("SELECT * FROM d.t2")

	// Make producer job easily times out
	c.srcSysSQL.ExecMultiple(t, configureClusterSettings(map[string]string{
		`stream_replication.job_liveness_timeout`: `'100ms'`,
	})...)

	jobutils.WaitForJobToFail(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToFail(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	// Make dest cluster to ingest KV events faster.
	c.srcSysSQL.ExecMultiple(t, configureClusterSettings(map[string]string{
		`stream_replication.min_checkpoint_frequency`: `'100ms'`,
	})...)
	c.srcTenantSQL.Exec(t, "INSERT INTO d.t2 VALUES (3);")

	// Check the dst cluster didn't receive the change after a while.
	<-time.NewTimer(3 * time.Second).C
	require.Equal(t, [][]string{{"0"}},
		c.destTenantSQL.QueryStr(t, "SELECT count(*) FROM d.t2 WHERE i = 3"))
}
