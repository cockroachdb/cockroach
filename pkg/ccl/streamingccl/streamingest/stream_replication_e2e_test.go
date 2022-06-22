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
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
	testingKnobs *sql.StreamingTestingKnobs
}

var defaultTenantStreamingClustersArgs = tenantStreamingClustersArgs{
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
func (c *tenantStreamingClusters) startStreamReplication() (int, int) {
	var ingestionJobID, streamProducerJobID int
	streamReplStmt := fmt.Sprintf("RESTORE TENANT %s FROM REPLICATION STREAM FROM '%s' AS TENANT %s",
		c.args.srcTenantID, c.srcURL.String(), c.args.destTenantID)
	c.destSysSQL.QueryRow(c.t, streamReplStmt).Scan(&ingestionJobID, &streamProducerJobID)
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
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			DistSQL: &execinfra.TestingKnobs{
				StreamingTestingKnobs: args.testingKnobs,
			},
		},
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
	`stream_replication.job_liveness_timeout`:            `'3m'`,
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

func streamIngestionStats(
	t *testing.T, sqlRunner *sqlutils.SQLRunner, ingestionJobID int,
) *streampb.StreamIngestionStats {
	stats, rawStats := &streampb.StreamIngestionStats{}, make([]byte, 0)
	row := sqlRunner.QueryRow(t, "SELECT crdb_internal.stream_ingestion_stats($1)", ingestionJobID)
	row.Scan(&rawStats)
	require.NoError(t, protoutil.Unmarshal(rawStats, stats))
	return stats
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

	// 'startTime' is a timestamp before we insert any data into the source cluster.
	c, cleanup := createTenantStreamingClusters(ctx, t, defaultTenantStreamingClustersArgs)
	defer cleanup()

	producerJobID, ingestionJobID := c.startStreamReplication()
	c.srcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, "CREATE TABLE d.x (id INT PRIMARY KEY, n INT)")
		tenantSQL.Exec(t, "IMPORT INTO d.x CSV DATA ($1)", dataSrv.URL)
	})

	var cutoverTime time.Time
	c.srcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		sysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)
	})

	stats := streamIngestionStats(t, c.destSysSQL, ingestionJobID)
	require.Equal(t, jobspb.StreamIngestionProgress_NotStarted, stats.IngestionProgress.CutoverStatus)
	require.True(t, stats.IngestionProgress.CutoverTime.IsEmpty())

	c.cutover(producerJobID, ingestionJobID, cutoverTime)
	// Check if the cutover status is finished.
	stats = streamIngestionStats(t, c.destSysSQL, ingestionJobID)
	require.Equal(t, jobspb.StreamIngestionProgress_Finished, stats.IngestionProgress.CutoverStatus)
	require.Equal(t, cutoverTime, stats.IngestionProgress.CutoverTime.GoTime())

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

func TestTenantStreamingProducerJobTimedOut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	c, cleanup := createTenantStreamingClusters(ctx, t, defaultTenantStreamingClustersArgs)
	defer cleanup()

	// initial scan
	producerJobID, ingestionJobID := c.startStreamReplication()

	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	var srcTime time.Time
	c.srcSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&srcTime)
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))

	c.compareResult("SELECT * FROM d.t1")
	c.compareResult("SELECT * FROM d.t2")

	stats := streamIngestionStats(t, c.destSysSQL, ingestionJobID)
	require.True(t, srcTime.Before(stats.ReplicationLagInfo.HighWatermark.GoTime()))
	require.Equal(t, "", stats.ProducerError)

	// Make producer job easily times out
	c.srcSysSQL.ExecMultiple(t, configureClusterSettings(map[string]string{
		`stream_replication.job_liveness_timeout`: `'100ms'`,
	})...)

	jobutils.WaitForJobToFail(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToPause(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	// Make dest cluster to ingest KV events faster.
	c.srcSysSQL.ExecMultiple(t, configureClusterSettings(map[string]string{
		`stream_replication.min_checkpoint_frequency`: `'100ms'`,
	})...)
	c.srcTenantSQL.Exec(t, "INSERT INTO d.t2 VALUES (3);")

	// Check the dst cluster didn't receive the change after a while.
	<-time.NewTimer(3 * time.Second).C
	require.Equal(t, [][]string{{"0"}},
		c.destTenantSQL.QueryStr(t, "SELECT count(*) FROM d.t2 WHERE i = 3"))

	// Confirm no cutover happens.
	stats = streamIngestionStats(t, c.destSysSQL, ingestionJobID)
	require.True(t, stats.IngestionProgress.CutoverTime.IsEmpty())
	require.Equal(t, jobspb.StreamIngestionProgress_NotStarted, stats.IngestionProgress.CutoverStatus)

	// After resumed, the ingestion job paused on failure again.
	c.destSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))
	jobutils.WaitForJobToPause(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
}

func TestTenantStreamingPauseResumeIngestion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := defaultTenantStreamingClustersArgs
	c, cleanup := createTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.startStreamReplication()

	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	var srcTime time.Time
	c.srcSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&srcTime)
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))

	c.compareResult("SELECT * FROM d.t1")
	c.compareResult("SELECT * FROM d.t2")

	// Pause ingestion.
	c.destSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", ingestionJobID))
	jobutils.WaitForJobToPause(t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	pausedCheckpoint := streamIngestionStats(t, c.destSysSQL, ingestionJobID).ReplicationLagInfo.HighWatermark
	// Check we paused at a timestamp greater than the previously reached high watermark
	require.True(t, srcTime.Before(pausedCheckpoint.GoTime()))

	// Introduce new update to the src.
	c.srcTenantSQL.Exec(t, "INSERT INTO d.t2 VALUES (3);")
	// Check the dst cluster didn't receive the new change after pausing for a while.
	<-time.NewTimer(3 * time.Second).C
	require.Equal(t, [][]string{{"0"}},
		c.destTenantSQL.QueryStr(t, "SELECT count(*) FROM d.t2 WHERE i = 3"))
	// Confirm that the job high watermark doesn't change. If the dest cluster is still subscribing
	// to src cluster checkpoints events, the job high watermark may change.
	require.Equal(t, pausedCheckpoint,
		streamIngestionStats(t, c.destSysSQL, ingestionJobID).ReplicationLagInfo.HighWatermark)

	// Resume ingestion.
	c.destSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))
	jobutils.WaitForJobToRun(t, c.srcSysSQL, jobspb.JobID(producerJobID))

	// Confirm that dest tenant has received the new change after resumption.
	c.srcSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&srcTime)
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))
	c.compareResult("SELECT * FROM d.t2")
	// Confirm this new run resumed from the previous checkpoint.
	require.Equal(t, pausedCheckpoint,
		streamIngestionStats(t, c.destSysSQL, ingestionJobID).IngestionProgress.StartTime)
}

func TestTenantStreamingPauseOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ingestErrCh := make(chan error, 1)
	args := defaultTenantStreamingClustersArgs
	args.testingKnobs = &sql.StreamingTestingKnobs{RunAfterReceivingEvent: func(ctx context.Context) error {
		return <-ingestErrCh
	}}
	c, cleanup := createTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	// Make ingestion error out only once.
	ingestErrCh <- errors.Newf("ingestion error from test")
	close(ingestErrCh)

	producerJobID, ingestionJobID := c.startStreamReplication()
	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToPause(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	// Check we didn't make any progress.
	require.Nil(t, streamIngestionStats(t, c.destSysSQL, ingestionJobID).ReplicationLagInfo)
	// Confirm we don't receive any change from src.
	c.destTenantSQL.ExpectErr(t, "\"d.t1\" does not exist", "SELECT * FROM d.t1")
	c.destTenantSQL.ExpectErr(t, "\"d.t2\" does not exist", "SELECT * FROM d.t2")

	// Resume ingestion.
	c.destSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))
	jobutils.WaitForJobToRun(t, c.srcSysSQL, jobspb.JobID(producerJobID))

	// Check dest has caught up the previous updates.
	var srcTime time.Time
	c.srcSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&srcTime)
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))
	c.compareResult("SELECT * FROM d.t1")
	c.compareResult("SELECT * FROM d.t2")

	// Confirm this new run resumed from the empty checkpoint.
	require.True(t, streamIngestionStats(t, c.destSysSQL, ingestionJobID).IngestionProgress.StartTime.IsEmpty())
}
