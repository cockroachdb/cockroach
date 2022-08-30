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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type srcInitExecFunc func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner)
type destInitExecFunc func(t *testing.T, sysSQL *sqlutils.SQLRunner) // Tenant is created by the replication stream

type tenantStreamingClustersArgs struct {
	srcTenantID roachpb.TenantID
	srcInitFunc srcInitExecFunc
	srcNumNodes int

	destTenantID roachpb.TenantID
	destInitFunc destInitExecFunc
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
	destInitFunc: func(t *testing.T, sysSQL *sqlutils.SQLRunner) {
		sysSQL.ExecMultiple(t, configureClusterSettings(destClusterSetting)...)
	},
	destNumNodes: 1,
}

type tenantStreamingClusters struct {
	t            *testing.T
	args         tenantStreamingClustersArgs
	srcCluster   *testcluster.TestCluster
	srcSysServer serverutils.TestServerInterface
	srcSysSQL    *sqlutils.SQLRunner
	srcTenantSQL *sqlutils.SQLRunner
	srcURL       url.URL

	destCluster   *testcluster.TestCluster
	destSysServer serverutils.TestServerInterface
	destSysSQL    *sqlutils.SQLRunner
}

// This function will fail the test if ran prior to the Replication stream
// closing as the tenant will not yet be active
func (c *tenantStreamingClusters) getDestTenantSQL() *sqlutils.SQLRunner {
	_, destTenantConn := serverutils.StartTenant(c.t, c.destSysServer, base.TestTenantArgs{TenantID: c.args.destTenantID, DisableCreateTenant: true, SkipTenantCheck: true})
	return sqlutils.MakeSQLRunner(destTenantConn)
}

func (c *tenantStreamingClusters) compareResult(query string) {
	sourceData := c.srcTenantSQL.QueryStr(c.t, query)
	destData := c.getDestTenantSQL().QueryStr(c.t, query)
	require.Equal(c.t, sourceData, destData)
}

// Waits for the ingestion job high watermark to reach the given high watermark.
func (c *tenantStreamingClusters) waitUntilHighWatermark(
	highWatermark hlc.Timestamp, ingestionJobID jobspb.JobID,
) {
	testutils.SucceedsSoon(c.t, func() error {
		progress := jobutils.GetJobProgress(c.t, c.destSysSQL, ingestionJobID)
		if progress.GetHighWater() == nil {
			return errors.Newf("stream ingestion has not recorded any progress yet, waiting to advance pos %s",
				highWatermark.String())
		}
		highwater := *progress.GetHighWater()
		if highwater.Less(highWatermark) {
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

	startTestCluster := func(
		ctx context.Context,
		t *testing.T,
		serverArgs base.TestServerArgs,
		numNodes int,
	) (*testcluster.TestCluster, url.URL, func()) {
		params := base.TestClusterArgs{ServerArgs: serverArgs}
		c := testcluster.StartTestCluster(t, numNodes, params)
		c.Server(0).Clock().Now()
		// TODO(casper): support adding splits when we have multiple nodes.
		pgURL, cleanupSinkCert := sqlutils.PGUrl(t, c.Server(0).ServingSQLAddr(), t.Name(), url.User(username.RootUser))
		return c, pgURL, func() {
			c.Stopper().Stop(ctx)
			cleanupSinkCert()
		}
	}

	// Start the source cluster and tenant.
	srcCluster, srcURL, srcCleanup := startTestCluster(ctx, t, serverArgs, args.srcNumNodes)
	_, srcTenantConn := serverutils.StartTenant(t, srcCluster.Server(0), base.TestTenantArgs{TenantID: args.srcTenantID})

	// Start the destination cluster.
	destCluster, _, destCleanup := startTestCluster(ctx, t, serverArgs, args.destNumNodes)

	tsc := &tenantStreamingClusters{
		t:             t,
		args:          args,
		srcCluster:    srcCluster,
		srcSysSQL:     sqlutils.MakeSQLRunner(srcCluster.ServerConn(0)),
		srcTenantSQL:  sqlutils.MakeSQLRunner(srcTenantConn),
		srcSysServer:  srcCluster.Server(0),
		srcURL:        srcURL,
		destCluster:   destCluster,
		destSysSQL:    sqlutils.MakeSQLRunner(destCluster.ServerConn(0)),
		destSysServer: destCluster.Server(0),
	}

	if args.srcInitFunc != nil {
		args.srcInitFunc(t, tsc.srcSysSQL, tsc.srcTenantSQL)
	}
	if args.destInitFunc != nil {
		args.destInitFunc(t, tsc.destSysSQL)
	}
	// Enable stream replication on dest by default.
	tsc.destSysSQL.Exec(t, `SET enable_experimental_stream_replication = true;`)
	return tsc, func() {
		require.NoError(t, srcTenantConn.Close())
		destCleanup()
		srcCleanup()
	}
}

func (c *tenantStreamingClusters) srcExec(exec srcInitExecFunc) {
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
	`stream_replication.job_checkpoint_frequency`:          `'100ms'`,
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
	row := sqlRunner.QueryRow(t, "SELECT crdb_internal.stream_ingestion_stats_pb($1)", ingestionJobID)
	row.Scan(&rawStats)
	require.NoError(t, protoutil.Unmarshal(rawStats, stats))
	return stats
}

func TestTenantStreamingSuccessfulIngestion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRaceWithIssue(t, 83867)

	dataSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			if _, err := w.Write([]byte("42,42\n43,43\n")); err != nil {
				t.Logf("failed to write: %s", err.Error())
			}
		}
	}))
	defer dataSrv.Close()

	ctx := context.Background()
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

	// TODO(samiskin): enable this check once #83650 is resolved
	// // We should not be able to connect to the tenant prior to the cutoff time
	// <-time.NewTimer(2 * time.Second).C
	// _, err := c.destSysServer.StartTenant(context.Background(), base.TestTenantArgs{TenantID: c.args.destTenantID, DisableCreateTenant: true, SkipTenantCheck: true})
	// require.Error(t, err)

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
	require.Equal(t, [][]string{{"2"}}, c.getDestTenantSQL().QueryStr(t, "SELECT * FROM d.t2"))
}

func TestTenantStreamingProducerJobTimedOut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(casper): now this has the same race issue with
	// TestPartitionedStreamReplicationClient, please fix them together in the future.
	skip.UnderRace(t, "disabled under race")

	ctx := context.Background()
	c, cleanup := createTenantStreamingClusters(ctx, t, defaultTenantStreamingClustersArgs)
	defer cleanup()

	// initial scan
	producerJobID, ingestionJobID := c.startStreamReplication()

	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.srcCluster.Server(0).Clock().Now()
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))

	c.compareResult("SELECT * FROM d.t1")
	c.compareResult("SELECT * FROM d.t2")

	stats := streamIngestionStats(t, c.destSysSQL, ingestionJobID)
	require.True(t, srcTime.LessEq(stats.ReplicationLagInfo.MinIngestedTimestamp))
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
		c.getDestTenantSQL().QueryStr(t, "SELECT count(*) FROM d.t2 WHERE i = 3"))

	// After resumed, the ingestion job paused on failure again.
	c.destSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))
	jobutils.WaitForJobToPause(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
}

func TestTenantStreamingPauseResumeIngestion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(casper): now this has the same race issue with
	// TestPartitionedStreamReplicationClient, please fix them together in the future.
	skip.UnderRace(t, "disabled under race")

	ctx := context.Background()
	args := defaultTenantStreamingClustersArgs
	c, cleanup := createTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.startStreamReplication()

	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.srcCluster.Server(0).Clock().Now()
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))

	c.compareResult("SELECT * FROM d.t1")
	c.compareResult("SELECT * FROM d.t2")

	// Pause ingestion.
	c.destSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", ingestionJobID))
	jobutils.WaitForJobToPause(t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	pausedCheckpoint := streamIngestionStats(t, c.destSysSQL, ingestionJobID).
		ReplicationLagInfo.MinIngestedTimestamp
	// Check we paused at a timestamp greater than the previously reached high watermark
	require.True(t, srcTime.LessEq(pausedCheckpoint))

	// Introduce new update to the src.
	c.srcTenantSQL.Exec(t, "INSERT INTO d.t2 VALUES (3);")
	// Confirm that the job high watermark doesn't change. If the dest cluster is still subscribing
	// to src cluster checkpoints events, the job high watermark may change.
	<-time.NewTimer(3 * time.Second).C
	require.Equal(t, pausedCheckpoint,
		streamIngestionStats(t, c.destSysSQL, ingestionJobID).ReplicationLagInfo.MinIngestedTimestamp)

	// Resume ingestion.
	c.destSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))
	jobutils.WaitForJobToRun(t, c.srcSysSQL, jobspb.JobID(producerJobID))

	// Confirm that dest tenant has received the new change after resumption.
	srcTime = c.srcCluster.Server(0).Clock().Now()
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))
	c.compareResult("SELECT * FROM d.t2")
	// Confirm this new run resumed from the previous checkpoint.
	require.Equal(t, pausedCheckpoint,
		streamIngestionStats(t, c.destSysSQL, ingestionJobID).IngestionProgress.StartTime)
}

func TestTenantStreamingPauseOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(casper): now this has the same race issue with
	// TestPartitionedStreamReplicationClient, please fix them together in the future.
	skip.UnderRace(t, "disabled under race")

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

	// Resume ingestion.
	c.destSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))
	jobutils.WaitForJobToRun(t, c.srcSysSQL, jobspb.JobID(producerJobID))

	// Check dest has caught up the previous updates.
	srcTime := c.srcCluster.Server(0).Clock().Now()
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))
	c.compareResult("SELECT * FROM d.t1")
	c.compareResult("SELECT * FROM d.t2")

	// Confirm this new run resumed from the empty checkpoint.
	require.True(t,
		streamIngestionStats(t, c.destSysSQL, ingestionJobID).IngestionProgress.StartTime.IsEmpty())
}

func TestTenantStreamingCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRaceWithIssue(t, 83867)
	skip.UnderStressRace(t, "slow under stressrace")

	ctx := context.Background()

	lastClientStart := make(map[string]hlc.Timestamp)
	args := defaultTenantStreamingClustersArgs
	args.testingKnobs = &sql.StreamingTestingKnobs{
		BeforeClientSubscribe: func(token string, clientStartTime hlc.Timestamp) {
			lastClientStart[token] = clientStartTime
		},
	}
	c, cleanup := createTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.startStreamReplication()

	// Helper to read job progress
	jobRegistry := c.destSysServer.JobRegistry().(*jobs.Registry)
	loadIngestProgress := func() *jobspb.StreamIngestionProgress {
		job, err := jobRegistry.LoadJob(context.Background(), jobspb.JobID(ingestionJobID))
		require.NoError(t, err)

		progress := job.Progress()
		ingestProgress := progress.Details.(*jobspb.Progress_StreamIngest).StreamIngest
		return ingestProgress
	}

	c.srcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, "CREATE TABLE d.x (id INT PRIMARY KEY, n INT)")
		tenantSQL.Exec(t, "INSERT INTO d.x VALUES (1, 1)")
	})

	srcCodec := keys.MakeSQLCodec(c.args.srcTenantID)
	t1Desc := desctestutils.TestingGetPublicTableDescriptor(
		c.srcSysServer.DB(), srcCodec, "d", "t1")
	t2Desc := desctestutils.TestingGetPublicTableDescriptor(
		c.srcSysServer.DB(), keys.MakeSQLCodec(c.args.srcTenantID), "d", "t2")
	xDesc := desctestutils.TestingGetPublicTableDescriptor(
		c.srcSysServer.DB(), keys.MakeSQLCodec(c.args.srcTenantID), "d", "x")
	t1Span := t1Desc.PrimaryIndexSpan(srcCodec)
	t2Span := t2Desc.PrimaryIndexSpan(srcCodec)
	xSpan := xDesc.PrimaryIndexSpan(srcCodec)
	tableSpans := []roachpb.Span{t1Span, t2Span, xSpan}

	var checkpointMinTime time.Time
	c.srcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		sysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&checkpointMinTime)
	})
	testutils.SucceedsSoon(t, func() error {
		prog := loadIngestProgress()
		if len(prog.Checkpoint.ResolvedSpans) == 0 {
			return errors.New("waiting for checkpoint")
		}
		var checkpointSpanGroup roachpb.SpanGroup
		for _, resolvedSpan := range prog.Checkpoint.ResolvedSpans {
			checkpointSpanGroup.Add(resolvedSpan.Span)
			if checkpointMinTime.After(resolvedSpan.Timestamp.GoTime()) {
				return errors.New("checkpoint not yet advanced")
			}
		}
		if !checkpointSpanGroup.Encloses(tableSpans...) {
			return errors.Newf("checkpoint %v is missing spans from table spans (%+v)", checkpointSpanGroup, tableSpans)
		}
		return nil
	})

	c.destSysSQL.Exec(t, `PAUSE JOB $1`, ingestionJobID)
	jobutils.WaitForJobToPause(t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	// Clear out the map to ignore the initial client starts
	lastClientStart = make(map[string]hlc.Timestamp)

	c.destSysSQL.Exec(t, `RESUME JOB $1`, ingestionJobID)
	jobutils.WaitForJobToRun(t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	var cutoverTime time.Time
	c.srcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		sysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)
	})
	c.cutover(producerJobID, ingestionJobID, cutoverTime)

	// Clients should never be started prior to a checkpointed timestamp
	for _, clientStartTime := range lastClientStart {
		require.Less(t, checkpointMinTime.UnixNano(), clientStartTime.GoTime().UnixNano())
	}

	c.compareResult("SELECT * FROM d.t1")
	c.compareResult("SELECT * FROM d.t2")
	c.compareResult("SELECT * FROM d.x")
	// After cutover, changes to source won't be streamed into destination cluster.
	c.srcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, `INSERT INTO d.t2 VALUES (3);`)
	})
	// Check the dst cluster didn't receive the change after a while.
	<-time.NewTimer(3 * time.Second).C
	require.Equal(t, [][]string{{"2"}}, c.getDestTenantSQL().QueryStr(t, "SELECT * FROM d.t2"))
}
