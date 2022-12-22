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
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type srcInitExecFunc func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner)
type destInitExecFunc func(t *testing.T, sysSQL *sqlutils.SQLRunner) // Tenant is created by the replication stream

type tenantStreamingClustersArgs struct {
	srcTenantName      roachpb.TenantName
	srcTenantID        roachpb.TenantID
	srcInitFunc        srcInitExecFunc
	srcNumNodes        int
	srcClusterSettings map[string]string

	destTenantName      roachpb.TenantName
	destTenantID        roachpb.TenantID
	destInitFunc        destInitExecFunc
	destNumNodes        int
	destClusterSettings map[string]string
	retentionTTLSeconds int
	testingKnobs        *sql.StreamingTestingKnobs
}

var defaultTenantStreamingClustersArgs = tenantStreamingClustersArgs{
	srcTenantName: roachpb.TenantName("source"),
	srcTenantID:   roachpb.MustMakeTenantID(10),
	srcInitFunc: func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, `
	CREATE DATABASE d;
	CREATE TABLE d.t1(i int primary key, a string, b string);
	CREATE TABLE d.t2(i int primary key);
	INSERT INTO d.t1 (i) VALUES (42);
	INSERT INTO d.t2 VALUES (2);
	UPDATE d.t1 SET b = 'world' WHERE i = 42;
	`)
	},
	srcNumNodes:         1,
	srcClusterSettings:  defaultSrcClusterSetting,
	destTenantName:      roachpb.TenantName("destination"),
	destTenantID:        roachpb.MustMakeTenantID(2),
	destNumNodes:        1,
	destClusterSettings: defaultDestClusterSetting,
}

type tenantStreamingClusters struct {
	t               *testing.T
	args            tenantStreamingClustersArgs
	srcCluster      *testcluster.TestCluster
	srcTenantConn   *gosql.DB
	srcSysServer    serverutils.TestServerInterface
	srcSysSQL       *sqlutils.SQLRunner
	srcTenantSQL    *sqlutils.SQLRunner
	srcTenantServer serverutils.TestTenantInterface
	srcURL          url.URL
	srcCleanup      func()

	destCluster   *testcluster.TestCluster
	destSysServer serverutils.TestServerInterface
	destSysSQL    *sqlutils.SQLRunner
	destTenantSQL *sqlutils.SQLRunner
}

// Creates a dest tenant SQL runner and returns a cleanup function that shuts
// tenant SQL instance and closes all sessions.
// This function will fail the test if ran prior to the Replication stream
// closing as the tenant will not yet be active
func (c *tenantStreamingClusters) createDestTenantSQL(ctx context.Context) func() error {
	testTenant, destTenantConn := serverutils.StartTenant(c.t, c.destSysServer,
		base.TestTenantArgs{TenantID: c.args.destTenantID, DisableCreateTenant: true, SkipTenantCheck: true})
	c.destTenantSQL = sqlutils.MakeSQLRunner(destTenantConn)
	return func() error {
		if err := destTenantConn.Close(); err != nil {
			return err
		}
		testTenant.Stopper().Stop(ctx)
		return nil
	}
}

// This function has to be called after createTenantSQL.
func (c *tenantStreamingClusters) compareResult(query string) {
	require.NotNil(c.t, c.destTenantSQL,
		"destination tenant SQL runner should be created first")
	sourceData := c.srcTenantSQL.QueryStr(c.t, query)
	destData := c.destTenantSQL.QueryStr(c.t, query)
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

// Waits for the ingestion job high watermark to reach the recorded start time of the job.
func (c *tenantStreamingClusters) waitUntilStartTimeReached(ingestionJobID jobspb.JobID) {
	waitUntilStartTimeReached(c.t, c.destSysSQL, ingestionJobID)
}

func waitUntilStartTimeReached(t *testing.T, db *sqlutils.SQLRunner, ingestionJobID jobspb.JobID) {
	testutils.SucceedsSoon(t, func() error {
		payload := jobutils.GetJobPayload(t, db, ingestionJobID)
		details, ok := payload.Details.(*jobspb.Payload_StreamIngestion)
		if !ok {
			return errors.New("job does not appear to be a stream ingestion job")
		}
		if details.StreamIngestion == nil {
			return errors.New("no stream ingestion details")
		}
		startTime := details.StreamIngestion.ReplicationStartTime
		if startTime.IsEmpty() {
			return errors.New("ingestion start time not yet recorded")
		}

		progress := jobutils.GetJobProgress(t, db, ingestionJobID)
		if progress.GetHighWater() == nil {
			return errors.Newf("stream ingestion has not recorded any progress yet, waiting to advance pos %s",
				startTime.String())
		}
		highwater := *progress.GetHighWater()
		if highwater.Less(startTime) {
			return errors.Newf("waiting for stream ingestion job progress %s to advance beyond %s",
				highwater.String(), startTime.String())
		}
		return nil
	})
}

func (c *tenantStreamingClusters) cutover(
	producerJobID, ingestionJobID int, cutoverTime time.Time,
) {
	// Cut over the ingestion job and the job will stop eventually.
	c.destSysSQL.Exec(c.t, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`, c.args.destTenantName, cutoverTime)
	jobutils.WaitForJobToSucceed(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	jobutils.WaitForJobToSucceed(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
}

// Returns producer job ID and ingestion job ID.
func (c *tenantStreamingClusters) startStreamReplication() (int, int) {
	var ingestionJobID, streamProducerJobID int
	c.destSysSQL.QueryRow(c.t, c.buildCreateTenantQuery()).Scan(&ingestionJobID, &streamProducerJobID)
	return streamProducerJobID, ingestionJobID
}

func (c *tenantStreamingClusters) buildCreateTenantQuery() string {
	streamReplStmt := fmt.Sprintf("CREATE TENANT %s FROM REPLICATION OF %s ON '%s'",
		c.args.destTenantName,
		c.args.srcTenantName,
		c.srcURL.String())
	if c.args.retentionTTLSeconds > 0 {
		streamReplStmt = fmt.Sprintf("%s WITH RETENTION = '%ds'", streamReplStmt, c.args.retentionTTLSeconds)
	}
	return streamReplStmt
}

func waitForTenantPodsActive(
	t testing.TB, tenantServer serverutils.TestTenantInterface, numPods int,
) {
	testutils.SucceedsWithin(t, func() error {
		status := tenantServer.StatusServer().(serverpb.SQLStatusServer)
		var nodes *serverpb.NodesListResponse
		var err error
		for nodes == nil || len(nodes.Nodes) != numPods {
			nodes, err = status.NodesList(context.Background(), nil)
			if err != nil {
				return err
			}
		}
		return nil
	}, 10*time.Second)
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
			Streaming: args.testingKnobs,
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

	// Start the source cluster.
	srcCluster, srcURL, srcCleanup := startTestCluster(ctx, t, serverArgs, args.srcNumNodes)

	tenantArgs := base.TestTenantArgs{
		TenantName: args.srcTenantName,
		TenantID:   args.srcTenantID,
		TestingKnobs: base.TestingKnobs{
			TenantTestingKnobs: &sql.TenantTestingKnobs{
				AllowSplitAndScatter: true,
			}},
	}
	srcTenantServer, srcTenantConn := serverutils.StartTenant(t, srcCluster.Server(0), tenantArgs)
	waitForTenantPodsActive(t, srcTenantServer, 1)

	// Start the destination cluster.
	destCluster, _, destCleanup := startTestCluster(ctx, t, serverArgs, args.destNumNodes)

	tsc := &tenantStreamingClusters{
		t:               t,
		args:            args,
		srcCluster:      srcCluster,
		srcTenantConn:   srcTenantConn,
		srcTenantServer: srcTenantServer,
		srcSysSQL:       sqlutils.MakeSQLRunner(srcCluster.ServerConn(0)),
		srcTenantSQL:    sqlutils.MakeSQLRunner(srcTenantConn),
		srcSysServer:    srcCluster.Server(0),
		srcURL:          srcURL,
		srcCleanup:      srcCleanup,
		destCluster:     destCluster,
		destSysSQL:      sqlutils.MakeSQLRunner(destCluster.ServerConn(0)),
		destSysServer:   destCluster.Server(0),
	}

	tsc.srcSysSQL.ExecMultiple(t, configureClusterSettings(args.srcClusterSettings)...)
	if args.srcInitFunc != nil {
		args.srcInitFunc(t, tsc.srcSysSQL, tsc.srcTenantSQL)
	}
	tsc.destSysSQL.ExecMultiple(t, configureClusterSettings(args.destClusterSettings)...)
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

func createScatteredTable(t *testing.T, c *tenantStreamingClusters, numNodes int) {
	// Create a source table with multiple ranges spread across multiple nodes
	numRanges := 50
	rowsPerRange := 20
	c.srcTenantSQL.Exec(t, fmt.Sprintf(`
  CREATE TABLE d.scattered (key INT PRIMARY KEY);
  INSERT INTO d.scattered (key) SELECT * FROM generate_series(1, %d);
  ALTER TABLE d.scattered SPLIT AT (SELECT * FROM generate_series(%d, %d, %d));
  ALTER TABLE d.scattered SCATTER;
  `, numRanges*rowsPerRange, rowsPerRange, (numRanges-1)*rowsPerRange, rowsPerRange))
	c.srcSysSQL.CheckQueryResultsRetry(t, "SELECT count(distinct lease_holder) from crdb_internal.ranges", [][]string{{fmt.Sprint(numNodes)}})
}

var defaultSrcClusterSetting = map[string]string{
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

var defaultDestClusterSetting = map[string]string{
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

func runningStatus(t *testing.T, sqlRunner *sqlutils.SQLRunner, ingestionJobID int) string {
	p := jobutils.GetJobProgress(t, sqlRunner, jobspb.JobID(ingestionJobID))
	return p.RunningStatus
}

func TestTenantStreamingSuccessfulIngestion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(casper): disabled due to error when setting a cluster setting
	// "setting updated but timed out waiting to read new value"
	skip.UnderStressRace(t, "disabled under stress race")

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

	c.waitUntilStartTimeReached(jobspb.JobID(ingestionJobID))

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

	require.Equal(t, "stream ingestion finished successfully",
		runningStatus(t, c.destSysSQL, ingestionJobID))

	cleanupTenant := c.createDestTenantSQL(ctx)
	defer func() {
		require.NoError(t, cleanupTenant())
	}()

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

	// TODO(casper): disabled due to error when setting a cluster setting
	// "setting updated but timed out waiting to read new value"
	skip.UnderStressRace(t, "disabled under stress race")

	ctx := context.Background()
	args := defaultTenantStreamingClustersArgs
	args.srcClusterSettings[`stream_replication.job_liveness_timeout`] = `'1m'`
	c, cleanup := createTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.startStreamReplication()

	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.srcCluster.Server(0).Clock().Now()
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))

	cleanupTenant := c.createDestTenantSQL(ctx)
	defer func() {
		require.NoError(t, cleanupTenant())
	}()
	c.compareResult("SELECT * FROM d.t1")
	c.compareResult("SELECT * FROM d.t2")

	stats := streamIngestionStats(t, ctx, c.destSysSQL, ingestionJobID)

	require.NotNil(t, stats.ReplicationLagInfo)
	require.True(t, srcTime.LessEq(stats.ReplicationLagInfo.MinIngestedTimestamp))
	require.Equal(t, "", stats.ProducerError)

	// Make producer job easily times out
	c.srcSysSQL.ExecMultiple(t, configureClusterSettings(map[string]string{
		`stream_replication.job_liveness_timeout`: `'100ms'`,
	})...)

	jobutils.WaitForJobToFail(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	// The ingestion job will stop retrying as this is a permanent job error.
	jobutils.WaitForJobToPause(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	require.Regexp(t, "ingestion job failed .* but is being paused", runningStatus(t, c.destSysSQL, ingestionJobID))

	// Make dest cluster to ingest KV events faster.
	c.srcSysSQL.ExecMultiple(t, configureClusterSettings(map[string]string{
		`stream_replication.min_checkpoint_frequency`: `'100ms'`,
	})...)
	c.srcTenantSQL.Exec(t, "INSERT INTO d.t2 VALUES (3);")

	// Check the dst cluster didn't receive the change after a while.
	<-time.NewTimer(3 * time.Second).C
	require.Equal(t, [][]string{{"0"}},
		c.destTenantSQL.QueryStr(t, "SELECT count(*) FROM d.t2 WHERE i = 3"))

	// After resumed, the ingestion job paused on failure again.
	c.destSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))
	jobutils.WaitForJobToPause(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
}

func TestTenantStreamingPauseResumeIngestion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(casper): disabled due to error when setting a cluster setting
	// "setting updated but timed out waiting to read new value"
	skip.UnderStressRace(t, "disabled under stress race")

	ctx := context.Background()
	args := defaultTenantStreamingClustersArgs
	c, cleanup := createTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.startStreamReplication()

	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.srcCluster.Server(0).Clock().Now()
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))

	cleanupTenant := c.createDestTenantSQL(ctx)
	defer func() {
		require.NoError(t, cleanupTenant())
	}()

	c.compareResult("SELECT * FROM d.t1")
	c.compareResult("SELECT * FROM d.t2")

	// Pause ingestion.
	c.destSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", ingestionJobID))
	jobutils.WaitForJobToPause(t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	pausedCheckpoint := streamIngestionStats(t, ctx, c.destSysSQL, ingestionJobID).
		ReplicationLagInfo.MinIngestedTimestamp
	// Check we paused at a timestamp greater than the previously reached high watermark
	require.True(t, srcTime.LessEq(pausedCheckpoint))

	// Introduce new update to the src.
	c.srcTenantSQL.Exec(t, "INSERT INTO d.t2 VALUES (3);")
	// Confirm that the job high watermark doesn't change. If the dest cluster is still subscribing
	// to src cluster checkpoints events, the job high watermark may change.
	<-time.NewTimer(3 * time.Second).C
	require.Equal(t, pausedCheckpoint,
		streamIngestionStats(t, ctx, c.destSysSQL, ingestionJobID).ReplicationLagInfo.MinIngestedTimestamp)

	// Resume ingestion.
	c.destSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))
	jobutils.WaitForJobToRun(t, c.srcSysSQL, jobspb.JobID(producerJobID))

	// Confirm that dest tenant has received the new change after resumption.
	srcTime = c.srcCluster.Server(0).Clock().Now()
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))
	c.compareResult("SELECT * FROM d.t2")
}

func TestTenantStreamingPauseOnPermanentJobError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(casper): disabled due to error when setting a cluster setting
	// "setting updated but timed out waiting to read new value"
	skip.UnderStressRace(t, "disabled under stress race")

	ctx := context.Background()
	ingestErrCh := make(chan error, 1)
	ingestionStarts := 0
	args := defaultTenantStreamingClustersArgs
	args.testingKnobs = &sql.StreamingTestingKnobs{
		RunAfterReceivingEvent: func(ctx context.Context) error {
			return <-ingestErrCh
		},
		BeforeIngestionStart: func(ctx context.Context) error {
			ingestionStarts++
			if ingestionStarts == 2 {
				return jobs.MarkAsPermanentJobError(errors.New("test error"))
			}
			return nil
		},
	}
	c, cleanup := createTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	// Make ingestion error out only once.
	ingestErrCh <- errors.Newf("ingestion error from test")
	close(ingestErrCh)

	producerJobID, ingestionJobID := c.startStreamReplication()
	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToPause(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	// Ingestion is retried once after having an ingestion error.
	require.Equal(t, 2, ingestionStarts)

	// Check we didn't make any progress.
	require.Nil(t, streamIngestionStats(t, ctx, c.destSysSQL, ingestionJobID).ReplicationLagInfo)

	// Resume ingestion.
	c.destSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))
	jobutils.WaitForJobToRun(t, c.srcSysSQL, jobspb.JobID(producerJobID))

	// Check dest has caught up the previous updates.
	srcTime := c.srcCluster.Server(0).Clock().Now()
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))

	cleanupTenant := c.createDestTenantSQL(ctx)
	defer func() {
		require.NoError(t, cleanupTenant())
	}()

	c.compareResult("SELECT * FROM d.t1")
	c.compareResult("SELECT * FROM d.t2")

	// Ingestion happened one more time after resuming the ingestion job.
	require.Equal(t, 3, ingestionStarts)
}

func TestTenantStreamingCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(casper): disabled due to error when setting a cluster setting
	// "setting updated but timed out waiting to read new value"
	skip.UnderStressRace(t, "disabled under stress race")

	ctx := context.Background()

	lastClientStart := make(map[string]hlc.Timestamp)
	args := defaultTenantStreamingClustersArgs
	args.testingKnobs = &sql.StreamingTestingKnobs{
		BeforeClientSubscribe: func(addr string, token string, clientStartTime hlc.Timestamp) {
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

	cleanupTenant := c.createDestTenantSQL(ctx)
	defer func() {
		require.NoError(t, cleanupTenant())
	}()

	c.destSysSQL.Exec(t, `PAUSE JOB $1`, ingestionJobID)
	jobutils.WaitForJobToPause(t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	// Clear out the map to ignore the initial client starts
	lastClientStart = make(map[string]hlc.Timestamp)

	c.destSysSQL.Exec(t, `RESUME JOB $1`, ingestionJobID)
	jobutils.WaitForJobToRun(t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	cutoverTime := c.destSysServer.Clock().Now()
	c.waitUntilHighWatermark(cutoverTime, jobspb.JobID(ingestionJobID))
	c.cutover(producerJobID, ingestionJobID, cutoverTime.GoTime())

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
	require.Equal(t, [][]string{{"2"}}, c.destTenantSQL.QueryStr(t, "SELECT * FROM d.t2"))
}

func TestTenantStreamingCancelIngestion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := defaultTenantStreamingClustersArgs

	testCancelIngestion := func(t *testing.T, cancelAfterPaused bool) {
		c, cleanup := createTenantStreamingClusters(ctx, t, args)
		defer cleanup()
		producerJobID, ingestionJobID := c.startStreamReplication()

		jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
		jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

		srcTime := c.srcCluster.Server(0).Clock().Now()
		c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))

		cleanUpTenant := c.createDestTenantSQL(ctx)
		c.compareResult("SELECT * FROM d.t1")
		c.compareResult("SELECT * FROM d.t2")

		if cancelAfterPaused {
			c.destSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", ingestionJobID))
			jobutils.WaitForJobToPause(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
		}

		// Close all tenant SQL sessions before we cancel the job so that
		// we don't have any process still writing to 'sqlliveness' table after
		// the tenant key range is cleared.
		require.NoError(t, cleanUpTenant())

		c.destSysSQL.Exec(t, fmt.Sprintf("CANCEL JOB %d", ingestionJobID))
		jobutils.WaitForJobToCancel(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
		jobutils.WaitForJobToCancel(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))

		// Check if the producer job has released protected timestamp.
		stats := streamIngestionStats(t, ctx, c.destSysSQL, ingestionJobID)
		require.NotNil(t, stats.ProducerStatus)
		require.Nil(t, stats.ProducerStatus.ProtectedTimestamp)

		// Check if dest tenant key ranges are not cleaned up.
		destTenantPrefix := keys.MakeTenantPrefix(args.destTenantID)

		rows, err := c.destCluster.Server(0).DB().
			Scan(ctx, destTenantPrefix, destTenantPrefix.PrefixEnd(), 10)
		require.NoError(t, err)
		require.NotEmpty(t, rows)

		// Check if the tenant record still exits.
		c.destSysSQL.CheckQueryResults(t,
			fmt.Sprintf("SELECT count(*) FROM system.tenants WHERE id = %s", args.destTenantID),
			[][]string{{"1"}})

		// Check if we can successfully GC the tenant.
		c.destSysSQL.Exec(t, "SELECT crdb_internal.destroy_tenant($1, true)",
			args.destTenantID.ToUint64())
		rows, err = c.destCluster.Server(0).DB().
			Scan(ctx, destTenantPrefix, destTenantPrefix.PrefixEnd(), 10)
		require.NoError(t, err)
		require.Empty(t, rows)

		c.destSysSQL.CheckQueryResults(t,
			fmt.Sprintf("SELECT count(*) FROM system.tenants WHERE id = %s", args.destTenantID),
			[][]string{{"0"}})
	}

	t.Run("cancel-ingestion-after-paused", func(t *testing.T) {
		testCancelIngestion(t, true)
	})

	t.Run("cancel-ingestion-while-running", func(t *testing.T) {
		testCancelIngestion(t, false)
	})
}

func TestTenantStreamingDropTenantCancelsStream(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := defaultTenantStreamingClustersArgs

	testCancelIngestion := func(t *testing.T, cancelAfterPaused bool) {
		c, cleanup := createTenantStreamingClusters(ctx, t, args)
		defer cleanup()
		producerJobID, ingestionJobID := c.startStreamReplication()

		c.destSysSQL.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
		c.destSysSQL.Exec(t, "SET CLUSTER SETTING kv.protectedts.reconciliation.interval = '1ms';")

		jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
		jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

		c.waitUntilHighWatermark(c.srcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))
		if cancelAfterPaused {
			c.destSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", ingestionJobID))
			jobutils.WaitForJobToPause(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
		}

		// Set GC TTL low, so that the GC job completes quickly in the test.
		c.destSysSQL.Exec(t, "ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = 1;")
		c.destSysSQL.Exec(t, fmt.Sprintf("DROP TENANT %s", c.args.destTenantName))
		jobutils.WaitForJobToCancel(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
		jobutils.WaitForJobToCancel(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))

		// Check if the producer job has released protected timestamp.
		stats := streamIngestionStats(t, ctx, c.destSysSQL, ingestionJobID)
		require.NotNil(t, stats.ProducerStatus)
		require.Nil(t, stats.ProducerStatus.ProtectedTimestamp)

		// Wait for the GC job to finish
		c.destSysSQL.Exec(t, "SHOW JOBS WHEN COMPLETE SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE GC'")

		// Check if dest tenant key range is cleaned up.
		destTenantPrefix := keys.MakeTenantPrefix(args.destTenantID)
		rows, err := c.destCluster.Server(0).DB().
			Scan(ctx, destTenantPrefix, destTenantPrefix.PrefixEnd(), 10)
		require.NoError(t, err)
		require.Empty(t, rows)

		c.destSysSQL.CheckQueryResults(t,
			fmt.Sprintf("SELECT count(*) FROM system.tenants WHERE id = %s", args.destTenantID),
			[][]string{{"0"}})
	}

	t.Run("drop-tenant-after-paused", func(t *testing.T) {
		testCancelIngestion(t, true)
	})

	t.Run("drop-tenant-while-running", func(t *testing.T) {
		testCancelIngestion(t, false)
	})
}

func TestTenantStreamingUnavailableStreamAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "takes too long with multiple nodes")

	ctx := context.Background()
	args := defaultTenantStreamingClustersArgs

	args.srcNumNodes = 3
	args.destNumNodes = 3

	c, cleanup := createTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	createScatteredTable(t, c, 3)
	srcScatteredData := c.srcTenantSQL.QueryStr(c.t, "SELECT * FROM d.scattered ORDER BY key")

	producerJobID, ingestionJobID := c.startStreamReplication()
	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.srcCluster.Server(0).Clock().Now()
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))

	c.destSysSQL.Exec(t, `PAUSE JOB $1`, ingestionJobID)
	jobutils.WaitForJobToPause(t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	// We should've persisted the original topology
	progress := jobutils.GetJobProgress(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	streamAddresses := progress.GetStreamIngest().StreamAddresses
	require.Greater(t, len(streamAddresses), 1)

	destroyedAddress := c.srcURL.String()

	require.NoError(t, c.srcTenantConn.Close())
	c.srcTenantServer.Stopper().Stop(ctx)
	c.srcCluster.StopServer(0)

	// Once srcCluster.Server(0) is shut down queries must be ran against a different server
	alternateSrcSysSQL := sqlutils.MakeSQLRunner(c.srcCluster.ServerConn(1))
	_, alternateSrcTenantConn := serverutils.StartTenant(t, c.srcCluster.Server(1),
		base.TestTenantArgs{
			TenantID:            c.args.srcTenantID,
			TenantName:          c.args.srcTenantName,
			DisableCreateTenant: true,
		})
	defer alternateSrcTenantConn.Close()
	alternateSrcTenantSQL := sqlutils.MakeSQLRunner(alternateSrcTenantConn)

	cleanUpTenant := c.createDestTenantSQL(ctx)
	defer func() {
		require.NoError(t, cleanUpTenant())
	}()

	alternateCompareResult := func(query string) {
		sourceData := alternateSrcTenantSQL.QueryStr(c.t, query)
		destData := c.destTenantSQL.QueryStr(c.t, query)
		require.Equal(c.t, sourceData, destData)
	}

	c.destSysSQL.Exec(t, `RESUME JOB $1`, ingestionJobID)
	jobutils.WaitForJobToRun(t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	alternateSrcTenantSQL.Exec(t, "CREATE TABLE d.x (id INT PRIMARY KEY, n INT)")
	alternateSrcTenantSQL.Exec(t, `INSERT INTO d.x VALUES (3);`)

	var cutoverTime time.Time
	alternateSrcSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)

	c.destSysSQL.Exec(c.t, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`, c.args.destTenantName, cutoverTime)
	jobutils.WaitForJobToSucceed(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	// The destroyed address should have been removed from the topology
	progress = jobutils.GetJobProgress(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	newStreamAddresses := progress.GetStreamIngest().StreamAddresses
	require.Contains(t, streamAddresses, destroyedAddress)
	require.NotContains(t, newStreamAddresses, destroyedAddress)

	alternateCompareResult("SELECT * FROM d.t1")
	alternateCompareResult("SELECT * FROM d.t2")
	alternateCompareResult("SELECT * FROM d.x")

	// We can't use alternateCompareResult because it'll try to contact the deceased
	// n1 even if the lease holders for d.scattered have all moved to other nodes
	dstScatteredData := c.destTenantSQL.QueryStr(c.t, "SELECT * FROM d.scattered ORDER BY key")
	require.Equal(t, srcScatteredData, dstScatteredData)
}

func TestTenantStreamingCutoverOnSourceFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := defaultTenantStreamingClustersArgs
	c, cleanup := createTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.startStreamReplication()

	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	c.srcTenantSQL.Exec(t, "INSERT INTO d.t2 VALUES (3);")

	c.waitUntilStartTimeReached(jobspb.JobID(ingestionJobID))

	cutoverTime := c.srcCluster.Server(0).Clock().Now()
	c.waitUntilHighWatermark(cutoverTime, jobspb.JobID(ingestionJobID))

	// Pause ingestion.
	c.destSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", ingestionJobID))
	jobutils.WaitForJobToPause(t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	// Destroy the source cluster
	c.srcCleanup()

	c.destSysSQL.Exec(c.t, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`, c.args.destTenantName, cutoverTime.AsOfSystemTime())

	// Resume ingestion.
	c.destSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))

	// Ingestion job should succeed despite source failure due to the successful cutover
	jobutils.WaitForJobToSucceed(t, c.destSysSQL, jobspb.JobID(ingestionJobID))
}

func TestTenantStreamingDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(casper): disabled due to error when setting a cluster setting
	// "setting updated but timed out waiting to read new value"
	skip.UnderStressRace(t, "disabled under stress race")

	ctx := context.Background()
	c, cleanup := createTenantStreamingClusters(ctx, t, defaultTenantStreamingClustersArgs)
	defer cleanup()

	producerJobID, ingestionJobID := c.startStreamReplication()
	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.srcSysServer.Clock().Now()
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))

	cleanUpTenant := c.createDestTenantSQL(ctx)
	defer func() {
		require.NoError(t, cleanUpTenant())
	}()

	c.compareResult("SELECT * FROM d.t1")
	c.compareResult("SELECT * FROM d.t2")

	// Introduce a DeleteRange on t1 and t2.
	checkDelRangeOnTable := func(table string, embeddedInSST bool) {
		srcCodec := keys.MakeSQLCodec(c.args.srcTenantID)
		desc := desctestutils.TestingGetPublicTableDescriptor(
			c.srcSysServer.DB(), srcCodec, "d", table)
		tableSpan := desc.PrimaryIndexSpan(srcCodec)

		// Introduce a DelRange on the table span.
		srcTimeBeforeDelRange := c.srcSysServer.Clock().Now()
		// Put the DelRange in the SST.
		if embeddedInSST {
			batchHLCTime := c.srcSysServer.Clock().Now()
			batchHLCTime.Logical = 0
			data, start, end := storageutils.MakeSST(t, c.srcSysServer.ClusterSettings(), []interface{}{
				storageutils.RangeKV(string(tableSpan.Key), string(tableSpan.EndKey), int(batchHLCTime.WallTime), ""),
			})
			_, _, _, err := c.srcSysServer.DB().AddSSTableAtBatchTimestamp(ctx, start, end, data, false,
				false, hlc.Timestamp{}, nil, false, batchHLCTime)
			require.NoError(t, err)
		} else {
			// Use DelRange directly.
			// Inserted two out-of-order overlapping DelRanges to check if it works
			// on multiple ranges keys in the same batch.
			require.NoError(t, c.srcSysServer.DB().DelRangeUsingTombstone(ctx,
				tableSpan.Key.Next(), tableSpan.EndKey))
			require.NoError(t, c.srcSysServer.DB().DelRangeUsingTombstone(ctx,
				tableSpan.Key, tableSpan.Key.Next().Next()))
		}
		c.waitUntilHighWatermark(c.srcSysServer.Clock().Now(), jobspb.JobID(ingestionJobID))
		c.compareResult(fmt.Sprintf("SELECT * FROM d.%s", table))

		// Point-in-time query, check if the DeleteRange is MVCC-compatible.
		c.compareResult(fmt.Sprintf("SELECT * FROM d.%s AS OF SYSTEM TIME %d",
			table, srcTimeBeforeDelRange.WallTime))
	}

	// Test on two tables to check if the range keys sst batcher
	// can work on multiple flushes.
	checkDelRangeOnTable("t1", true /* embeddedInSST */)
	checkDelRangeOnTable("t2", false /* embeddedInSST */)
}

func TestTenantStreamingMultipleNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "takes too long with multiple nodes")

	ctx := context.Background()
	args := defaultTenantStreamingClustersArgs
	args.srcNumNodes = 3
	args.destNumNodes = 3

	// Track the number of unique addresses that were connected to
	clientAddresses := make(map[string]struct{})
	var addressesMu syncutil.Mutex
	args.testingKnobs = &sql.StreamingTestingKnobs{
		BeforeClientSubscribe: func(addr string, token string, clientStartTime hlc.Timestamp) {
			addressesMu.Lock()
			defer addressesMu.Unlock()
			clientAddresses[addr] = struct{}{}
		},
	}

	c, cleanup := createTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	createScatteredTable(t, c, 3)

	producerJobID, ingestionJobID := c.startStreamReplication()
	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	c.srcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, "CREATE TABLE d.x (id INT PRIMARY KEY, n INT)")
		tenantSQL.Exec(t, "INSERT INTO d.x VALUES (1, 1)")
	})

	c.destSysSQL.Exec(t, `PAUSE JOB $1`, ingestionJobID)
	jobutils.WaitForJobToPause(t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	c.srcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, "INSERT INTO d.x VALUES (2, 2)")
	})
	c.destSysSQL.Exec(t, `RESUME JOB $1`, ingestionJobID)
	jobutils.WaitForJobToRun(t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	c.srcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, "INSERT INTO d.x VALUES (3, 3)")
	})

	c.waitUntilStartTimeReached(jobspb.JobID(ingestionJobID))

	cutoverTime := c.destSysServer.Clock().Now()
	c.cutover(producerJobID, ingestionJobID, cutoverTime.GoTime())

	cleanupTenant := c.createDestTenantSQL(ctx)
	defer func() {
		require.NoError(t, cleanupTenant())
	}()

	c.compareResult("SELECT * FROM d.t1")
	c.compareResult("SELECT * FROM d.t2")
	c.compareResult("SELECT * FROM d.x")
	c.compareResult("SELECT * FROM d.scattered ORDER BY key")

	// Since the data was distributed across multiple nodes, multiple nodes should've been connected to
	require.Greater(t, len(clientAddresses), 1)
}

// TestTenantReplicationProtectedTimestampManagement tests the active protected
// timestamps management on the destination tenant's keyspan.
func TestTenantReplicationProtectedTimestampManagement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := defaultTenantStreamingClustersArgs
	// Override the replication job details ReplicationTTLSeconds to a small value
	// so that every progress update results in a protected timestamp update.
	args.retentionTTLSeconds = 1

	testProtectedTimestampManagement := func(t *testing.T, pauseBeforeTerminal bool, completeReplication bool) {
		// waitForProducerProtection asserts that there is a PTS record protecting
		// the source tenant. We ensure the PTS record is protecting a timestamp
		// greater or equal to the frontier we know we have replicated up until.
		waitForProducerProtection := func(c *tenantStreamingClusters, frontier hlc.Timestamp, replicationJobID int) {
			testutils.SucceedsSoon(t, func() error {
				stats := streamIngestionStats(t, ctx, c.destSysSQL, replicationJobID)
				if stats.ProducerStatus == nil {
					return errors.New("nil ProducerStatus")
				}
				if stats.ProducerStatus.ProtectedTimestamp == nil {
					return errors.New("nil ProducerStatus.ProtectedTimestamp")
				}
				pts := *stats.ProducerStatus.ProtectedTimestamp
				if pts.Less(frontier) {
					return errors.Newf("protection is at %s, expected to be >= %s",
						pts.String(), frontier.String())
				}
				return nil
			})
		}

		// checkNoDestinationProtections asserts that there is no PTS record
		// protecting the destination tenant.
		checkNoDestinationProtection := func(c *tenantStreamingClusters, replicationJobID int) {
			execCfg := c.destSysServer.ExecutorConfig().(sql.ExecutorConfig)
			require.NoError(t, c.destCluster.Server(0).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				j, err := execCfg.JobRegistry.LoadJobWithTxn(ctx, jobspb.JobID(replicationJobID), txn)
				require.NoError(t, err)
				payload := j.Payload()
				replicationDetails := payload.GetStreamIngestion()
				_, err = execCfg.ProtectedTimestampProvider.GetRecord(ctx, txn, *replicationDetails.ProtectedTimestampRecordID)
				require.EqualError(t, err, protectedts.ErrNotExists.Error())
				return nil
			}))
		}
		checkDestinationProtection := func(c *tenantStreamingClusters, frontier hlc.Timestamp, replicationJobID int) {
			execCfg := c.destSysServer.ExecutorConfig().(sql.ExecutorConfig)
			ptp := execCfg.ProtectedTimestampProvider
			require.NoError(t, c.destCluster.Server(0).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				j, err := execCfg.JobRegistry.LoadJobWithTxn(ctx, jobspb.JobID(replicationJobID), txn)
				if err != nil {
					return err
				}
				payload := j.Payload()
				progress := j.Progress()
				replicationDetails := payload.GetStreamIngestion()

				require.NotNil(t, replicationDetails.ProtectedTimestampRecordID)
				rec, err := ptp.GetRecord(ctx, txn, *replicationDetails.ProtectedTimestampRecordID)
				if err != nil {
					return err
				}
				require.True(t, frontier.LessEq(*progress.GetHighWater()))
				frontier := progress.GetHighWater().GoTime().Round(time.Millisecond)
				window := frontier.Sub(rec.Timestamp.GoTime().Round(time.Millisecond))
				require.Equal(t, time.Second, window)
				return nil
			}))
		}

		c, cleanup := createTenantStreamingClusters(ctx, t, args)
		defer cleanup()

		c.destSysSQL.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
		c.destSysSQL.Exec(t, "SET CLUSTER SETTING kv.protectedts.reconciliation.interval = '1ms';")

		producerJobID, replicationJobID := c.startStreamReplication()

		jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
		jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(replicationJobID))

		// Ensure that we wait at least a second so that the gap between the first
		// time we write the protected timestamp (t1) during replication job
		// startup, and the first progress update (t2) is greater than 1s. This is
		// important because if `frontier@t2 - ReplicationTTLSeconds < t1` then we
		// will not update the PTS record.
		now := c.srcCluster.Server(0).Clock().Now().Add(int64(time.Second)*2, 0)
		c.waitUntilHighWatermark(now, jobspb.JobID(replicationJobID))

		// Check that the producer and replication job have written a protected
		// timestamp.
		waitForProducerProtection(c, now, replicationJobID)
		checkDestinationProtection(c, now, replicationJobID)

		now2 := now.Add(time.Second.Nanoseconds(), 0)
		c.waitUntilHighWatermark(now2, jobspb.JobID(replicationJobID))
		// Let the replication progress for a second before checking that the
		// protected timestamp record has also been updated on the destination
		// cluster. This update happens in the same txn in which we update the
		// replication job's progress.
		waitForProducerProtection(c, now2, replicationJobID)
		checkDestinationProtection(c, now2, replicationJobID)

		if pauseBeforeTerminal {
			c.destSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", replicationJobID))
			jobutils.WaitForJobToPause(c.t, c.destSysSQL, jobspb.JobID(replicationJobID))
		}

		if completeReplication {
			c.destSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", replicationJobID))
			jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(replicationJobID))
			var cutoverTime time.Time
			c.destSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)
			c.cutover(producerJobID, replicationJobID, cutoverTime)
			jobutils.WaitForJobToSucceed(t, c.destSysSQL, jobspb.JobID(replicationJobID))
		}

		// Set GC TTL low, so that the GC job completes quickly in the test.
		c.destSysSQL.Exec(t, "ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = 1;")
		c.destSysSQL.Exec(t, fmt.Sprintf("DROP TENANT %s", c.args.destTenantName))

		if !completeReplication {
			jobutils.WaitForJobToCancel(c.t, c.destSysSQL, jobspb.JobID(replicationJobID))
			jobutils.WaitForJobToCancel(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
		}

		// Check if the producer job has released protected timestamp.
		stats := streamIngestionStats(t, ctx, c.destSysSQL, replicationJobID)
		require.NotNil(t, stats.ProducerStatus)
		require.Nil(t, stats.ProducerStatus.ProtectedTimestamp)

		// Check if the replication job has released protected timestamp.
		checkNoDestinationProtection(c, replicationJobID)

		// Wait for the GC job to finish, this should happen once the protected
		// timestamp has been released.
		c.destSysSQL.Exec(t, "SHOW JOBS WHEN COMPLETE SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE GC'")

		// Check if dest tenant key range is cleaned up.
		destTenantPrefix := keys.MakeTenantPrefix(args.destTenantID)
		rows, err := c.destCluster.Server(0).DB().
			Scan(ctx, destTenantPrefix, destTenantPrefix.PrefixEnd(), 10)
		require.NoError(t, err)
		require.Empty(t, rows)

		c.destSysSQL.CheckQueryResults(t,
			fmt.Sprintf("SELECT count(*) FROM system.tenants WHERE id = %s", args.destTenantID),
			[][]string{{"0"}})
	}

	testutils.RunTrueAndFalse(t, "pause-before-terminal", func(t *testing.T, pauseBeforeTerminal bool) {
		testutils.RunTrueAndFalse(t, "complete-replication", func(t *testing.T, completeReplication bool) {
			testProtectedTimestampManagement(t, pauseBeforeTerminal, completeReplication)
		})
	})
}

// TODO(lidor): consider rewriting this test as a data driven test when #92609 is merged.
func TestTenantStreamingShowTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := defaultTenantStreamingClustersArgs

	c, cleanup := createTenantStreamingClusters(ctx, t, args)
	defer cleanup()
	producerJobID, ingestionJobID := c.startStreamReplication()

	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	highWatermark := c.srcCluster.Server(0).Clock().Now()
	c.waitUntilHighWatermark(highWatermark, jobspb.JobID(ingestionJobID))
	destRegistry := c.destCluster.Server(0).JobRegistry().(*jobs.Registry)
	details, err := destRegistry.LoadJob(ctx, jobspb.JobID(ingestionJobID))
	require.NoError(t, err)
	replicationDetails := details.Details().(jobspb.StreamIngestionDetails)

	var (
		id                   int
		dest                 string
		status               string
		source               string
		sourceUri            string
		jobId                int
		maxReplTime          time.Time
		protectedTime        time.Time
		replicationStartTime time.Time
	)
	row := c.destSysSQL.QueryRow(t, fmt.Sprintf("SHOW TENANT %s WITH REPLICATION STATUS", args.destTenantName))
	row.Scan(&id, &dest, &status, &source, &sourceUri, &jobId, &maxReplTime, &protectedTime, &replicationStartTime)
	require.Equal(t, 2, id)
	require.Equal(t, "destination", dest)
	require.Equal(t, "REPLICATING", status)
	require.Equal(t, "source", source)
	require.Equal(t, c.srcURL.String(), sourceUri)
	require.Equal(t, ingestionJobID, jobId)
	require.Less(t, maxReplTime, timeutil.Now())
	require.Less(t, protectedTime, timeutil.Now())
	require.GreaterOrEqual(t, maxReplTime, highWatermark.GoTime())
	require.GreaterOrEqual(t, protectedTime, replicationDetails.ReplicationStartTime.GoTime())
	require.Equal(t, replicationStartTime.UnixMicro(),
		timeutil.Unix(0, replicationDetails.ReplicationStartTime.WallTime).UnixMicro())
}
