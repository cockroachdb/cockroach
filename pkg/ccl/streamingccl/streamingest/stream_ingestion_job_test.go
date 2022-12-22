// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingtest"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamproducer"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestTenantStreaming tests that tenants can stream changes end-to-end.
func TestTenantStreaming(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "slow under race")

	ctx := context.Background()

	args := base.TestServerArgs{
		// Disabling the test tenant because the test below assumes that
		// when it's monitoring the streaming job, it's doing so from the system
		// tenant and not from within a secondary tenant. When inside
		// a secondary tenant, it won't be able to see the streaming job.
		// This may also be impacted by the fact that we don't currently support
		// tenant->tenant streaming. Tracked with #76378.
		DisableDefaultTestTenant: true,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	}

	// Start the source server.
	source, sourceDB, _ := serverutils.StartServer(t, args)
	defer source.Stopper().Stop(ctx)

	// Start tenant server in the source cluster.
	sourceTenantID := serverutils.TestTenantID()
	sourceTenantName := roachpb.TenantName("source-tenant")
	_, tenantConn := serverutils.StartTenant(t, source, base.TestTenantArgs{
		TenantID:   sourceTenantID,
		TenantName: sourceTenantName,
	})
	defer func() {
		require.NoError(t, tenantConn.Close())
	}()
	// sourceSQL refers to the tenant generating the data.
	sourceSQL := sqlutils.MakeSQLRunner(tenantConn)

	// Make changefeeds run faster.
	resetFreq := changefeedbase.TestingSetDefaultMinCheckpointFrequency(50 * time.Millisecond)
	defer resetFreq()
	// Set required cluster settings.
	sourceDBRunner := sqlutils.MakeSQLRunner(sourceDB)
	sourceDBRunner.ExecMultiple(t, strings.Split(`
SET CLUSTER SETTING kv.rangefeed.enabled = true;
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s';
SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms';
SET CLUSTER SETTING stream_replication.min_checkpoint_frequency = '1s';
SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '500ms';
`,
		";")...)

	// Start the destination server.
	hDest, cleanupDest := streamingtest.NewReplicationHelper(t,
		// Test fails when run from within the test tenant. More investigation
		// is required. Tracked with #76378.
		// TODO(ajstorm): This may be the right course of action here as the
		//  replication is now being run inside a tenant.
		base.TestServerArgs{DisableDefaultTestTenant: true})
	defer cleanupDest()
	// destSQL refers to the system tenant as that's the one that's running the
	// job.
	destSQL := hDest.SysSQL
	destSQL.ExecMultiple(t, strings.Split(`
SET CLUSTER SETTING stream_replication.consumer_heartbeat_frequency = '100ms';
SET CLUSTER SETTING bulkio.stream_ingestion.minimum_flush_interval = '500ms';
SET CLUSTER SETTING bulkio.stream_ingestion.cutover_signal_poll_interval = '100ms';
SET CLUSTER SETTING stream_replication.job_checkpoint_frequency = '100ms';
SET enable_experimental_stream_replication = true;
`,
		";")...)

	// Sink to read data from.
	pgURL, cleanupSink := sqlutils.PGUrl(t, source.ServingSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanupSink()

	var ingestionJobID, streamProducerJobID int64
	var startTime string
	sourceSQL.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&startTime)

	destSQL.QueryRow(t,
		`CREATE TENANT "destination-tenant" FROM REPLICATION OF "source-tenant" ON $1 `,
		pgURL.String(),
	).Scan(&ingestionJobID, &streamProducerJobID)

	sourceSQL.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.t1(i int primary key, a string, b string);
CREATE TABLE d.t2(i int primary key);
INSERT INTO d.t1 (i) VALUES (42);
INSERT INTO d.t2 VALUES (2);
`)

	waitUntilStartTimeReached(t, destSQL, jobspb.JobID(ingestionJobID))
	cutoverTime := timeutil.Now().Round(time.Microsecond)
	destSQL.Exec(t, `ALTER TENANT "destination-tenant" COMPLETE REPLICATION TO SYSTEM TIME $1::string`, hlc.Timestamp{WallTime: cutoverTime.UnixNano()}.AsOfSystemTime())
	jobutils.WaitForJobToSucceed(t, destSQL, jobspb.JobID(ingestionJobID))
	jobutils.WaitForJobToSucceed(t, sourceDBRunner, jobspb.JobID(streamProducerJobID))

	stats := streamIngestionStats(t, ctx, destSQL, int(ingestionJobID))
	require.Equal(t, cutoverTime, stats.IngestionProgress.CutoverTime.GoTime())
	require.Equal(t, streampb.StreamReplicationStatus_STREAM_INACTIVE, stats.ProducerStatus.StreamStatus)

	_, destTenantConn := serverutils.StartTenant(t, hDest.SysServer, base.TestTenantArgs{
		TenantID:            roachpb.MustMakeTenantID(2),
		TenantName:          "destination-tenant",
		DisableCreateTenant: true,
	})
	defer func() {
		require.NoError(t, destTenantConn.Close())
	}()
	destTenantSQL := sqlutils.MakeSQLRunner(destTenantConn)

	query := "SELECT * FROM d.t1"
	sourceData := sourceSQL.QueryStr(t, query)
	destData := destTenantSQL.QueryStr(t, query)
	require.Equal(t, sourceData, destData)
}

func TestTenantStreamingCreationErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srcServer, srcDB, _ := serverutils.StartServer(t, base.TestServerArgs{DisableDefaultTestTenant: true})
	defer srcServer.Stopper().Stop(ctx)
	destServer, destDB, _ := serverutils.StartServer(t, base.TestServerArgs{DisableDefaultTestTenant: true})
	defer destServer.Stopper().Stop(ctx)

	srcTenantID := serverutils.TestTenantID()
	srcTenantName := roachpb.TenantName("source")
	_, srcTenantConn := serverutils.StartTenant(t, srcServer, base.TestTenantArgs{
		TenantID:   srcTenantID,
		TenantName: srcTenantName,
	})
	defer func() { require.NoError(t, srcTenantConn.Close()) }()

	// Set required cluster settings.
	srcSysSQL := sqlutils.MakeSQLRunner(srcDB)
	srcSysSQL.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	destSysSQL := sqlutils.MakeSQLRunner(destDB)
	destSysSQL.Exec(t, `SET enable_experimental_stream_replication = true`)

	// Sink to read data from.
	srcPgURL, cleanupSink := sqlutils.PGUrl(t, srcServer.ServingSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanupSink()

	destSysSQL.ExpectErr(t, "pq: neither the source tenant \"source\" nor the destination tenant \"system\" can be the system tenant",
		`CREATE TENANT system FROM REPLICATION OF source ON $1`, srcPgURL.String())

	destSysSQL.Exec(t, "CREATE TENANT \"100\"")
	destSysSQL.ExpectErr(t, "pq: tenant with name \"100\" already exists",
		`CREATE TENANT "100" FROM REPLICATION OF source ON $1`, srcPgURL.String())
}

func TestCutoverBuiltin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Disable the test tenant as the test below looks for a
			// streaming job assuming that it's within the system tenant.
			// Tracked with #76378.
			DisableDefaultTestTenant: true,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	registry := tc.Server(0).JobRegistry().(*jobs.Registry)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db := sqlDB.DB

	streamIngestJobRecord := jobs.Record{
		Description: "test stream ingestion",
		Username:    username.RootUserName(),
		Details: jobspb.StreamIngestionDetails{
			StreamAddress: "randomgen://test",
			Span:          roachpb.Span{Key: keys.LocalMax, EndKey: keys.LocalMax.Next()},
		},
		Progress: jobspb.StreamIngestionProgress{},
	}
	var job *jobs.StartableJob
	id := registry.MakeJobID()
	err := tc.Server(0).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		return registry.CreateStartableJobWithTxn(ctx, &job, id, txn, streamIngestJobRecord)
	})
	require.NoError(t, err)

	// Check that sentinel is not set.
	progress := job.Progress()
	sp, ok := progress.GetDetails().(*jobspb.Progress_StreamIngest)
	require.True(t, ok)
	require.True(t, sp.StreamIngest.CutoverTime.IsEmpty())

	var highWater time.Time
	err = job.Update(ctx, nil, func(_ *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		highWater = timeutil.Now().Round(time.Microsecond)
		hlcHighWater := hlc.Timestamp{WallTime: highWater.UnixNano()}
		return jobs.UpdateHighwaterProgressed(hlcHighWater, md, ju)
	})
	require.NoError(t, err)

	// Ensure that the builtin runs locally.
	var explain string
	err = db.QueryRowContext(ctx,
		`EXPLAIN SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`, job.ID(),
		highWater).Scan(&explain)
	require.NoError(t, err)
	require.Equal(t, "distribution: local", explain)

	var jobID int64
	err = db.QueryRowContext(
		ctx,
		`SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`,
		job.ID(), highWater).Scan(&jobID)
	require.NoError(t, err)
	require.Equal(t, job.ID(), jobspb.JobID(jobID))

	// This should fail since we already have a cutover time set on the job
	// progress.
	_, err = db.ExecContext(
		ctx,
		`SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`,
		job.ID(), highWater)
	require.Error(t, err, "cutover timestamp already set")

	// Check that sentinel is set on the job progress.
	sj, err := registry.LoadJob(ctx, job.ID())
	require.NoError(t, err)
	progress = sj.Progress()
	sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest)
	require.True(t, ok)
	require.Equal(t, hlc.Timestamp{WallTime: highWater.UnixNano()}, sp.StreamIngest.CutoverTime)
}

// TestReplicationJobResumptionStartTime tests that a replication job picks the
// correct timestamps to resume from across multiple resumptions.
func TestReplicationJobResumptionStartTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	planned := make(chan struct{})
	canContinue := make(chan struct{})
	args := defaultTenantStreamingClustersArgs

	replicationSpecs := make([]*execinfrapb.StreamIngestionDataSpec, 0)
	frontier := &execinfrapb.StreamIngestionFrontierSpec{}
	args.testingKnobs = &sql.StreamingTestingKnobs{
		AfterReplicationFlowPlan: func(ingestionSpecs []*execinfrapb.StreamIngestionDataSpec,
			frontierSpec *execinfrapb.StreamIngestionFrontierSpec) {
			replicationSpecs = ingestionSpecs
			frontier = frontierSpec
			planned <- struct{}{}
			<-canContinue
		},
	}
	c, cleanup := createTenantStreamingClusters(ctx, t, args)
	defer cleanup()
	defer close(planned)
	defer close(canContinue)

	producerJobID, replicationJobID := c.startStreamReplication()
	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(replicationJobID))

	// Wait for the distsql plan to be created.
	<-planned
	registry := c.destSysServer.ExecutorConfig().(sql.ExecutorConfig).JobRegistry
	var replicationJobDetails jobspb.StreamIngestionDetails
	require.NoError(t, c.destSysServer.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		j, err := registry.LoadJobWithTxn(ctx, jobspb.JobID(replicationJobID), txn)
		require.NoError(t, err)
		var ok bool
		replicationJobDetails, ok = j.Details().(jobspb.StreamIngestionDetails)
		if !ok {
			t.Fatalf("job with id %d is not a stream ingestion job", replicationJobID)
		}
		return nil
	}))

	// Let's verify the timestamps on the first resumption of the replication job.
	startTime := replicationJobDetails.ReplicationStartTime
	require.NotEmpty(t, startTime)

	for _, r := range replicationSpecs {
		require.Equal(t, startTime, r.InitialScanTimestamp)
		require.Empty(t, r.PreviousHighWaterTimestamp)
	}
	require.Empty(t, frontier.HighWaterAtStart)

	// Allow the job to make some progress.
	canContinue <- struct{}{}
	srcTime := c.srcCluster.Server(0).Clock().Now()
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(replicationJobID))

	// Pause the job.
	c.destSysSQL.Exec(t, `PAUSE JOB $1`, replicationJobID)
	jobutils.WaitForJobToPause(c.t, c.destSysSQL, jobspb.JobID(replicationJobID))

	// Unpause the job and ensure the resumption takes place at a later timestamp
	// than the initial scan timestamp.
	c.destSysSQL.Exec(t, `RESUME JOB $1`, replicationJobID)
	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(replicationJobID))

	<-planned
	stats := streamIngestionStats(t, ctx, c.destSysSQL, replicationJobID)

	// Assert that the start time hasn't changed.
	require.Equal(t, startTime, stats.IngestionDetails.ReplicationStartTime)

	// Assert that the previous highwater mark is greater than the replication
	// start time.
	var previousHighWaterTimestamp hlc.Timestamp
	for _, r := range replicationSpecs {
		require.Equal(t, startTime, r.InitialScanTimestamp)
		require.True(t, r.InitialScanTimestamp.Less(r.PreviousHighWaterTimestamp))
		if previousHighWaterTimestamp.IsEmpty() {
			previousHighWaterTimestamp = r.PreviousHighWaterTimestamp
		} else {
			require.Equal(t, r.PreviousHighWaterTimestamp, previousHighWaterTimestamp)
		}
	}
	require.Equal(t, frontier.HighWaterAtStart, previousHighWaterTimestamp)
	canContinue <- struct{}{}
	srcTime = c.srcCluster.Server(0).Clock().Now()
	c.waitUntilHighWatermark(srcTime, jobspb.JobID(replicationJobID))
	c.cutover(producerJobID, replicationJobID, srcTime.GoTime())
	jobutils.WaitForJobToSucceed(t, c.destSysSQL, jobspb.JobID(replicationJobID))
}
