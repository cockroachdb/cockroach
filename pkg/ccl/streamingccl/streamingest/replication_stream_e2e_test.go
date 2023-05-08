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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestTenantStreamingProducerJobTimedOut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(casper): disabled due to error when setting a cluster setting
	// "setting updated but timed out waiting to read new value"
	skip.UnderStressRace(t, "disabled under stress race")

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.SrcClusterSettings[`stream_replication.job_liveness_timeout`] = `'1m'`
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))
	c.RequireFingerprintMatchAtTimestamp(srcTime.AsOfSystemTime())

	stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)

	require.NotNil(t, stats.ReplicationLagInfo)
	require.True(t, srcTime.LessEq(stats.ReplicationLagInfo.MinIngestedTimestamp))
	require.Equal(t, "", stats.ProducerError)

	// Make producer job easily times out
	c.SrcSysSQL.ExecMultiple(t, replicationtestutils.ConfigureClusterSettings(map[string]string{
		`stream_replication.job_liveness_timeout`: `'100ms'`,
	})...)

	jobutils.WaitForJobToFail(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	// The ingestion job will stop retrying as this is a permanent job error.
	jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	require.Regexp(t, "ingestion job failed .* but is being paused",
		replicationtestutils.RunningStatus(t, c.DestSysSQL, ingestionJobID))

	ts := c.DestCluster.Server(0).Clock().Now()
	afterPauseFingerprint := replicationtestutils.FingerprintTenantAtTimestampNoHistory(t, c.DestSysSQL, c.Args.DestTenantID.ToUint64(), ts.AsOfSystemTime())
	// Make dest cluster to ingest KV events faster.
	c.SrcSysSQL.ExecMultiple(t, replicationtestutils.ConfigureClusterSettings(map[string]string{
		`stream_replication.min_checkpoint_frequency`: `'100ms'`,
	})...)

	c.SrcTenantSQL.Exec(t, "INSERT INTO d.t2 VALUES (3);")

	// Check the dst cluster didn't receive the change after a while.
	<-time.NewTimer(3 * time.Second).C
	c.RequireDestinationFingerprintAtTimestamp(afterPauseFingerprint, ts.AsOfSystemTime())

	// After resumed, the ingestion job paused on failure again.
	c.DestSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))
	jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
}

func TestTenantStreamingPauseResumeIngestion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(casper): disabled due to error when setting a cluster setting
	// "setting updated but timed out waiting to read new value"
	skip.UnderStressRace(t, "disabled under stress race")

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))
	c.RequireFingerprintMatchAtTimestamp(srcTime.AsOfSystemTime())

	// Pause ingestion.
	c.DestSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", ingestionJobID))
	jobutils.WaitForJobToPause(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	pausedCheckpoint := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID).
		ReplicationLagInfo.MinIngestedTimestamp
	// Check we paused at a timestamp greater than the previously reached high watermark
	require.True(t, srcTime.LessEq(pausedCheckpoint))

	// Introduce new update to the src.
	c.SrcTenantSQL.Exec(t, "INSERT INTO d.t2 VALUES (3);")
	// Confirm that the job high watermark doesn't change. If the dest cluster is still subscribing
	// to src cluster checkpoints events, the job high watermark may change.
	<-time.NewTimer(3 * time.Second).C
	require.Equal(t, pausedCheckpoint,
		replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL,
			ingestionJobID).ReplicationLagInfo.MinIngestedTimestamp)

	// Resume ingestion.
	c.DestSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))
	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))

	// Confirm that dest tenant has received the new change after resumption.
	srcTime = c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))
	c.RequireFingerprintMatchAtTimestamp(srcTime.AsOfSystemTime())
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
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.TestingKnobs = &sql.StreamingTestingKnobs{
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
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	// Make ingestion error out only once.
	ingestErrCh <- errors.Newf("ingestion error from test")
	close(ingestErrCh)

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	// Ingestion is retried once after having an ingestion error.
	require.Equal(t, 2, ingestionStarts)

	// Check we didn't make any progress.
	require.Nil(t, replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL,
		ingestionJobID).ReplicationLagInfo)

	// Resume ingestion.
	c.DestSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))
	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))

	// Check dest has caught up the previous updates.
	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.Cutover(producerJobID, ingestionJobID, srcTime.GoTime(), false)
	c.RequireFingerprintMatchAtTimestamp(srcTime.AsOfSystemTime())

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
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.TestingKnobs = &sql.StreamingTestingKnobs{
		BeforeClientSubscribe: func(addr string, token string, clientStartTime hlc.Timestamp) {
			lastClientStart[token] = clientStartTime
		},
	}
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	// Helper to read job progress
	jobRegistry := c.DestSysServer.JobRegistry().(*jobs.Registry)
	loadIngestProgress := func() *jobspb.StreamIngestionProgress {
		job, err := jobRegistry.LoadJob(context.Background(), jobspb.JobID(ingestionJobID))
		require.NoError(t, err)

		progress := job.Progress()
		ingestProgress := progress.Details.(*jobspb.Progress_StreamIngest).StreamIngest
		return ingestProgress
	}

	c.SrcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, "CREATE TABLE d.x (id INT PRIMARY KEY, n INT)")
		tenantSQL.Exec(t, "INSERT INTO d.x VALUES (1, 1)")
	})

	srcCodec := keys.MakeSQLCodec(c.Args.SrcTenantID)
	t1Desc := desctestutils.TestingGetPublicTableDescriptor(
		c.SrcSysServer.DB(), srcCodec, "d", "t1")
	t2Desc := desctestutils.TestingGetPublicTableDescriptor(
		c.SrcSysServer.DB(), keys.MakeSQLCodec(c.Args.SrcTenantID), "d", "t2")
	xDesc := desctestutils.TestingGetPublicTableDescriptor(
		c.SrcSysServer.DB(), keys.MakeSQLCodec(c.Args.SrcTenantID), "d", "x")
	t1Span := t1Desc.PrimaryIndexSpan(srcCodec)
	t2Span := t2Desc.PrimaryIndexSpan(srcCodec)
	xSpan := xDesc.PrimaryIndexSpan(srcCodec)
	tableSpans := []roachpb.Span{t1Span, t2Span, xSpan}

	var checkpointMinTime time.Time
	c.SrcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
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

	c.DestSysSQL.Exec(t, `PAUSE JOB $1`, ingestionJobID)
	jobutils.WaitForJobToPause(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	// Clear out the map to ignore the initial client starts
	lastClientStart = make(map[string]hlc.Timestamp)

	c.DestSysSQL.Exec(t, `RESUME JOB $1`, ingestionJobID)
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	cutoverTime := c.DestSysServer.Clock().Now()
	c.WaitUntilHighWatermark(cutoverTime, jobspb.JobID(ingestionJobID))
	c.Cutover(producerJobID, ingestionJobID, cutoverTime.GoTime(), false)
	cutoverFingerprint := c.RequireFingerprintMatchAtTimestamp(cutoverTime.AsOfSystemTime())

	// Clients should never be started prior to a checkpointed timestamp
	for _, clientStartTime := range lastClientStart {
		require.Less(t, checkpointMinTime.UnixNano(), clientStartTime.GoTime().UnixNano())
	}

	// After cutover, changes to source won't be streamed into destination cluster.
	c.SrcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, `INSERT INTO d.t2 VALUES (3);`)
	})
	// Check the dst cluster didn't receive the change after a while.
	<-time.NewTimer(3 * time.Second).C
	c.RequireDestinationFingerprintAtTimestamp(cutoverFingerprint, c.DestSysServer.Clock().Now().AsOfSystemTime())
}

func TestTenantStreamingCancelIngestion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	testCancelIngestion := func(t *testing.T, cancelAfterPaused bool) {
		c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
		defer cleanup()
		producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

		jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
		jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

		srcTime := c.SrcCluster.Server(0).Clock().Now()
		c.WaitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))
		c.RequireFingerprintMatchAtTimestamp(srcTime.AsOfSystemTime())

		if cancelAfterPaused {
			c.DestSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", ingestionJobID))
			jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
		}

		c.DestSysSQL.Exec(t, fmt.Sprintf("CANCEL JOB %d", ingestionJobID))
		jobutils.WaitForJobToCancel(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
		jobutils.WaitForJobToCancel(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))

		// Check if the producer job has released protected timestamp.
		stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)
		require.NotNil(t, stats.ProducerStatus)
		require.Nil(t, stats.ProducerStatus.ProtectedTimestamp)

		// Check if dest tenant key ranges are not cleaned up.
		destTenantPrefix := keys.MakeTenantPrefix(args.DestTenantID)

		rows, err := c.DestCluster.Server(0).DB().
			Scan(ctx, destTenantPrefix, destTenantPrefix.PrefixEnd(), 10)
		require.NoError(t, err)
		require.NotEmpty(t, rows)

		// Check if the tenant record still exits.
		c.DestSysSQL.CheckQueryResults(t,
			fmt.Sprintf("SELECT count(*) FROM system.tenants WHERE id = %s", args.DestTenantID),
			[][]string{{"1"}})

		// Check if we can successfully GC the tenant.
		c.DestSysSQL.Exec(t, "DROP TENANT [$1] IMMEDIATE",
			args.DestTenantID.ToUint64())
		rows, err = c.DestCluster.Server(0).DB().
			Scan(ctx, destTenantPrefix, destTenantPrefix.PrefixEnd(), 10)
		require.NoError(t, err)
		require.Empty(t, rows)

		c.DestSysSQL.CheckQueryResults(t,
			fmt.Sprintf("SELECT count(*) FROM system.tenants WHERE id = %s", args.DestTenantID),
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
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	testCancelIngestion := func(t *testing.T, cancelAfterPaused bool) {
		c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
		defer cleanup()
		producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

		c.DestSysSQL.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
		c.DestSysSQL.Exec(t, "SET CLUSTER SETTING kv.protectedts.reconciliation.interval = '1ms';")

		jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
		jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

		c.WaitUntilHighWatermark(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))
		if cancelAfterPaused {
			c.DestSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", ingestionJobID))
			jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
		}

		// Set GC TTL low, so that the GC job completes quickly in the test.
		c.DestSysSQL.Exec(t, "ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = 1;")
		c.DestSysSQL.Exec(t, fmt.Sprintf("DROP TENANT %s", c.Args.DestTenantName))
		jobutils.WaitForJobToCancel(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
		jobutils.WaitForJobToCancel(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))

		// Check if the producer job has released protected timestamp.
		stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)
		require.NotNil(t, stats.ProducerStatus)
		require.Nil(t, stats.ProducerStatus.ProtectedTimestamp)

		// Wait for the GC job to finish
		c.DestSysSQL.Exec(t, "SHOW JOBS WHEN COMPLETE SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE GC'")

		// Check if dest tenant key range is cleaned up.
		destTenantPrefix := keys.MakeTenantPrefix(args.DestTenantID)
		rows, err := c.DestCluster.Server(0).DB().
			Scan(ctx, destTenantPrefix, destTenantPrefix.PrefixEnd(), 10)
		require.NoError(t, err)
		require.Empty(t, rows)

		c.DestSysSQL.CheckQueryResults(t,
			fmt.Sprintf("SELECT count(*) FROM system.tenants WHERE id = %s", args.DestTenantID),
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

	skip.WithIssue(t, 94738, "flaky test")
	skip.UnderRace(t, "takes too long with multiple nodes")

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	args.SrcNumNodes = 3
	args.DestNumNodes = 3

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	replicationtestutils.CreateScatteredTable(t, c, 3)
	srcScatteredData := c.SrcTenantSQL.QueryStr(c.T, "SELECT * FROM d.scattered ORDER BY key")

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))

	c.DestSysSQL.Exec(t, `PAUSE JOB $1`, ingestionJobID)
	jobutils.WaitForJobToPause(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	// We should've persisted the original topology
	progress := jobutils.GetJobProgress(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	streamAddresses := progress.GetStreamIngest().StreamAddresses
	require.Greater(t, len(streamAddresses), 1)

	destroyedAddress := c.SrcURL.String()

	require.NoError(t, c.SrcTenantConn.Close())
	c.SrcTenantServer.Stopper().Stop(ctx)
	c.SrcCluster.StopServer(0)

	// Once SrcCluster.Server(0) is shut down queries must be ran against a different server
	alternateSrcSysSQL := sqlutils.MakeSQLRunner(c.SrcCluster.ServerConn(1))
	_, alternateSrcTenantConn := serverutils.StartTenant(t, c.SrcCluster.Server(1),
		base.TestTenantArgs{
			TenantID:            c.Args.SrcTenantID,
			TenantName:          c.Args.SrcTenantName,
			DisableCreateTenant: true,
		})
	defer alternateSrcTenantConn.Close()
	alternateSrcTenantSQL := sqlutils.MakeSQLRunner(alternateSrcTenantConn)

	cleanUpTenant := c.CreateDestTenantSQL(ctx)
	defer func() {
		require.NoError(t, cleanUpTenant())
	}()

	alternateCompareResult := func(query string) {
		sourceData := alternateSrcTenantSQL.QueryStr(c.T, query)
		destData := c.DestTenantSQL.QueryStr(c.T, query)
		require.Equal(c.T, sourceData, destData)
	}

	c.DestSysSQL.Exec(t, `RESUME JOB $1`, ingestionJobID)
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	alternateSrcTenantSQL.Exec(t, "CREATE TABLE d.x (id INT PRIMARY KEY, n INT)")
	alternateSrcTenantSQL.Exec(t, `INSERT INTO d.x VALUES (3);`)

	var cutoverTime time.Time
	alternateSrcSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)

	var cutoverStr string
	c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		c.Args.DestTenantName, cutoverTime).Scan(&cutoverStr)
	cutoverOutput := replicationtestutils.DecimalTimeToHLC(t, cutoverStr)
	require.Equal(c.T, cutoverTime, cutoverOutput.GoTime())
	jobutils.WaitForJobToSucceed(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	// The destroyed address should have been removed from the topology
	progress = jobutils.GetJobProgress(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	newStreamAddresses := progress.GetStreamIngest().StreamAddresses
	require.Contains(t, streamAddresses, destroyedAddress)
	require.NotContains(t, newStreamAddresses, destroyedAddress)

	alternateCompareResult("SELECT * FROM d.t1")
	alternateCompareResult("SELECT * FROM d.t2")
	alternateCompareResult("SELECT * FROM d.x")

	// We can't use alternateCompareResult because it'll try to contact the deceased
	// n1 even if the lease holders for d.scattered have all moved to other nodes
	dstScatteredData := c.DestTenantSQL.QueryStr(c.T, "SELECT * FROM d.scattered ORDER BY key")
	require.Equal(t, srcScatteredData, dstScatteredData)
}

func TestTenantStreamingCutoverOnSourceFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.SrcTenantSQL.Exec(t, "INSERT INTO d.t2 VALUES (3);")

	c.WaitUntilStartTimeReached(jobspb.JobID(ingestionJobID))

	cutoverTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilHighWatermark(cutoverTime, jobspb.JobID(ingestionJobID))

	// Pause ingestion.
	c.DestSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", ingestionJobID))
	jobutils.WaitForJobToPause(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	// Destroy the source cluster
	c.SrcCleanup()

	var cutoverStr string
	c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		c.Args.DestTenantName, cutoverTime.AsOfSystemTime()).Scan(&cutoverStr)
	cutoverOutput := replicationtestutils.DecimalTimeToHLC(t, cutoverStr)
	require.Equal(c.T, cutoverTime, cutoverOutput)

	// Ingestion job should succeed despite source failure due to the successful cutover
	jobutils.WaitForJobToSucceed(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
}

func TestTenantStreamingDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(casper): disabled due to error when setting a cluster setting
	// "setting updated but timed out waiting to read new value"
	skip.UnderStressRace(t, "disabled under stress race")

	ctx := context.Background()
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, replicationtestutils.DefaultTenantStreamingClustersArgs)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcSysServer.Clock().Now()
	c.WaitUntilHighWatermark(srcTime, jobspb.JobID(ingestionJobID))
	c.RequireFingerprintMatchAtTimestamp(srcTime.AsOfSystemTime())

	// Introduce a DeleteRange on t1 and t2.
	checkDelRangeOnTable := func(table string, embeddedInSST bool) {
		srcCodec := keys.MakeSQLCodec(c.Args.SrcTenantID)
		desc := desctestutils.TestingGetPublicTableDescriptor(
			c.SrcSysServer.DB(), srcCodec, "d", table)
		tableSpan := desc.PrimaryIndexSpan(srcCodec)

		// Introduce a DelRange on the table span.
		srcTimeBeforeDelRange := c.SrcSysServer.Clock().Now()
		// Put the DelRange in the SST.
		if embeddedInSST {
			batchHLCTime := c.SrcSysServer.Clock().Now()
			batchHLCTime.Logical = 0
			data, start, end := storageutils.MakeSST(t, c.SrcSysServer.ClusterSettings(), []interface{}{
				storageutils.RangeKV(string(tableSpan.Key), string(tableSpan.EndKey), int(batchHLCTime.WallTime), ""),
			})
			_, _, _, err := c.SrcSysServer.DB().AddSSTableAtBatchTimestamp(ctx, start, end, data, false,
				false, hlc.Timestamp{}, nil, false, batchHLCTime)
			require.NoError(t, err)
		} else {
			// Use DelRange directly.
			// Inserted two out-of-order overlapping DelRanges to check if it works
			// on multiple ranges keys in the same batch.
			require.NoError(t, c.SrcSysServer.DB().DelRangeUsingTombstone(ctx,
				tableSpan.Key.Next(), tableSpan.EndKey))
			require.NoError(t, c.SrcSysServer.DB().DelRangeUsingTombstone(ctx,
				tableSpan.Key, tableSpan.Key.Next().Next()))
		}
		srcTimeAfterDelRange := c.SrcSysServer.Clock().Now()
		c.WaitUntilHighWatermark(srcTimeAfterDelRange, jobspb.JobID(ingestionJobID))

		c.RequireFingerprintMatchAtTimestamp(srcTimeAfterDelRange.AsOfSystemTime())
		c.RequireFingerprintMatchAtTimestamp(srcTimeBeforeDelRange.AsOfSystemTime())
	}

	// Test on two tables to check if the range keys sst batcher
	// can work on multiple flushes.
	checkDelRangeOnTable("t1", true /* embeddedInSST */)
	checkDelRangeOnTable("t2", false /* embeddedInSST */)
}

func TestTenantStreamingMultipleNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 86206)

	skip.UnderRace(t, "takes too long with multiple nodes")

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.SrcNumNodes = 3
	args.DestNumNodes = 3

	// Track the number of unique addresses that were connected to
	clientAddresses := make(map[string]struct{})
	var addressesMu syncutil.Mutex
	args.TestingKnobs = &sql.StreamingTestingKnobs{
		BeforeClientSubscribe: func(addr string, token string, clientStartTime hlc.Timestamp) {
			addressesMu.Lock()
			defer addressesMu.Unlock()
			clientAddresses[addr] = struct{}{}
		},
	}

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	replicationtestutils.CreateScatteredTable(t, c, 3)

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.SrcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, "CREATE TABLE d.x (id INT PRIMARY KEY, n INT)")
		tenantSQL.Exec(t, "INSERT INTO d.x VALUES (1, 1)")
	})

	c.DestSysSQL.Exec(t, `PAUSE JOB $1`, ingestionJobID)
	jobutils.WaitForJobToPause(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	c.SrcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, "INSERT INTO d.x VALUES (2, 2)")
	})
	c.DestSysSQL.Exec(t, `RESUME JOB $1`, ingestionJobID)
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.SrcExec(func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, "INSERT INTO d.x VALUES (3, 3)")
	})

	c.WaitUntilStartTimeReached(jobspb.JobID(ingestionJobID))

	cutoverTime := c.DestSysServer.Clock().Now()
	c.Cutover(producerJobID, ingestionJobID, cutoverTime.GoTime(), false)
	c.RequireFingerprintMatchAtTimestamp(cutoverTime.AsOfSystemTime())

	// Since the data was distributed across multiple nodes, multiple nodes should've been connected to
	require.Greater(t, len(clientAddresses), 1)
}

// TestTenantReplicationProtectedTimestampManagement tests the active protected
// timestamps management on the destination tenant's keyspan.
func TestTenantReplicationProtectedTimestampManagement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	// Override the replication job details ReplicationTTLSeconds to a small value
	// so that every progress update results in a protected timestamp update.
	args.RetentionTTLSeconds = 1

	testProtectedTimestampManagement := func(t *testing.T, pauseBeforeTerminal bool, completeReplication bool) {
		// waitForProducerProtection asserts that there is a PTS record protecting
		// the source tenant. We ensure the PTS record is protecting a timestamp
		// greater or equal to the frontier we know we have replicated up until.
		waitForProducerProtection := func(c *replicationtestutils.TenantStreamingClusters, frontier hlc.Timestamp, replicationJobID int) {
			testutils.SucceedsSoon(t, func() error {
				stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, replicationJobID)
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
		checkNoDestinationProtection := func(c *replicationtestutils.TenantStreamingClusters, replicationJobID int) {
			execCfg := c.DestSysServer.ExecutorConfig().(sql.ExecutorConfig)
			require.NoError(t, c.DestCluster.Server(0).InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				j, err := execCfg.JobRegistry.LoadJobWithTxn(ctx, jobspb.JobID(replicationJobID), txn)
				require.NoError(t, err)
				payload := j.Payload()
				replicationDetails := payload.GetStreamIngestion()
				ptp := execCfg.ProtectedTimestampProvider.WithTxn(txn)
				_, err = ptp.GetRecord(ctx, *replicationDetails.ProtectedTimestampRecordID)
				require.EqualError(t, err, protectedts.ErrNotExists.Error())
				return nil
			}))
		}
		checkDestinationProtection := func(c *replicationtestutils.TenantStreamingClusters, frontier hlc.Timestamp, replicationJobID int) {
			execCfg := c.DestSysServer.ExecutorConfig().(sql.ExecutorConfig)
			ptp := execCfg.ProtectedTimestampProvider
			require.NoError(t, c.DestCluster.Server(0).InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				j, err := execCfg.JobRegistry.LoadJobWithTxn(ctx, jobspb.JobID(replicationJobID), txn)
				if err != nil {
					return err
				}
				payload := j.Payload()
				progress := j.Progress()
				replicationDetails := payload.GetStreamIngestion()

				require.NotNil(t, replicationDetails.ProtectedTimestampRecordID)
				rec, err := ptp.WithTxn(txn).GetRecord(ctx, *replicationDetails.ProtectedTimestampRecordID)
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

		c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
		defer cleanup()

		c.DestSysSQL.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
		c.DestSysSQL.Exec(t, "SET CLUSTER SETTING kv.protectedts.reconciliation.interval = '1ms';")

		producerJobID, replicationJobID := c.StartStreamReplication(ctx)

		jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
		jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(replicationJobID))

		// Ensure that we wait at least a second so that the gap between the first
		// time we write the protected timestamp (t1) during replication job
		// startup, and the first progress update (t2) is greater than 1s. This is
		// important because if `frontier@t2 - ReplicationTTLSeconds < t1` then we
		// will not update the PTS record.
		now := c.SrcCluster.Server(0).Clock().Now().Add(int64(time.Second)*2, 0)
		c.WaitUntilHighWatermark(now, jobspb.JobID(replicationJobID))

		// Check that the producer and replication job have written a protected
		// timestamp.
		waitForProducerProtection(c, now, replicationJobID)
		checkDestinationProtection(c, now, replicationJobID)

		now2 := now.Add(time.Second.Nanoseconds(), 0)
		c.WaitUntilHighWatermark(now2, jobspb.JobID(replicationJobID))
		// Let the replication progress for a second before checking that the
		// protected timestamp record has also been updated on the destination
		// cluster. This update happens in the same txn in which we update the
		// replication job's progress.
		waitForProducerProtection(c, now2, replicationJobID)
		checkDestinationProtection(c, now2, replicationJobID)

		if pauseBeforeTerminal {
			c.DestSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", replicationJobID))
			jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(replicationJobID))
		}

		if completeReplication {
			c.DestSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", replicationJobID))
			jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(replicationJobID))
			var cutoverTime time.Time
			c.DestSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)
			c.Cutover(producerJobID, replicationJobID, cutoverTime, false)
		}

		// Set GC TTL low, so that the GC job completes quickly in the test.
		c.DestSysSQL.Exec(t, "ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = 1;")
		c.DestSysSQL.Exec(t, fmt.Sprintf("DROP TENANT %s", c.Args.DestTenantName))

		if !completeReplication {
			jobutils.WaitForJobToCancel(c.T, c.DestSysSQL, jobspb.JobID(replicationJobID))
			jobutils.WaitForJobToCancel(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
		}

		// Check if the producer job has released protected timestamp.
		stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, replicationJobID)
		require.NotNil(t, stats.ProducerStatus)
		require.Nil(t, stats.ProducerStatus.ProtectedTimestamp)

		// Check if the replication job has released protected timestamp.
		checkNoDestinationProtection(c, replicationJobID)

		// Wait for the GC job to finish, this should happen once the protected
		// timestamp has been released.
		c.DestSysSQL.Exec(t, "SHOW JOBS WHEN COMPLETE SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE GC'")

		// Check if dest tenant key range is cleaned up.
		destTenantPrefix := keys.MakeTenantPrefix(args.DestTenantID)
		rows, err := c.DestCluster.Server(0).DB().
			Scan(ctx, destTenantPrefix, destTenantPrefix.PrefixEnd(), 10)
		require.NoError(t, err)
		require.Empty(t, rows)

		c.DestSysSQL.CheckQueryResults(t,
			fmt.Sprintf("SELECT count(*) FROM system.tenants WHERE id = %s", args.DestTenantID),
			[][]string{{"0"}})
	}

	testutils.RunTrueAndFalse(t, "pause-before-terminal", func(t *testing.T, pauseBeforeTerminal bool) {
		testutils.RunTrueAndFalse(t, "complete-replication", func(t *testing.T, completeReplication bool) {
			testProtectedTimestampManagement(t, pauseBeforeTerminal, completeReplication)
		})
	})
}

func TestTenantStreamingShowTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()
	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	rowStr := c.DestSysSQL.QueryStr(t, fmt.Sprintf("SHOW TENANT %s WITH REPLICATION STATUS", args.DestTenantName))
	require.Equal(t, "2", rowStr[0][0])
	require.Equal(t, "destination", rowStr[0][1])
	if rowStr[0][3] == "NULL" {
		// There is no source yet, therefore the replication is not fully initialized.
		require.Equal(t, "initializing replication", rowStr[0][2])
	}

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	highWatermark := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilHighWatermark(highWatermark, jobspb.JobID(ingestionJobID))
	destRegistry := c.DestCluster.Server(0).JobRegistry().(*jobs.Registry)
	details, err := destRegistry.LoadJob(ctx, jobspb.JobID(ingestionJobID))
	require.NoError(t, err)
	replicationDetails := details.Details().(jobspb.StreamIngestionDetails)

	var (
		id            int
		dest          string
		status        string
		serviceMode   string
		source        string
		sourceUri     string
		jobId         int
		maxReplTime   time.Time
		protectedTime time.Time
		cutoverTime   []byte // should be nil
	)
	row := c.DestSysSQL.QueryRow(t, fmt.Sprintf("SHOW TENANT %s WITH REPLICATION STATUS", args.DestTenantName))
	row.Scan(&id, &dest, &status, &serviceMode, &source, &sourceUri, &jobId, &maxReplTime, &protectedTime, &cutoverTime)
	require.Equal(t, 2, id)
	require.Equal(t, "destination", dest)
	require.Equal(t, "replicating", status)
	require.Equal(t, "none", serviceMode)
	require.Equal(t, "source", source)
	expectedURI, err := redactSourceURI(c.SrcURL.String())
	require.NoError(t, err)
	require.Equal(t, expectedURI, sourceUri)
	require.Equal(t, ingestionJobID, jobId)
	require.Less(t, maxReplTime, timeutil.Now())
	require.Less(t, protectedTime, timeutil.Now())
	require.GreaterOrEqual(t, maxReplTime, highWatermark.GoTime())
	require.GreaterOrEqual(t, protectedTime, replicationDetails.ReplicationStartTime.GoTime())
	require.Nil(t, cutoverTime)

	// Verify the SHOW command prints the right cutover timestamp. Adding some
	// logical component to make sure we handle it correctly.
	futureTime := c.DestSysServer.Clock().Now().Add(24*time.Hour.Nanoseconds(), 7)
	var cutoverStr string
	c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		c.Args.DestTenantName, futureTime.AsOfSystemTime()).Scan(&cutoverStr)
	var showCutover string
	c.DestSysSQL.QueryRow(c.T, fmt.Sprintf("SELECT cutover_time FROM [SHOW TENANT %s WITH REPLICATION STATUS]",
		c.Args.DestTenantName)).Scan(&showCutover)
	require.Equal(c.T, cutoverStr, showCutover)
	cutoverOutput := replicationtestutils.DecimalTimeToHLC(c.T, showCutover)
	require.Equal(c.T, futureTime, cutoverOutput)
}
