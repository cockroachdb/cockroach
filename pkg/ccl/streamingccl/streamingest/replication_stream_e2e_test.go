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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestTenantStreamingProducerJobTimedOut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.SrcClusterSettings[`stream_replication.job_liveness.timeout`] = `'1m'`
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)

	require.NotNil(t, stats.ReplicationLagInfo)
	require.True(t, srcTime.LessEq(stats.ReplicationLagInfo.MinIngestedTimestamp))

	// Make producer job easily times out
	c.SrcSysSQL.ExecMultiple(t, replicationtestutils.ConfigureClusterSettings(map[string]string{
		`stream_replication.job_liveness.timeout`: `'100ms'`,
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

// TestTenantStreamingJobRetryReset tests that the job level retry counter
// resets after the replicated time progresses. To do this, the test conducts a
// few retries before the initial scan completes, lets the initial scan
// complete, and then maxes out the retry counter, causing the job to pause.
// Since the job made progress when it completed the initial scan, the test
// asserts that the total number of retries counted is larger than the max
// number of retries allowed.
func TestTenantStreamingJobRetryReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mu := struct {
		syncutil.Mutex
		initialScanComplete bool
		ingestionStarts     int
	}{}

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.TestingKnobs = &sql.StreamingTestingKnobs{
		RunAfterReceivingEvent: func(ctx context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			if mu.ingestionStarts > replicationtestutils.
				TestingMaxDistSQLRetries-1 && !mu.initialScanComplete {
				return nil
			}
			return errors.New("throwing a retryable error")
		},
		BeforeIngestionStart: func(ctx context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			mu.ingestionStarts++
			return nil
		},
	}
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))
	mu.Lock()
	mu.initialScanComplete = true
	mu.Unlock()

	jobutils.WaitForJobToPause(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	require.Greater(t, mu.ingestionStarts, replicationtestutils.TestingMaxDistSQLRetries+1)
}
func TestTenantStreamingPauseResumeIngestion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))
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
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))
	c.RequireFingerprintMatchAtTimestamp(srcTime.AsOfSystemTime())
}

func TestTenantStreamingPauseOnPermanentJobError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	// Make ingestion error out only once to ensure the job conducts one retryable
	// error. It's fine to close the channel-- the receiver still gets the error.
	ingestErrCh <- errors.Newf("ingestion error from test")
	close(ingestErrCh)

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
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
	c.WaitUntilReplicatedTime(cutoverTime, jobspb.JobID(ingestionJobID))
	c.Cutover(producerJobID, ingestionJobID, cutoverTime.GoTime(), false)
	c.RequireFingerprintMatchAtTimestamp(cutoverTime.AsOfSystemTime())

	// Clients should never be started prior to a checkpointed timestamp
	for _, clientStartTime := range lastClientStart {
		require.Less(t, checkpointMinTime.UnixNano(), clientStartTime.GoTime().UnixNano())
	}

}

func requireReleasedProducerPTSRecord(
	t *testing.T,
	ctx context.Context,
	srv serverutils.ApplicationLayerInterface,
	producerJobID jobspb.JobID,
) {
	t.Helper()
	job, err := srv.JobRegistry().(*jobs.Registry).LoadJob(ctx, producerJobID)
	require.NoError(t, err)
	ptsRecordID := job.Payload().Details.(*jobspb.Payload_StreamReplication).StreamReplication.ProtectedTimestampRecordID
	ptsProvider := srv.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
	err = srv.InternalDB().(descs.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := ptsProvider.WithTxn(txn).GetRecord(ctx, ptsRecordID)
		return err
	})
	require.ErrorIs(t, err, protectedts.ErrNotExists)
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
		c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))
		c.RequireFingerprintMatchAtTimestamp(srcTime.AsOfSystemTime())

		if cancelAfterPaused {
			c.DestSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", ingestionJobID))
			jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
		}

		c.DestSysSQL.Exec(t, fmt.Sprintf("CANCEL JOB %d", ingestionJobID))
		jobutils.WaitForJobToCancel(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
		jobutils.WaitForJobToFail(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))

		// Check if the producer job has released protected timestamp.
		requireReleasedProducerPTSRecord(t, ctx, c.SrcSysServer.ApplicationLayer(), jobspb.JobID(producerJobID))

		// Check if dest tenant key ranges are not cleaned up.
		destTenantSpan := keys.MakeTenantSpan(args.DestTenantID)

		rows, err := c.DestCluster.Server(0).DB().
			Scan(ctx, destTenantSpan.Key, destTenantSpan.EndKey, 10)
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
			Scan(ctx, destTenantSpan.Key, destTenantSpan.EndKey, 10)
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

		c.WaitUntilReplicatedTime(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))
		if cancelAfterPaused {
			c.DestSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", ingestionJobID))
			jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
		}

		// Set GC TTL low, so that the GC job completes quickly in the test.
		c.DestSysSQL.Exec(t, "ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = 1;")
		c.DestSysSQL.Exec(t, fmt.Sprintf("DROP TENANT %s", c.Args.DestTenantName))
		jobutils.WaitForJobToCancel(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
		jobutils.WaitForJobToFail(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))

		// Check if the producer job has released protected timestamp.
		requireReleasedProducerPTSRecord(t, ctx, c.SrcSysServer.ApplicationLayer(), jobspb.JobID(producerJobID))

		// Wait for the GC job to finish
		c.DestSysSQL.Exec(t, "SHOW JOBS WHEN COMPLETE SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE GC'")

		// Check if dest tenant key range is cleaned up.
		destTenantSpan := keys.MakeTenantSpan(args.DestTenantID)
		rows, err := c.DestCluster.Server(0).DB().
			Scan(ctx, destTenantSpan.Key, destTenantSpan.EndKey, 10)
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

// TestTenantStreamingUnavailableStreamAddress verifies that after a
// pause/resume (replan) we will not use a dead server as a source.
func TestTenantStreamingUnavailableStreamAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "multi-node may time out under deadlock")
	skip.UnderRace(t, "takes too long with multiple nodes")

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	args.SrcNumNodes = 4
	args.DestNumNodes = 3

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	replicationtestutils.CreateScatteredTable(t, c, 3)
	srcScatteredData := c.SrcTenantSQL.QueryStr(c.T, "SELECT * FROM d.scattered ORDER BY key")

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	c.DestSysSQL.Exec(t, `PAUSE JOB $1`, ingestionJobID)
	jobutils.WaitForJobToPause(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	// We should've persisted the original topology
	progress := jobutils.GetJobProgress(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	streamAddresses := progress.GetStreamIngest().StreamAddresses
	require.Greater(t, len(streamAddresses), 1)

	// Write something to the source cluster, note that the job is paused - and
	// therefore not replicated for now.
	c.SrcTenantSQL.Exec(t, "CREATE TABLE d.x (id INT PRIMARY KEY, n INT)")
	c.SrcTenantSQL.Exec(t, `INSERT INTO d.x VALUES (3);`)

	// Stop a server on the source cluster. Note that in this test we are trying
	// to avoid using the source cluster after this point because if we do the
	// test flakes, see #107499 for more info.
	destroyedAddress := c.SrcURL.String()
	require.NoError(t, c.SrcTenantConn.Close())
	c.SrcTenantServer.AppStopper().Stop(ctx)
	c.SrcCluster.StopServer(0)

	c.DestSysSQL.Exec(t, `RESUME JOB $1`, ingestionJobID)
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	cutoverTime := c.SrcCluster.Server(1).Clock().Now().GoTime()
	var cutoverStr string
	c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		c.Args.DestTenantName, cutoverTime).Scan(&cutoverStr)
	cutoverOutput := replicationtestutils.DecimalTimeToHLC(t, cutoverStr)
	require.Equal(c.T, cutoverTime, cutoverOutput.GoTime())
	jobutils.WaitForJobToSucceed(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	cleanUpTenant := c.StartDestTenant(ctx)
	defer func() {
		require.NoError(t, cleanUpTenant())
	}()

	// The destroyed address should have been removed from the topology.
	progress = jobutils.GetJobProgress(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	newStreamAddresses := progress.GetStreamIngest().StreamAddresses
	require.Contains(t, streamAddresses, destroyedAddress)
	require.NotContains(t, newStreamAddresses, destroyedAddress)

	// Verify the destination tenant is fully replicated.
	destData := c.DestTenantSQL.QueryStr(c.T, "SELECT * FROM d.x")
	require.Equal(c.T, [][]string{{"3", "NULL"}}, destData)
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
	c.WaitUntilReplicatedTime(cutoverTime, jobspb.JobID(ingestionJobID))

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

	ctx := context.Background()
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, replicationtestutils.DefaultTenantStreamingClustersArgs)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcSysServer.Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))
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
		c.WaitUntilReplicatedTime(srcTimeAfterDelRange, jobspb.JobID(ingestionJobID))

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

	skip.UnderDeadlock(t, "multi-node may time out under deadlock")
	skip.UnderRace(t, "takes too long with multiple nodes")

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.SrcNumNodes = 4
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

	// Make sure we have data on all nodes, so that we will have multiple
	// connections and client addresses (and actually test multi-node).
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

// TestStreamingAutoReplan asserts that if a new node can participate in the
// replication job, it will trigger distSQL replanning.
func TestStreamingAutoReplan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.MultitenantSingleClusterNumNodes = 1

	retryErrorChan := make(chan error)
	turnOffReplanning := make(chan struct{})
	var alreadyReplanned atomic.Bool

	// Track the number of unique addresses that we're connected to.
	clientAddresses := make(map[string]struct{})
	var addressesMu syncutil.Mutex
	args.TestingKnobs = &sql.StreamingTestingKnobs{
		BeforeClientSubscribe: func(addr string, token string, clientStartTime hlc.Timestamp) {
			addressesMu.Lock()
			defer addressesMu.Unlock()
			clientAddresses[addr] = struct{}{}
		},
		AfterRetryIteration: func(err error) {

			if err != nil && !alreadyReplanned.Load() {
				retryErrorChan <- err
				<-turnOffReplanning
				alreadyReplanned.Swap(true)
			}
		},
	}
	c, cleanup := replicationtestutils.CreateMultiTenantStreamingCluster(ctx, t, args)
	defer cleanup()
	// Don't allow for replanning until the new nodes and scattered table have been created.
	serverutils.SetClusterSetting(t, c.DestCluster, "stream_replication.replan_flow_threshold", 0)
	serverutils.SetClusterSetting(t, c.DestCluster, "stream_replication.replan_flow_frequency", time.Millisecond*500)

	// Begin the job on a single source node.
	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.WaitUntilStartTimeReached(jobspb.JobID(ingestionJobID))
	require.Equal(t, len(clientAddresses), 1)

	c.SrcCluster.AddAndStartServer(c.T, replicationtestutils.CreateServerArgs(c.Args))
	c.SrcCluster.AddAndStartServer(c.T, replicationtestutils.CreateServerArgs(c.Args))
	require.NoError(t, c.SrcCluster.WaitForFullReplication())

	replicationtestutils.CreateScatteredTable(t, c, 3)

	// Configure the ingestion job to replan eagerly.
	serverutils.SetClusterSetting(t, c.DestCluster, "stream_replication.replan_flow_threshold", 0.1)

	// The ingestion job should eventually retry because it detects new nodes to add to the plan.
	require.Error(t, <-retryErrorChan, sql.ErrPlanChanged)

	// Prevent continuous replanning to reduce test runtime. dsp.PartitionSpans()
	// on the src cluster may return a different set of src nodes that can
	// participate in the replication job (especially under stress), so if we
	// repeatedly replan the job, we will repeatedly restart the job, preventing
	// job progress.
	serverutils.SetClusterSetting(t, c.DestCluster, "stream_replication.replan_flow_threshold", 0)
	serverutils.SetClusterSetting(t, c.DestCluster, "stream_replication.replan_flow_frequency", time.Minute*10)
	close(turnOffReplanning)

	cutoverTime := c.DestSysServer.Clock().Now()
	c.WaitUntilReplicatedTime(cutoverTime, jobspb.JobID(ingestionJobID))

	require.Greater(t, len(clientAddresses), 1)
}

// TestProtectedTimestampManagement tests the active protected
// timestamps management on the destination tenant's keyspan.
func TestProtectedTimestampManagement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	// Override the replication job details ReplicationTTLSeconds to a small value
	// so that every progress update results in a protected timestamp update.
	args.RetentionTTLSeconds = 1

	testutils.RunTrueAndFalse(t, "pause-before-terminal", func(t *testing.T, pauseBeforeTerminal bool) {
		testutils.RunTrueAndFalse(t, "complete-replication", func(t *testing.T, completeReplication bool) {

			// waitForProducerProtection asserts that there is a PTS record protecting
			// the source tenant. We ensure the PTS record is protecting a timestamp
			// greater or equal to the frontier we know we have replicated up until.
			waitForProducerProtection := func(c *replicationtestutils.TenantStreamingClusters, frontier hlc.Timestamp, producerJobID int) {
				testutils.SucceedsSoon(t, func() error {
					srv := c.SrcSysServer.ApplicationLayer()
					job, err := srv.JobRegistry().(*jobs.Registry).LoadJob(ctx, jobspb.JobID(producerJobID))
					if err != nil {
						return err
					}
					ptsRecordID := job.Payload().Details.(*jobspb.Payload_StreamReplication).StreamReplication.ProtectedTimestampRecordID
					ptsProvider := srv.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider

					var ptsRecord *ptpb.Record
					if err := srv.InternalDB().(descs.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
						var err error
						ptsRecord, err = ptsProvider.WithTxn(txn).GetRecord(ctx, ptsRecordID)
						return err
					}); err != nil {
						return err
					}
					if ptsRecord.Timestamp.Less(frontier) {
						return errors.Newf("protection is at %s, expected to be >= %s",
							ptsRecord.Timestamp.String(), frontier.String())
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

					replicatedTime := replicationutils.ReplicatedTimeFromProgress(&progress)
					require.True(t, frontier.LessEq(replicatedTime))

					roundedReplicatedTime := replicatedTime.GoTime().Round(time.Millisecond)
					roundedProtectedTime := rec.Timestamp.GoTime().Round(time.Millisecond)
					window := roundedReplicatedTime.Sub(roundedProtectedTime)
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
			c.WaitUntilReplicatedTime(now, jobspb.JobID(replicationJobID))

			// Check that the producer and replication job have written a protected
			// timestamp.
			waitForProducerProtection(c, now, producerJobID)
			checkDestinationProtection(c, now, replicationJobID)

			now2 := now.Add(time.Second.Nanoseconds(), 0)
			c.WaitUntilReplicatedTime(now2, jobspb.JobID(replicationJobID))
			// Let the replication progress for a second before checking that the
			// protected timestamp record has also been updated on the destination
			// cluster. This update happens in the same txn in which we update the
			// replication job's progress.
			waitForProducerProtection(c, now2, producerJobID)
			checkDestinationProtection(c, now2, replicationJobID)

			if pauseBeforeTerminal {
				c.DestSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", replicationJobID))
				jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(replicationJobID))
			}

			if completeReplication {
				c.DestSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", replicationJobID))
				jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(replicationJobID))
				var emptyCutoverTime time.Time
				c.Cutover(producerJobID, replicationJobID, emptyCutoverTime, false)
			}

			// Set GC TTL low, so that the GC job completes quickly in the test.
			c.DestSysSQL.Exec(t, "ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = 1;")
			c.DestSysSQL.Exec(t, fmt.Sprintf("DROP TENANT %s", c.Args.DestTenantName))

			if !completeReplication {
				jobutils.WaitForJobToCancel(c.T, c.DestSysSQL, jobspb.JobID(replicationJobID))
				jobutils.WaitForJobToFail(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
			}

			// Check if the producer job has released protected timestamp.
			requireReleasedProducerPTSRecord(t, ctx, c.SrcSysServer.ApplicationLayer(), jobspb.JobID(producerJobID))

			// Check if the replication job has released protected timestamp.
			checkNoDestinationProtection(c, replicationJobID)

			// Wait for the GC job to finish, this should happen once the protected
			// timestamp has been released.
			c.DestSysSQL.Exec(t, "SHOW JOBS WHEN COMPLETE SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE GC'")

			// Check if dest tenant key range is cleaned up.
			destTenantSpan := keys.MakeTenantSpan(args.DestTenantID)
			rows, err := c.DestCluster.Server(0).DB().
				Scan(ctx, destTenantSpan.Key, destTenantSpan.EndKey, 10)
			require.NoError(t, err)
			require.Empty(t, rows)

			c.DestSysSQL.CheckQueryResults(t,
				fmt.Sprintf("SELECT count(*) FROM system.tenants WHERE id = %s", args.DestTenantID),
				[][]string{{"0"}})
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
	targetReplicatedTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(targetReplicatedTime, jobspb.JobID(ingestionJobID))
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
	require.GreaterOrEqual(t, maxReplTime, targetReplicatedTime.GoTime())
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

// TestTenantStreamingPauseProducer verifies that pausing the producer job pauses the ingestion job.
func TestTenantStreamingPauseProducer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	// Pause the producer job.
	c.SrcSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", producerJobID))
	jobutils.WaitForJobToPause(t, c.SrcSysSQL, jobspb.JobID(producerJobID))

	// Verify the ingestion job is paused.
	jobutils.WaitForJobToPause(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
}

// TestTenantStreamingCancelProducer verifies that canceling the producer job pauses the ingestion job.
func TestTenantStreamingCancelProducer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	// Cancel the producer job, which should fail the ingestion job.
	c.SrcSysSQL.Exec(t, fmt.Sprintf("CANCEL JOB %d", producerJobID))
	jobutils.WaitForJobToCancel(t, c.SrcSysSQL, jobspb.JobID(producerJobID))

	// Verify the ingestion job is paused.
	jobutils.WaitForJobToPause(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
}

// TestTenantStreamingRetryLoadJob verifies the resumer retries loading the job
// if that fails, otherwise we might fail when, for example, the node is busy.
func TestTenantStreamingRetryLoadJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var knobLoadErr error
	var mu syncutil.Mutex
	knobDoneCh := make(chan struct{})
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.TestingKnobs = &sql.StreamingTestingKnobs{
		AfterResumerJobLoad: func(err error) error {
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				return err
			}
			if knobLoadErr == nil {
				return nil
			}
			// We only need to see the error once, and then we can clear it.
			close(knobDoneCh)
			defer func() { knobLoadErr = nil }()
			return knobLoadErr
		},
	}
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	c.SrcSysSQL.Exec(t, fmt.Sprintf("PAUSE JOB %d", producerJobID))
	jobutils.WaitForJobToPause(t, c.SrcSysSQL, jobspb.JobID(producerJobID))

	// Write a bit more to be verified at the end.
	c.SrcTenantSQL.Exec(t, "INSERT INTO d.t2 VALUES (3);")

	// Inject an error to fail the resumer.
	mu.Lock()
	knobLoadErr = errors.Newf("test error")
	mu.Unlock()

	// Resume ingestion.
	c.SrcSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", producerJobID))
	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))

	// Wait for the resumer to see the error and clear it, after this it should
	// succeed resuming.
	<-knobDoneCh

	// Verify the job succeeds now.
	srcTime = c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))
	c.RequireFingerprintMatchAtTimestamp(srcTime.AsOfSystemTime())
}

func TestLoadProducerAndIngestionProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, replicationJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(replicationJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(replicationJobID))

	srcDB := c.SrcSysServer.ExecutorConfig().(sql.ExecutorConfig).InternalDB
	producerProgress, err := replicationutils.LoadReplicationProgress(ctx, srcDB, jobspb.JobID(producerJobID))
	require.NoError(t, err)
	require.Equal(t, jobspb.StreamReplicationProgress_NOT_FINISHED, producerProgress.StreamIngestionStatus)

	destDB := c.DestSysServer.ExecutorConfig().(sql.ExecutorConfig).InternalDB
	ingestionProgress, err := replicationutils.LoadIngestionProgress(ctx, destDB, jobspb.JobID(replicationJobID))
	require.NoError(t, err)
	require.Equal(t, jobspb.Replicating, ingestionProgress.ReplicationStatus)
}

// TestStreamingRegionalConstraint ensures that the replicating tenants regional
// constraints are obeyed during replication. This test serves as an end to end
// test of span config replication.
func TestStreamingRegionalConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "takes too long under stress race")

	ctx := context.Background()
	regions := []string{"mars", "venus", "mercury"}
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.MultitenantSingleClusterNumNodes = 3
	args.MultiTenantSingleClusterTestRegions = regions
	marsNodeID := roachpb.NodeID(1)

	c, cleanup := replicationtestutils.CreateMultiTenantStreamingCluster(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.SrcTenantSQL.Exec(t, "CREATE DATABASE test")
	c.SrcTenantSQL.Exec(t, `ALTER DATABASE test CONFIGURE ZONE USING constraints = '[+region=mars]', num_replicas = 1;`)
	c.SrcTenantSQL.Exec(t, "CREATE TABLE test.x (id INT PRIMARY KEY, n INT)")
	c.SrcTenantSQL.Exec(t, "INSERT INTO test.x VALUES (1, 1)")

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	checkLocalities := func(targetSpan roachpb.Span, scanner rangedesc.Scanner) func() error {
		// make pageSize large enough to not affect the test
		pageSize := 10000
		init := func() {}

		return func() error {
			return scanner.Scan(ctx, pageSize, init, targetSpan, func(descriptors ...roachpb.RangeDescriptor) error {
				for _, desc := range descriptors {
					for _, replica := range desc.InternalReplicas {
						if replica.NodeID != marsNodeID {
							return errors.Newf("found table data located on another node %d", replica.NodeID)
						}
					}
				}
				return nil
			})
		}
	}

	srcCodec := keys.MakeSQLCodec(c.Args.SrcTenantID)
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(
		c.SrcSysServer.DB(), srcCodec, "test", "x")
	destCodec := keys.MakeSQLCodec(c.Args.DestTenantID)

	testutils.SucceedsSoon(t,
		checkLocalities(tableDesc.PrimaryIndexSpan(srcCodec), rangedesc.NewScanner(c.SrcSysServer.DB())))

	testutils.SucceedsSoon(t,
		checkLocalities(tableDesc.PrimaryIndexSpan(destCodec), rangedesc.NewScanner(c.DestSysServer.DB())))

	tableName := "test"
	tabledIDQuery := fmt.Sprintf(`SELECT id FROM system.namespace WHERE name ='%s'`, tableName)

	var tableID uint32
	c.SrcTenantSQL.QueryRow(t, tabledIDQuery).Scan(&tableID)
	fmt.Printf("%d", tableID)

	checkLocalityRanges(t, c.SrcSysSQL, srcCodec, uint32(tableDesc.GetID()), "mars")

}

func checkLocalityRanges(
	t *testing.T, sysSQL *sqlutils.SQLRunner, codec keys.SQLCodec, tableID uint32, region string,
) {
	targetPrefix := codec.TablePrefix(tableID)
	distinctQuery := fmt.Sprintf(`
SELECT 
  DISTINCT replica_localities
FROM 
  [SHOW CLUSTER RANGES]
WHERE 
  start_key ~ '%s'
`, targetPrefix)
	var locality string
	sysSQL.QueryRow(t, distinctQuery).Scan(&locality)
	require.Contains(t, locality, region)
}

// TestReproIncorrectJobQuery reproduces a bug in which a crdb_internal.jobs
// query incorrectly returns 0 rows. To reproduce, the test conducts the following:
//
// 1. Spin up a single multinode, multitenant test cluster: the cluster will
// have a system tenant, a "source" tenant, and a "destination" tenant. This
// specific cluster configuration is most likely unnecessary, and was only used
// to speed development of this test.
//
// 2. Start a c2c job. Any job should repro this, but a c2c job was used to
// speed development of a repro.
//
// 3. Shut down the node coordinating the consumer side job, called the "stream
// ingestion" job, running on the system tenant.
//
// 4. Issue a cutover command, which eventually will complete the replication
// job and the test.
//
// 5. While the job is running, poll crdb_internal.jobs and check that the
// stream_ingestion job exists at a specific AOST timestamp.
//
// 6. If the query above returns the unexpected 0 row result, run debugCrdbInternalJobs():
// - validate the above query with the same AOST timestamp returns results.
// - also validate that the system.jobs and system.job_info table contain expected results.
// - this validation step points to the _query_ returning incorrect results, rather than incorrect job state.
func TestReproIncorrectJobQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	rng, _ := randutil.NewPseudoRand()
	numNodes := 4

	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.MultitenantSingleClusterNumNodes = numNodes

	c, cleanup := replicationtestutils.CreateMultiTenantStreamingCluster(ctx, t, args)
	defer cleanup()

	// These cluster setting changes are attempts to speed up the test.
	serverutils.SetClusterSetting(t, c.DestCluster, "server.shutdown.query_wait", "10ms")
	serverutils.SetClusterSetting(t, c.DestCluster, "server.shutdown.lease_transfer_wait", "10ms")

	replicationtestutils.CreateScatteredTable(t, c, 4)

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.WaitUntilStartTimeReached(jobspb.JobID(ingestionJobID))

	var coordinatorNodeIndexByOne int
	c.SrcSysSQL.QueryRow(t, `SELECT coordinator_id FROM crdb_internal.jobs WHERE job_id = $1`, ingestionJobID).Scan(&coordinatorNodeIndexByOne)
	coordinatorNode := coordinatorNodeIndexByOne - 1

	// Find a different node to run queries on once the coordinator node has shut
	// down.
	findAnotherNode := func(notThisNode int) int {
		for {
			anotherNode := rng.Intn(numNodes)
			if notThisNode != anotherNode {
				return anotherNode
			}
		}
	}
	watcherNode := findAnotherNode(coordinatorNode)
	db := c.DestCluster.Conns[watcherNode]

	var aost string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT clock_timestamp()::timestamp::string`).Scan(&aost))
	res := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT status, payload FROM crdb_internal.system_jobs AS OF SYSTEM TIME '%s' WHERE id = $1`, aost), ingestionJobID)
	require.NoError(t, res.Err())

	group := ctxgroup.WithContext(ctx)
	defer func() {
		require.NoError(t, group.Wait())
	}()

	group.GoCtx(func(ctx context.Context) error {
		// Continuously poll crdb_internal.system_jobs and check the ingestion job
		// is still there.
		return retry.ForDuration(time.Second*100, func() error {
			var status string
			var payloadBytes []byte
			var aost string
			if err := db.QueryRowContext(ctx, `SELECT clock_timestamp()::timestamp::string`).Scan(&aost); err != nil {
				return err
			}
			res := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT status, payload FROM crdb_internal.system_jobs AS OF SYSTEM TIME '%s' WHERE id = $1`, aost), ingestionJobID)
			if res.Err() != nil {
				// This query can fail if a node shuts down during the query execution;
				// therefore, tolerate errors.
				return res.Err()
			}
			if err := res.Scan(&status, &payloadBytes); err != nil && strings.Contains(err.Error(), "sql: no rows in result set") {
				debugCrdbInternalJobs(ctx, t, ingestionJobID, db, aost)
				t.Fatalf("incorrect results %s", err.Error())

			} else if err != nil {
				return err
			}

			if jobs.Status(status) == jobs.StatusFailed {
				payload := &jobspb.Payload{}
				if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
					t.Fatalf("job failed: %s", payload.Error)
				}
				t.Fatalf("job failed")
			}

			// Cutover has complete, so we can end the test.
			if e, a := jobs.StatusSucceeded, jobs.Status(status); e != a {
				return errors.Errorf("expected job status %s, but got %s", e, a)
			}
			return nil
		})
	})

	group.GoCtx(func(ctx context.Context) error {
		sleepBeforeShutdown := time.Duration(rng.Intn(3))
		time.Sleep(sleepBeforeShutdown * time.Second)
		var emptyCutoverTime time.Time
		c.Cutover(producerJobID, ingestionJobID, emptyCutoverTime, true)
		c.DestCluster.Server(coordinatorNode).Stopper().Stop(ctx)
		return nil
	})
}

// debugCrdbInternalJobs checks for the existence of the ingestionJob via a
// variety of queries.
func debugCrdbInternalJobs(ctx context.Context, t *testing.T, ingestionJob int, db *gosql.DB, aost string) {
	t.Logf("sadness")
	sameQueryRes := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT status, payload FROM crdb_internal.system_jobs AS OF SYSTEM TIME '%s' WHERE id = $1 `, aost), ingestionJob)
	if sameQueryRes.Err() != nil {
		t.Logf("same query failed: %s", sameQueryRes.Err())
	} else {
		var status string
		var payloadBytes []byte
		if err := sameQueryRes.Scan(&status, &payloadBytes); err != nil {
			t.Logf("same query returned zero bytes: %s", err.Error())
		} else {
			t.Logf("query did not fail! the job status is %s", status)
		}
	}

	jobIDQuery := func(ctx context.Context, label string, query string) {
		t.Logf("try just querying %s", label)
		jobsRes := db.QueryRowContext(ctx, query, ingestionJob)
		if jobsRes.Err() != nil {
			t.Logf("job query failed,%s", jobsRes.Err())
			return
		}
		var id int
		if err := jobsRes.Scan(&id); err != nil {
			t.Logf("query returned zero bytes: %s", err.Error())
		} else {
			t.Logf("query did not fail! the job id is %d", id)
		}
	}

	jobIDQuery(ctx, "system.jobs", fmt.Sprintf(`SELECT id FROM system.jobs AS OF SYSTEM TIME '%s' WHERE id = $1`, aost))
	jobIDQuery(ctx, "system.job_info", fmt.Sprintf(`SELECT job_id FROM system.job_info AS OF SYSTEM TIME '%s' WHERE job_id=$1 LIMIT 1`, aost))

}
