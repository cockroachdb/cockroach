// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPCRPrivs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	c.DestSysSQL.Exec(t, fmt.Sprintf("CREATE USER %s", username.TestUser))
	c.SrcSysSQL.Exec(t, fmt.Sprintf("CREATE USER %s", username.TestUser+"2"))
	testuser := sqlutils.MakeSQLRunner(c.DestSysServer.SQLConn(t, serverutils.User(username.TestUser)))
	srcURL, cleanupSinkCert := pgurlutils.PGUrl(t, c.SrcSysServer.AdvSQLAddr(), t.Name(), url.User(username.TestUser+"2"))
	defer cleanupSinkCert()

	streamReplStmt := fmt.Sprintf("CREATE TENANT %s FROM REPLICATION OF %s ON '%s'",
		c.Args.DestTenantName,
		c.Args.SrcTenantName,
		srcURL.String())

	testuser.ExpectErr(t, "user testuser does not have MANAGEVIRTUALCLUSTER system privilege", streamReplStmt)

	c.DestSysSQL.Exec(t, fmt.Sprintf("GRANT SYSTEM MANAGEVIRTUALCLUSTER TO %s", username.TestUser))
	testuser.ExpectErr(t, "user testuser2 does not have REPLICATION system privilege", streamReplStmt)

	c.SrcSysSQL.Exec(t, fmt.Sprintf("GRANT SYSTEM REPLICATION TO %s", username.TestUser+"2"))
	c.DestSysSQL.Exec(t, streamReplStmt)

}
func TestTenantStreamingProducerJobTimedOut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	c.SrcSysSQL.Exec(t, fmt.Sprintf(`ALTER TENANT '%s' SET REPLICATION EXPIRATION WINDOW ='1m'`, c.Args.SrcTenantName))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	stats := replicationtestutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)

	require.NotNil(t, stats.ReplicationLagInfo)
	require.True(t, srcTime.LessEq(stats.ReplicationLagInfo.MinIngestedTimestamp))

	// Make producer job easily times out
	c.SrcSysSQL.Exec(t, fmt.Sprintf(`ALTER TENANT '%s' SET REPLICATION EXPIRATION WINDOW ='100ms'`, c.Args.SrcTenantName))

	jobutils.WaitForJobToFail(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	// The ingestion job will stop retrying as this is a permanent job error.
	jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	require.Regexp(t, "ingestion job failed .* but is being paused",
		replicationtestutils.GetStatusMesssage(t, c.DestSysSQL, ingestionJobID))

	ts := c.DestCluster.Server(0).Clock().Now()
	afterPauseFingerprint := replicationtestutils.FingerprintTenantAtTimestampNoHistory(t, c.DestSysSQL, c.Args.DestTenantName, ts.AsOfSystemTime())
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
// complete, and then maxes out the retry counter, which may cause the job to
// pause. Since the job made progress when it completed the initial scan, the
// test asserts that the total number of retries counted is larger than the max
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
	// This may cause the job to pause if no replicated span advances after this.
	// Either way, we still expect to see more restarts than the max retries.
	mu.initialScanComplete = true
	mu.Unlock()

	testutils.SucceedsSoon(t, func() error {
		mu.Lock()
		defer mu.Unlock()
		if mu.ingestionStarts > replicationtestutils.TestingMaxDistSQLRetries+1 {
			return nil
		}
		return errors.Errorf("ingestion job failed %d times", mu.ingestionStarts)

	})
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
	pausedCheckpoint := replicationtestutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID).
		ReplicationLagInfo.MinIngestedTimestamp
	// Check we paused at a timestamp greater than the previously reached high watermark
	require.True(t, srcTime.LessEq(pausedCheckpoint))

	// Introduce new update to the src.
	c.SrcTenantSQL.Exec(t, "INSERT INTO d.t2 VALUES (3);")
	// Confirm that the job high watermark doesn't change. If the dest cluster is still subscribing
	// to src cluster checkpoints events, the job high watermark may change.
	<-time.NewTimer(3 * time.Second).C
	require.Equal(t, pausedCheckpoint,
		replicationtestutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL,
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
	require.Nil(t, replicationtestutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL,
		ingestionJobID).ReplicationLagInfo)

	// Resume ingestion.
	c.DestSysSQL.Exec(t, fmt.Sprintf("RESUME JOB %d", ingestionJobID))
	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))

	// Check dest has caught up the previous updates.
	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.Cutover(ctx, producerJobID, ingestionJobID, srcTime.GoTime(), false)
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
		BeforeClientSubscribe: func(addr string, token string, clientStartTimes span.Frontier, _ bool) {
			lastClientStart[token] = clientStartTimes.Frontier()
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
	c.Cutover(ctx, producerJobID, ingestionJobID, cutoverTime.GoTime(), false)
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
	testutils.SucceedsSoon(t, func() error {
		job, err := srv.JobRegistry().(*jobs.Registry).LoadJob(ctx, producerJobID)
		require.NoError(t, err)
		ptsRecordID := job.Payload().Details.(*jobspb.Payload_StreamReplication).StreamReplication.ProtectedTimestampRecordID
		ptsProvider := srv.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		err = srv.InternalDB().(descs.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := ptsProvider.WithTxn(txn).GetRecord(ctx, ptsRecordID)
			return err
		})
		if !errors.Is(err, protectedts.ErrNotExists) {
			return errors.Wrapf(err, "unexpected error")
		}
		return nil
	})
}

func TestTenantStreamingCancelIngestion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	testCancelIngestion := func(t *testing.T, cancelAfterPaused bool) {
		telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)
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
		requireReleasedProducerPTSRecord(t, ctx, c.SrcSysServer, jobspb.JobID(producerJobID))

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

		counts := telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)
		require.GreaterOrEqual(t, counts["physical_replication.canceled"], int32(1))
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

	skip.UnderRace(t, "slow test") // takes >1mn under race

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
		requireReleasedProducerPTSRecord(t, ctx, c.SrcSysServer, jobspb.JobID(producerJobID))

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

func TestTenantStreamingImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, replicationtestutils.DefaultTenantStreamingClustersArgs)
	defer cleanup()

	dataSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			if _, err := w.Write([]byte("42,hello,goodbye\n")); err != nil {
				t.Logf("failed to write: %s", err.Error())
			}
		}
	}))
	defer dataSrv.Close()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	srcTime := c.SrcSysServer.Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	c.SrcTenantSQL.Exec(t, `
CREATE TABLE d.t_for_import(i int primary key, a string, b string);
INSERT INTO d.t_for_import (i) VALUES (1);
`)
	c.SrcSysSQL.Exec(t, "SET CLUSTER SETTING kv.bulk_io_write.small_write_size = '1'")
	c.SrcTenantSQL.Exec(t, "SET CLUSTER SETTING kv.bulk_io_write.small_write_size = '1'")
	c.SrcTenantSQL.Exec(t, "IMPORT INTO d.t_for_import CSV DATA ($1)", dataSrv.URL)

	srcTime = c.SrcSysServer.Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	cutoverTime := c.SrcSysServer.Clock().Now()
	c.Cutover(ctx, producerJobID, ingestionJobID, cutoverTime.GoTime(), false)
	c.RequireFingerprintMatchAtTimestamp(cutoverTime.AsOfSystemTime())
}

func TestReplicateManualSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 135968, "flaky test")

	ctx := context.Background()
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, replicationtestutils.DefaultTenantStreamingClustersArgs)
	defer cleanup()
	c.DestSysSQL.Exec(t, "SET CLUSTER SETTING physical_replication.consumer.ingest_split_event.enabled = true")

	c.SrcTenantSQL.Exec(t, "CREATE TABLE foo AS SELECT generate_series(1, 100)")
	var fooTableID int
	c.SrcTenantSQL.QueryRow(t, "SELECT id FROM system.namespace WHERE name = 'foo'").Scan(&fooTableID)

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	c.WaitUntilReplicatedTime(c.SrcSysServer.Clock().Now(), jobspb.JobID(ingestionJobID))

	c.SrcTenantSQL.Exec(t, "ALTER TABLE foo SPLIT AT VALUES (50)")
	expectedSplitKey := fmt.Sprintf("Table/%d/1/50", fooTableID)
	splitKeyQuery := fmt.Sprintf("SELECT count(*) FROM crdb_internal.ranges WHERE start_pretty ~ '%s'", expectedSplitKey)

	testutils.SucceedsSoon(t, func() error {
		var splitKeyPresent int
		c.DestSysSQL.QueryRow(t, splitKeyQuery).Scan(&splitKeyPresent)
		if splitKeyPresent == 0 {
			return errors.Newf("split key not present")
		}
		return nil
	})
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
				hlc.Timestamp{}, nil, false, batchHLCTime)
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

func TestSpecsPersistedOnlyAfterInitialPlan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	var persistedPhysicalSpecsCount int
	var replanCandidateCount int
	replannedThreeTimes := make(chan struct{})
	args.TestingKnobs = &sql.StreamingTestingKnobs{
		AfterReplicationFlowPlan: func(ingestionSpecs map[base.SQLInstanceID][]execinfrapb.StreamIngestionDataSpec,
			frontierSpec *execinfrapb.StreamIngestionFrontierSpec) {
			replanCandidateCount++
			if replanCandidateCount > 2 {
				close(replannedThreeTimes)
			}
		},
		AfterPersistingPartitionSpecs: func() {
			persistedPhysicalSpecsCount++
		},
	}

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	// Make the replanner generate candidates frequently, but never actually induce a replanning event
	serverutils.SetClusterSetting(t, c.DestCluster, "stream_replication.replan_flow_frequency", time.Millisecond*500)
	serverutils.SetClusterSetting(t, c.DestCluster, "stream_replication.replan_flow_threshold", 0)

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	// Wait for a couple replan candidates to generate and then check there's only one persisted plan.
	<-replannedThreeTimes
	require.Equal(t, 1, persistedPhysicalSpecsCount)
}

// TestStreamingAutoReplan asserts that if a new node can participate in the
// replication job, it will trigger distSQL replanning.
func TestStreamingAutoReplan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "multi cluster/node config exhausts hardware")
	skip.UnderDeadlock(t, "scatter may take too long")

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.MultitenantSingleClusterNumNodes = 1
	args.RoutingMode = streamclient.RoutingModeNode

	retryErrorChan := make(chan error)
	turnOffReplanning := make(chan struct{})
	var alreadyReplanned atomic.Bool

	// Track the number of unique addresses that we're connected to.
	clientAddresses := make(map[string]struct{})
	var addressesMu syncutil.Mutex
	args.TestingKnobs = &sql.StreamingTestingKnobs{
		BeforeClientSubscribe: func(addr string, token string, _ span.Frontier, _ bool) {
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

	// Don't allow inter node lag replanning to affect the test.
	serverutils.SetClusterSetting(t, c.DestCluster, "physical_replication.consumer.node_lag_replanning_threshold", 0)

	// Begin the job on a single source node.
	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.WaitUntilStartTimeReached(jobspb.JobID(ingestionJobID))
	require.Equal(t, len(clientAddresses), 1)

	c.SrcCluster.AddAndStartServer(c.T, replicationtestutils.CreateServerArgs(c.Args))
	c.SrcCluster.AddAndStartServer(c.T, replicationtestutils.CreateServerArgs(c.Args))
	require.NoError(t, c.SrcCluster.WaitForFullReplication())

	// Only need at least two nodes as leaseholders for test.
	replicationtestutils.CreateScatteredTable(t, c, 2)

	// Configure the ingestion job to replan eagerly.
	serverutils.SetClusterSetting(t, c.DestCluster, "stream_replication.replan_flow_threshold", 0.1)

	// The ingestion job should eventually retry because it detects new nodes to add to the plan.
	require.ErrorContains(t, <-retryErrorChan, sql.ErrPlanChanged.Error())

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

// TestStreamingReplanOnLag asserts that the c2c job retries if a node lags far
// behind other nodes. To do this, the test spins up a multi node c2c job, waits
// for the initial scan to complete, then elides checkpoints on node 1's stream
// ingestion processor, triggering a lagging node error.
func TestStreamingReplanOnLag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuressWithIssue(t, 115850, "time to scatter ranges takes too long under duress")
	skip.UnderMetamorphic(t, "time to scatter ranges takes too long under non-default settings")
	skip.WithIssue(t, 115850)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.MultitenantSingleClusterNumNodes = 3

	retryErrorChan := make(chan error)
	turnOffReplanning := make(chan struct{})
	var alreadyReplanned atomic.Bool

	// Track the number of unique addresses that we're connected to, to ensure
	// that all destination nodes participate in the replication stream.
	clientAddresses := make(map[string]struct{})
	var addressesMu syncutil.Mutex
	args.TestingKnobs = &sql.StreamingTestingKnobs{
		BeforeClientSubscribe: func(addr string, token string, _ span.Frontier, _ bool) {
			addressesMu.Lock()
			defer addressesMu.Unlock()
			clientAddresses[addr] = struct{}{}
		},
		AfterRetryIteration: func(err error) {
			// Surface the job level retry error.
			if err != nil && !alreadyReplanned.Load() {
				retryErrorChan <- err
				<-turnOffReplanning
				alreadyReplanned.Swap(true)
			}
		},
		ElideCheckpointEvent: func(nodeID base.SQLInstanceID, frontier hlc.Timestamp) bool {
			if nodeID == base.SQLInstanceID(1) && !frontier.IsEmpty() && !alreadyReplanned.Load() {
				// Elide checkpoints on Node 1 after the initial scan has complete and
				// before the automatic replanning event.
				return true
			}
			return false
		},
	}
	c, cleanup := replicationtestutils.CreateMultiTenantStreamingCluster(ctx, t, args)
	defer cleanup()
	// Don't allow for replanning based on node participation.
	serverutils.SetClusterSetting(t, c.DestCluster, "stream_replication.replan_flow_threshold", 0)

	replicationtestutils.CreateScatteredTable(t, c, 3)

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.WaitUntilStartTimeReached(jobspb.JobID(ingestionJobID))
	require.Greater(t, len(clientAddresses), 1)

	// Configure the ingestion job to replan eagerly based on node lagging.
	serverutils.SetClusterSetting(t, c.DestCluster, "physical_replication.consumer.node_lag_replanning_threshold", time.Second)
	serverutils.SetClusterSetting(t, c.DestCluster, "stream_replication.lag_check_frequency", time.Millisecond*500)

	// The ingestion job should eventually retry because it detects a lagging node.
	require.ErrorContains(t, <-retryErrorChan, ErrNodeLagging.Error())

	// Prevent continuous replanning to reduce test runtime.
	serverutils.SetClusterSetting(t, c.DestCluster, "physical_replication.consumer.node_lag_replanning_threshold", time.Minute*10)
	serverutils.SetClusterSetting(t, c.DestCluster, "stream_replication.lag_check_frequency", time.Minute*10)
	close(turnOffReplanning)

	cutoverTime := c.DestSysServer.Clock().Now()
	c.WaitUntilReplicatedTime(cutoverTime, jobspb.JobID(ingestionJobID))
}

// TestProtectedTimestampManagement tests the active protected
// timestamps management on the destination tenant's keyspan.
func TestProtectedTimestampManagement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "slow test") // takes >1mn under race.

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
					srv := c.SrcSysServer
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
				require.NoError(t, c.DestSysServer.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
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
				require.NoError(t, c.DestSysServer.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
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
			now := c.SrcCluster.Server(0).SystemLayer().Clock().Now().Add(int64(time.Second)*2, 0)
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
				c.Cutover(ctx, producerJobID, replicationJobID, emptyCutoverTime, false)
				c.SrcSysSQL.Exec(t, fmt.Sprintf(`ALTER TENANT '%s' SET REPLICATION EXPIRATION WINDOW ='100ms'`, c.Args.SrcTenantName))
			}

			// Set GC TTL low, so that the GC job completes quickly in the test.
			c.DestSysSQL.Exec(t, "ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = 1;")
			c.DestSysSQL.Exec(t, fmt.Sprintf("DROP TENANT %s", c.Args.DestTenantName))

			if !completeReplication {
				c.SrcSysSQL.Exec(t, fmt.Sprintf(`ALTER TENANT '%s' SET REPLICATION EXPIRATION WINDOW ='1ms'`, c.Args.SrcTenantName))
				jobutils.WaitForJobToCancel(c.T, c.DestSysSQL, jobspb.JobID(replicationJobID))
				jobutils.WaitForJobToFail(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
			}

			requireReleasedProducerPTSRecord(t, ctx, c.SrcSysServer, jobspb.JobID(producerJobID))

			// Check if the replication job has released protected timestamp.
			checkNoDestinationProtection(c, replicationJobID)

			// Wait for the GC job to finish, this should happen once the protected
			// timestamp has been released.
			c.DestSysSQL.Exec(t, "SHOW JOBS WHEN COMPLETE SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE GC'")

			// Check if dest tenant key range is cleaned up.
			destTenantSpan := keys.MakeTenantSpan(args.DestTenantID)
			rows, err := c.DestSysServer.DB().
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
	args.NoMetamorphicExternalConnection = true

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()
	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	rowStr := c.DestSysSQL.QueryStr(t, fmt.Sprintf("SHOW TENANT %s WITH REPLICATION STATUS", args.DestTenantName))
	require.Equal(t, "2", rowStr[0][0])
	require.Equal(t, "destination", rowStr[0][1])
	require.Equal(t, fmt.Sprintf("%d", ingestionJobID), rowStr[0][2])
	if rowStr[0][4] == "NULL" {
		// There is no source yet, therefore the replication is not fully initialized.
		require.Equal(t, "initializing replication", rowStr[0][3])
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
		id             int
		dest           string
		jobID          int
		status         string
		source         string
		sourceUri      string
		replicationLag string
		maxReplTime    time.Time
		protectedTime  time.Time
		cutoverTime    []byte // should be nil
	)
	row := c.DestSysSQL.QueryRow(t, fmt.Sprintf("SHOW TENANT %s WITH REPLICATION STATUS", args.DestTenantName))
	row.Scan(&id, &dest, &jobID, &source, &sourceUri, &protectedTime, &maxReplTime, &replicationLag, &cutoverTime, &status)
	require.Equal(t, 2, id)
	require.Equal(t, ingestionJobID, jobID)
	require.Equal(t, "destination", dest)
	require.Equal(t, "replicating", status)
	parsedUri, err := streamclient.ParseClusterUri(c.SrcURL.String())
	require.NoError(t, err)
	require.Equal(t, parsedUri.Redacted(), sourceUri)
	require.Less(t, maxReplTime, timeutil.Now())
	require.Less(t, protectedTime, timeutil.Now())
	require.GreaterOrEqual(t, maxReplTime, targetReplicatedTime.GoTime())
	require.GreaterOrEqual(t, protectedTime, replicationDetails.ReplicationStartTime.GoTime())
	require.Nil(t, cutoverTime)
	repLagDuration, err := duration.ParseInterval(duration.IntervalStyle_POSTGRES, replicationLag, types.DefaultIntervalTypeMetadata)
	require.NoError(t, err)
	require.Greater(t, repLagDuration.Nanos(), int64(0))
	require.Less(t, repLagDuration.Nanos(), time.Second*30)

	// Verify the SHOW command prints the right cutover timestamp. Adding some
	// logical component to make sure we handle it correctly.
	futureTime := c.DestSysServer.Clock().Now().Add(24*time.Hour.Nanoseconds(), 7)
	var cutoverStr string
	c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		c.Args.DestTenantName, futureTime.AsOfSystemTime()).Scan(&cutoverStr)
	var showCutover string
	c.DestSysSQL.QueryRow(c.T, fmt.Sprintf("SELECT failover_time FROM [SHOW TENANT %s WITH REPLICATION STATUS]",
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

	// Inject an error to fail the resumer.
	mu.Lock()
	knobLoadErr = errors.Newf("test error")
	mu.Unlock()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	// Wait for the resumer to see the error and clear it, after this it should
	// succeed resuming.
	<-knobDoneCh
	require.NoError(t, knobLoadErr)

	// Write a bit more to be verified at the end.
	c.SrcTenantSQL.Exec(t, "INSERT INTO d.t2 VALUES (3);")

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
	skip.UnderRace(t, "takes too long under race")
	skip.UnderDeadlock(t, "takes too long under deadlock")

	testutils.RunTrueAndFalse(t, "fromSys", func(t *testing.T, sys bool) {
		ctx := context.Background()
		regions := []string{"mars", "venus", "mercury"}
		args := replicationtestutils.DefaultTenantStreamingClustersArgs
		args.MultitenantSingleClusterNumNodes = 3
		args.MultiTenantSingleClusterTestRegions = regions
		if sys {
			args.SrcTenantID = roachpb.SystemTenantID
			args.SrcTenantName = "system"
		}
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
								return errors.Newf("found table data located on another node %d, desc %v",
									replica.NodeID, desc)
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

		testutils.SucceedsWithin(t,
			checkLocalities(tableDesc.PrimaryIndexSpan(srcCodec), rangedesc.NewScanner(c.SrcSysServer.DB())),
			time.Second*45*5)

		testutils.SucceedsWithin(t,
			checkLocalities(tableDesc.PrimaryIndexSpan(destCodec), rangedesc.NewScanner(c.DestSysServer.DB())),
			time.Second*45*5)

		tableName := "test"
		tabledIDQuery := fmt.Sprintf(`SELECT id FROM system.namespace WHERE name ='%s'`, tableName)

		var tableID uint32
		c.SrcTenantSQL.QueryRow(t, tabledIDQuery).Scan(&tableID)
		fmt.Printf("%d", tableID)

		checkLocalityRanges(t, c.SrcSysSQL, srcCodec, uint32(tableDesc.GetID()), "mars")
	})
}

func TestStreamingMismatchedMRDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "multi node c2c is very flaky")

	ctx := context.Background()
	regions := []string{"mars", "venus", "mercury"}
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.SrcClusterTestRegions = regions
	args.SrcNumNodes = 3

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.SrcTenantSQL.Exec(t, "CREATE DATABASE prim PRIMARY REGION mars")
	c.SrcTenantSQL.Exec(t, "CREATE TABLE prim.x (id INT PRIMARY KEY, n INT)")
	c.SrcTenantSQL.Exec(t, "INSERT INTO prim.x VALUES (1, 1)")

	c.SrcTenantSQL.Exec(t, "CREATE DATABASE many PRIMARY REGION mars REGIONS = mars,mercury,venus")
	c.SrcTenantSQL.Exec(t, "CREATE TABLE many.x (id INT PRIMARY KEY, n INT)")
	c.SrcTenantSQL.Exec(t, "INSERT INTO many.x VALUES (1, 1)")

	c.WaitUntilStartTimeReached(jobspb.JobID(ingestionJobID))
	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.Cutover(ctx, producerJobID, ingestionJobID, srcTime.GoTime(), false)

	defer c.StartDestTenant(ctx, nil, 0)()

	// Check how MR primitives have replicated to non-mr stand by cluster
	t.Run("mr db only with primary region", func(t *testing.T) {
		var res string
		c.DestTenantSQL.QueryRow(c.T, `SELECT create_statement FROM [SHOW CREATE DATABASE prim]`).Scan(&res)
		require.Equal(t, "CREATE DATABASE prim PRIMARY REGION mars REGIONS = mars SURVIVE ZONE FAILURE", res)

		var region string
		c.DestTenantSQL.QueryRow(c.T, "SELECT region FROM [SHOW REGIONS FROM DATABASE prim];").Scan(&region)
		require.Equal(t, "mars", region)

		c.DestTenantSQL.Exec(t, "INSERT INTO prim.x VALUES (2, 2)")

		c.DestTenantSQL.Exec(c.T, `ALTER DATABASE prim DROP REGION "mars"`)
	})
	t.Run("mr db with several regions", func(t *testing.T) {
		var res string
		c.DestTenantSQL.QueryRow(c.T, `SELECT create_statement FROM [SHOW CREATE DATABASE many]`).Scan(&res)
		require.Equal(t, "CREATE DATABASE many PRIMARY REGION mars REGIONS = mars, mercury, venus SURVIVE ZONE FAILURE", res)

		c.DestTenantSQL.Exec(t, "INSERT INTO many.x VALUES (2, 2)")

		// As a sanity check, drop a region on the source and destination cluster.
		c.SrcTenantSQL.ExecSucceedsSoon(c.T, `ALTER DATABASE many DROP REGION "venus"`)
		c.DestTenantSQL.ExecSucceedsSoon(c.T, `ALTER DATABASE many DROP REGION "venus"`)
	})
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

// TestStreamingZoneConfigsMismatchedRegions tests that c2c cutover proceeds
// smoothly even if the user replicated an unsatisfiable zone config.
func TestStreamingZoneConfigsMismatchedRegions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "multi node c2c is very flaky")

	ctx := context.Background()
	regions := []string{"mars", "venus", "mercury"}
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.SrcClusterTestRegions = regions
	args.SrcNumNodes = 3

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.SrcTenantSQL.Exec(t, "CREATE DATABASE test")
	c.SrcTenantSQL.Exec(t, `ALTER DATABASE test CONFIGURE ZONE USING constraints = '[+region=mars]', num_replicas = 1;`)
	c.SrcTenantSQL.Exec(t, "CREATE TABLE test.x (id INT PRIMARY KEY, n INT)")
	c.SrcTenantSQL.Exec(t, "INSERT INTO test.x VALUES (1, 1)")

	c.WaitUntilStartTimeReached(jobspb.JobID(ingestionJobID))
	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.Cutover(ctx, producerJobID, ingestionJobID, srcTime.GoTime(), false)

	defer c.StartDestTenant(ctx, nil, 0)()

	// Note that the unsatisfiable zone config does not appear in the create statement.
	var res string
	c.DestTenantSQL.QueryRow(c.T, `SELECT create_statement FROM [SHOW CREATE DATABASE test]`).Scan(&res)
	require.Equal(t, "CREATE DATABASE test", res)

	var zcfg string
	c.DestTenantSQL.QueryRow(c.T, `SELECT raw_config_sql FROM [SHOW ZONE CONFIGURATION FROM DATABASE test]`).Scan(&zcfg)
	// Note that the Zone configuration contains an unsatisfiable region constraint.
	require.Contains(t, zcfg, `region=mars`)

	// Ensure the db's table is available
	c.DestTenantSQL.Exec(t, "INSERT INTO test.x VALUES (2, 2)")
	var rowCount int
	c.DestTenantSQL.QueryRow(t, "SELECT count(*) FROM test.x").Scan(&rowCount)
	require.Equal(t, 2, rowCount)

	// Ensure we can remove the unsatsfiable region constraint
	c.DestTenantSQL.Exec(t, `ALTER DATABASE test CONFIGURE ZONE DISCARD`)

	var newZcfg string
	c.DestTenantSQL.QueryRow(c.T, `SELECT raw_config_sql FROM [SHOW ZONE CONFIGURATION FROM DATABASE test]`).Scan(&zcfg)
	require.NotContains(t, newZcfg, `region=mars`)
}

func TestReplicationJobWithReaderTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	stats := replicationtestutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)
	require.NotNil(t, stats.IngestionDetails.ReadTenantID)

	var (
		name   string
		status string
	)
	expectedReaderTenantName := fmt.Sprintf("%s-readonly", args.DestTenantName)

	c.DestSysSQL.QueryRow(t, fmt.Sprintf(`
SELECT 
	name,
	status
FROM [SHOW VIRTUAL CLUSTER '%s' WITH REPLICATION STATUS]
`, expectedReaderTenantName)).Scan(&name, &status)

	require.Equal(t, expectedReaderTenantName, name)
	require.Equal(t, "ready", status)
}
