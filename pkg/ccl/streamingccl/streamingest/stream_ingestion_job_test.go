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
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamproducer"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestTenantStreamingCreationErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srcServer, srcDB, _ := serverutils.StartServer(t, base.TestServerArgs{DefaultTestTenant: base.TODOTestTenantDisabled})
	defer srcServer.Stopper().Stop(ctx)
	destServer, destDB, _ := serverutils.StartServer(t, base.TestServerArgs{DefaultTestTenant: base.TODOTestTenantDisabled})
	defer destServer.Stopper().Stop(ctx)

	srcTenantID := serverutils.TestTenantID()
	srcTenantName := roachpb.TenantName("source")
	_, srcTenantConn := serverutils.StartTenant(t, srcServer, base.TestTenantArgs{
		TenantID:   srcTenantID,
		TenantName: srcTenantName,
	})
	defer func() { require.NoError(t, srcTenantConn.Close()) }()

	// Set required cluster settings.
	SrcSysSQL := sqlutils.MakeSQLRunner(srcDB)
	SrcSysSQL.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	DestSysSQL := sqlutils.MakeSQLRunner(destDB)
	DestSysSQL.Exec(t, `SET CLUSTER SETTING cross_cluster_replication.enabled = true;`)

	// Sink to read data from.
	srcPgURL, cleanupSink := sqlutils.PGUrl(t, srcServer.AdvSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanupSink()

	DestSysSQL.ExpectErr(t, "pq: neither the source tenant \"source\" nor the destination tenant \"system\" \\(0\\) can be the system tenant",
		`CREATE TENANT system FROM REPLICATION OF source ON $1`, srcPgURL.String())

	DestSysSQL.Exec(t, "CREATE TENANT \"100\"")
	DestSysSQL.ExpectErr(t, "pq: tenant with name \"100\" already exists",
		`CREATE TENANT "100" FROM REPLICATION OF source ON $1`, srcPgURL.String())

	badPgURL := srcPgURL
	badPgURL.Host = "nonexistent_test_endpoint"
	DestSysSQL.ExpectErr(t, "pq: failed to construct External Connection details: failed to connect",
		fmt.Sprintf(`CREATE EXTERNAL CONNECTION "replication-source-addr" AS "%s"`,
			badPgURL.String()))
}

func TestCutoverBuiltin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Disable the test tenant as the test below looks for a
			// streaming job assuming that it's within the system tenant.
			// Tracked with #76378.
			DefaultTestTenant: base.TODOTestTenantDisabled,
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
	err := tc.Server(0).InternalDB().(isql.DB).Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) (err error) {
		return registry.CreateStartableJobWithTxn(ctx, &job, id, txn, streamIngestJobRecord)
	})
	require.NoError(t, err)

	// Check that sentinel is not set.
	progress := job.Progress()
	sp, ok := progress.GetDetails().(*jobspb.Progress_StreamIngest)
	require.True(t, ok)
	require.True(t, sp.StreamIngest.CutoverTime.IsEmpty())

	var replicatedTime time.Time
	err = job.NoTxn().Update(ctx, func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}
		replicatedTime = timeutil.Now().Round(time.Microsecond)
		hlcReplicatedTime := hlc.Timestamp{WallTime: replicatedTime.UnixNano()}

		progress := md.Progress
		streamProgress := progress.Details.(*jobspb.Progress_StreamIngest).StreamIngest
		streamProgress.ReplicatedTime = hlcReplicatedTime
		progress.Progress = &jobspb.Progress_HighWater{
			HighWater: &hlcReplicatedTime,
		}

		ju.UpdateProgress(progress)
		return nil
	})
	require.NoError(t, err)

	// Ensure that the builtin runs locally.
	var explain string
	err = db.QueryRowContext(ctx,
		`EXPLAIN SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`, job.ID(),
		replicatedTime).Scan(&explain)
	require.NoError(t, err)
	require.Equal(t, "distribution: local", explain)

	var jobID int64
	err = db.QueryRowContext(
		ctx,
		`SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`,
		job.ID(), replicatedTime).Scan(&jobID)
	require.NoError(t, err)
	require.Equal(t, job.ID(), jobspb.JobID(jobID))

	// Check that sentinel is set on the job progress.
	sj, err := registry.LoadJob(ctx, job.ID())
	require.NoError(t, err)
	progress = sj.Progress()
	sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest)
	require.True(t, ok)
	require.Equal(t, hlc.Timestamp{WallTime: replicatedTime.UnixNano()}, sp.StreamIngest.CutoverTime)
}

// TestReplicationJobResumptionStartTime tests that a replication job picks the
// correct timestamps to resume from across multiple resumptions.
func TestReplicationJobResumptionStartTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	planned := make(chan struct{})
	canContinue := make(chan struct{})
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	replicationSpecs := make(map[base.SQLInstanceID]*execinfrapb.StreamIngestionDataSpec, 0)
	frontier := &execinfrapb.StreamIngestionFrontierSpec{}
	args.TestingKnobs = &sql.StreamingTestingKnobs{
		AfterReplicationFlowPlan: func(ingestionSpecs map[base.SQLInstanceID]*execinfrapb.StreamIngestionDataSpec,
			frontierSpec *execinfrapb.StreamIngestionFrontierSpec) {
			replicationSpecs = ingestionSpecs
			frontier = frontierSpec
			planned <- struct{}{}
			<-canContinue
		},
	}
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()
	defer close(planned)
	defer close(canContinue)

	producerJobID, replicationJobID := c.StartStreamReplication(ctx)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(replicationJobID))

	// Wait for the distsql plan to be created.
	<-planned
	registry := c.DestSysServer.ExecutorConfig().(sql.ExecutorConfig).JobRegistry
	var replicationJobDetails jobspb.StreamIngestionDetails
	require.NoError(t, c.DestSysServer.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
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
		require.Empty(t, r.PreviousReplicatedTimestamp)
	}
	require.Empty(t, frontier.ReplicatedTimeAtStart)

	// Allow the job to make some progress.
	canContinue <- struct{}{}
	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(replicationJobID))

	// Pause the job.
	c.DestSysSQL.Exec(t, `PAUSE JOB $1`, replicationJobID)
	jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(replicationJobID))

	// Unpause the job and ensure the resumption takes place at a later timestamp
	// than the initial scan timestamp.
	c.DestSysSQL.Exec(t, `RESUME JOB $1`, replicationJobID)
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(replicationJobID))

	<-planned
	stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, replicationJobID)

	// Assert that the start time hasn't changed.
	require.Equal(t, startTime, stats.IngestionDetails.ReplicationStartTime)

	// Assert that the previous highwater mark is greater than the replication
	// start time.
	var previousReplicatedTimestamp hlc.Timestamp
	for _, r := range replicationSpecs {
		require.Equal(t, startTime, r.InitialScanTimestamp)
		require.True(t, r.InitialScanTimestamp.Less(r.PreviousReplicatedTimestamp))
		if previousReplicatedTimestamp.IsEmpty() {
			previousReplicatedTimestamp = r.PreviousReplicatedTimestamp
		} else {
			require.Equal(t, r.PreviousReplicatedTimestamp, previousReplicatedTimestamp)
		}
	}
	require.Equal(t, frontier.ReplicatedTimeAtStart, previousReplicatedTimestamp)
	canContinue <- struct{}{}
	srcTime = c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(replicationJobID))
	c.Cutover(producerJobID, replicationJobID, srcTime.GoTime(), false)
	jobutils.WaitForJobToSucceed(t, c.DestSysSQL, jobspb.JobID(replicationJobID))
}

func makeTableSpan(codec keys.SQLCodec, tableID uint32) roachpb.Span {
	k := codec.TablePrefix(tableID)
	return roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
}

func TestCutoverFractionProgressed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	progressUpdated := make(chan struct{})
	progressRead := make(chan struct{})
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Streaming: &sql.StreamingTestingKnobs{
				OverrideRevertRangeBatchSize: 1,
				CutoverProgressShouldUpdate:  func() bool { return true },
				OnCutoverProgressUpdate: func(_ roachpb.Spans) {
					progressUpdated <- struct{}{}

					// Only begin next progress update once the test has read the latest progress update.
					<-progressRead
				},
			},
		},
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec(`CREATE TABLE foo(id) AS SELECT generate_series(1, 10)`)
	require.NoError(t, err)

	cutover := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	// Insert some revisions which we can revert to a timestamp before the update.
	_, err = sqlDB.Exec(`UPDATE foo SET id = id + 1`)
	require.NoError(t, err)

	// Split every other row into its own range. Progress updates are on a
	// per-range basis so we need >1 range to see the fraction progress.
	_, err = sqlDB.Exec(`ALTER TABLE foo SPLIT AT (SELECT rowid FROM foo WHERE rowid % 2 = 0)`)
	require.NoError(t, err)

	var nRanges int
	require.NoError(t, sqlDB.QueryRow(
		`SELECT count(*) FROM [SHOW RANGES FROM TABLE foo]`).Scan(&nRanges))

	require.Equal(t, nRanges, 6)
	var id int
	err = sqlDB.QueryRow(`SELECT id FROM system.namespace WHERE name = 'foo'`).Scan(&id)
	require.NoError(t, err)

	// Create a mock replication job with the `foo` table span so that on cut over
	// we can revert the table's ranges.
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	jobExecCtx, ctxClose := sql.MakeJobExecContext(ctx, "test-cutover-fraction-progressed", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg)
	defer ctxClose()

	mockReplicationJobDetails := jobspb.StreamIngestionDetails{
		Span: makeTableSpan(execCfg.Codec, uint32(id)),
	}
	mockReplicationJobRecord := jobs.Record{
		Details: mockReplicationJobDetails,
		Progress: jobspb.StreamIngestionProgress{
			CutoverTime:           cutover,
			RemainingCutoverSpans: roachpb.Spans{mockReplicationJobDetails.Span},
		},
		Username: username.TestUserName(),
	}
	registry := execCfg.JobRegistry
	jobID := registry.MakeJobID()
	replicationJob, err := registry.CreateJobWithTxn(ctx, mockReplicationJobRecord, jobID, nil)
	require.NoError(t, err)
	require.NoError(t, replicationJob.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}
		progress := md.Progress
		streamProgress := progress.Details.(*jobspb.Progress_StreamIngest).StreamIngest
		streamProgress.ReplicatedTime = cutover
		progress.Progress = &jobspb.Progress_HighWater{
			HighWater: &cutover,
		}
		ju.UpdateProgress(progress)
		return nil
	}))

	metrics := registry.MetricsStruct().StreamIngest.(*Metrics)
	require.Equal(t, int64(0), metrics.ReplicationCutoverProgress.Value())

	loadProgress := func() jobspb.Progress {
		j, err := execCfg.JobRegistry.LoadJob(ctx, jobID)
		require.NoError(t, err)
		return j.Progress()
	}

	var lastRangesLeft int64 = 6
	var lastFraction float32 = 0
	var progressUpdates = 0
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		for range progressUpdated {
			sip := loadProgress()
			curProgress := sip.GetFractionCompleted()
			progressRead <- struct{}{}
			progressUpdates++
			if lastFraction >= curProgress {
				return errors.Newf("unexpected progress fraction: %f (previous) >= %f (current)",
					lastFraction,
					curProgress)
			}
			rangesLeft := metrics.ReplicationCutoverProgress.Value()
			if lastRangesLeft < rangesLeft {
				return errors.Newf("unexpected range count from metric: %d (current) > %d (previous)",
					rangesLeft, lastRangesLeft)
			}
			lastRangesLeft = rangesLeft
			lastFraction = curProgress
		}
		return nil
	})

	revert, err := maybeRevertToCutoverTimestamp(ctx, jobExecCtx, replicationJob)
	require.NoError(t, err)
	require.True(t, revert)

	close(progressUpdated)
	require.NoError(t, g.Wait())

	sip := loadProgress()
	require.Equal(t, float32(1), sip.GetFractionCompleted())
	require.Equal(t, int64(0), metrics.ReplicationCutoverProgress.Value())
	require.True(t, progressUpdates > 1)
}

// TestCutoverCheckpointing asserts that cutover progress persists to the job
// record and ensures the cutover job does not duplicate persisted work after
// the job is paused after a few updates.
func TestCutoverCheckpointing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	progressUpdated := make(chan struct{})
	pauseRequested := make(chan struct{})
	var updateCount int
	remainingSpanUpdates := make(map[string]struct{})
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.TestingKnobs = &sql.StreamingTestingKnobs{
		CutoverProgressShouldUpdate:  func() bool { return true },
		OverrideRevertRangeBatchSize: 1,
		OnCutoverProgressUpdate: func(remainingSpansUpdate roachpb.Spans) {

			// If checkpointing works properly, we expect no repeating remaining span updates.
			_, ok := remainingSpanUpdates[remainingSpansUpdate.String()]
			require.False(t, ok, fmt.Sprintf("repeated remaining span update %s", remainingSpansUpdate.String()))
			remainingSpanUpdates[remainingSpansUpdate.String()] = struct{}{}

			updateCount++
			if updateCount == 3 {
				close(progressUpdated)
				// Wait until the job is in a pause-requested state, which causes subsequent
				// updates to the job record (like cutover progress updates) to
				// error with a pause-requested tag, causing the whole job to pause.
				<-pauseRequested
			}
		},
	}

	ctx := context.Background()
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobIDInt, replicationJobIDInt := c.StartStreamReplication(ctx)
	replicationJobID := jobspb.JobID(replicationJobIDInt)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobIDInt))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, replicationJobID)
	c.WaitUntilStartTimeReached(replicationJobID)

	c.SrcTenantSQL.Exec(t, `CREATE TABLE foo(id) AS SELECT generate_series(1, 10)`)

	cutoverTime := c.SrcCluster.Server(0).Clock().Now()

	// Insert some revisions which we can revert to a timestamp before the update.
	c.SrcTenantSQL.Exec(t, `UPDATE foo SET id = id + 1`)

	c.WaitUntilReplicatedTime(c.SrcCluster.Server(0).Clock().Now(), replicationJobID)

	getCutoverRemainingSpans := func() roachpb.Spans {
		progress := jobutils.GetJobProgress(t, c.DestSysSQL, replicationJobID).GetStreamIngest()
		return progress.RemainingCutoverSpans
	}

	// Ensure there are no remaining cutover spans before cutover begins.
	require.Equal(t, len(getCutoverRemainingSpans()), 0)

	c.Cutover(producerJobIDInt, replicationJobIDInt, cutoverTime.GoTime(), true)
	<-progressUpdated

	c.DestSysSQL.Exec(t, `PAUSE JOB $1`, &replicationJobID)
	close(pauseRequested)
	jobutils.WaitForJobToPause(t, c.DestSysSQL, replicationJobID)

	details := jobutils.GetJobPayload(t, c.DestSysSQL, replicationJobID).GetStreamIngestion()

	// Assert that some progress has been persisted.
	remainingSpans := getCutoverRemainingSpans()
	require.Greater(t, len(remainingSpans), 0)
	require.NotEqual(t, remainingSpans, roachpb.Spans{details.Span})

	c.DestSysSQL.Exec(t, `RESUME JOB $1`, &replicationJobID)
	jobutils.WaitForJobToSucceed(t, c.DestSysSQL, replicationJobID)

	// Ensure no spans are left to cutover. Stringify during comparison because
	// the empty remainingSpans are encoded as
	// roachpb.Spans{roachpb.Span{Key:/Min, EndKey:/Min}.
	require.Equal(t, getCutoverRemainingSpans().String(), roachpb.Spans{}.String())
}
