// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamingest

import (
	"context"
	gosql "database/sql"
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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{DefaultTestTenant: base.TestControlsTenantsExplicitly})
	defer srv.Stopper().Stop(context.Background())

	sysSQL := sqlutils.MakeSQLRunner(db)
	sysSQL.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	sysSQL.Exec(t, `SET CLUSTER SETTING physical_replication.enabled = true`)

	srcPgURL, cleanupSink := sqlutils.PGUrl(t, srv.SystemLayer().AdvSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanupSink()

	t.Run("source cannot be system tenant", func(t *testing.T) {
		sysSQL.ExpectErr(t, `pq: neither the source tenant "system" nor the destination tenant "dest" \(0\) can be the system tenant`,
			"CREATE TENANT dest FROM REPLICATION OF system ON $1", srcPgURL.String())
	})
	t.Run("destination cannot be system tenant", func(t *testing.T) {
		sysSQL.ExpectErr(t, `pq: neither the source tenant "source" nor the destination tenant "system" \(0\) can be the system tenant`,
			"CREATE TENANT system FROM REPLICATION OF source ON $1", srcPgURL.String())
	})
	t.Run("destination cannot exist without resume timestamp", func(t *testing.T) {
		sysSQL.Exec(t, "CREATE TENANT foo")
		sysSQL.ExpectErr(t, "pq: tenant with name \"foo\" already exists",
			"CREATE TENANT foo FROM REPLICATION OF source ON $1", srcPgURL.String())
	})
	t.Run("destination tenant cannot be online", func(t *testing.T) {
		sysSQL.Exec(t, "CREATE TENANT bar")
		sysSQL.Exec(t, "ALTER VIRTUAL CLUSTER bar START SERVICE SHARED")
		sysSQL.ExpectErr(t, "service mode must be none",
			"CREATE TENANT bar FROM REPLICATION OF source ON $1 WITH RESUME TIMESTAMP = now()", srcPgURL.String())
	})
	t.Run("destination tenant must have known revert timestamp", func(t *testing.T) {
		sysSQL.Exec(t, "CREATE TENANT baz")
		sysSQL.ExpectErr(t, "no last revert timestamp found",
			"CREATE TENANT baz FROM REPLICATION OF source ON $1 WITH RESUME TIMESTAMP = now()", srcPgURL.String())
	})
	t.Run("destination tenant revert timestamp must match resume timestamp", func(t *testing.T) {
		sysSQL.Exec(t, "CREATE TENANT bat")
		sysSQL.Exec(t, "SELECT crdb_internal.unsafe_revert_tenant_to_timestamp('bat', cluster_logical_timestamp())")
		sysSQL.ExpectErr(t, "doesn't match last revert timestamp",
			"CREATE TENANT bat FROM REPLICATION OF source ON $1 WITH RESUME TIMESTAMP = cluster_logical_timestamp()", srcPgURL.String())
	})
	t.Run("external connection must be reachable", func(t *testing.T) {
		badPgURL := srcPgURL
		badPgURL.Host = "nonexistent_test_endpoint"
		sysSQL.ExpectErr(t, "pq: failed to construct External Connection details: failed to connect",
			fmt.Sprintf(`CREATE EXTERNAL CONNECTION "replication-source-addr" AS "%s"`,
				badPgURL.String()))
	})
}

func TestTenantStreamingFailback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	skip.WithIssue(t, 121221, "failback unsuppported on 23.2")

	serverA, aDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer serverA.Stopper().Stop(ctx)
	serverB, bDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer serverB.Stopper().Stop(ctx)

	newTenantConn := func(t *testing.T, srv serverutils.ApplicationLayerInterface, tenantName string) *gosql.DB {
		var conn *gosql.DB
		testutils.SucceedsSoon(t, func() error {
			db, err := srv.SQLConnE(serverutils.DBName(fmt.Sprintf("cluster:%s", tenantName)))
			if err != nil {
				return err
			}
			if err := db.Ping(); err != nil {
				return err
			}
			conn = db
			return nil
		})
		return conn
	}

	sqlA := sqlutils.MakeSQLRunner(aDB)
	sqlB := sqlutils.MakeSQLRunner(bDB)

	serverAURL, cleanupURLA := sqlutils.PGUrl(t, serverA.SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanupURLA()
	serverBURL, cleanupURLB := sqlutils.PGUrl(t, serverB.SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanupURLB()

	for _, s := range []string{
		"SET CLUSTER SETTING physical_replication.enabled = true",
		"SET CLUSTER SETTING kv.rangefeed.enabled = true",
		"SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '200ms'",
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'",
		"SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '50ms'",

		"SET CLUSTER SETTING physical_replication.consumer.heartbeat_frequency = '1s'",
		"SET CLUSTER SETTING physical_replication.consumer.job_checkpoint_frequency = '100ms'",
		"SET CLUSTER SETTING physical_replication.consumer.minimum_flush_interval = '10ms'",
		"SET CLUSTER SETTING physical_replication.consumer.cutover_signal_poll_interval = '100ms'",
		"SET CLUSTER SETTING spanconfig.reconciliation_job.checkpoint_interval = '100ms'",
	} {
		sqlA.Exec(t, s)
		sqlB.Exec(t, s)
	}
	compareAtTimetamp := func(ts string) {
		fingerprintQueryFmt := "SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TENANT %s] AS OF SYSTEM TIME %s"
		var fingerprintF int64
		sqlA.QueryRow(t, fmt.Sprintf(fingerprintQueryFmt, "f", ts)).Scan(&fingerprintF)
		var fingerprintG int64
		sqlB.QueryRow(t, fmt.Sprintf(fingerprintQueryFmt, "g", ts)).Scan(&fingerprintG)
		require.Equal(t, fingerprintF, fingerprintG, "fingerprint mismatch at %s", ts)

	}

	// The overall test plan looks like:
	//
	// SETUP
	//   Create tenant f on severA
	//   Start service for tenant f
	//   Write to tenant f
	//   Replicate tenant f on serverA to tenant g on serverB
	//
	// FAILOVER
	//   Complete replication on tenant g as of ts1
	//   Fingerprint f and g as of ts1
	//
	// SPLIT BRAIN
	//   Start service for tenant g
	//   Write to f and g
	//   Get ts2
	//
	// RESET AND RESYNC
	//   Stop service for tenant f
	//   Replicate tenant g on serverB to tenant f on serverA as of ts1
	//   Fingerprint f and g as of ts1
	//   Fingerprint f and g as of ts2
	//
	// FAIL BACK
	//   Get ts3
	//   Complete replication on tenant f as of ts3
	//   Replicate tenant f on serverA to tenant g on serverB
	//   Fingerprint f and g as of ts1
	//   Fingerprint f and g as of ts2
	//   Fingerprint f and g as of ts3
	//   Confirm rows written to f during split brain have been reverted; rows written to g remain

	// SETUP
	t.Logf("creating tenant f")
	sqlA.Exec(t, "CREATE VIRTUAL CLUSTER f")
	sqlA.Exec(t, "ALTER VIRTUAL CLUSTER f START SERVICE SHARED")

	tenFDB := newTenantConn(t, serverA.SystemLayer(), "f")
	defer tenFDB.Close()
	sqlTenF := sqlutils.MakeSQLRunner(tenFDB)

	sqlTenF.Exec(t, "CREATE DATABASE test")
	sqlTenF.Exec(t, "CREATE TABLE test.t (k PRIMARY KEY) AS SELECT generate_series(1, 100)")

	t.Logf("starting replication f->g")
	sqlB.Exec(t, "CREATE VIRTUAL CLUSTER g FROM REPLICATION OF f ON $1", serverAURL.String())

	// FAILOVER
	_, consumerGJobID := replicationtestutils.GetStreamJobIds(t, ctx, sqlB, roachpb.TenantName("g"))
	var ts1 string
	sqlA.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&ts1)
	t.Logf("waiting for g@%s", ts1)
	replicationtestutils.WaitUntilReplicatedTime(t,
		replicationtestutils.DecimalTimeToHLC(t, ts1),
		sqlB,
		jobspb.JobID(consumerGJobID))

	t.Logf("completing replication on g@%s", ts1)
	sqlB.Exec(t, fmt.Sprintf("ALTER VIRTUAL CLUSTER g COMPLETE REPLICATION TO SYSTEM TIME '%s'", ts1))

	jobutils.WaitForJobToSucceed(t, sqlB, jobspb.JobID(consumerGJobID))
	compareAtTimetamp(ts1)

	// SPLIT BRAIN
	// g is now the "primary"
	// f is still running unfortunately
	sqlB.Exec(t, "ALTER VIRTUAL CLUSTER g START SERVICE SHARED")
	tenGDB := newTenantConn(t, serverB.SystemLayer(), "g")
	defer tenGDB.Close()
	sqlTenG := sqlutils.MakeSQLRunner(tenGDB)

	sqlTenF.Exec(t, "INSERT INTO test.t VALUES (777)") // This value should be abandoned
	sqlTenG.Exec(t, "INSERT INTO test.t VALUES (555)") // This value should be synced later
	var ts2 string
	sqlA.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&ts2)

	// RESET AND RESYNC
	//   Stop service for tenant f
	//   Replicate tenant g on serverB to tenant f on serverA as of ts1
	//   Fingerprint f and g as of ts1
	//   Fingerprint f and g as of ts2
	sqlA.Exec(t, "ALTER VIRTUAL CLUSTER f STOP SERVICE")
	waitUntilTenantServerStopped(t, serverA.SystemLayer(), "f")
	t.Logf("starting replication g->f")
	sqlA.Exec(t, fmt.Sprintf("SELECT crdb_internal.unsafe_revert_tenant_to_timestamp('f', %s)", ts1))
	sqlA.Exec(t, fmt.Sprintf("CREATE VIRTUAL CLUSTER f FROM REPLICATION OF g ON $1 WITH RESUME TIMESTAMP = '%s'", ts1), serverBURL.String())
	_, consumerFJobID := replicationtestutils.GetStreamJobIds(t, ctx, sqlA, roachpb.TenantName("f"))
	t.Logf("waiting for f@%s", ts2)
	replicationtestutils.WaitUntilReplicatedTime(t,
		replicationtestutils.DecimalTimeToHLC(t, ts2),
		sqlA,
		jobspb.JobID(consumerFJobID))

	compareAtTimetamp(ts1)
	compareAtTimetamp(ts2)

	// FAIL BACK
	//   Get ts3
	//   Complete replication on tenant f as of ts3
	//   Replicate tenant f on serverA to tenant g on serverB
	//   Fingerprint f and g as of ts1
	//   Fingerprint f and g as of ts2
	//   Fingerprint f and g as of ts3
	//   Confirm rows written to f during split brain have been reverted; rows written to g remain
	var ts3 string
	sqlA.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&ts3)
	t.Logf("completing replication on f@%s", ts3)
	sqlA.Exec(t, fmt.Sprintf("ALTER VIRTUAL CLUSTER f COMPLETE REPLICATION TO SYSTEM TIME '%s'", ts3))
	jobutils.WaitForJobToSucceed(t, sqlA, jobspb.JobID(consumerFJobID))
	sqlA.Exec(t, "ALTER VIRTUAL CLUSTER f START SERVICE SHARED")

	sqlB.Exec(t, "ALTER VIRTUAL CLUSTER g STOP SERVICE")
	waitUntilTenantServerStopped(t, serverB.SystemLayer(), "g")
	t.Logf("starting replication f->g")
	sqlB.Exec(t, fmt.Sprintf("SELECT crdb_internal.unsafe_revert_tenant_to_timestamp('g', %s)", ts3))
	sqlB.Exec(t, fmt.Sprintf("CREATE VIRTUAL CLUSTER g FROM REPLICATION OF f ON $1 WITH RESUME TIMESTAMP = '%s'", ts3), serverAURL.String())
	_, consumerGJobID = replicationtestutils.GetStreamJobIds(t, ctx, sqlB, roachpb.TenantName("g"))
	t.Logf("waiting for g@%s", ts3)
	replicationtestutils.WaitUntilReplicatedTime(t,
		replicationtestutils.DecimalTimeToHLC(t, ts3),
		sqlB,
		jobspb.JobID(consumerGJobID))

	// As of now, we are back in our original position, but with
	// an extra write from when we were failed over.
	compareAtTimetamp(ts1)
	compareAtTimetamp(ts2)
	compareAtTimetamp(ts3)

	tenF2DB := newTenantConn(t, serverA.SystemLayer(), "f")
	defer tenF2DB.Close()
	sqlTenF = sqlutils.MakeSQLRunner(tenF2DB)
	sqlTenF.CheckQueryResults(t, "SELECT max(k) FROM test.t", [][]string{{"555"}})
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

	_, revert, err := maybeRevertToCutoverTimestamp(ctx, jobExecCtx, replicationJob)
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

// ALTER VIRTUAL CLUSTER STOP SERVICE does not block until the service
// is stopped. But, we need to wait until the SQLServer is stopped to
// ensure that nothing is writing to the relevant keyspace.
func waitUntilTenantServerStopped(
	t *testing.T, srv serverutils.ApplicationLayerInterface, tenantName string,
) {
	t.Helper()
	// TODO(ssd): We may want to do something like this,
	// but this query is driven first from the
	// system.tenants table and doesn't strictly represent
	// the in-memory state of the tenant controller.
	//
	// client := srv.GetAdminClient(t)
	// testutils.SucceedsSoon(t, func() error {
	// 	resp, err := client.ListTenants(ctx, &serverpb.ListTenantsRequest{})
	// 	if err != nil {
	// 		return err
	// 	}
	// 	for _, tenant := range resp.Tenants {
	// 		if tenant.TenantName == tenantName {
	// 			t.Logf("tenant %q is still running", tenantName)
	// 			return errors.Newf("tenant %q still running")
	// 		}
	// 	}
	// 	t.Logf("tenant %q is not running", tenantName)
	// 	return nil
	// })
	testutils.SucceedsSoon(t, func() error {
		db, err := srv.SQLConnE(serverutils.DBName(fmt.Sprintf("cluster:%s", tenantName)))
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()
		if err := db.Ping(); err == nil {
			t.Logf("tenant %q is still accepting connections", tenantName)
			return errors.Newf("tenant %q still accepting connections")
		}
		t.Logf("tenant %q is not accepting connections", tenantName)
		return nil
	})
}
