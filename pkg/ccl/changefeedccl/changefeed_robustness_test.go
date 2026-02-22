package changefeedccl

import (
	"context"
	gosql "database/sql"
	"encoding/base64"
	gojson "encoding/json"
	"fmt"
	"maps"
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/checkpoint"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/resolvedspan"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/cidr"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randident"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChangefeedIdentifyDependentTablesForProtecting identifies (system) tables
// that are accessed in the course of running a changefeed and ensures that they
// are all in the list of tables we protect with PTS. It does this by running a
// sinkless changefeed and capturing a trace of its execution. For completeness,
// it includes a cdc query in the changefeed, under the assumption that doing so
// will require more tables.
func TestChangefeedIdentifyDependentTablesForProtecting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cfCreated := atomic.Bool{}

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		foo := feed(t, f, `CREATE CHANGEFEED AS SELECT *, event_op() AS op, cdc_prev FROM foo`)
		defer closeFeed(t, foo)
		cfCreated.Store(true)

		assertPayloads(t, foo, []string{
			`foo: [0]->{"a": 0, "b": "initial", "cdc_prev": null, "op": "insert"}`,
		})

		// Do some operations so the changefeed does some scanning.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (2, 'c'), (3, 'd')`)
		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)

		assertPayloads(t, foo, []string{
			`foo: [1]->{"a": 1, "b": "a", "cdc_prev": null, "op": "insert"}`,
			`foo: [2]->{"a": 2, "b": "b", "cdc_prev": null, "op": "insert"}`,
			`foo: [2]->{"a": 2, "b": "c", "cdc_prev": {"a": 2, "b": "b"}, "op": "update"}`,
			`foo: [3]->{"a": 3, "b": "d", "cdc_prev": null, "op": "insert"}`,
			`foo: [1]->{"a": 1, "b": null, "cdc_prev": {"a": 1, "b": "a"}, "op": "delete"}`,
		})
	}

	trimRx := regexp.MustCompile(`executing (.*), \[txn:.*`)
	tableIdRx := regexp.MustCompile(`(/Tenant/[0-9]+)?/(Table|NamespaceTable)/([0-9]+)`)

	tableIDsAccessed := map[catid.DescID]struct{}{}

	// Parse trace logs of the following format into table ids and add them to our map:
	// `executing Scan [/Tenant/10/Table/3/1,/Tenant/10/Table/3/2), Scan [/Tenant/10/NamespaceTable/30/1,/Tenant/10/NamespaceTable/30/2), ...`
	// This seems a bit brittle, but it's the best we can do currently.
	noteExecutingScansLog := func(msg string) {
		trimmed := trimRx.FindStringSubmatch(msg)[1]
		spanStmts := strings.Split(trimmed, ", ")
		for _, spanStmt := range spanStmts {
			spanStmt = strings.Replace(spanStmt, "Scan ", "", 1)
			startEnd := strings.Split(strings.Trim(spanStmt, "[)"), ",")
			require.Len(t, startEnd, 2)

			start := startEnd[0]
			matches := tableIdRx.FindStringSubmatch(start)
			require.NotEmpty(t, matches)

			tableID, err := strconv.ParseInt(matches[3], 10, 64)
			require.NoError(t, err)
			tableIDsAccessed[catid.DescID(tableID)] = struct{}{}
		}
	}

	traceCb := func(trace tracingpb.Recording, stmt string) {
		if !cfCreated.Load() {
			return
		}
		if !strings.HasPrefix(stmt, "CREATE CHANGEFEED") {
			return
		}
		for _, span := range trace {
			for _, log := range span.Logs {
				msg := log.Message.StripMarkers()
				if !strings.Contains(msg, "executing Scan") {
					continue
				}
				noteExecutingScansLog(msg)
			}
		}
	}

	cdcTest(t, testFn, withKnobsFn(func(knobs *base.TestingKnobs) {
		knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
		if knobs.SQLExecutor == nil {
			knobs.SQLExecutor = &sql.ExecutorTestingKnobs{}
		}
		knobs.SQLExecutor.(*sql.ExecutorTestingKnobs).WithStatementTrace = traceCb
	}), feedTestForceSink("sinkless"))

	// NOTE: not all the required tables will necessarily show up in every run due to caching. However we should always see SOME tables.
	require.NotEmpty(t, tableIDsAccessed)

	var unexpectedTableIDs []catid.DescID
	for id := range tableIDsAccessed {
		if !slices.Contains(systemTablesToProtect, id) {
			unexpectedTableIDs = append(unexpectedTableIDs, id)
		}
	}
	require.Empty(t, unexpectedTableIDs)
}

func TestRLSBlocking(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE rls (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO rls VALUES (0, 'initial')`)
		sqlDB.Exec(t, `INSERT INTO rls VALUES (1, 'second')`)

		// Make sure CDC query cannot start if table is RLS enabled.
		sqlDB.Exec(t, `ALTER TABLE rls ENABLE ROW LEVEL SECURITY`)
		expErrSubstr := "CDC queries are not supported on tables with row-level security enabled"
		expectErrCreatingFeed(t, f, `CREATE CHANGEFEED AS SELECT * FROM rls WHERE a != 0`, expErrSubstr)

		// Ensure that CDC query fails after creating if table becomes RLS enabled.
		sqlDB.Exec(t, "ALTER TABLE rls DISABLE ROW LEVEL SECURITY")
		tf := feed(t, f, `CREATE CHANGEFEED AS SELECT * FROM rls WHERE a != 0`)
		defer closeFeed(t, tf)
		assertPayloads(t, tf, []string{
			`rls: [1]->{"a": 1, "b": "second"}`,
		})
		sqlDB.Exec(t, `ALTER TABLE rls ENABLE ROW LEVEL SECURITY`)
		sqlDB.Exec(t, `INSERT INTO rls VALUES (2, 'third')`)
		_, err := readNextMessages(context.Background(), tf, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), expErrSubstr)
	}

	cdcTest(t, testFn, withAllowChangefeedErr("expects terminal error"))
}

func TestChangefeedIdleness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		changefeedbase.IdleTimeout.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 3*time.Second)

		registry := s.Server.JobRegistry().(*jobs.Registry)
		currentlyIdle := registry.MetricsStruct().JobMetrics[jobspb.TypeChangefeed].CurrentlyIdle
		// Use a wait group for cases when the number of idle changefeeds temporarily
		// decreases, to avoid a race condition where the changefeed becomes idle
		// before the idleness is checked.
		var wg sync.WaitGroup
		waitForIdleCount := func(numIdle int64) {
			wg.Add(1)
			testutils.SucceedsSoon(t, func() error {
				if currentlyIdle.Value() != numIdle {
					return fmt.Errorf("expected (%+v) idle changefeeds, found (%+v)", numIdle, currentlyIdle.Value())
				}
				return nil
			})
			wg.Done()
		}
		done := make(chan bool)
		workload := func() {
			for {
				select {
				case <-done:
					return
				default:
					sqlDB.Exec(t, `INSERT INTO foo VALUES (0)`)
					sqlDB.Exec(t, `DELETE FROM foo WHERE a = 0`)
					sqlDB.Exec(t, `INSERT INTO bar VALUES (0)`)
					sqlDB.Exec(t, `DELETE FROM bar WHERE b = 0`)
				}
			}
		}

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE bar (b INT PRIMARY KEY)`)
		cf1 := feed(t, f, "CREATE CHANGEFEED FOR TABLE foo WITH resolved='10ms'") // higher resolved frequency for faster test
		cf2 := feed(t, f, "CREATE CHANGEFEED FOR TABLE bar WITH resolved='10ms'")
		defer closeFeed(t, cf1)

		go workload()
		go waitForIdleCount(0)
		wg.Wait()
		done <- true
		waitForIdleCount(2) // Both should eventually be considered idle

		jobFeed := cf2.(cdctest.EnterpriseTestFeed)
		require.NoError(t, jobFeed.Pause())
		waitForIdleCount(1) // Paused jobs aren't considered idle

		require.NoError(t, jobFeed.Resume())
		waitForIdleCount(2) // Resumed job should eventually become idle

		closeFeed(t, cf2)
		waitForIdleCount(1) // The cancelled changefeed isn't considered idle

		go workload()
		go waitForIdleCount(0)
		wg.Wait()
		done <- true
		waitForIdleCount(1)

	}, feedTestEnterpriseSinks)
}

// Regression test for https://github.com/cockroachdb/cockroach/issues/106358
// Ensure that changefeeds upgraded from the version that did not set job record
// cluster ID continue functioning.
func TestChangefeedCanResumeWhenClusterIDMissing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE foo (id int primary key, a string)`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'a')`)
		foo := feed(t, f, `CREATE CHANGEFEED WITH envelope='wrapped' AS SELECT * FROM foo`)
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{`foo: [0]->{"after": {"a": "a", "id": 0}}`})
		jobFeed := foo.(cdctest.EnterpriseTestFeed)

		// Pause the job and delete the row.
		require.NoError(t, jobFeed.Pause())
		sqlDB.Exec(t, `DELETE FROM foo WHERE id = 0`)

		// clear out creation cluster id.
		jobRegistry := s.Server.JobRegistry().(*jobs.Registry)
		require.NoError(t, func() error {
			job, err := jobRegistry.LoadJob(context.Background(), jobFeed.JobID())
			if err != nil {
				return err
			}
			return job.NoTxn().Update(context.Background(), func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
				md.Payload.CreationClusterID = uuid.Nil
				ju.UpdatePayload(md.Payload)
				return nil
			})
		}())

		// Resume; we expect to see deleted row.
		require.NoError(t, jobFeed.Resume())
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": null}`,
		})

		// The job payload now has clusterID set.
		job, err := jobRegistry.LoadJob(context.Background(), jobFeed.JobID())
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, job.Payload().CreationClusterID)
	}
	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestChangefeedExternalIODisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	t.Run("sinkful changefeeds not allowed with disabled external io", func(t *testing.T) {
		disallowedSinkProtos := []string{
			changefeedbase.SinkSchemeExperimentalSQL,
			changefeedbase.SinkSchemeKafka,
			changefeedbase.SinkSchemeNull, // Doesn't work because all sinkful changefeeds are disallowed
			// Cloud sink schemes
			"experimental-s3",
			"experimental-gs",
			"experimental-nodelocal",
			"experimental-http",
			"experimental-https",
			"experimental-azure",
		}
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			DefaultTestTenant: base.TODOTestTenantDisabled,
			ExternalIODirConfig: base.ExternalIODirConfig{
				DisableOutbound: true,
			},
		})
		defer s.Stopper().Stop(ctx)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.ExecMultiple(t, strings.Split(serverSetupStatements, ";")...)
		sqlDB.Exec(t, "CREATE TABLE target_table (pk INT PRIMARY KEY)")
		for _, proto := range disallowedSinkProtos {
			sqlDB.ExpectErrWithTimeout(t, "Outbound IO is disabled by configuration, cannot create changefeed",
				"CREATE CHANGEFEED FOR target_table INTO $1",
				fmt.Sprintf("%s://does-not-matter", proto),
			)
		}
	})

	withDisabledOutbound := func(args *base.TestServerArgs) { args.ExternalIODirConfig.DisableOutbound = true }
	cdcTestNamed(t, "sinkless changfeeds are allowed with disabled external io", func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, "CREATE TABLE target_table (pk INT PRIMARY KEY)")
		sqlDB.Exec(t, "INSERT INTO target_table VALUES (1)")
		feed := feed(t, f, "CREATE CHANGEFEED FOR target_table")
		defer closeFeed(t, feed)
		assertPayloads(t, feed, []string{
			`target_table: [1]->{"after": {"pk": 1}}`,
		})
	}, feedTestForceSink("sinkless"), withArgsFn(withDisabledOutbound))
}

// TestChangefeedLaggingSpanCheckpointing tests checkpointing when the highwater
// does not advance due to specific spans lagging behind.
func TestChangefeedLaggingSpanCheckpointing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, stopServer := startTestFullServer(t, makeOptions(t, feedTestNoTenants))
	defer stopServer()
	sqlDB := sqlutils.MakeSQLRunner(db)

	knobs := s.TestingKnobs().
		DistSQL.(*execinfra.TestingKnobs).
		Changefeed.(*TestingKnobs)

	// Initialize table with 20 ranges.
	sqlDB.Exec(t, `
  CREATE TABLE foo (key INT PRIMARY KEY);
  INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000);
  ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(1, 1000, 50));
  `)

	fooDesc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "d", "foo")
	tableSpan := fooDesc.PrimaryIndexSpan(s.Codec())

	// Checkpoint progress frequently, allow a large enough checkpoint, and
	// reduce the lag threshold to allow lag checkpointing to trigger
	changefeedbase.SpanCheckpointInterval.Override(
		context.Background(), &s.ClusterSettings().SV, 10*time.Millisecond)
	changefeedbase.SpanCheckpointMaxBytes.Override(
		context.Background(), &s.ClusterSettings().SV, 100<<20 /* 100 MiB */)
	changefeedbase.SpanCheckpointLagThreshold.Override(
		context.Background(), &s.ClusterSettings().SV, 10*time.Millisecond)

	// We'll start the changefeed with the cursor set to the current time (not insert time).
	// NB: The changefeed created in this test doesn't actually send any message events.
	var tsStr string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp() from foo`).Scan(&tsStr)
	cursor := parseTimeToHLC(t, tsStr)
	t.Logf("cursor: %v", cursor)

	// Rangefeed will skip some of the checkpoints to simulate lagging spans.
	var laggingSpans roachpb.SpanGroup
	nonLaggingSpans := make(map[string]int)
	var numSpans int
	knobs.FeedKnobs.ShouldSkipCheckpoint = func(checkpoint *kvpb.RangeFeedCheckpoint) (skip bool) {
		// Skip spans for the whole table.
		if checkpoint.Span.Equal(tableSpan) {
			return true
		}
		// Skip spans that we already picked to be lagging.
		if laggingSpans.Encloses(checkpoint.Span) {
			return true
		}
		// Skip additional updates for every 3rd non-lagging span so that we have
		// a few spans lagging at a second timestamp above the cursor.
		if i, ok := nonLaggingSpans[checkpoint.Span.String()]; ok {
			return i%3 == 0
		}
		numSpans++
		// Skip updates for every 3rd span so that we have a few spans lagging
		// at the cursor.
		if numSpans%3 == 0 {
			laggingSpans.Add(checkpoint.Span)
			return true
		}
		nonLaggingSpans[checkpoint.Span.String()] = len(nonLaggingSpans) + 1
		return false
	}

	var jobID jobspb.JobID
	sqlDB.QueryRow(t,
		`CREATE CHANGEFEED FOR foo INTO 'null://' WITH resolved='50ms', no_initial_scan, cursor=$1`, tsStr,
	).Scan(&jobID)

	// Helper to read job progress
	jobRegistry := s.JobRegistry().(*jobs.Registry)
	loadProgress := func() jobspb.Progress {
		job, err := jobRegistry.LoadJob(context.Background(), jobID)
		require.NoError(t, err)
		return job.Progress()
	}

	// We should eventually checkpoint some spans that are ahead of the highwater.
	// We'll wait until we have two unique timestamps.
	testutils.SucceedsSoon(t, func() error {
		progress := loadProgress()
		cp := maps.Collect(loadCheckpoint(t, progress).All())
		if len(cp) >= 2 {
			return nil
		}
		return errors.New("waiting for checkpoint with two different timestamps")
	})

	sqlDB.Exec(t, "PAUSE JOB $1", jobID)
	waitForJobState(sqlDB, t, jobID, jobs.StatePaused)

	// We expect highwater to be 0 (because we skipped some spans) or exactly cursor
	// (this is mostly due to racy updates sent from aggregators to the frontier).
	// However, the checkpoint timestamp should be at least at the cursor.
	progress := loadProgress()
	require.True(t, progress.GetHighWater().IsEmpty() || progress.GetHighWater().Equal(cursor),
		"expected empty highwater or %s, found %s", cursor, progress.GetHighWater())
	spanLevelCheckpoint := loadCheckpoint(t, progress)
	require.NotNil(t, spanLevelCheckpoint)
	require.True(t, cursor.LessEq(spanLevelCheckpoint.MinTimestamp()))

	// Construct a reverse index from spans to timestamps.
	spanTimestamps := make(map[string]hlc.Timestamp)
	for ts, spans := range spanLevelCheckpoint.All() {
		for _, s := range spans {
			spanTimestamps[s.String()] = ts
		}
	}

	var rangefeedStartedOnce bool
	var incorrectCheckpointErr error
	knobs.FeedKnobs.OnRangeFeedStart = func(spans []kvcoord.SpanTimePair) {
		// We only need to check the first rangefeed restart since
		// any additional restarts (likely due to transient errors)
		// may be using newer span-level checkpoints than the one
		// we saved after the last pause.
		if rangefeedStartedOnce {
			return
		}

		setErr := func(stp kvcoord.SpanTimePair, expectedTS hlc.Timestamp) {
			incorrectCheckpointErr = errors.Newf(
				"rangefeed for span %s expected to start @%s, started @%s instead",
				stp.Span, expectedTS, stp.StartAfter)
		}

		// Verify that the start time for each span is correct.
		for _, sp := range spans {
			if checkpointTS := spanTimestamps[sp.Span.String()]; checkpointTS.IsSet() {
				// Any span in the checkpoint should be resumed at its checkpoint timestamp.
				if !sp.StartAfter.Equal(checkpointTS) {
					setErr(sp, checkpointTS)
				}
			} else {
				// Any spans not in the checkpoint should be at the cursor.
				if !sp.StartAfter.Equal(cursor) {
					setErr(sp, cursor)
				}
			}
		}

		rangefeedStartedOnce = true
	}
	knobs.FeedKnobs.ShouldSkipCheckpoint = nil

	sqlDB.Exec(t, "RESUME JOB $1", jobID)
	waitForJobState(sqlDB, t, jobID, jobs.StateRunning)

	// Wait until highwater advances past cursor.
	testutils.SucceedsSoon(t, func() error {
		progress := loadProgress()
		if hw := progress.GetHighWater(); hw != nil && cursor.Less(*hw) {
			return nil
		}
		return errors.New("waiting for checkpoint advance")
	})

	sqlDB.Exec(t, "PAUSE JOB $1", jobID)
	waitForJobState(sqlDB, t, jobID, jobs.StatePaused)
	// Verify the rangefeed started. This guards against the testing knob
	// not being called, which was happening in earlier versions of the code.
	require.True(t, rangefeedStartedOnce)
	// Verify we didn't see incorrect timestamps when resuming.
	require.NoError(t, incorrectCheckpointErr)
}

func TestCoreChangefeedRequiresSelectPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		rootDB := sqlutils.MakeSQLRunner(s.DB)
		rootDB.Exec(t, `CREATE USER user1`)
		rootDB.Exec(t, `CREATE TYPE type_a as enum ('a')`)
		rootDB.Exec(t, `CREATE TABLE table_a (id int, type type_a)`)
		rootDB.Exec(t, `CREATE TABLE table_b (id int, type type_a)`)
		rootDB.Exec(t, `INSERT INTO table_a(id) values (0)`)

		expectSuccess := func(stmt string) {
			successfulFeed := feed(t, f, stmt)
			defer closeFeed(t, successfulFeed)
			_, err := successfulFeed.Next()
			require.NoError(t, err)
		}

		asUser(t, f, `user1`, func(_ *sqlutils.SQLRunner) {
			expectErrCreatingFeed(t, f, `CREATE CHANGEFEED FOR table_a`,
				`user user1 requires the SELECT privilege on all target tables to be able to run a core changefeed`)
		})
		rootDB.Exec(t, `GRANT SELECT ON table_a TO user1`)
		asUser(t, f, `user1`, func(_ *sqlutils.SQLRunner) {
			expectSuccess(`CREATE CHANGEFEED FOR table_a`)
		})
		asUser(t, f, `user1`, func(_ *sqlutils.SQLRunner) {
			expectErrCreatingFeed(t, f, `CREATE CHANGEFEED FOR table_a, table_b`,
				`user user1 requires the SELECT privilege on all target tables to be able to run a core changefeed`)
		})

		rootDB.Exec(t, `GRANT SELECT ON table_b TO user1`)
		asUser(t, f, `user1`, func(_ *sqlutils.SQLRunner) {
			expectSuccess(`CREATE CHANGEFEED FOR table_a, table_b`)
		})
	}
	cdcTest(t, testFn, feedTestForceSink("sinkless"))
}

// TODO(#94757): remove CONTROLCHANGEFEED entirely
func TestControlChangefeedRoleOption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		rootDB := sqlutils.MakeSQLRunner(s.DB)
		rootDB.Exec(t, `CREATE USER user1 WITH CONTROLCHANGEFEED`)
		rootDB.Exec(t, `CREATE TYPE type_a as enum ('a')`)
		rootDB.Exec(t, `CREATE TABLE table_a (id int, type type_a)`)
		rootDB.Exec(t, `CREATE TABLE table_b (id int, type type_a)`)
		rootDB.Exec(t, `INSERT INTO table_a(id) values (0)`)

		expectSuccess := func(stmt string) {
			successfulFeed := feed(t, f, stmt)
			defer closeFeed(t, successfulFeed)
			_, err := successfulFeed.Next()
			require.NoError(t, err)
		}

		asUser(t, f, `user1`, func(_ *sqlutils.SQLRunner) {
			expectErrCreatingFeed(t, f, `CREATE CHANGEFEED FOR table_a, table_b`,
				`pq: user user1 with CONTROLCHANGEFEED role option requires the SELECT privilege on all target tables to be able to run an enterprise changefeed`)
		})
		rootDB.Exec(t, `GRANT SELECT ON table_a TO user1`)
		asUser(t, f, `user1`, func(_ *sqlutils.SQLRunner) {
			expectErrCreatingFeed(t, f, `CREATE CHANGEFEED FOR table_a, table_b`,
				`pq: user user1 with CONTROLCHANGEFEED role option requires the SELECT privilege on all target tables to be able to run an enterprise changefeed`)
		})
		rootDB.Exec(t, `GRANT SELECT ON table_b TO user1`)
		asUser(t, f, `user1`, func(_ *sqlutils.SQLRunner) {
			expectSuccess(`CREATE CHANGEFEED FOR table_a`)
		})
	}
	cdcTest(t, testFn, feedTestOmitSinks("sinkless"))
}

func TestChangefeedCreateAuthorizationWithChangefeedPriv(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TODOTestTenantDisabled,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			DistSQL: &execinfra.TestingKnobs{
				Changefeed: &TestingKnobs{
					WrapSink: func(s Sink, _ jobspb.JobID) Sink {
						if _, ok := s.(*externalConnectionKafkaSink); ok {
							return s
						}
						return &externalConnectionKafkaSink{sink: s, ignoreDialError: true}
					},
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	rootDB := sqlutils.MakeSQLRunner(db)
	rootDB.Exec(t, `CREATE USER user1`)
	rootDB.Exec(t, `CREATE TYPE type_a as enum ('a')`)
	rootDB.Exec(t, `CREATE TABLE table_a (id int, type type_a)`)
	rootDB.Exec(t, `CREATE TABLE table_b (id int, type type_a)`)
	rootDB.Exec(t, `INSERT INTO table_a(id) values (0)`)
	rootDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	enableEnterprise := utilccl.TestingDisableEnterprise()
	enableEnterprise()

	withUser := func(t *testing.T, user string, fn func(*sqlutils.SQLRunner)) {
		password := `password`
		rootDB.Exec(t, fmt.Sprintf(`ALTER USER %s WITH PASSWORD '%s'`, user, password))

		pgURL := url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(user, password),
			Host:   s.SQLAddr(),
		}
		db2, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		defer db2.Close()
		userDB := sqlutils.MakeSQLRunner(db2)

		fn(userDB)
	}

	rootDB.Exec(t, `CREATE EXTERNAL CONNECTION "nope" AS 'kafka://nope'`)

	withUser(t, "user1", func(userDB *sqlutils.SQLRunner) {
		userDB.ExpectErr(t,
			"user user1 requires the CHANGEFEED privilege on all target tables to be able to run an enterprise changefeed",
			"CREATE CHANGEFEED for table_a, table_b INTO 'external://nope'",
		)
	})
	rootDB.Exec(t, "GRANT CHANGEFEED ON table_a TO user1")
	withUser(t, "user1", func(userDB *sqlutils.SQLRunner) {
		userDB.ExpectErr(t,
			"user user1 requires the CHANGEFEED privilege on all target tables to be able to run an enterprise changefeed",
			"CREATE CHANGEFEED for table_a, table_b INTO 'external://nope'",
		)
	})
	rootDB.Exec(t, "GRANT CHANGEFEED ON table_b TO user1")
	withUser(t, "user1", func(userDB *sqlutils.SQLRunner) {
		userDB.Exec(t,
			"CREATE CHANGEFEED for table_a, table_b INTO 'external://nope'",
		)
	})

	// With require_external_connection_sink enabled, the user requires USAGE on the external connection.
	rootDB.Exec(t, "SET CLUSTER SETTING changefeed.permissions.require_external_connection_sink.enabled = true")
	withUser(t, "user1", func(userDB *sqlutils.SQLRunner) {
		userDB.ExpectErr(t,
			"pq: the CHANGEFEED privilege on all tables can only be used with external connection sinks",
			"CREATE CHANGEFEED for table_a, table_b INTO 'kafka://nope'",
		)
	})
	rootDB.Exec(t, "GRANT USAGE ON EXTERNAL CONNECTION nope to user1")
	withUser(t, "user1", func(userDB *sqlutils.SQLRunner) {
		userDB.Exec(t,
			"CREATE CHANGEFEED for table_a, table_b INTO 'external://nope'",
		)
	})
	rootDB.Exec(t, "SET CLUSTER SETTING changefeed.permissions.require_external_connection_sink.enabled = false")
}

func TestChangefeedGrant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		rootDB := sqlutils.MakeSQLRunner(s.DB)
		rootDB.Exec(t, `create user guest`)

		// CHANGEFEED can be granted as a default privilege on all new tables in a schema
		rootDB.ExecMultiple(t,
			`ALTER DEFAULT PRIVILEGES IN SCHEMA d.public GRANT CHANGEFEED ON TABLES TO guest`,
			`CREATE TABLE table_c (id int primary key)`,
			`INSERT INTO table_c values (0)`,
		)

		// SHOW GRANTS includes CHANGEFEED privileges.
		var count int
		rootDB.QueryRow(t, `select count(*) from [show grants] where privilege_type = 'CHANGEFEED';`).Scan(&count)
		require.Greater(t, count, 0, `Number of CHANGEFEED grants`)

	}
	cdcTest(t, testFn)
}

// TestChangefeedJobControl tests if a user can control a changefeed
// based on their permissions.
func TestChangefeedJobControl(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		ChangefeedJobPermissionsTestSetup(t, s)

		createFeed := func(stmt string) (cdctest.EnterpriseTestFeed, func()) {
			successfulFeed := feed(t, f, stmt)
			closeCf := func() {
				closeFeed(t, successfulFeed)
			}
			_, err := successfulFeed.Next()
			require.NoError(t, err)
			return successfulFeed.(cdctest.EnterpriseTestFeed), closeCf
		}

		// Create a changefeed and assert who can control the job.
		var currentFeed cdctest.EnterpriseTestFeed
		var closeCf func()
		asUser(t, f, `feedCreator`, func(_ *sqlutils.SQLRunner) {
			currentFeed, closeCf = createFeed(`CREATE CHANGEFEED FOR table_a, table_b`)
		})
		asUser(t, f, `adminUser`, func(userDB *sqlutils.SQLRunner) {
			userDB.Exec(t, "PAUSE job $1", currentFeed.JobID())
			waitForJobState(userDB, t, currentFeed.JobID(), "paused")
			userDB.Exec(t, "ALTER JOB $1 OWNER TO feedowner", currentFeed.JobID())
		})
		asUser(t, f, `userWithAllGrants`, func(userDB *sqlutils.SQLRunner) {
			userDB.Exec(t, "RESUME job $1", currentFeed.JobID())
			waitForJobState(userDB, t, currentFeed.JobID(), "running")
		})
		asUser(t, f, `jobController`, func(userDB *sqlutils.SQLRunner) {
			userDB.Exec(t, "RESUME job $1", currentFeed.JobID())
			waitForJobState(userDB, t, currentFeed.JobID(), "running")
		})
		asUser(t, f, `userWithSomeGrants`, func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t, "does not have privileges for job", "PAUSE job $1", currentFeed.JobID())
		})
		asUser(t, f, `regularUser`, func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t, "does not have privileges for job", "PAUSE job $1", currentFeed.JobID())
		})
		closeCf()

		// No one can modify changefeeds created by admins, except for admins.
		// In this case, the root user creates the changefeed.
		asUser(t, f, "adminUser", func(runner *sqlutils.SQLRunner) {
			currentFeed, closeCf = createFeed(`CREATE CHANGEFEED FOR table_a, table_b`)
		})
		asUser(t, f, `adminUser`, func(userDB *sqlutils.SQLRunner) {
			userDB.Exec(t, "PAUSE job $1", currentFeed.JobID())
			waitForJobState(userDB, t, currentFeed.JobID(), "paused")
		})
		asUser(t, f, `userWithAllGrants`, func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t, "only admins can control jobs owned by other admins", "PAUSE job $1", currentFeed.JobID())
		})
		asUser(t, f, `jobController`, func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t, "only admins can control jobs owned by other admins", "PAUSE job $1", currentFeed.JobID())
		})
		closeCf()
	}

	// Only enterprise sinks create jobs.
	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

// TestChangefeedsParallelEnriched tests that multiple changefeeds can run in
// parallel with the enriched envelope. It is most useful under race, to ensure
// that there is no accidental data races in the encoders and source providers.
func TestChangefeedsParallelEnriched(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	const numFeeds = 10
	const maxIterations = 1_000_000_000
	const maxRows = 100

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		db := sqlutils.MakeSQLRunner(s.DB)
		db.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		ctx, cancel := context.WithCancel(ctx)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			db := sqlutils.MakeSQLRunner(s.Server.SQLConn(t))
			var i int
			for i = 0; i < maxIterations && ctx.Err() == nil; i++ {
				db.Exec(t, `UPSERT INTO d.foo VALUES ($1, $2)`, i%maxRows, fmt.Sprintf("hello %d", i))
			}
		}()

		opts := `envelope='enriched'`

		_, isKafka := f.(*kafkaFeedFactory)
		useAvro := isKafka && rand.Intn(2) == 0
		if useAvro {
			t.Logf("using avro")
			opts += `, format='avro'`
		}
		var feeds []cdctest.TestFeed
		for range numFeeds {
			feed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR foo WITH %s`, opts))
			feeds = append(feeds, feed)
		}

		// consume from the feeds
		for _, feed := range feeds {
			feed := feed
			msgCount := 0

			wg.Add(1)
			go func() {
				defer wg.Done()
				for ctx.Err() == nil {
					_, err := feed.Next()
					if err != nil {
						if errors.Is(err, context.Canceled) {
							t.Errorf("error reading from feed: %v", err)
						}
						break
					}
					msgCount++
				}
				assert.GreaterOrEqual(t, msgCount, 0)
			}()
		}

		// let the feeds run for a few seconds
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			t.Fatalf("%v", ctx.Err())
		}

		cancel()

		for _, feed := range feeds {
			closeFeed(t, feed)
		}

		doneWaiting := make(chan struct{})
		go func() {
			defer close(doneWaiting)
			wg.Wait()
		}()
		select {
		case <-doneWaiting:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for goroutines to finish")
		}
	}
	// Sinkless testfeeds have some weird shutdown behaviours, so exclude them for now.
	cdcTest(t, testFn, feedTestRestrictSinks("kafka", "pubsub", "webhook"))
}

func TestChangefeedExpressionUsesSerializedSessionData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.ExecMultiple(t,
			// Create target table in a different database.
			// Session data should be serialized to point to the
			// correct database.
			`CREATE DATABASE session`,
			`USE session`,
			`CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`,
			`INSERT INTO foo values (0, 'hello')`,
			`INSERT INTO foo values (1, 'howdy')`,
		)

		// Trigram similarity threshold should be 30%; so that "howdy" matches,
		// but hello doesn't.  This threshold should be serialized
		// in the changefeed jobs record, and correctly propagated to the aggregators.
		foo := feed(t, f, `CREATE CHANGEFEED WITH schema_change_policy=stop `+
			`AS SELECT * FROM foo WHERE b % 'how'`)
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{`foo: [1]->{"a": 1, "b": "howdy"}`})
	}
	cdcTest(t, testFn, feedTestForceSink("kafka"), feedTestUseRootUserConnection)
}

func TestChangefeedAvroNotice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stop := makeServer(t)
	defer stop()
	schemaReg := cdctest.StartTestSchemaRegistry()
	defer schemaReg.Close()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	sqlDB.Exec(t, "CREATE table foo (i int)")
	sqlDB.Exec(t, `INSERT INTO foo VALUES (0)`)

	sql := fmt.Sprintf("CREATE CHANGEFEED FOR d.foo INTO 'null://' WITH format=experimental_avro, confluent_schema_registry='%s'", schemaReg.URL())
	expectNotice(t, s.Server, sql, `avro is no longer experimental, use format=avro`)
}

func TestChangefeedResolvedNotice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cluster, _, cleanup := startTestCluster(t)
	defer cleanup()
	s := cluster.Server(1)

	// Set the default min_checkpoint_frequency to 30 seconds for this test
	restoreDefault := changefeedbase.TestingSetDefaultMinCheckpointFrequency(30 * time.Second)
	defer restoreDefault()

	pgURL, cleanup := pgurlutils.PGUrl(t, s.SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()
	pgBase, err := pq.NewConnector(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	var actual string
	connector := pq.ConnectorWithNoticeHandler(pgBase, func(n *pq.Error) {
		actual = n.Message
	})

	dbWithHandler := gosql.OpenDB(connector)
	defer dbWithHandler.Close()

	sqlDB := sqlutils.MakeSQLRunner(dbWithHandler)

	sqlDB.Exec(t, `CREATE TABLE ☃ (i INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO ☃ VALUES (0)`)

	t.Run("resolved<min_checkpoint_frequency", func(t *testing.T) {
		actual = "(no notice)"
		f := makeKafkaFeedFactory(t, s, dbWithHandler)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR ☃ INTO 'kafka://does.not.matter/' WITH resolved='5s', min_checkpoint_frequency='10s'`)
		defer closeFeed(t, testFeed)
		require.Equal(t, `resolved (5s) messages will not be emitted more frequently than the configured min_checkpoint_frequency (10s), but may be emitted less frequently`, actual)
	})
	t.Run("resolved<min_checkpoint_frequency default", func(t *testing.T) {
		actual = "(no notice)"
		f := makeKafkaFeedFactory(t, s, dbWithHandler)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR ☃ INTO 'kafka://does.not.matter/' WITH resolved='5s'`)
		defer closeFeed(t, testFeed)
		require.Equal(t, `resolved (5s) messages will not be emitted more frequently than the default min_checkpoint_frequency (30s), but may be emitted less frequently`, actual)
	})
	t.Run("resolved=min_checkpoint_frequency", func(t *testing.T) {
		actual = "(no notice)"
		f := makeKafkaFeedFactory(t, s, dbWithHandler)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR ☃ INTO 'kafka://does.not.matter/' WITH resolved='5s', min_checkpoint_frequency='5s'`)
		defer closeFeed(t, testFeed)
		require.Equal(t, `changefeed will emit to topic _u2603_`, actual)
	})
	t.Run("resolved>min_checkpoint_frequency", func(t *testing.T) {
		actual = "(no notice)"
		f := makeKafkaFeedFactory(t, s, dbWithHandler)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR ☃ INTO 'kafka://does.not.matter/' WITH resolved='10s', min_checkpoint_frequency='5s'`)
		defer closeFeed(t, testFeed)
		require.Equal(t, `changefeed will emit to topic _u2603_`, actual)
	})
	t.Run("resolved default", func(t *testing.T) {
		actual = "(no notice)"
		f := makeKafkaFeedFactory(t, s, dbWithHandler)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR ☃ INTO 'kafka://does.not.matter/' WITH resolved, min_checkpoint_frequency='10s'`)
		defer closeFeed(t, testFeed)
		require.Equal(t, `resolved (0s by default) messages will not be emitted more frequently than the configured min_checkpoint_frequency (10s), but may be emitted less frequently`, actual)
	})
}

func TestChangefeedLowFrequencyNotices(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cluster, _, cleanup := startTestCluster(t)
	defer cleanup()
	s := cluster.Server(1)

	pgURL, cleanup := pgurlutils.PGUrl(t, s.SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()
	pgBase, err := pq.NewConnector(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	var actual string
	connector := pq.ConnectorWithNoticeHandler(pgBase, func(n *pq.Error) {
		actual = n.Message
	})

	dbWithHandler := gosql.OpenDB(connector)
	defer dbWithHandler.Close()

	sqlDB := sqlutils.MakeSQLRunner(dbWithHandler)

	sqlDB.Exec(t, `CREATE TABLE ☃ (i INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO ☃ VALUES (0)`)

	t.Run("no options specified", func(t *testing.T) {
		actual = "(no notice)"
		f := makeKafkaFeedFactory(t, s, dbWithHandler)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR ☃ INTO 'kafka://does.not.matter/'`)
		defer closeFeed(t, testFeed)
		require.Equal(t, `changefeed will emit to topic _u2603_`, actual)
	})
	t.Run("normal resolved and min_checkpoint_frequency", func(t *testing.T) {
		actual = "(no notice)"
		f := makeKafkaFeedFactory(t, s, dbWithHandler)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR ☃ INTO 'kafka://does.not.matter/' WITH resolved='10s', min_checkpoint_frequency='10s'`)
		defer closeFeed(t, testFeed)
		require.Equal(t, `changefeed will emit to topic _u2603_`, actual)
	})
	t.Run("low resolved timestamp", func(t *testing.T) {
		actual = "(no notice)"
		f := makeKafkaFeedFactory(t, s, dbWithHandler)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR ☃ INTO 'kafka://does.not.matter/' WITH resolved='200ms'`)
		defer closeFeed(t, testFeed)
		require.Equal(t, `the 'resolved' timestamp interval (200ms) is very low; consider increasing it to at least 500ms`, actual)
	})
	t.Run("low min_checkpoint_frequency timestamp", func(t *testing.T) {
		actual = "(no notice)"
		f := makeKafkaFeedFactory(t, s, dbWithHandler)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR ☃ INTO 'kafka://does.not.matter/' WITH min_checkpoint_frequency='200ms'`)
		defer closeFeed(t, testFeed)
		require.Equal(t, `the 'min_checkpoint_frequency' timestamp interval (200ms) is very low; consider increasing it to at least 500ms`, actual)
	})
}

func TestChangefeedTruncateOrDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	assertFailuresCounter := func(t *testing.T, m *Metrics, exp int64) {
		t.Helper()
		// If this changefeed is running as a job, we anticipate that it will move
		// through the failed state and will increment the metric. Sinkless feeds
		// don't contribute to the failures counter.
		if strings.Contains(t.Name(), `sinkless`) {
			return
		}
		testutils.SucceedsSoon(t, func() error {
			if got := m.Failures.Count(); got != exp {
				return errors.Errorf("expected %d failures, got %d", exp, got)
			}
			return nil
		})
	}

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		registry := s.Server.JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics)

		drainUntilErr := func(f cdctest.TestFeed) (err error) {
			var msg *cdctest.TestFeedMessage
			for msg, err = f.Next(); msg != nil; msg, err = f.Next() {
			}
			return err
		}

		sqlDB.Exec(t, `CREATE TABLE truncate (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE truncate_cascade (b INT PRIMARY KEY REFERENCES truncate (a))`)
		sqlDB.Exec(t,
			`BEGIN; INSERT INTO truncate VALUES (1); INSERT INTO truncate_cascade VALUES (1); COMMIT`)
		truncate := feed(t, f, `CREATE CHANGEFEED FOR truncate`)
		defer closeFeed(t, truncate)
		truncateCascade := feed(t, f, `CREATE CHANGEFEED FOR truncate_cascade`)
		defer closeFeed(t, truncateCascade)
		assertPayloads(t, truncate, []string{`truncate: [1]->{"after": {"a": 1}}`})
		assertPayloads(t, truncateCascade, []string{`truncate_cascade: [1]->{"after": {"b": 1}}`})
		sqlDB.Exec(t, `TRUNCATE TABLE truncate CASCADE`)
		if err := drainUntilErr(truncate); !testutils.IsError(err, `"truncate" was truncated`) {
			t.Fatalf(`expected ""truncate" was truncated" error got: %+v`, err)
		}
		if err := drainUntilErr(truncateCascade); !testutils.IsError(
			err, `"truncate_cascade" was truncated`,
		) {
			t.Fatalf(`expected ""truncate_cascade" was truncated" error got: %+v`, err)
		}
		assertFailuresCounter(t, metrics, 2)

		sqlDB.Exec(t, `CREATE TABLE drop (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO drop VALUES (1)`)
		drop := feed(t, f, `CREATE CHANGEFEED FOR drop`)
		defer closeFeed(t, drop)
		assertPayloads(t, drop, []string{`drop: [1]->{"after": {"a": 1}}`})
		sqlDB.Exec(t, `DROP TABLE drop`)
		// Dropping the table should cause the schema feed to return an error.
		// This error can either come from validateDescriptor (the first two)
		// or the lease manager (catalog.ErrDescriptorDropped).
		dropOrOfflineRE := fmt.Sprintf(
			`"drop" was dropped|CHANGEFEED cannot target offline table: drop|%s`,
			catalog.ErrDescriptorDropped,
		)
		if err := drainUntilErr(drop); !testutils.IsError(err, dropOrOfflineRE) {
			t.Errorf(`expected %q error, instead got: %+v`, dropOrOfflineRE, err)
		}
		assertFailuresCounter(t, metrics, 3)
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, withAllowChangefeedErr("expects errors"))
	// will sometimes fail, non deterministic
}

func TestChangefeedRetryableError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		var failEmit int64
		knobs.BeforeEmitRow = func(_ context.Context) error {
			switch atomic.LoadInt64(&failEmit) {
			case 1:
				return changefeedbase.MarkRetryableError(fmt.Errorf("synthetic retryable error"))
			case 2:
				return changefeedbase.WithTerminalError(errors.New("synthetic terminal error"))
			default:
				return nil
			}
		}

		// Set up a new feed and verify that the sink is started up.
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1}}`,
		})

		// Set sink to return unique retryable errors and insert a row. Verify that
		// sink is failing requests.
		atomic.StoreInt64(&failEmit, 1)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)
		registry := s.Server.JobRegistry().(*jobs.Registry)

		sli, err := registry.MetricsStruct().Changefeed.(*Metrics).getSLIMetrics(defaultSLIScope)
		require.NoError(t, err)
		retryCounter := sli.ErrorRetries
		testutils.SucceedsSoon(t, func() error {
			if retryCounter.Value() < 3 {
				return fmt.Errorf("insufficient error retries detected")
			}
			return nil
		})

		// Verify job progress contains retryable error status.
		jobID := foo.(cdctest.EnterpriseTestFeed).JobID()
		job, err := registry.LoadJob(context.Background(), jobID)
		require.NoError(t, err)
		require.Contains(t, job.Progress().StatusMessage, "synthetic retryable error")

		// Verify `SHOW JOBS` also shows this information.
		var statusMessage string
		sqlDB.QueryRow(t,
			`SELECT running_status FROM [SHOW JOBS] WHERE job_id = $1`, jobID,
		).Scan(&statusMessage)
		require.Contains(t, statusMessage, "synthetic retryable error")

		// Fix the sink and insert another row. Check that nothing funky happened.
		atomic.StoreInt64(&failEmit, 0)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (3)`)
		assertPayloads(t, foo, []string{
			`foo: [2]->{"after": {"a": 2}}`,
			`foo: [3]->{"after": {"a": 3}}`,
		})

		// Set sink to return a terminal error and insert a row. Ensure that we
		// eventually get the error message back out.
		atomic.StoreInt64(&failEmit, 2)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (4)`)
		for {
			_, err := foo.Next()
			if err == nil {
				continue
			}
			require.EqualError(t, err, `synthetic terminal error`)
			break
		}
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, withAllowChangefeedErr("expects error"))
}

func TestChangefeedJobUpdateFailsIfNotClaimed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set TestingKnobs to return a known session for easier
	// comparison.
	adoptionInterval := 20 * time.Minute
	sessionOverride := withKnobsFn(func(knobs *base.TestingKnobs) {
		// This is a hack to avoid the job adoption loop from
		// immediately re-adopting the job that is running. The job
		// adoption loop basically just sets the claim ID, which will
		// undo our deletion of the claim ID below.
		knobs.JobsTestingKnobs.(*jobs.TestingKnobs).IntervalOverrides.Adopt = &adoptionInterval
	})
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		knobs := s.TestingKnobs.DistSQL.(*execinfra.TestingKnobs).Changefeed.(*TestingKnobs)
		errChan := make(chan error, 1)
		knobs.HandleDistChangefeedError = func(err error) error {
			select {
			case errChan <- err:
			default:
			}
			return err
		}

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b INT)`)
		sqlDB.Exec(t, `INSERT INTO foo (a, b) VALUES (1, 1)`)

		cf := feed(t, f, "CREATE CHANGEFEED FOR TABLE foo")
		jobID := cf.(cdctest.EnterpriseTestFeed).JobID()
		defer func() {
			// Manually update job status to avoid closeFeed waitng for the registry to cancel it
			sqlDB.Exec(t, `UPDATE system.jobs SET status = $1 WHERE id = $2`, jobs.StateFailed, jobID)
			closeFeed(t, cf)
		}()

		assertPayloads(t, cf, []string{
			`foo: [1]->{"after": {"a": 1, "b": 1}}`,
		})

		// Mimic the claim dying and being cleaned up by
		// another node.
		sqlDB.Exec(t, `UPDATE system.jobs SET claim_session_id = NULL WHERE id = $1`, jobID)

		timeout := (5 * time.Second) + changefeedbase.Quantize.Get(&s.Server.ClusterSettings().SV)

		if util.RaceEnabled {
			// Timeout should be at least 30s to allow for race conditions.
			timeout += 25 * time.Second
		}
		// Expect that the distflow fails since it can't
		// update the checkpoint.
		select {
		case err := <-errChan:
			require.Error(t, err)
			// TODO(ssd): Replace this error in the jobs system with
			// an error type we can check against.
			require.Regexp(t, "expected session .* but found NULL", err.Error())
		case <-time.After(timeout):
			t.Fatal("expected distflow to fail")
		}
	}

	// TODO: Figure out why this freezes on tenants
	cdcTest(t, testFn, sessionOverride, feedTestNoTenants, feedTestEnterpriseSinks)
}

// TestChangefeedDataTTL ensures that changefeeds fail with an error in the case
// where the feed has fallen behind the GC TTL of the table data.
func TestChangefeedDataTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		// Set a very simple channel-based, wait-and-resume function as the
		// BeforeEmitRow hook.
		var shouldWait int32
		wait := make(chan struct{})
		resume := make(chan struct{})
		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.FeedKnobs.BeforeScanRequest = func(_ *kv.Batch) error {
			if atomic.LoadInt32(&shouldWait) == 0 {
				return nil
			}
			wait <- struct{}{}
			<-resume
			return nil
		}

		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Create the data table; it will only contain a
		// single row with multiple versions.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b INT)`)

		counter := 0
		upsertedValues := make(map[int]struct{})
		upsertRow := func() {
			counter++
			sqlDB.Exec(t, `UPSERT INTO foo (a, b) VALUES (1, $1)`, counter)
			upsertedValues[counter] = struct{}{}
		}

		// Create the initial version of the row and the
		// changefeed itself. The initial version is necessary
		// to ensure that there is at least one row to
		// backfill.
		upsertRow()

		// Set emit trap to ensure the backfill will pause.
		// The backfill happens before the construction of the
		// rangefeed. Further the backfill sends rows to the
		// changeAggregator via an unbuffered channel, so
		// blocking the emit should block the scan from
		// finishing.
		atomic.StoreInt32(&shouldWait, 1)

		// The changefeed needs to be initialized in a background goroutine because
		// pgx will try to pull results from it as soon as it runs the conn.Query
		// method, but that will block until `resume` is signaled.
		changefeedInit := make(chan cdctest.TestFeed, 1)
		var dataExpiredRows cdctest.TestFeed
		defer func() {
			if dataExpiredRows != nil {
				closeFeed(t, dataExpiredRows)
			}
		}()
		go func() {
			feed, err := f.Feed("CREATE CHANGEFEED FOR TABLE foo")
			if err == nil {
				changefeedInit <- feed
			}
			close(changefeedInit)
		}()

		// Ensure our changefeed is started and waiting during the backfill.
		<-wait

		// Upsert additional versions. One of these will be
		// deleted by the GC process before the rangefeed is
		// started.
		upsertRow()
		upsertRow()
		upsertRow()

		// Force a GC of the table. This should cause both
		// versions of the table to be deleted.
		forceTableGC(t, s.SystemServer, sqlDB, "d", "foo")

		// Resume our changefeed normally.
		atomic.StoreInt32(&shouldWait, 0)
		resume <- struct{}{}
		dataExpiredRows = <-changefeedInit
		require.NotNil(t, dataExpiredRows)

		// Verify that, at some point, Next() returns a "must
		// be after replica GC threshold" error. In the common
		// case, that'll be the second call, the first will
		// should return the row from the backfill and the
		// second should be returning
		for {
			msg, err := dataExpiredRows.Next()
			if testutils.IsError(err, `must be after replica GC threshold`) {
				t.Logf("got expected GC error: %s", err)
				break
			}
			if msg != nil {
				t.Logf("ignoring message: %s", msg)
				var decodedMessage struct {
					After struct {
						A int
						B int
					}
				}
				err = gojson.Unmarshal(msg.Value, &decodedMessage)
				require.NoError(t, err)
				delete(upsertedValues, decodedMessage.After.B)
				if len(upsertedValues) == 0 {
					t.Error("TestFeed emitted all values despite GC running")
					return
				}
			}
		}
	}
	// NOTE(ssd): This test doesn't apply to enterprise
	// changefeeds since enterprise changefeeds create a protected
	// timestamp before beginning their backfill.
	// TODO(samiskin): Tenant test disabled because this test requires
	// forceTableGC which doesn't work on tenants
	cdcTestWithSystem(t, testFn, feedTestForceSink("sinkless"), feedTestNoTenants, withAllowChangefeedErr("expects batch ts gc error"))
}

// TestChangefeedOutdatedCursor ensures that create changefeeds fail with an
// error in the case where the cursor is older than the GC TTL of the table.
func TestChangefeedOutdatedCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE f (a INT PRIMARY KEY)`)
		outdatedTS := s.Server.Clock().Now().AsOfSystemTime()
		sqlDB.Exec(t, `INSERT INTO f VALUES (1)`)
		forceTableGC(t, s.SystemServer, sqlDB, "system", "descriptor")
		createChangefeed :=
			fmt.Sprintf(`CREATE CHANGEFEED FOR TABLE f with cursor = '%s'`, outdatedTS)
		expectedErrorSubstring :=
			fmt.Sprintf(
				"could not create changefeed: cursor %s is older than the GC threshold", outdatedTS)
		expectErrCreatingFeed(t, f, createChangefeed, expectedErrorSubstring)
	}

	cdcTestWithSystem(t, testFn, feedTestNoTenants)
}

// TestChangefeedCursorWarning ensures that we show a warning if
// any of the tables we're creating a changefeed is past
// the warning threshold.
func TestChangefeedCursorAgeWarning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var cursorAges = []time.Duration{
		time.Hour,
		6 * time.Hour,
	}

	testutils.RunValues(t, "cursor age", cursorAges, func(t *testing.T, cursorAge time.Duration) {
		s, stopServer := makeServer(t, withAllowChangefeedErr("expects batch ts gc error"))
		defer stopServer()
		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.OverrideCursorAge = func() int64 {
			return int64(cursorAge)
		}

		warning := fmt.Sprintf(
			"the provided cursor is %d hours old; older cursors can result in increased changefeed latency",
			int64(cursorAge/time.Hour))
		noWarning := "(no notice)"

		expectedWarning := func(initial_scan string) string {
			if cursorAge == time.Hour || initial_scan == "only" {
				return noWarning
			}
			return warning
		}

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE f (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO f VALUES (1)`)
		timeNow := strings.Split(s.Server.Clock().Now().AsOfSystemTime(), ".")[0]

		expectNotice(t, s.Server,
			fmt.Sprintf(
				`CREATE CHANGEFEED FOR TABLE d.f INTO 'null://' with cursor = '%s', initial_scan='only'`,
				timeNow), expectedWarning("only"))

		expectNotice(t, s.Server,
			fmt.Sprintf(
				`CREATE CHANGEFEED FOR TABLE d.f INTO 'null://' with cursor = '%s', initial_scan='yes'`,
				timeNow), expectedWarning("yes"))

		expectNotice(t, s.Server,
			fmt.Sprintf(
				`CREATE CHANGEFEED FOR TABLE d.f INTO 'null://' with cursor = '%s', initial_scan='no'`,
				timeNow), expectedWarning("no"))
	})
}

// TestChangefeedSchemaTTL ensures that changefeeds fail with an error in the case
// where the feed has fallen behind the GC TTL of the table's schema.
func TestChangefeedSchemaTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		// Set a very simple channel-based, wait-and-resume function as the
		// BeforeEmitRow hook.
		var shouldWait int32
		wait := make(chan struct{})
		resume := make(chan struct{})
		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.BeforeEmitRow = func(_ context.Context) error {
			if atomic.LoadInt32(&shouldWait) == 0 {
				return nil
			}
			wait <- struct{}{}
			<-resume
			return nil
		}

		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Create the data table; it will only contain a single row with multiple
		// versions.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		counter := 0
		upsertRow := func() {
			counter++
			sqlDB.Exec(t, `UPSERT INTO foo (a, b) VALUES (1, $1)`, fmt.Sprintf("version %d", counter))
		}

		// Create the initial version of the row and the changefeed itself. The initial
		// version is necessary to prevent CREATE CHANGEFEED itself from hanging.
		upsertRow()
		dataExpiredRows := feed(t, f, "CREATE CHANGEFEED FOR TABLE foo")
		defer closeFeed(t, dataExpiredRows)

		// Set up our emit trap and update the row, which will allow us to "pause" the
		// changefeed in order to force a GC.
		atomic.StoreInt32(&shouldWait, 1)
		upsertRow()
		<-wait

		// Upsert two additional versions. One of these will be deleted by the GC
		// process before changefeed polling is resumed.
		waitForSchemaChange(t, sqlDB, "ALTER TABLE foo ADD COLUMN c STRING")
		upsertRow()
		waitForSchemaChange(t, sqlDB, "ALTER TABLE foo ADD COLUMN d STRING")
		upsertRow()

		// Force a GC of the table. This should cause both older versions of the
		// table to be deleted, with the middle version being lost to the changefeed.
		forceTableGC(t, s.SystemServer, sqlDB, "system", "descriptor")

		// Resume our changefeed normally.
		atomic.StoreInt32(&shouldWait, 0)
		resume <- struct{}{}

		// Verify that the third call to Next() returns an error (the first is the
		// initial row, the second is the first change.
		// Note: rows, and the error message may arrive in any order, so we just loop
		// until we see an error.
		for {
			_, err := dataExpiredRows.Next()
			if err != nil {
				require.Regexp(t, `GC threshold`, err)
				break
			}
		}

	}

	// TODO(samiskin): tenant tests skipped because of forceTableGC not working
	// with a ApplicationLayerInterface
	cdcTestWithSystem(t, testFn, feedTestNoTenants, withAllowChangefeedErr("expects batch ts gc error"))
}

func TestChangefeedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{{
				Key:   "region",
				Value: testServerRegion,
			}},
		},
	})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()

	schemaReg := cdctest.StartTestSchemaRegistry()
	defer schemaReg.Close()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.SucceedsSoonDuration = 5 * time.Second

	// Another SQLDB that has a longer "SucceedsSoonDuration", because some tests will take longer to fail due to DNS resolution retries.
	longTimeoutSQLDB := sqlutils.MakeSQLRunner(db)
	longTimeoutSQLDB.SucceedsSoonDuration = 30 * time.Second

	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, j JSONB)`)
	sqlDB.Exec(t, `CREATE DATABASE d`)

	// Changefeeds default to rangefeed, but for now, rangefeed defaults to off.
	// Verify that this produces a useful error.
	for _, l := range []serverutils.ApplicationLayerInterface{s, srv.SystemLayer()} {
		kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, false)
	}

	sqlDB.Exec(t, `CREATE TABLE rangefeed_off (a INT PRIMARY KEY)`)
	sqlDB.ExpectErrWithTimeout(
		t, `rangefeeds require the kv.rangefeed.enabled setting`,
		`EXPERIMENTAL CHANGEFEED FOR rangefeed_off`,
	)

	for _, l := range []serverutils.ApplicationLayerInterface{s, srv.SystemLayer()} {
		kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
	}

	// Feature flag for changefeeds is off — test that CREATE CHANGEFEED and
	// EXPERIMENTAL CHANGEFEED FOR surface error.
	featureChangefeedEnabled.Override(ctx, &s.ClusterSettings().SV, false)
	sqlDB.ExpectErrWithTimeout(t, `feature CHANGEFEED was disabled by the database administrator`,
		`CREATE CHANGEFEED FOR foo`)
	sqlDB.ExpectErrWithTimeout(t, `feature CHANGEFEED was disabled by the database administrator`,
		`EXPERIMENTAL CHANGEFEED FOR foo`)
	featureChangefeedEnabled.Override(ctx, &s.ClusterSettings().SV, true)

	sqlDB.ExpectErrWithTimeout(
		t, `unknown format: nope`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH format=nope`,
	)

	sqlDB.ExpectErrWithTimeout(
		t, `unknown envelope: nope`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH envelope=nope`,
	)

	sqlDB.ExpectErrWithTimeout(
		t, `time: invalid duration "bar"`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH resolved='bar'`,
	)

	sqlDB.ExpectErrWithTimeout(
		t, `negative durations are not accepted: resolved='-1s'`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH resolved='-1s'`,
	)

	sqlDB.ExpectErrWithTimeout(
		t, `timestamp '.*' is in the future`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH cursor=$1`, timeutil.Now().Add(time.Hour),
	)

	sqlDB.ExpectErrWithTimeout(
		t, `omit the SINK clause`,
		`CREATE CHANGEFEED FOR foo INTO ''`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `omit the SINK clause`,
		`CREATE CHANGEFEED FOR foo INTO $1`, ``,
	)

	// Watching system.jobs would create a cycle, since the resolved timestamp
	// high-water mark is saved in it.
	sqlDB.ExpectErrWithTimeout(
		t, `not supported on system tables`,
		`EXPERIMENTAL CHANGEFEED FOR system.jobs`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `table "bar" does not exist`,
		`EXPERIMENTAL CHANGEFEED FOR bar`,
	)
	sqlDB.Exec(t, `CREATE SEQUENCE seq`)
	sqlDB.ExpectErrWithTimeout(
		t, `CHANGEFEED cannot target sequences: seq`,
		`EXPERIMENTAL CHANGEFEED FOR seq`,
	)
	sqlDB.Exec(t, `CREATE VIEW vw AS SELECT a, b FROM foo`)
	sqlDB.ExpectErrWithTimeout(
		t, `CHANGEFEED cannot target views: vw`,
		`EXPERIMENTAL CHANGEFEED FOR vw`,
	)

	sqlDB.ExpectErrWithTimeout(
		t, `CHANGEFEED targets TABLE foo and TABLE foo are duplicates`,
		`EXPERIMENTAL CHANGEFEED FOR foo, foo`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `CHANGEFEED targets TABLE foo and TABLE defaultdb.foo are duplicates`,
		`EXPERIMENTAL CHANGEFEED FOR foo, defaultdb.foo`,
	)
	sqlDB.Exec(t,
		`CREATE TABLE threefams (a int, b int, c int, family f_a(a), family f_b(b), family f_c(c))`)
	sqlDB.ExpectErrWithTimeout(
		t, `CHANGEFEED targets TABLE foo FAMILY f_a and TABLE foo FAMILY f_a are duplicates`,
		`EXPERIMENTAL CHANGEFEED FOR foo family f_a, foo FAMILY f_b, foo FAMILY f_a`,
	)

	// Backup has the same bad error message #28170.
	sqlDB.ExpectErrWithTimeout(
		t, `"information_schema.tables" does not exist`,
		`EXPERIMENTAL CHANGEFEED FOR information_schema.tables`,
	)

	// TODO(dan): These two tests shouldn't need initial data in the table
	// to pass.
	sqlDB.Exec(t, `CREATE TABLE dec (a DECIMAL PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO dec VALUES (1.0)`)
	sqlDB.ExpectErrWithTimeout(
		t, `.*column a: decimal with no precision`,
		`EXPERIMENTAL CHANGEFEED FOR dec WITH format=$1, confluent_schema_registry=$2`,
		changefeedbase.OptFormatAvro, schemaReg.URL(),
	)
	sqlDB.Exec(t, `CREATE TABLE "oid" (a OID PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO "oid" VALUES (3::OID)`)
	sqlDB.ExpectErrWithTimeout(
		t, `.*column a: type OID not yet supported with avro`,
		`EXPERIMENTAL CHANGEFEED FOR "oid" WITH format=$1, confluent_schema_registry=$2`,
		changefeedbase.OptFormatAvro, schemaReg.URL(),
	)

	unknownParams := func(sink string, params ...string) string {
		return fmt.Sprintf(`unknown %s sink query parameters: [%s]`, sink, strings.Join(params, ", "))
	}

	// Check that sink URLs have valid scheme
	sqlDB.ExpectErrWithTimeout(
		t, `no scheme found for sink URL`,
		`CREATE CHANGEFEED FOR foo INTO 'kafka%3A%2F%2Fnope%0A'`,
	)

	// Check that confluent_schema_registry is only accepted if format is avro.
	// TODO: This should be testing it as a WITH option and check avro_schema_prefix too
	sqlDB.ExpectErrWithTimeout(
		t, unknownParams("SQL", "confluent_schema_registry", "weird"),
		`CREATE CHANGEFEED FOR foo INTO $1`, `experimental-sql://d/?confluent_schema_registry=foo&weird=bar`,
	)

	badHostErrRE := "(no such host|connection refused|network is unreachable)"
	if KafkaV2Enabled.Get(&s.ClusterSettings().SV) {
		badHostErrRE = "(unable to dial|unable to open connection to broker|lookup .* on .*: server misbehaving|connection refused)"
	}

	// Check unavailable kafka - bad dns.
	longTimeoutSQLDB.ExpectErrWithTimeout(
		t, badHostErrRE,
		`CREATE CHANGEFEED FOR foo INTO 'kafka://nope:9999'`,
	)

	// Check unavailable kafka - not running.
	sqlDB.ExpectErrWithTimeout(
		t, badHostErrRE,
		`CREATE CHANGEFEED FOR foo INTO 'kafka://localhost:9999'`,
	)

	// Test that a well-formed URI gets as far as unavailable kafka error.
	longTimeoutSQLDB.ExpectErrWithTimeout(
		t, badHostErrRE,
		`CREATE CHANGEFEED FOR foo INTO 'kafka://nope:9999/?tls_enabled=true&insecure_tls_skip_verify=true&topic_name=foo'`,
	)

	// kafka_topic_prefix was referenced by an old version of the RFC, it's
	// "topic_prefix" now.
	sqlDB.ExpectErrWithTimeout(
		t, unknownParams(`kafka`, `kafka_topic_prefix`),
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?kafka_topic_prefix=foo`,
	)

	// topic_name is only honored for kafka sinks
	sqlDB.ExpectErrWithTimeout(
		t, unknownParams("SQL", "topic_name"),
		`CREATE CHANGEFEED FOR foo INTO $1`, `experimental-sql://d/?topic_name=foo`,
	)

	// schema_topic will be implemented but isn't yet.
	sqlDB.ExpectErrWithTimeout(
		t, `schema_topic is not yet supported`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?schema_topic=foo`,
	)

	// Sanity check kafka tls parameters.
	sqlDB.ExpectErrWithTimeout(
		t, `param tls_enabled must be a bool`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?tls_enabled=foo`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `param insecure_tls_skip_verify must be a bool`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?tls_enabled=true&insecure_tls_skip_verify=foo`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `param ca_cert must be base 64 encoded`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?ca_cert=!`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `ca_cert requires tls_enabled=true`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?&ca_cert=Zm9v`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `param client_cert must be base 64 encoded`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?client_cert=!`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `param client_key must be base 64 encoded`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?client_key=!`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `client_cert requires tls_enabled=true`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?client_cert=Zm9v`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `client_cert requires client_key to be set`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?tls_enabled=true&client_cert=Zm9v`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `client_key requires client_cert to be set`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?tls_enabled=true&client_key=Zm9v`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `invalid client certificate`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?tls_enabled=true&client_cert=Zm9v&client_key=Zm9v`,
	)

	// Sanity check kafka sasl parameters.
	sqlDB.ExpectErrWithTimeout(
		t, `param sasl_enabled must be a bool`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=maybe`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `param sasl_handshake must be a bool`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true&sasl_user=x&sasl_password=y&sasl_handshake=maybe`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `sasl_enabled must be enabled to configure SASL handshake behavior`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_handshake=false`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `sasl_user must be provided when SASL is enabled`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `sasl_password must be provided when SASL is enabled`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true&sasl_user=a`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `sasl_user must be provided when SASL is enabled using mechanism SCRAM-SHA-256`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true&sasl_mechanism=SCRAM-SHA-256`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `sasl_client_id must be provided when SASL is enabled using mechanism OAUTHBEARER`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true&sasl_mechanism=OAUTHBEARER`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `sasl_enabled must be enabled if sasl_user is provided`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_user=a`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `sasl_enabled must be enabled if sasl_password is provided`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_password=a`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `sasl_client_id is only a valid parameter for sasl_mechanism=OAUTHBEARER`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true&sasl_client_id=a`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `sasl_enabled must be enabled to configure SASL mechanism`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_mechanism=SCRAM-SHA-256`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `param sasl_mechanism must be one of AWS_MSK_IAM, OAUTHBEARER, PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true&sasl_mechanism=unsuppported`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, badHostErrRE,
		`CREATE CHANGEFEED FOR foo INTO 'kafka://nope:9999/' WITH kafka_sink_config='{"Flush": {"Messages": 100, "Frequency": "1s"}}'`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `this sink is incompatible with option webhook_client_timeout`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_client_timeout='1s'`,
		`kafka://nope:9999/`,
	)
	// The avro format doesn't support key_in_value or topic_in_value yet.
	sqlDB.ExpectErrWithTimeout(
		t, `key_in_value is not supported with format=avro`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH key_in_value, format='experimental_avro'`,
		`kafka://nope`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `topic_in_value is not supported with format=avro`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH topic_in_value, format='experimental_avro'`,
		`kafka://nope`,
	)

	// Unordered flag required for some options, disallowed for others.
	sqlDB.ExpectErrWithTimeout(t, `resolved timestamps cannot be guaranteed to be correct in unordered mode`, `CREATE CHANGEFEED FOR foo WITH resolved, unordered`)
	sqlDB.ExpectErrWithTimeout(t, `Use of gcpubsub without specifying a region requires the WITH unordered option.`, `CREATE CHANGEFEED FOR foo INTO "gcpubsub://foo"`)
	sqlDB.ExpectErrWithTimeout(t, `key_column requires the unordered option`, `CREATE CHANGEFEED FOR foo WITH key_column='b'`)

	// The topics option should not be exposed to users since it is used
	// internally to display topics in the show changefeed jobs query
	sqlDB.ExpectErrWithTimeout(
		t, `invalid option "topics"`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH topics='foo,bar'`,
		`kafka://nope`,
	)

	// The cloudStorageSink is particular about the options it will work with.
	sqlDB.ExpectErrWithTimeout(
		t, `this sink is incompatible with option confluent_schema_registry`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format='avro', confluent_schema_registry=$2`,
		`experimental-nodelocal://1/bar`, schemaReg.URL(),
	)
	sqlDB.ExpectErrWithTimeout(
		t, `this sink is incompatible with envelope=key_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH envelope='key_only'`,
		`experimental-nodelocal://1/bar`,
	)

	// WITH key_in_value requires envelope=wrapped
	sqlDB.ExpectErrWithTimeout(
		t, `key_in_value is only usable with envelope=wrapped`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH key_in_value, envelope='key_only'`, `kafka://nope`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `key_in_value is only usable with envelope=wrapped`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH key_in_value, envelope='row'`, `kafka://nope`,
	)

	// WITH topic_in_value requires envelope=wrapped
	sqlDB.ExpectErrWithTimeout(
		t, `topic_in_value is only usable with envelope=wrapped`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH topic_in_value, envelope='key_only'`, `kafka://nope`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `topic_in_value is only usable with envelope=wrapped`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH topic_in_value, envelope='row'`, `kafka://nope`,
	)

	// WITH initial_scan and no_initial_scan disallowed
	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan and no_initial_scan`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan, no_initial_scan`, `kafka://nope`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan and no_initial_scan`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH no_initial_scan, initial_scan`, `kafka://nope`,
	)

	// WITH only_initial_scan and no_initial_scan disallowed
	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan='only' and no_initial_scan`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan_only, no_initial_scan`, `kafka://nope`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan='only' and no_initial_scan`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH no_initial_scan, initial_scan_only`, `kafka://nope`,
	)

	// WITH initial_scan_only and initial_scan disallowed
	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan and initial_scan_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan_only, initial_scan`, `kafka://nope`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan and initial_scan_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan, initial_scan_only`, `kafka://nope`,
	)

	// WITH only_initial_scan and end_time disallowed
	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan='only' and end_time`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan_only, end_time = '1'`, `kafka://nope`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan='only' and end_time`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH end_time = '1', initial_scan_only`, `kafka://nope`,
	)

	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan='only' and end_time`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH end_time = '1', initial_scan = 'only'`, `kafka://nope`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan='only' and end_time`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan = 'only', end_time = '1'`, `kafka://nope`,
	)

	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan='only' and resolved`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH resolved, initial_scan = 'only'`, `kafka://nope`,
	)

	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan='only' and diff`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH diff, initial_scan = 'only'`, `kafka://nope`,
	)

	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan='only' and mvcc_timestamp`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH mvcc_timestamp, initial_scan = 'only'`, `kafka://nope`,
	)

	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan='only' and updated`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH updated, initial_scan = 'only'`, `kafka://nope`,
	)

	sqlDB.ExpectErrWithTimeout(
		t, `unknown initial_scan: foo`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan = 'foo'`, `kafka://nope`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan and no_initial_scan`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan = 'yes', no_initial_scan`, `kafka://nope`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `cannot specify both initial_scan and initial_scan_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan = 'no', initial_scan_only`, `kafka://nope`,
	)

	sqlDB.ExpectErrWithTimeout(
		t, `format=csv is only usable with initial_scan_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format = csv`, `kafka://nope`,
	)

	var tsCurrent string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsCurrent)

	sqlDB.ExpectErrWithTimeout(
		t,
		fmt.Sprintf(`specified end time 1.0000000000 cannot be less than statement time %s`, tsCurrent),
		`CREATE CHANGEFEED FOR foo INTO $1 WITH cursor = $2, end_time = '1.0000000000'`, `kafka://nope`, tsCurrent,
	)

	// Sanity check schema registry tls parameters.
	sqlDB.ExpectErrWithTimeout(
		t, `param ca_cert must be base 64 encoded`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format='experimental_avro', confluent_schema_registry=$2`,
		`kafka://nope`, `https://schemareg-nope/?ca_cert=!`,
	)
	longTimeoutSQLDB.ExpectErrWithTimeout(
		t, `failed to parse certificate data`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format='experimental_avro', confluent_schema_registry=$2`,
		`kafka://nope`, `https://schemareg-nope/?ca_cert=Zm9v`,
	)

	// Sanity check webhook sink options.
	sqlDB.ExpectErrWithTimeout(
		t, `param insecure_tls_skip_verify must be a bool`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `webhook-https://fake-host?insecure_tls_skip_verify=foo`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `param ca_cert must be base 64 encoded`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `webhook-https://fake-host?ca_cert=?`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `failed to parse certificate data`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `webhook-https://fake-host?ca_cert=Zm9v`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `sink requires webhook-https`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `webhook-http://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `this sink is incompatible with option confluent_schema_registry`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format='avro', confluent_schema_registry=$2`,
		`webhook-https://fake-host`, schemaReg.URL(),
	)
	sqlDB.ExpectErrWithTimeout(
		t, `problem parsing option webhook_client_timeout: time: invalid duration "not_an_integer"`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_client_timeout='not_an_integer'`, `webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `option webhook_client_timeout must be a duration greater than 0`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_client_timeout='0s'`, `webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `negative durations are not accepted: webhook_client_timeout='-500s'`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_client_timeout='-500s'`, `webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `problem parsing option webhook_client_timeout: time: missing unit in duration`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_client_timeout='0.5'`, `webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `this sink is incompatible with envelope=key_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH envelope='key_only'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `this sink is incompatible with envelope=row`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH envelope='row'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `invalid sink config, all values must be non-negative`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Flush": {"Messages": -100, "Frequency": "1s"}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `invalid sink config, all values must be non-negative`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Flush": {"Messages": 100, "Frequency": "-1s"}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `invalid sink config, Flush.Frequency is not set, messages may never be sent`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Flush": {"Messages": 100}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `error unmarshalling json: time: invalid duration "Zm9v"`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Flush": {"Frequency": "Zm9v"}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `error unmarshalling json: invalid character`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='not json'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `unknown compression: invalid, valid values are 'gzip' and 'zstd'`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH compression='invalid'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `Retry.Max must be either a positive int or 'inf' for infinite retries.`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Retry": {"Max": "not valid"}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `Retry.Max must be a positive integer. use 'inf' for infinite retries.`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Retry": {"Max": 0}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `Retry.Max must be a positive integer. use 'inf' for infinite retries.`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Retry": {"Max": -1}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, ``,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH updated, webhook_sink_config='{"Retry":{"Max":"inf"}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `client_cert requires client_key to be set`,
		`CREATE CHANGEFEED FOR foo INTO $1`,
		`webhook-https://fake-host?client_cert=Zm9v`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `client_key requires client_cert to be set`,
		`CREATE CHANGEFEED FOR foo INTO $1`,
		`webhook-https://fake-host?client_key=Zm9v`,
	)

	// Sanity check on_error option
	sqlDB.ExpectErrWithTimeout(
		t, `option "on_error" requires a value`,
		`CREATE CHANGEFEED FOR foo into $1 WITH on_error`,
		`kafka://nope`)
	sqlDB.ExpectErrWithTimeout(
		t, `unknown on_error: not_valid, valid values are 'pause' and 'fail'`,
		`CREATE CHANGEFEED FOR foo into $1 WITH on_error='not_valid'`,
		`kafka://nope`)

	// Sanity check for options compatibility validation.
	sqlDB.ExpectErrWithTimeout(
		t, `this sink is incompatible with option compression`,
		`CREATE CHANGEFEED FOR foo into $1 WITH compression='gzip'`,
		`kafka://nope`)

	sqlDB.ExpectErrWithTimeout(
		t, `required column idk not present on table foo`,
		`CREATE CHANGEFEED FOR foo into $1 WITH headers_json_column_name='idk'`,
		`kafka://nope`)

	sqlDB.ExpectErrWithTimeout(
		t, `column b of type string does not match required type json`,
		`CREATE CHANGEFEED FOR foo into $1 WITH headers_json_column_name='b'`,
		`kafka://nope`)

	sqlDB.ExpectErrWithTimeout(
		t, `this sink is incompatible with option headers_json_column_name`,
		`CREATE CHANGEFEED FOR foo into $1 WITH headers_json_column_name='j'`,
		`nodelocal://.`)

	sqlDB.ExpectErrWithTimeout(
		t, `headers_json_column_name is only usable with format=json/avro`,
		`CREATE CHANGEFEED FOR foo into $1 WITH headers_json_column_name='j', format=csv, initial_scan='only'`,
		`kafka://nope`)

	sqlDB.ExpectErrWithTimeout(
		t, `envelope=enriched is incompatible with SELECT statement`,
		`CREATE CHANGEFEED INTO 'null://' WITH envelope=enriched AS SELECT * from foo`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `envelope=enriched is only usable with format=json/avro`,
		`CREATE CHANGEFEED FOR foo INTO 'null://' WITH envelope=enriched, format=csv, initial_scan='only'`,
	)
	sqlDB.ExpectErrWithTimeout(
		// I also would have accepted "this sink is incompatible with envelope=enriched".
		t, `envelope=enriched is only usable with format=json/avro`,
		`CREATE CHANGEFEED FOR foo INTO 'nodelocal://.' WITH envelope=enriched, format=parquet`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `this sink is incompatible with envelope=enriched`,
		`CREATE CHANGEFEED FOR foo INTO 'pulsar://.' WITH envelope=enriched`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `enriched_properties is only usable with envelope=enriched`,
		`CREATE CHANGEFEED FOR foo INTO 'null://' WITH enriched_properties='schema'`,
	)
	sqlDB.ExpectErrWithTimeout(
		t, `unknown enriched_properties: potato, valid values are: source, schema`,
		`CREATE CHANGEFEED FOR foo INTO 'null://' WITH enriched_properties='schema,potato'`,
	)

	t.Run("sinkless enriched non-json", func(t *testing.T) {
		skip.WithIssue(t, 130949, "sinkless feed validations are subpar")
		sqlDB.ExpectErrWithTimeout(
			t, `some error`,
			`CREATE CHANGEFEED FOR foo WITH envelope=enriched, format=avro, confluent_schema_registry='http://localhost:8888'`,
		)
	})

	t.Run("enriched alters", func(t *testing.T) {
		res := sqlDB.QueryStr(t, `CREATE CHANGEFEED FOR FOO INTO 'null://' WITH envelope=enriched`)
		jobIDStr := res[0][0]
		jobID, err := strconv.Atoi(jobIDStr)
		require.NoError(t, err)
		sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
		waitForJobState(sqlDB, t, catpb.JobID(jobID), jobs.StatePaused)
		sqlDB.ExpectErrWithTimeout(
			t, `envelope=enriched is only usable with format=json/avro`,
			`ALTER CHANGEFEED $1 SET format=parquet`, jobIDStr,
		)
	})

}

func TestChangefeedDescription(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Intentionally don't use the TestFeedFactory because we want to
	// control the placeholders.
	s, stopServer := makeServerWithOptions(t, makeOptions(t, withAllowChangefeedErr("create strange changefeeds that don't actually run")))
	defer stopServer()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	// Create enum to ensure enum values displayed correctly in the summary.
	sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, status status)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

	sink, cleanup := pgurlutils.PGUrl(t, s.Server.SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()
	sink.Scheme = changefeedbase.SinkSchemeExperimentalSQL
	sink.Path = `d`

	redactedSink := strings.Replace(sink.String(), username.RootUser, `redacted`, 1)
	for _, tc := range []struct {
		create string
		descr  string
	}{
		{
			create: "CREATE CHANGEFEED FOR foo INTO $1 WITH updated, envelope = $2",
			descr:  `CREATE CHANGEFEED FOR TABLE foo INTO '` + redactedSink + `' WITH OPTIONS (envelope = 'wrapped', updated)`,
		},
		{
			create: "CREATE CHANGEFEED FOR public.foo INTO $1 WITH updated, envelope = $2",
			descr:  `CREATE CHANGEFEED FOR TABLE public.foo INTO '` + redactedSink + `' WITH OPTIONS (envelope = 'wrapped', updated)`,
		},
		{
			create: "CREATE CHANGEFEED FOR d.public.foo INTO $1 WITH updated, envelope = $2",
			descr:  `CREATE CHANGEFEED FOR TABLE d.public.foo INTO '` + redactedSink + `' WITH OPTIONS (envelope = 'wrapped', updated)`,
		},
		{
			// TODO(#85143): remove schema_change_policy='stop' from this test.
			create: "CREATE CHANGEFEED INTO $1 WITH updated, envelope = $2, schema_change_policy='stop' AS SELECT a FROM foo WHERE a % 2 = 0",
			descr:  `CREATE CHANGEFEED INTO '` + redactedSink + `' WITH OPTIONS (envelope = 'wrapped', schema_change_policy = 'stop', updated) AS SELECT a FROM foo WHERE (a % 2) = 0`,
		},
		{
			// TODO(#85143): remove schema_change_policy='stop' from this test.
			create: "CREATE CHANGEFEED INTO $1 WITH updated, envelope = $2, schema_change_policy='stop' AS SELECT a FROM public.foo AS bar WHERE a % 2 = 0",
			descr:  `CREATE CHANGEFEED INTO '` + redactedSink + `' WITH OPTIONS (envelope = 'wrapped', schema_change_policy = 'stop', updated) AS SELECT a FROM public.foo AS bar WHERE (a % 2) = 0`,
		},
		{
			// TODO(#85143): remove schema_change_policy='stop' from this test.
			create: "CREATE CHANGEFEED INTO $1 WITH updated, envelope = $2, schema_change_policy='stop' AS SELECT a FROM foo WHERE status IN ('open', 'closed')",
			descr:  `CREATE CHANGEFEED INTO '` + redactedSink + `' WITH OPTIONS (envelope = 'wrapped', schema_change_policy = 'stop', updated) AS SELECT a FROM foo WHERE status IN ('open', 'closed')`,
		},
	} {
		t.Run(tc.create, func(t *testing.T) {
			var jobID jobspb.JobID
			sqlDB.QueryRow(t, tc.create, sink.String(), `wrapped`).Scan(&jobID)

			var description string
			sqlDB.QueryRow(t,
				`SELECT description FROM [SHOW JOB $1]`, jobID,
			).Scan(&description)

			require.Equal(t, tc.descr, description)
		})
	}
}

func TestChangefeedKafkaV1ConnectionError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		KafkaV2Enabled.Override(context.Background(), &s.Server.ClusterSettings().SV, false)
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo(id int primary key, s string)`)
		sqlDB.Exec(t, `INSERT INTO foo(id, s) VALUES (0, 'hello'), (1, null)`)
		_, err := f.Feed(`CREATE CHANGEFEED FOR foo`)
		require.ErrorContains(t, err, "client has run out of available brokers")
	}
	cdcTest(t, testFn, feedTestForceSink("kafka"), feedTestForceKafkaV1ConnectionCheck)
}

func TestChangefeedPanicRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Panics can mess with the test setup so run these each in their own test.

	defer cdceval.TestingDisableFunctionsBlacklist()()

	prep := func(t *testing.T, sqlDB *sqlutils.SQLRunner) {
		sqlDB.Exec(t, `CREATE TABLE foo(id int primary key, s string)`)
		sqlDB.Exec(t, `INSERT INTO foo(id, s) VALUES (0, 'hello'), (1, null)`)
	}

	waitForFeedErr := func(t *testing.T, feed cdctest.TestFeed, timeout time.Duration) error {
		start := timeutil.Now()
		for {
			if time.Since(start) >= timeout {
				t.Fatalf("feed did not return error before timeout of %s", timeout)
			}
			_, err := feed.Next()
			if err != nil {
				return err
			}
		}
	}

	cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		prep(t, sqlDB)
		// Check that disallowed expressions have a good error message.
		// Also regression test for https://github.com/cockroachdb/cockroach/issues/90416
		sqlDB.ExpectErrWithTimeout(t, "sub-query expressions not supported by CDC",
			`CREATE CHANGEFEED WITH schema_change_policy='stop' AS SELECT 1 FROM foo WHERE EXISTS (SELECT true)`)
	})

	// Check that all panics while evaluating the WHERE clause in an expression are recovered from.
	// NB: REPAIRCLUSTER is required to use crdb_internal.force_panic.
	cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		prep(t, sqlDB)
		foo := feed(t, f,
			`CREATE CHANGEFEED WITH schema_change_policy='stop' AS SELECT 1 FROM foo WHERE crdb_internal.force_panic('wat') IS NULL`)
		defer closeFeed(t, foo)
		err := waitForFeedErr(t, foo, 2*time.Minute)
		require.ErrorContains(t, err, "error evaluating CDC expression", "expected panic recovery while evaluating WHERE clause")
	}, feedTestAdditionalSystemPrivs("REPAIRCLUSTER"), withAllowChangefeedErr("expects error"))

	// Check that all panics while evaluating the SELECT clause in an expression are recovered from.
	// NB: REPAIRCLUSTER is required to use crdb_internal.force_panic.
	cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		prep(t, sqlDB)
		foo := feed(t, f,
			`CREATE CHANGEFEED WITH schema_change_policy='stop' AS SELECT crdb_internal.force_panic('wat') FROM foo`)
		defer closeFeed(t, foo)
		err := waitForFeedErr(t, foo, 2*time.Minute)
		require.ErrorContains(t, err, "error evaluating CDC expression", "expected panic recovery while evaluating SELECT clause")
	}, feedTestAdditionalSystemPrivs("REPAIRCLUSTER"), withAllowChangefeedErr("expects error"))
}

func TestChangefeedPauseUnpause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved`)
		defer closeFeed(t, foo)

		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			`foo: [2]->{"after": {"a": 2, "b": "b"}}`,
			`foo: [4]->{"after": {"a": 4, "b": "c"}}`,
			`foo: [7]->{"after": {"a": 7, "b": "d"}}`,
			`foo: [8]->{"after": {"a": 8, "b": "e"}}`,
		})

		// Wait for the high-water mark on the job to be updated after the initial
		// scan, to make sure we don't get the initial scan data again.
		m, err := foo.Next()
		if err != nil {
			t.Fatal(err)
		} else if m.Key != nil {
			t.Fatalf(`expected a resolved timestamp got %s: %s->%s`, m.Topic, m.Key, m.Value)
		}

		feedJob := foo.(cdctest.EnterpriseTestFeed)
		sqlDB.Exec(t, `PAUSE JOB $1`, feedJob.JobID())
		// PAUSE JOB only requests the job to be paused. Block until it's paused.
		waitForJobState(sqlDB, t, feedJob.JobID(), jobs.StatePaused)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (16, 'f')`)
		sqlDB.Exec(t, `RESUME JOB $1`, feedJob.JobID())
		assertPayloads(t, foo, []string{
			`foo: [16]->{"after": {"a": 16, "b": "f"}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestChangefeedPauseUnpauseCursorAndInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRaceWithIssue(t, 67565)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)
		var tsStr string
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp() from foo`).Scan(&tsStr)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo `+
			`WITH initial_scan, resolved='10ms', cursor='`+tsStr+`'`)
		defer closeFeed(t, foo)

		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			`foo: [2]->{"after": {"a": 2, "b": "b"}}`,
			`foo: [4]->{"after": {"a": 4, "b": "c"}}`,
			`foo: [7]->{"after": {"a": 7, "b": "d"}}`,
			`foo: [8]->{"after": {"a": 8, "b": "e"}}`,
		})

		// Wait for the high-water mark on the job to be updated after the initial
		// scan, to make sure we don't get the initial scan data again.
		expectResolvedTimestamp(t, foo)
		expectResolvedTimestamp(t, foo)

		feedJob := foo.(cdctest.EnterpriseTestFeed)
		require.NoError(t, feedJob.Pause())

		foo.(seenTracker).reset()
		sqlDB.Exec(t, `INSERT INTO foo VALUES (16, 'f')`)
		require.NoError(t, feedJob.Resume())
		assertPayloads(t, foo, []string{
			`foo: [16]->{"after": {"a": 16, "b": "f"}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestChangefeedHandlesDrainingNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "Takes too long with race enabled")

	var shouldDrain int32
	knobs := base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			DrainFast:  true,
			Changefeed: &TestingKnobs{},
			Flowinfra: &flowinfra.TestingKnobs{
				FlowRegistryDraining: func() bool {
					if atomic.LoadInt32(&shouldDrain) > 0 {
						atomic.StoreInt32(&shouldDrain, 0)
						return true
					}
					return false
				},
			},
		},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	sinkDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	tc := serverutils.StartCluster(t, 4, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Test uses SPLIT AT, which isn't currently supported for
			// secondary tenants. Tracked with #76378.
			DefaultTestTenant: base.TODOTestTenantDisabled,
			UseDatabase:       "test",
			Knobs:             knobs,
			ExternalIODir:     sinkDir,
		}})
	defer tc.Stopper().Stop(context.Background())

	db := tc.ServerConn(1)
	sqlDB := sqlutils.MakeSQLRunner(db)
	serverutils.SetClusterSetting(t, tc, "kv.rangefeed.enabled", true)
	serverutils.SetClusterSetting(t, tc, "kv.closed_timestamp.target_duration", time.Second)
	serverutils.SetClusterSetting(t, tc, "changefeed.experimental_poll_interval", 10*time.Millisecond)

	sqlutils.CreateTable(
		t, db, "foo",
		"k INT PRIMARY KEY, v INT",
		10,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(2)),
	)

	// Introduce 4 splits to get 5 ranges.  We need multiple ranges in order to run distributed
	// flow.
	sqlDB.Exec(t, "ALTER TABLE test.foo SPLIT AT (SELECT i*2 FROM generate_series(1, 4) AS g(i))")
	sqlDB.Exec(t, "ALTER TABLE test.foo SCATTER")

	// Create a factory which executes the CREATE CHANGEFEED statement on server 0.
	// This statement should fail, but the job itself ought to be created.
	// After some time, that job should be adopted by another node, and executed successfully.
	//
	// We use feedTestUseRootUserConnection to prevent the
	// feed factory from trying to create a test user. Because the registry is draining, creating the test user
	// will fail and the test will fail prematurely.
	f, closeSink := makeFeedFactory(t, randomSinkType(t, feedTestEnterpriseSinks), tc.Server(1), tc.ServerConn(0),
		feedTestUseRootUserConnection)
	defer closeSink()

	atomic.StoreInt32(&shouldDrain, 1)
	feed := feed(t, f, "CREATE CHANGEFEED FOR foo")
	defer closeFeed(t, feed)

	jobID := feed.(cdctest.EnterpriseTestFeed).JobID()
	registry := tc.Server(1).JobRegistry().(*jobs.Registry)
	loadProgress := func() jobspb.Progress {
		job, err := registry.LoadJob(context.Background(), jobID)
		require.NoError(t, err)
		return job.Progress()
	}

	// Wait until highwater advances.
	testutils.SucceedsSoon(t, func() error {
		progress := loadProgress()
		if hw := progress.GetHighWater(); hw == nil || hw.IsEmpty() {
			return errors.New("waiting for highwater")
		}
		return nil
	})
}

// Verifies changefeed updates checkpoint when cluster undergoes rolling
// restart.
func TestChangefeedHandlesRollingRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer testingUseFastRetry()()

	skip.UnderRace(t, "Takes too long with race enabled")

	const numNodes = 4

	opts := makeOptions(t)
	opts.forceRootUserConnection = true
	defer addCloudStorageOptions(t, &opts)()

	var checkpointHW atomic.Value
	checkpointHW.Store(hlc.Timestamp{})
	var nodeDrainChannels [numNodes]atomic.Value // of chan struct

	proceed := make(chan struct{}, 1)
	errCh := make(chan error, 1)

	makeTestServerArgs := func(n int) base.TestServerArgs {
		nodeDrainChannels[n].Store(make(chan struct{}))

		return base.TestServerArgs{
			UseDatabase: "test",
			Knobs: base.TestingKnobs{
				DistSQL: &execinfra.TestingKnobs{
					DrainFast: true,
					Changefeed: &TestingKnobs{
						// Filter out draining nodes; normally we rely on dist sql planner
						// to do that for us.
						FilterDrainingNodes: func(
							partitions []sql.SpanPartition, draining []roachpb.NodeID,
						) ([]sql.SpanPartition, error) {
							toSkip := map[roachpb.NodeID]struct{}{}
							for _, n := range draining {
								toSkip[n] = struct{}{}
							}
							var filtered []sql.SpanPartition
							var filteredSpans []roachpb.Span
							for _, p := range partitions {
								if _, s := toSkip[roachpb.NodeID(p.SQLInstanceID)]; s {
									filteredSpans = append(filteredSpans, p.Spans...)
								} else {
									filtered = append(filtered, p)
								}
							}
							if len(filtered) == 0 {
								return nil, errors.AssertionFailedf("expected non-empty filtered span partitions")
							}
							if len(filteredSpans) == 0 {
								return partitions, nil
							}
							filtered[0].Spans = append(filtered[0].Spans, filteredSpans...)
							return filtered, nil
						},

						// Disable all checkpoints.  This test verifies that even when
						// checkpoints are behind, changefeed can handle rolling restarts by
						// utilizing the most up-to-date checkpoint information transmitted by
						// the aggregators to the change frontier processor.
						ShouldCheckpointToJobRecord: func(hw hlc.Timestamp) bool {
							checkpointHW.Store(hw)
							return false
						},

						OnDrain: func() <-chan struct{} {
							return nodeDrainChannels[n].Load().(chan struct{})
						},

						BeforeDistChangefeed: func() {
							ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
							defer cancel()
							select {
							case <-proceed:
							case <-ctx.Done():
								t.Fatal("did not get signal to proceed")
							}
						},
						// Handle transient changefeed error.  We expect to see node drain error.
						// When we do, notify drainNotification, and reset node drain channel.
						HandleDistChangefeedError: func(err error) error {
							errCh <- err
							return err
						},
					},
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
			ExternalIODir: opts.externalIODir,
		}
	}

	tc := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgsPerNode: func() map[int]base.TestServerArgs {
			perNode := make(map[int]base.TestServerArgs)
			for i := 0; i < numNodes; i++ {
				perNode[i] = makeTestServerArgs(i)
			}
			return perNode
		}(),
		ServerArgs: base.TestServerArgs{
			// Test uses SPLIT AT, which isn't currently supported for
			// secondary tenants. Tracked with #76378.
			DefaultTestTenant: base.TODOTestTenantDisabled,
		},
	})
	defer tc.Stopper().Stop(context.Background())

	db := tc.ServerConn(1)
	sqlDB := sqlutils.MakeSQLRunner(db)
	serverutils.SetClusterSetting(t, tc, "changefeed.shutdown_checkpoint.enabled", true)
	serverutils.SetClusterSetting(t, tc, "kv.rangefeed.enabled", true)
	serverutils.SetClusterSetting(t, tc, "kv.closed_timestamp.target_duration", 10*time.Millisecond)
	serverutils.SetClusterSetting(t, tc, "changefeed.experimental_poll_interval", 10*time.Millisecond)
	serverutils.SetClusterSetting(t, tc, "changefeed.aggregator.heartbeat", 10*time.Millisecond)
	// Randomizing replica assignment can cause timeouts or other
	// failures due to assumptions in the testing knobs about balanced
	// assignments.
	serverutils.SetClusterSetting(t, tc, "changefeed.random_replica_selection.enabled", false)

	sqlutils.CreateTable(
		t, db, "foo",
		"k INT PRIMARY KEY, v INT",
		400,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(2)),
	)

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(
		tc.Server(0).DB(), keys.SystemSQLCodec, "test", "foo")
	tc.SplitTable(t, tableDesc, []serverutils.SplitPoint{
		{TargetNodeIdx: 1, Vals: []interface{}{100}},
		{TargetNodeIdx: 2, Vals: []interface{}{200}},
		{TargetNodeIdx: 3, Vals: []interface{}{300}},
	})

	// Create a factory which executes the CREATE CHANGEFEED statement on server 1.
	// Feed logic (helpers) running on node 4.

	f, closeSink := makeFeedFactoryWithOptions(t, "cloudstorage", tc.Server(3), tc.ServerConn(0), opts)
	defer closeSink()

	proceed <- struct{}{} // Allow changefeed to start.
	feed := feed(t, f, "CREATE CHANGEFEED FOR foo WITH initial_scan='no', min_checkpoint_frequency='100ms'")
	defer closeFeed(t, feed)

	jf := feed.(cdctest.EnterpriseTestFeed)

	// waitCheckpointAttempt waits until an attempt to checkpoint is made.
	waitCheckpoint := func(minHW hlc.Timestamp) {
		t.Helper()
		testutils.SucceedsSoon(t, func() error {
			if minHW.Less(checkpointHW.Load().(hlc.Timestamp)) {
				return nil
			}
			return errors.New("still waiting for checkpoint")
		})
	}

	// Shutdown each node, one at a time.
	// Insert few values on each iteration.
	// Even though checkpointing is disabled via testing knobs,
	// the drain logic should preserve up-to-date restart information.
	for i := 0; i < numNodes; i++ {
		beforeInsert := tc.Server(3).Clock().Now()
		sqlDB.Exec(t, "UPDATE test.foo SET v=$1 WHERE k IN (10, 110, 220, 330)", 42+i)
		assertPayloads(t, feed, []string{
			fmt.Sprintf(`foo: [10]->{"after": {"k": 10, "v": %d}}`, 42+i),
			fmt.Sprintf(`foo: [110]->{"after": {"k": 110, "v": %d}}`, 42+i),
			fmt.Sprintf(`foo: [220]->{"after": {"k": 220, "v": %d}}`, 42+i),
			fmt.Sprintf(`foo: [330]->{"after": {"k": 330, "v": %d}}`, 42+i),
		})

		// Wait for a checkpoint attempt.  The checkpoint will not be committed
		// to the jobs table (due to testing knobs), but when we trigger drain
		// below, we expect correct restart information to be checkpointed anyway.
		waitCheckpoint(beforeInsert)

		// Send drain notification.
		close(nodeDrainChannels[i].Load().(chan struct{}))

		// Changefeed should encounter node draining error.
		var err error
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		select {
		case err = <-errCh:
		case <-ctx.Done():
			t.Fatal("could not get draining error on channel")
		}
		cancel()
		require.True(t, errors.Is(err, changefeedbase.ErrNodeDraining))

		// Reset drain channel.
		nodeDrainChannels[i].Store(make(chan struct{}))

		// Even though checkpointing was disabled, when we drain, an attempt is
		// made to persist up-to-date checkpoint.
		require.NoError(t, jf.WaitForHighWaterMark(beforeInsert))

		// Let the retry proceed.
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*60)
		select {
		case proceed <- struct{}{}:
		case <-ctx.Done():
			t.Fatal("could not send signal to proceed")
		}
		cancel()
	}
}

// TestChangefeedTimelyResolvedTimestampUpdatePostRollingRestart verifies that
// a changefeed over a large number of quiesced ranges is able to quickly
// advance its resolved timestamp after a rolling restart. At the lowest level,
// the test ensures that lease acquisitions required to advance the closed
// timestamp of the constituent changefeed ranges is fast.
func TestChangefeedTimelyResolvedTimestampUpdatePostRollingRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Add verbose logging to help debug future failures.
	require.NoError(t, log.SetVModule("changefeed_processors=1,replica_rangefeed=2,"+
		"replica_range_lease=3,raft=3"))

	// This test requires many range splits, which can be slow under certain test
	// conditions. Skip potentially slow tests.
	skip.UnderDeadlock(t)
	skip.UnderRace(t)

	opts := makeOptions(t)
	defer addCloudStorageOptions(t, &opts)()
	opts.forceRootUserConnection = true
	defer changefeedbase.TestingSetDefaultMinCheckpointFrequency(testSinkFlushFrequency)()
	defer testingUseFastRetry()()
	const numNodes = 3

	stickyVFSRegistry := fs.NewStickyRegistry()
	listenerReg := listenerutil.NewListenerRegistry()
	defer listenerReg.Close()

	perServerKnobs := make(map[int]base.TestServerArgs, numNodes)
	for i := 0; i < numNodes; i++ {
		perServerKnobs[i] = base.TestServerArgs{
			Knobs: base.TestingKnobs{
				DistSQL: &execinfra.TestingKnobs{
					Changefeed: &TestingKnobs{},
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				Server: &server.TestingKnobs{
					StickyVFSRegistry: stickyVFSRegistry,
				},
			},
			ExternalIODir: opts.externalIODir,
			UseDatabase:   "d",
		}
	}

	tc := serverutils.StartCluster(t, numNodes,
		base.TestClusterArgs{
			ServerArgsPerNode: perServerKnobs,
			ServerArgs: base.TestServerArgs{
				// Test uses SPLIT AT, which isn't currently supported for
				// secondary tenants. Tracked with #76378.
				DefaultTestTenant: base.TODOTestTenantDisabled,
			},
			ReusableListenerReg: listenerReg,
		})
	defer tc.Stopper().Stop(context.Background())

	db := tc.ServerConn(1)
	sqlDB := sqlutils.MakeSQLRunner(db)
	serverutils.SetClusterSetting(t, tc, "kv.rangefeed.enabled", true)

	// Create a table with 1000 ranges.
	sqlDB.ExecMultiple(t,
		`CREATE DATABASE d;`,
		`CREATE TABLE d.foo (k INT PRIMARY KEY);`,
		`INSERT INTO d.foo (k) SELECT * FROM generate_series(1, 1000);`,
		`ALTER TABLE d.foo SPLIT AT (SELECT * FROM generate_series(1, 1000));`,
	)

	// Wait for ranges to quiesce.
	testutils.SucceedsSoon(t, func() error {
		for i := range tc.NumServers() {
			store, err := tc.Server(i).GetStores().(*kvserver.Stores).GetStore(tc.Server(i).GetFirstStoreID())
			require.NoError(t, err)
			numQuiescent := store.Metrics().QuiescentCount.Value()
			numQualifyForQuiesence := store.Metrics().LeaseEpochCount.Value()
			if numQuiescent < numQualifyForQuiesence {
				return errors.Newf(
					"waiting for ranges to quiesce on node %d; quiescent: %d; should quiesce: %d",
					tc.Server(i).NodeID(), numQuiescent, numQualifyForQuiesence,
				)
			}
		}
		return nil
	})

	// Capture the pre-restart timestamp. We'll use this as the start time for the
	// changefeed later.
	var tsLogical string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsLogical)

	// Perform the rolling restart.
	require.NoError(t, tc.Restart())

	// For validation, the test requires an enterprise feed.
	feedTestEnterpriseSinks(&opts)
	sinkType := randomSinkTypeWithOptions(opts)
	f, closeSink := makeFeedFactoryWithOptions(t, sinkType, tc, tc.ServerConn(0), opts)
	defer closeSink()
	// The end time is captured post restart. The changefeed spans from before the
	// restart to after.
	endTime := tc.Server(0).Clock().Now().AddDuration(5 * time.Second)
	testFeed := feed(t, f, `CREATE CHANGEFEED FOR d.foo WITH cursor=$1, end_time=$2`,
		tsLogical, eval.TimestampToDecimalDatum(endTime).String())
	defer closeFeed(t, testFeed)

	defer DiscardMessages(testFeed)()

	// Ensure the changefeed is able to complete in a reasonable amount of time.
	require.NoError(t, testFeed.(cdctest.EnterpriseTestFeed).WaitDurationForState(5*time.Minute, func(s jobs.State) bool {
		return s == jobs.StateSucceeded
	}))
}

func TestChangefeedPropagatesTerminalError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	opts := makeOptions(t)
	defer addCloudStorageOptions(t, &opts)()
	defer changefeedbase.TestingSetDefaultMinCheckpointFrequency(testSinkFlushFrequency)()
	defer testingUseFastRetry()()
	const numNodes = 3

	perServerKnobs := make(map[int]base.TestServerArgs, numNodes)
	for i := 0; i < numNodes; i++ {
		perServerKnobs[i] = base.TestServerArgs{
			Knobs: base.TestingKnobs{
				DistSQL: &execinfra.TestingKnobs{
					DrainFast:  true,
					Changefeed: &TestingKnobs{},
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
			ExternalIODir: opts.externalIODir,
			UseDatabase:   "d",
		}
	}

	tc := serverutils.StartCluster(t, numNodes,
		base.TestClusterArgs{
			ServerArgsPerNode: perServerKnobs,
			ReplicationMode:   base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				// Test uses SPLIT AT, which isn't currently supported for
				// secondary tenants. Tracked with #76378.
				DefaultTestTenant: base.TODOTestTenantDisabled,
			},
		})
	defer tc.Stopper().Stop(context.Background())

	{
		db := tc.ServerConn(1)
		sqlDB := sqlutils.MakeSQLRunner(db)
		serverutils.SetClusterSetting(t, tc, "kv.rangefeed.enabled", true)

		sqlDB.ExecMultiple(t,
			`CREATE DATABASE d;`,
			`CREATE TABLE foo (k INT PRIMARY KEY);`,
			`INSERT INTO foo (k) SELECT * FROM generate_series(1, 1000);`,
			`ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(1, 1000, 50));`,
		)
		for i := 1; i <= 1000; i += 50 {
			sqlDB.ExecSucceedsSoon(t, "ALTER TABLE foo EXPERIMENTAL_RELOCATE VALUES (ARRAY[$1], $2)", 1+(i%numNodes), i)
		}
	}
	// changefeed coordinator will run on this node.
	const coordinatorID = 0

	testFn := func(t *testing.T, nodesToFail []int, opts feedTestOptions) {
		for _, n := range nodesToFail {
			// Configure changefeed to emit fatal error on the specified nodes.
			distSQLKnobs := perServerKnobs[n].Knobs.DistSQL.(*execinfra.TestingKnobs)
			var numEmitted int32
			nodeToFail := n
			distSQLKnobs.Changefeed.(*TestingKnobs).BeforeEmitRow = func(ctx context.Context) error {
				// Emit few rows before returning an error.
				if atomic.AddInt32(&numEmitted, 1) > 10 {
					// Mark error as terminal, but make it a bit more
					// interesting by wrapping it few times.
					err := errors.Wrap(
						changefeedbase.WithTerminalError(
							pgerror.Wrapf(
								errors.Newf("synthetic fatal error from node %d", nodeToFail),
								pgcode.Io, "something happened with IO")),
						"while doing something")
					log.Errorf(ctx, "BeforeEmitRow returning error %s", err)
					return err
				}
				return nil
			}
		}

		defer func() {
			// Reset all changefeed knobs.
			for i := 0; i < numNodes; i++ {
				perServerKnobs[i].Knobs.DistSQL.(*execinfra.TestingKnobs).Changefeed = &TestingKnobs{}
			}
		}()

		sinkType := randomSinkTypeWithOptions(opts)
		f, closeSink := makeFeedFactoryWithOptions(t, sinkType, tc, tc.ServerConn(coordinatorID), opts)
		defer closeSink()
		feed := feed(t, f, "CREATE CHANGEFEED FOR foo")
		defer closeFeed(t, feed)

		// We don't know if we picked enterprise or core feed; regardless, consuming
		// from feed should eventually return an error.
		var feedErr error
		for feedErr == nil {
			_, feedErr = feed.Next()
		}
		log.Errorf(context.Background(), "feedErr=%s", feedErr)
		require.Regexp(t, "synthetic fatal error", feedErr)

		// enterprise feeds should also have the job marked failed.
		if jobFeed, ok := feed.(cdctest.EnterpriseTestFeed); ok {
			require.NoError(t, jobFeed.WaitForState(func(s jobs.State) bool { return s == jobs.StateFailed }))
		}
	}

	for _, tc := range []struct {
		name        string
		nodesToFail []int
		opts        feedTestOptions
	}{
		{
			name:        "coordinator",
			nodesToFail: []int{coordinatorID},
			opts:        opts,
		},
		{
			name:        "aggregator",
			nodesToFail: []int{2},
			opts:        opts.omitSinks("sinkless"), // Sinkless run on coordinator only.
		},
		{
			name:        "many aggregators",
			nodesToFail: []int{0, 2},
			opts:        opts.omitSinks("sinkless"), // Sinkless run on coordinator only.
		},
	} {
		t.Run(tc.name, func(t *testing.T) { testFn(t, tc.nodesToFail, tc.opts) })
	}
}

func TestChangefeedBackfillCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)

	rnd, _ := randutil.NewTestRand()
	var maxCheckpointSize int64

	drainUntilTimestamp := func(f cdctest.TestFeed, ts hlc.Timestamp) (err error) {
		var msg *cdctest.TestFeedMessage
		for msg, err = f.Next(); msg != nil; msg, err = f.Next() {
			if msg.Resolved != nil {
				resolvedTs := extractResolvedTimestamp(t, msg)
				if ts.LessEq(resolvedTs) {
					break
				}
			}
		}
		return err
	}

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		valRange := []int{1, 1000}
		sqlDB.Exec(t, `CREATE TABLE foo(a INT PRIMARY KEY)`)
		sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO foo (a) SELECT * FROM generate_series(%d, %d)`, valRange[0], valRange[1]))

		fooDesc := desctestutils.TestingGetPublicTableDescriptor(
			s.SystemServer.DB(), s.Codec, "d", "foo")
		tableSpan := fooDesc.PrimaryIndexSpan(s.Codec)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// Ensure Scan Requests are always small enough that we receive multiple
		// resolved events during a backfill
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) error {
			b.Header.MaxSpanRequestKeys = 1 + rnd.Int63n(100)
			return nil
		}

		// Emit resolved events for majority of spans.  Be extra paranoid and ensure that
		// we have at least 1 span for which we don't emit resolved timestamp (to force checkpointing).
		haveGaps := false
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (bool, error) {
			if r.Span.Equal(tableSpan) {
				// Do not emit resolved events for the entire table span.
				// We "simulate" large table by splitting single table span into many parts, so
				// we want to resolve those sub-spans instead of the entire table span.
				// However, we have to emit something -- otherwise the entire changefeed
				// machine would not work.
				r.Span.EndKey = tableSpan.Key.Next()
				return false, nil
			}
			if haveGaps {
				return rnd.Intn(10) > 7, nil
			}
			haveGaps = true
			return true, nil
		}

		// Checkpoint progress frequently, and set the checkpoint size limit.
		changefeedbase.SpanCheckpointInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 1)
		changefeedbase.SpanCheckpointMaxBytes.Override(
			context.Background(), &s.Server.ClusterSettings().SV, maxCheckpointSize)

		registry := s.Server.JobRegistry().(*jobs.Registry)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved='100ms'`)
		// Some test feeds (kafka) are not buffered, so we have to consume messages.
		var shouldDrain int32 = 1
		g := ctxgroup.WithContext(context.Background())
		g.Go(func() error {
			for {
				if shouldDrain == 0 {
					return nil
				}
				m, err := foo.Next()
				if err != nil {
					return err
				}

				if m.Resolved != nil {
					ts := extractResolvedTimestamp(t, m)
					if ts.IsEmpty() {
						return errors.New("unexpected epoch resolved event")
					}
				}
			}
		})

		defer func() {
			closeFeed(t, foo)
		}()

		jobFeed := foo.(cdctest.EnterpriseTestFeed)
		loadProgress := func() jobspb.Progress {
			jobID := jobFeed.JobID()
			job, err := registry.LoadJob(context.Background(), jobID)
			require.NoError(t, err)
			return job.Progress()
		}

		// Wait for non-nil checkpoint.
		testutils.SucceedsSoon(t, func() error {
			progress := loadProgress()
			if loadCheckpoint(t, progress) != nil {
				return nil
			}
			return errors.New("waiting for checkpoint")
		})

		// Pause the job and read and verify the latest checkpoint information.
		require.NoError(t, jobFeed.Pause())
		progress := loadProgress()
		require.NotNil(t, progress.GetChangefeed())
		h := progress.GetHighWater()
		noHighWater := h == nil || h.IsEmpty()
		require.True(t, noHighWater)

		spanLevelCheckpoint := loadCheckpoint(t, progress)
		require.NotNil(t, spanLevelCheckpoint)
		checkpointSpanGroup := makeSpanGroupFromCheckpoint(t, spanLevelCheckpoint)

		// Collect spans we attempt to resolve after when we resume.
		var resolved []roachpb.Span
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (bool, error) {
			if !r.Span.Equal(tableSpan) {
				resolved = append(resolved, r.Span)
			}
			return false, nil
		}

		var actualFrontierStr atomic.Value
		knobs.AfterCoordinatorFrontierRestore = func(frontier *resolvedspan.CoordinatorFrontier) {
			require.NotNil(t, frontier)
			actualFrontierStr.Store(frontier.String())
		}

		// Resume job.
		require.NoError(t, jobFeed.Resume())

		// Verify that the resumed job has restored the progress from the checkpoint
		// to the change frontier.
		expectedFrontier, err := span.MakeFrontier(tableSpan)
		if err != nil {
			t.Fatal(err)
		}
		assert.NoError(t, checkpoint.Restore(expectedFrontier, spanLevelCheckpoint))
		expectedFrontierStr := expectedFrontier.String()
		testutils.SucceedsSoon(t, func() error {
			if s := actualFrontierStr.Load(); s != nil {
				require.Equal(t, expectedFrontierStr, s)
				return nil
			}
			return errors.New("waiting for frontier to be restored")
		})

		// Wait for the high water mark to be non-zero.
		testutils.SucceedsSoon(t, func() error {
			prog := loadProgress()
			if p := prog.GetHighWater(); p != nil && !p.IsEmpty() {
				return nil
			}
			return errors.New("waiting for highwater")
		})

		// At this point, highwater mark should be set, and previous checkpoint should be gone.
		progress = loadProgress()
		require.NotNil(t, progress.GetChangefeed())
		require.Nil(t, loadCheckpoint(t, progress))

		// Verify that none of the resolved spans after resume were checkpointed.
		for _, sp := range resolved {
			require.Falsef(t, checkpointSpanGroup.Contains(sp.Key), "span should not have been resolved: %s", sp)
		}

		// Consume all potentially buffered kv events
		atomic.StoreInt32(&shouldDrain, 0)
		if err := g.Wait(); err != nil {
			require.NotRegexp(t, "unexpected epoch resolved event", err)
		}
		err = drainUntilTimestamp(foo, *progress.GetHighWater())
		require.NoError(t, err)

		// Verify that the checkpoint does not affect future scans
		sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN b STRING DEFAULT 'd'`)
		var expected []string
		for i := valRange[0]; i <= valRange[1]; i++ {
			expected = append(expected, fmt.Sprintf(
				`foo: [%d]->{"after": {"a": %d, "b": "d"}}`, i, i,
			))
		}
		assertPayloads(t, foo, expected)
	}

	// TODO(ssd): Tenant testing disabled because of use of DB()
	for _, sz := range []int64{100 << 20, 100} {
		maxCheckpointSize = sz
		cdcTestNamedWithSystem(t, fmt.Sprintf("limit=%s", humanize.Bytes(uint64(sz))), testFn, feedTestEnterpriseSinks)
	}
}

// TestCoreChangefeedBackfillScanCheckpoint tests that a core changefeed
// successfully completes the initial scan of a table when transient errors occur.
// This test only succeeds if checkpoints are taken.
func TestCoreChangefeedBackfillScanCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)

	rnd, _ := randutil.NewPseudoRand()

	rowCount := 10000

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo(a INT PRIMARY KEY)`)
		sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO foo (a) SELECT * FROM generate_series(%d, %d)`, 0, rowCount))

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// Ensure Scan Requests are always small enough that we receive multiple
		// resolved events during a backfill. Also ensure that checkpoint frequency
		// and size are large enough to induce several checkpoints when
		// writing `rowCount` rows.
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) error {
			b.Header.MaxSpanRequestKeys = 1 + rnd.Int63n(25)
			return nil
		}
		changefeedbase.SpanCheckpointInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 1)
		changefeedbase.SpanCheckpointMaxBytes.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 100<<20)

		emittedCount := 0
		errorCount := 0
		knobs.RaiseRetryableError = func() error {
			emittedCount++
			if emittedCount%200 == 0 {
				errorCount++
				return errors.New("test transient error")
			}
			return nil
		}

		foo := feed(t, f, `CREATE CHANGEFEED FOR TABLE foo`)
		defer closeFeed(t, foo)

		payloads := make([]string, rowCount+1)
		for i := 0; i < rowCount+1; i++ {
			payloads[i] = fmt.Sprintf(`foo: [%d]->{"after": {"a": %d}}`, i, i)
		}
		assertPayloads(t, foo, payloads)
		require.GreaterOrEqual(t, errorCount, 1)
	}

	cdcTest(t, testFn, feedTestForceSink("sinkless"))
}

func TestCheckpointFrequency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const frontierAdvanced = true
	const frontierDidNotAdvance = false

	// Test the logic around throttling of job progress updates.
	// It's pretty difficult to set up a fast end-to-end test since we need to simulate slow
	// job table update.  Instead, we just test canCheckpointHighWatermark directly.
	ts := timeutil.NewManualTime(timeutil.Now())
	js := newJobState(nil, /* job */
		cluster.MakeTestingClusterSettings(),
		MakeMetrics(time.Second, cidr.NewTestLookup()).(*Metrics), ts,
	)

	ctx := context.Background()

	require.False(t, js.canCheckpointHighWatermark(frontierDidNotAdvance))
	require.True(t, js.canCheckpointHighWatermark(frontierAdvanced))

	// Pretend our mean time to update progress is 1 minute, and we just updated progress.
	require.EqualValues(t, 0, js.checkpointDuration)
	js.checkpointCompleted(ctx, 12*time.Second)
	require.Less(t, int64(0), js.checkpointDuration.Nanoseconds())

	// Even though frontier advanced, we shouldn't checkpoint.
	require.False(t, js.canCheckpointHighWatermark(frontierAdvanced))
	require.True(t, js.progressUpdatesSkipped)

	// Once enough time elapsed, we allow progress update, even if frontier did not advance.
	ts.Advance(js.checkpointDuration)
	require.True(t, js.canCheckpointHighWatermark(frontierDidNotAdvance))

	// If we also specify minimum amount of time between updates, we would skip updates
	// until enough time has elapsed.
	minAdvance := 10 * time.Minute
	changefeedbase.ResolvedTimestampMinUpdateInterval.Override(ctx, &js.settings.SV, minAdvance)

	require.False(t, js.canCheckpointHighWatermark(frontierAdvanced))
	ts.Advance(minAdvance)
	require.True(t, js.canCheckpointHighWatermark(frontierAdvanced))

	// When we mark checkpoint completion, job state updated to reflect that.
	completionTime := timeutil.Now().Add(time.Hour)
	ts.AdvanceTo(completionTime)
	js.checkpointCompleted(ctx, 42*time.Second)
	require.Equal(t, completionTime, js.lastProgressUpdate)
	require.False(t, js.progressUpdatesSkipped)
}

func TestFlushJitter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test the logic around applying jitter to the flush logic.
	// The more involved test that would try to capture flush times would likely
	// be pretty flaky due to the fact that flush times do not happen at exactly
	// min_flush_frequency period, and thus it would be hard to tell if the
	// difference is due to jitter or not.  Just verify nextFlushWithJitter function
	// works as expected with controlled time source.

	ts := timeutil.NewManualTime(timeutil.Now())
	const numIters = 100

	for _, tc := range []struct {
		flushFrequency        time.Duration
		jitter                float64
		expectedFlushDuration time.Duration
		expectedErr           bool
	}{
		// Negative jitter.
		{
			flushFrequency:        -1,
			jitter:                -0.1,
			expectedFlushDuration: 0,
			expectedErr:           true,
		},
		{
			flushFrequency:        0,
			jitter:                -0.1,
			expectedFlushDuration: 0,
			expectedErr:           true,
		},
		{
			flushFrequency:        10 * time.Millisecond,
			jitter:                -0.1,
			expectedFlushDuration: 0,
			expectedErr:           true,
		},
		{
			flushFrequency:        100 * time.Millisecond,
			jitter:                -0.1,
			expectedFlushDuration: 0,
			expectedErr:           true,
		},
		// Disable Jitter.
		{
			flushFrequency:        -1,
			jitter:                0,
			expectedFlushDuration: 0,
			expectedErr:           true,
		},
		{
			flushFrequency:        0,
			jitter:                0,
			expectedFlushDuration: 0,
			expectedErr:           false,
		},
		{
			flushFrequency:        10 * time.Millisecond,
			jitter:                0,
			expectedFlushDuration: 10 * time.Millisecond,
			expectedErr:           false,
		},
		{
			flushFrequency:        100 * time.Millisecond,
			jitter:                0,
			expectedFlushDuration: 100 * time.Millisecond,
			expectedErr:           false,
		},
		// Enable Jitter.
		{
			flushFrequency:        -1,
			jitter:                0.1,
			expectedFlushDuration: 0,
			expectedErr:           true,
		},
		{
			flushFrequency:        0,
			jitter:                0.1,
			expectedFlushDuration: 0,
			expectedErr:           false,
		},
		{
			flushFrequency:        10 * time.Millisecond,
			jitter:                0.1,
			expectedFlushDuration: 10 * time.Millisecond,
			expectedErr:           false,
		},
		{
			flushFrequency:        100 * time.Millisecond,
			jitter:                0.1,
			expectedFlushDuration: 100 * time.Millisecond,
			expectedErr:           false,
		},
		// Expect actual jitter to be 0 since flushFrequency * jitter < 1.
		{
			flushFrequency:        1,
			jitter:                0.1,
			expectedFlushDuration: 1,
			expectedErr:           false,
		},
		// Expect actual jitter to be 0 since flushFrequency * jitter < 1.
		{
			flushFrequency:        10,
			jitter:                0.01,
			expectedFlushDuration: 10,
			expectedErr:           false,
		},
	} {
		t.Run(fmt.Sprintf("flushfrequency=%sjitter=%f", tc.flushFrequency, tc.jitter), func(t *testing.T) {
			for i := 0; i < numIters; i++ {
				next, err := nextFlushWithJitter(ts, tc.flushFrequency, tc.jitter)
				if tc.expectedErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				if tc.jitter > 0 {
					minBound := tc.expectedFlushDuration
					maxBound := tc.expectedFlushDuration + time.Duration(float64(tc.expectedFlushDuration)*tc.jitter)
					actualDuration := next.Sub(ts.Now())
					require.LessOrEqual(t, minBound, actualDuration)
					require.LessOrEqual(t, actualDuration, maxBound)
				} else {
					require.Equal(t, tc.expectedFlushDuration, next.Sub(ts.Now()))
				}
				ts.AdvanceTo(next)
			}
		})
	}
}

func TestChangefeedOrderingWithErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH updated`)
		webhookFoo := foo.(*webhookFeed)
		// retry, then fail, then restart changefeed and successfully send messages
		webhookFoo.mockSink.SetStatusCodes(append(repeatStatusCode(
			http.StatusInternalServerError,
			defaultRetryConfig().MaxRetries+1),
			[]int{http.StatusOK, http.StatusOK, http.StatusOK}...))
		defer closeFeed(t, foo)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (1, 'b')`)
		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
		assertPayloadsPerKeyOrderedStripTs(t, foo, []string{
			`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			`foo: [1]->{"after": {"a": 1, "b": "b"}}`,
			`foo: [1]->{"after": null}`,
		})

		webhookFoo.mockSink.SetStatusCodes([]int{http.StatusInternalServerError})
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (1, 'c')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (1, 'd')`)
		feedJob := foo.(cdctest.EnterpriseTestFeed)

		// check that running status correctly updates with retryable error
		testutils.SucceedsSoon(t, func() error {
			status, err := feedJob.FetchStatusMessage()
			if err != nil {
				return err
			}
			require.Regexp(t, "500 Internal Server Error", status)
			return nil
		})

		webhookFoo.mockSink.SetStatusCodes([]int{http.StatusOK})
		// retryable error should disappear after request becomes successful
		assertPayloadsPerKeyOrderedStripTs(t, foo, []string{
			`foo: [1]->{"after": {"a": 1, "b": "c"}}`,
			`foo: [1]->{"after": {"a": 1, "b": "d"}}`,
		})
	}

	// only used for webhook sink for now since it's the only testfeed where
	// we can control the ordering of errors
	cdcTest(t, testFn, feedTestForceSink("webhook"), feedTestNoExternalConnection, withAllowChangefeedErr("expects error"))
}

func TestChangefeedEndTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		endTimeReached := make(chan struct{})
		knobs.FeedKnobs.EndTimeReached = func() bool {
			select {
			case <-endTimeReached:
				return true
			default:
				return false
			}
		}

		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY)")
		sqlDB.Exec(t, "INSERT INTO foo VALUES (1), (2), (3)")

		fakeEndTime := s.Server.Clock().Now().Add(int64(time.Hour), 0).AsOfSystemTime()
		feed := feed(t, f, "CREATE CHANGEFEED FOR foo WITH end_time = $1", fakeEndTime)
		defer closeFeed(t, feed)

		assertPayloads(t, feed, []string{
			`foo: [1]->{"after": {"a": 1}}`,
			`foo: [2]->{"after": {"a": 2}}`,
			`foo: [3]->{"after": {"a": 3}}`,
		})

		close(endTimeReached)

		testFeed := feed.(cdctest.EnterpriseTestFeed)
		require.NoError(t, testFeed.WaitForState(func(s jobs.State) bool {
			return s == jobs.StateSucceeded
		}))
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestChangefeedEndTimeWithCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY)")

		var tsCursor string
		sqlDB.QueryRow(t, "SELECT (cluster_logical_timestamp())").Scan(&tsCursor)

		// Insert 1k rows -- using separate statements to get different MVCC timestamps.
		for i := 0; i < 1024; i++ {
			sqlDB.Exec(t, "INSERT INTO foo VALUES ($1)", i)
		}

		// Split table into multiple ranges to make things more interesting.
		sqlDB.Exec(t, "ALTER TABLE foo SPLIT AT VALUES (100), (200), (400), (800)")

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		fooSpan := func() roachpb.Span {
			fooDesc := desctestutils.TestingGetPublicTableDescriptor(
				s.Server.DB(), s.Codec, "d", "foo")
			return fooDesc.PrimaryIndexSpan(s.Codec)
		}()

		// Capture resolved events emitted during changefeed.  We expect
		// every range to emit resolved event with end_time timestamp.
		frontier, err := span.MakeFrontier(fooSpan)
		require.NoError(t, err)
		knobs.FilterSpanWithMutation = func(rs *jobspb.ResolvedSpan) (bool, error) {
			_, err := frontier.Forward(rs.Span, rs.Timestamp)
			return false, err
		}

		// endTime must be after creation time (5 seconds should be enough
		// to reach create changefeed statement and process it).
		endTime := s.Server.Clock().Now().AddDuration(5 * time.Second)
		feed := feed(t, f, "CREATE CHANGEFEED FOR foo WITH cursor = $1, end_time = $2, no_initial_scan",
			tsCursor, eval.TimestampToDecimalDatum(endTime).String())
		defer closeFeed(t, feed)

		// Don't care much about the values emitted (tested elsewhere) -- all
		// we want to make sure is that the feed terminates.  However, we do need
		// to consume those values since some of the test sink implementations (kafka)
		// will block.
		defer DiscardMessages(feed)()

		testFeed := feed.(cdctest.EnterpriseTestFeed)
		require.NoError(t, testFeed.WaitForState(func(s jobs.State) bool {
			return s == jobs.StateSucceeded
		}))

		// After changefeed completes, verify we have seen all ranges emit resolved
		// event with end_time timestamp.  That is: verify frontier.Frontier() is at end_time.
		expectedFrontier := endTime.Prev()
		testutils.SucceedsWithin(t, func() error {
			if expectedFrontier == frontier.Frontier() {
				return nil
			}
			return errors.Newf("still waiting for frontier to reach %s, current %s",
				expectedFrontier, frontier.Frontier())
		}, 5*time.Second)
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestChangefeedPredicates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(alias string) cdcTestFn {
		return func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(s.DB)
			sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
			sqlDB.Exec(t, `
CREATE TABLE foo (
  a INT,
  b STRING,
  c STRING,
  d STRING AS (concat(b, c)) VIRTUAL,
  e status DEFAULT 'inactive',
  PRIMARY KEY (a, b)
)`)

			// TODO(#85143): remove schema_change_policy='stop' from this test.
			sqlDB.Exec(t, `
INSERT INTO foo (a, b) VALUES (0, 'zero'), (1, 'one');
INSERT INTO foo (a, b, e) VALUES (2, 'two', 'closed');
`)
			topic, fromClause := "foo", "foo"
			if alias != "" {
				topic, fromClause = "foo", "foo AS "+alias
			}
			feed := feed(t, f, `
CREATE CHANGEFEED
WITH schema_change_policy='stop'
AS SELECT * FROM `+fromClause+`
WHERE e IN ('open', 'closed') AND event_op() != 'delete'`)
			defer closeFeed(t, feed)

			assertPayloads(t, feed, []string{
				topic + `: [2, "two"]->{"a": 2, "b": "two", "c": null, "e": "closed"}`,
			})

			sqlDB.Exec(t, `
UPDATE foo SET e = 'open', c = 'really open' WHERE a=0;  -- should be emitted
DELETE FROM foo WHERE a=2; -- should be skipped
INSERT INTO foo (a, b, e) VALUES (3, 'tres', 'closed'); -- should be emitted
`)

			assertPayloads(t, feed, []string{
				topic + `: [0, "zero"]->{"a": 0, "b": "zero", "c": "really open", "e": "open"}`,
				topic + `: [3, "tres"]->{"a": 3, "b": "tres", "c": null, "e": "closed"}`,
			})
		}
	}

	testutils.RunTrueAndFalse(t, "alias", func(t *testing.T, useAlias bool) {
		alias := ""
		if useAlias {
			alias = "bar"
		}
		cdcTest(t, testFn(alias))
	})
}

// Some predicates and projections can be verified when creating changefeed.
// The types of errors that can be detected early on is restricted to simple checks
// (such as type checking, non-existent columns, etc).  More complex errors detected
// during execution.
// Verify that's the case.
func TestChangefeedInvalidPredicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	_, db, stopServer := startTestFullServer(t, makeOptions(t, feedTestNoTenants))
	defer stopServer()
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
	sqlDB.Exec(t, `
CREATE TABLE foo (
  a INT,
  b STRING,
  c STRING,
  d STRING AS (concat(b, c)) VIRTUAL,
  e status DEFAULT 'inactive',
  PRIMARY KEY (a, b)
)`)

	for _, tc := range []struct {
		name   string
		create string
		err    string
	}{
		{
			name:   "no such column",
			create: `CREATE CHANGEFEED INTO 'null://' AS SELECT no_such_column FROM foo`,
			err:    `column "no_such_column" does not exist`,
		},
		{
			name:   "wrong type",
			create: `CREATE CHANGEFEED INTO 'null://' AS SELECT * FROM foo WHERE a = 'wrong type'`,
			err:    `could not parse "wrong type" as type int`,
		},
		{
			name:   "invalid enum value",
			create: `CREATE CHANGEFEED INTO 'null://' AS SELECT * FROM foo WHERE e = 'bad'`,
			err:    `invalid input value for enum status: "bad"`,
		},
		{
			name:   "contradiction: a > 1 && a < 1",
			create: `CREATE CHANGEFEED INTO 'null://'  AS SELECT * FROM foo WHERE a > 1 AND a < 1`,
			err:    `does not match any rows`,
		},
		{
			name:   "contradiction: a IS null",
			create: `CREATE CHANGEFEED INTO 'null://' AS SELECT * FROM foo WHERE a IS NULL`,
			err:    `does not match any rows`,
		},
		{
			name:   "wrong table name",
			create: `CREATE CHANGEFEED INTO 'null://' AS SELECT * FROM foo AS bar WHERE foo.a > 0`,
			err:    `no data source matches prefix: foo in this context`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB.ExpectErrWithTimeout(t, tc.err, tc.create)
		})
	}
}

func TestChangefeedFlushesSinkToReleaseMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stopServer := makeServer(t)
	defer stopServer()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	knobs := s.TestingKnobs.
		DistSQL.(*execinfra.TestingKnobs).
		Changefeed.(*TestingKnobs)

	// Arrange for a small memory budget.
	knobs.MemMonitor = startMonitorWithBudget(4096)

	// Ignore resolved events delivered to this changefeed.  This has
	// an effect of never advancing the frontier, and thus never flushing
	// the sink due to frontier advancement.  The only time we flush the sink
	// is if the memory pressure causes flush request to be delivered.
	knobs.FilterSpanWithMutation = func(_ *jobspb.ResolvedSpan) (bool, error) {
		return true, nil
	}

	// Arrange for custom sink to be used -- a sink that does not
	// release its resources.
	sink := &memoryHoggingSink{}
	knobs.WrapSink = func(_ Sink, _ jobspb.JobID) Sink {
		return sink
	}

	// Create table, and insert 123 rows in it -- this fills up
	// our tiny memory buffer (~26 rows do)
	sqlDB.Exec(t, `CREATE TABLE foo(key INT PRIMARY KEY DEFAULT unique_rowid(), val INT)`)
	sqlDB.Exec(t, `INSERT INTO foo (val) SELECT * FROM generate_series(1, 123)`)

	// Expect 123 rows from backfill.
	allEmitted := sink.expectRows(123)

	sqlDB.Exec(t, `CREATE CHANGEFEED FOR foo INTO 'http://host/does/not/matter'`)

	<-allEmitted
	require.Greater(t, sink.numFlushes(), 0)

	// Insert another set of rows.  This now uses rangefeeds.
	allEmitted = sink.expectRows(123)
	sqlDB.Exec(t, `INSERT INTO foo (val) SELECT * FROM generate_series(1, 123)`)
	<-allEmitted
	require.Greater(t, sink.numFlushes(), 0)
}

// Test verifies that KV feed does not leak event memory allocation
// when it reaches end_time or scan boundary.
func TestKVFeedDoesNotLeakMemoryWhenSkippingEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stopServer := makeServer(t)
	defer stopServer()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	knobs := s.TestingKnobs.
		DistSQL.(*execinfra.TestingKnobs).
		Changefeed.(*TestingKnobs)

	// Arrange for a small memory budget.
	knobs.MemMonitor = startMonitorWithBudget(4096)

	// Arrange for custom sink to be used -- a sink that counts emitted rows.
	sink := &countEmittedRowsSink{}
	knobs.WrapSink = func(_ Sink, _ jobspb.JobID) Sink {
		return sink
	}
	sqlDB.Exec(t, `CREATE TABLE foo(key INT PRIMARY KEY DEFAULT unique_rowid(), val INT)`)

	startTime := s.Server.Clock().Now().AsOfSystemTime()

	// Insert 123 rows -- this fills up our tiny memory buffer (~26 rows do)
	// Collect statement timestamp -- this will become our end time.
	var insertTimeStr string
	sqlDB.QueryRow(t,
		`INSERT INTO foo (val) SELECT * FROM generate_series(1, 123) RETURNING cluster_logical_timestamp();`,
	).Scan(&insertTimeStr)
	endTime := parseTimeToHLC(t, insertTimeStr).AsOfSystemTime()

	// Start the changefeed, with end_time set to be equal to the insert time.
	// KVFeed should ignore all events.
	var jobID jobspb.JobID
	sqlDB.QueryRow(t, `CREATE CHANGEFEED FOR foo INTO 'null:' WITH cursor = $1, end_time = $2`,
		startTime, endTime).Scan(&jobID)

	// If everything is fine (events are ignored, but their memory allocation is released),
	// the changefeed should terminate.  If not, we'll time out waiting for job.
	waitForJobState(sqlDB, t, jobID, jobs.StateSucceeded)

	// No rows should have been emitted (all should have been filtered out due to end_time).
	require.EqualValues(t, 0, atomic.LoadInt64(&sink.numRows))
}

func TestChangefeedTestTimesOut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		nada := feed(t, f, "CREATE CHANGEFEED FOR foo WITH resolved='100ms'")
		defer func() {
			// close could return an error due to the race in withTimeout function
			// which cancels the job.
			_ = nada.Close()
		}()

		expectResolvedTimestamp(t, nada) // Make sure feed is running.

		const expectTimeout = 500 * time.Millisecond
		var observedError error
		require.NoError(t,
			testutils.SucceedsWithinError(func() error {
				observedError = withTimeout(
					nada, expectTimeout,
					func(ctx context.Context) error {
						return assertPayloadsBaseErr(
							ctx, nada, []string{`nada: [2]->{"after": {}}`}, false, false, nil, changefeedbase.OptEnvelopeWrapped)
					})
				return nil
			}, 20*expectTimeout))

		require.Error(t, observedError)
	}

	cdcTest(t, testFn)
}

func TestChangefeedKafkaMessageTooLarge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		if KafkaV2Enabled.Get(&s.Server.ClusterSettings().SV) {
			// This is already covered for the v2 sink in another test: TestKafkaSinkClientV2_Resize
			return
		}

		changefeedbase.BatchReductionRetryEnabled.Override(
			context.Background(), &s.Server.ClusterSettings().SV, true)

		knobs := mustBeKafkaFeedFactory(f).knobs
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)

		t.Run(`succeed eventually if batches are rejected by the server for being too large`, func(t *testing.T) {
			// MaxMessages of 0 means unlimited
			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH kafka_sink_config='{"Flush": {"MaxMessages": 0}}'`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{
				`foo: [1]->{"after": {"a": 1}}`,
				`foo: [2]->{"after": {"a": 2}}`,
			})

			// Messages should be sent by a smaller and smaller MaxMessages config
			// only until ErrMessageSizeTooLarge is no longer returned.
			knobs.kafkaInterceptor = func(m *sarama.ProducerMessage, client kafkaClient) error {
				maxMessages := client.Config().Producer.Flush.MaxMessages
				if maxMessages == 0 || maxMessages >= 250 {
					return sarama.ErrMessageSizeTooLarge
				}
				require.Greater(t, maxMessages, 100)
				return nil
			}

			sqlDB.Exec(t, `INSERT INTO foo VALUES (3)`)
			sqlDB.Exec(t, `INSERT INTO foo VALUES (4)`)
			assertPayloads(t, foo, []string{
				`foo: [3]->{"after": {"a": 3}}`,
				`foo: [4]->{"after": {"a": 4}}`,
			})
			sqlDB.Exec(t, `INSERT INTO foo VALUES (5)`)
			sqlDB.Exec(t, `INSERT INTO foo VALUES (6)`)
			assertPayloads(t, foo, []string{
				`foo: [5]->{"after": {"a": 5}}`,
				`foo: [6]->{"after": {"a": 6}}`,
			})
		})

		t.Run(`succeed against a large backfill`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE large (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO large (a) SELECT * FROM generate_series(1, 2000);`)

			foo := feed(t, f, `CREATE CHANGEFEED FOR large WITH kafka_sink_config='{"Flush": {"MaxMessages": 1000}}'`)
			defer closeFeed(t, foo)

			rnd, _ := randutil.NewPseudoRand()

			maxFailures := int32(200)
			var numFailures atomic.Int32
			knobs.kafkaInterceptor = func(m *sarama.ProducerMessage, client kafkaClient) error {
				if client.Config().Producer.Flush.MaxMessages > 1 && numFailures.Add(1) < maxFailures && rnd.Int()%10 == 0 {
					return sarama.ErrMessageSizeTooLarge
				}
				return nil
			}

			var expected []string
			for i := 1; i <= 2000; i++ {
				expected = append(expected, fmt.Sprintf(
					`large: [%d]->{"after": {"a": %d}}`, i, i,
				))
			}
			assertPayloads(t, foo, expected)
		})

		// Validate that different failure scenarios result in a full changefeed retry
		sqlDB.Exec(t, `CREATE TABLE errors (a INT PRIMARY KEY);`)
		sqlDB.Exec(t, `INSERT INTO errors (a) SELECT * FROM generate_series(1, 1000);`)
		for _, failTest := range []struct {
			failInterceptor func(m *sarama.ProducerMessage, client kafkaClient) error
			errMsg          string
		}{
			{
				func(m *sarama.ProducerMessage, client kafkaClient) error {
					return sarama.ErrMessageSizeTooLarge
				},
				"kafka server: Message was too large, server rejected it to avoid allocation error",
			},
			{
				func(m *sarama.ProducerMessage, client kafkaClient) error {
					return errors.Errorf("unrelated error")
				},
				"unrelated error",
			},
			{
				func(m *sarama.ProducerMessage, client kafkaClient) error {
					maxMessages := client.Config().Producer.Flush.MaxMessages
					if maxMessages == 0 || maxMessages > 250 {
						return sarama.ErrMessageSizeTooLarge
					}
					return errors.Errorf("unrelated error mid-retry")
				},
				"unrelated error mid-retry",
			},
			{
				func() func(m *sarama.ProducerMessage, client kafkaClient) error {
					// Trigger an internal retry for the first message but have successive
					// messages throw a non-retryable error. This can happen in practice
					// when the second message is on a different topic to the first.
					startedBuffering := false
					return func(m *sarama.ProducerMessage, client kafkaClient) error {
						if !startedBuffering {
							startedBuffering = true
							return sarama.ErrMessageSizeTooLarge
						}
						return errors.Errorf("unrelated error mid-buffering")
					}
				}(),
				"unrelated error mid-buffering",
			},
		} {
			t.Run(fmt.Sprintf(`eventually surface error for retry: %s`, failTest.errMsg), func(t *testing.T) {
				knobs.kafkaInterceptor = failTest.failInterceptor
				foo := feed(t, f, `CREATE CHANGEFEED FOR errors WITH kafka_sink_config='{"Flush": {"MaxMessages": 0}}'`)
				defer closeFeed(t, foo)

				feedJob := foo.(cdctest.EnterpriseTestFeed)

				// check that running status correctly updates with retryable error
				testutils.SucceedsSoon(t, func() error {
					status, err := feedJob.FetchStatusMessage()
					if err != nil {
						return err
					}

					if !strings.Contains(status, failTest.errMsg) {
						return errors.Errorf("expected error to contain '%s', got: %v", failTest.errMsg, status)
					}
					return nil
				})
			})
		}
	}

	cdcTest(t, testFn, feedTestForceSink(`kafka`), withAllowChangefeedErr("expects kafka error"))
}

// TestPubsubValidationErrors tests error messages during pubsub sink URI validations.
func TestPubsubValidationErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stopServer := makeServer(t)
	defer stopServer()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	enableEnterprise := utilccl.TestingDisableEnterprise()
	enableEnterprise()

	for _, tc := range []struct {
		name          string
		uri           string
		expectedError string
	}{
		{
			name:          "project name",
			expectedError: "missing project name",
			uri:           "gcpubsub://?region={region}",
		},
		{
			name:          "region",
			expectedError: "region query parameter not found",
			uri:           "gcpubsub://myproject",
		},
		{
			name:          "credentials for default auth specified",
			expectedError: "missing credentials parameter",
			uri:           "gcpubsub://myproject?region={region}&AUTH=specified",
		},
		{
			name:          "base64",
			expectedError: "illegal base64 data",
			uri:           "gcpubsub://myproject?region={region}&CREDENTIALS={credentials}",
		},
		{
			name:          "invalid json",
			expectedError: "creating credentials from json: invalid character",
			uri: fmt.Sprintf("gcpubsub://myproject?region={region}&CREDENTIALS=%s",
				base64.StdEncoding.EncodeToString([]byte("invalid json"))),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB.ExpectErrWithTimeout(t, tc.expectedError, fmt.Sprintf("CREATE CHANGEFEED FOR foo INTO '%s'", tc.uri))
		})
	}
}

func TestChangefeedTopicNames(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		rand, _ := randutil.NewTestRand()
		cfg := randident.DefaultNameGeneratorConfig()
		cfg.Noise = true
		cfg.Finalize()
		ng := randident.NewNameGenerator(&cfg, rand, "table")

		names, _ := ng.GenerateMultiple(context.Background(), 100, make(map[string]struct{}))

		var escapedNames []string
		for _, name := range names {
			escapedNames = append(escapedNames, strings.ReplaceAll(name, `"`, `""`))
		}

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		for _, name := range escapedNames {
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE "%s" (a INT PRIMARY KEY);`, name))
			sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO "%s" VALUES (1);`, name))
		}

		var quotedNames []string
		for _, name := range escapedNames {
			quotedNames = append(quotedNames, "\""+name+"\"")
		}
		createStmt := fmt.Sprintf(`CREATE CHANGEFEED FOR %s`, strings.Join(quotedNames, ", "))
		foo := feed(t, f, createStmt)
		defer closeFeed(t, foo)

		var expected []string
		for _, name := range names {
			expected = append(expected, fmt.Sprintf(`%s: [1]->{"after": {"a": 1}}`, name))
		}
		assertPayloads(t, foo, expected)
	}

	cdcTest(t, testFn, feedTestForceSink("pubsub"))
}

// Regression test for #108450. When a changefeed hits a retryable error
// and retries, it should start with the most up-to-date highwater (ie. the
// highwater in the job record). If there is an error reading the highwater
// from the job record, there should be retries until we are able to get the
// highwater.
func TestHighwaterDoesNotRegressOnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		defer changefeedbase.TestingSetDefaultMinCheckpointFrequency(10 * time.Millisecond)()
		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// NB: We call this in a testing knob which runs in a separate goroutine, so we prefer
		// not to use `require.NoError` because that may panic.
		loadProgressErr := func(jobID jobspb.JobID, jobRegistry *jobs.Registry) (jobspb.Progress, error) {
			job, err := jobRegistry.LoadJob(context.Background(), jobID)
			if err != nil {
				return jobspb.Progress{}, err
			}
			return job.Progress(), nil
		}

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved = '10ms'`)
		defer closeFeed(t, foo)

		// Rough estimate of the statement time. The test only asserts that
		// things happen after the statement time. Asserting things happen after
		// this is good enough.
		initialHighwater := s.Server.Clock().Now()

		jobFeed := foo.(cdctest.EnterpriseTestFeed)
		jobRegistry := s.Server.JobRegistry().(*jobs.Registry)

		// Pause the changefeed to configure testing knobs which need the job ID.
		require.NoError(t, jobFeed.Pause())

		// A flag we toggle on to put the changefeed in a retrying state.
		var changefeedIsRetrying atomic.Bool
		knobs.RaiseRetryableError = func() error {
			if changefeedIsRetrying.Load() {
				return errors.New("test retryable error")
			}
			return nil
		}

		// NB: We use the errCh to return errors in testing knobs because they run in separate goroutines.
		// Avoid using `require` because it can panic and the goroutines may `recover()` the panic.
		doneCh := make(chan struct{}, 1)
		errCh := make(chan error, 1)
		sendErrWithCtx := func(ctx context.Context, err error) {
			t.Errorf("sending error: %s", err)
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
				return
			}
		}

		knobs.StartDistChangefeedInitialHighwater = func(ctx context.Context, retryHighwater hlc.Timestamp) {
			if changefeedIsRetrying.Load() {
				progress, err := loadProgressErr(jobFeed.JobID(), jobRegistry)
				if err != nil {
					sendErrWithCtx(ctx, err)
					return
				}
				progressHighwater := progress.GetHighWater()
				// Sanity check that the highwater is not nil, meaning that a
				// highwater timestamp was written to the job record.
				if progressHighwater == nil {
					sendErrWithCtx(ctx, errors.AssertionFailedf("job highwater is nil"))
					return
				}
				// Assert that the retry highwater is equal to the one in the job
				// record.
				if !progressHighwater.Equal(retryHighwater) {
					sendErrWithCtx(ctx, errors.AssertionFailedf("highwater %s does not match job highwater %s",
						retryHighwater, progressHighwater))
					return
				}
				// Terminate the test.
				t.Log("signalling for test completion")
				select {
				case <-ctx.Done():
					return
				case doneCh <- struct{}{}:
					return
				}
			}
		}

		loadJobErrCount := 2
		knobs.LoadJobErr = func() error {
			if loadJobErrCount > 0 {
				loadJobErrCount -= 1
				return errors.New("test error")
			}
			return nil
		}

		require.NoError(t, jobFeed.Resume())

		// Step 1: Wait for the highwater to advance. This guarantees that there is some highwater
		//         in the changefeed job record to use when retrying.
		testutils.SucceedsSoon(t, func() error {
			progress, err := loadProgressErr(jobFeed.JobID(), jobRegistry)
			if err != nil {
				return err
			}
			progressHighwater := progress.GetHighWater()
			if progressHighwater != nil && initialHighwater.Less(*progressHighwater) {
				changefeedIsRetrying.Store(true)
				return nil
			}
			return errors.Newf("waiting for highwater %s to advance ahead of initial highwater %s",
				progressHighwater, initialHighwater)
		})

		// Check that the following happens soon.
		//
		// Step 2: Since `changefeedIsRetrying` is true, the changefeed will now attempt retries in
		//         via `knobs.RaiseRetryableError`.
		// Step 3: `knobs.LoadJobErr` will result an in error when reading the job record a couple of times, causing
		//          more retries.
		// Step 4: Eventually, a dist changefeed is started at a certain highwater timestamp.
		//         `knobs.StartDistChangefeedInitialHighwater`. should see this stimetsamp and assert that it's the one
		//         from the job record.
		select {
		case <-time.After(30 * time.Second):
			t.Fatal("test timed out")
		case err := <-errCh:
			t.Fatal(err)
		case <-doneCh:
		}
	}
	cdcTest(t, testFn, feedTestEnterpriseSinks, withAllowChangefeedErr("injects error"))
}

// TestChangefeedPubsubResolvedMessages tests that the pubsub sink emits
// resolved messages to each topic.
func TestChangefeedPubsubResolvedMessages(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {

		db := sqlutils.MakeSQLRunner(s.DB)
		db.Exec(t, "CREATE TABLE one (i int)")
		db.Exec(t, "CREATE TABLE two (i int)")
		db.Exec(t, "CREATE TABLE three (i int)")

		foo, err := f.Feed("CREATE CHANGEFEED FOR TABLE one, TABLE two, TABLE three with resolved = '10ms'")
		require.NoError(t, err)

		seenTopics := make(map[string]struct{})
		expectedTopics := map[string]struct{}{
			"projects/testfeed/topics/one":   {},
			"projects/testfeed/topics/two":   {},
			"projects/testfeed/topics/three": {},
		}

		// There may be retries, so we could get the same resolved message for a topic more than once.
		testutils.SucceedsSoon(t, func() error {
			for i := 0; i < 3; i++ {
				// We should only see resolved messages since there is no data in the table.
				msg, err := foo.Next()
				require.NoError(t, err)
				seenTopics[msg.Topic] = struct{}{}
			}
			if !reflect.DeepEqual(seenTopics, expectedTopics) {
				return errors.Newf("failed to see expected resolved messages on each topic. seen: %v, expected: %v",
					seenTopics, expectedTopics)
			}
			return nil
		})

		require.NoError(t, foo.Close())
	}

	cdcTest(t, testFn, feedTestForceSink("pubsub"))
}

func TestChangefeedHeadersJSONVals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("event_processing=3"))

	// Make it easier to test the logs.
	jsonHeaderWrongValTypeLogLim = log.Every(0)
	jsonHeaderWrongTypeLogLim = log.Every(0)

	cases := []struct {
		name           string
		headersJSONStr string
		expected       cdctest.Headers
		warn           string
	}{
		{
			name:           "empty",
			headersJSONStr: `'{}'`,
			expected:       cdctest.Headers{},
		},
		{
			name:           "flat primitives - happy path",
			headersJSONStr: `'{"a": "b", "c": "d", "e": 42, "f": false}'`,
			expected: cdctest.Headers{
				{K: "a", V: []byte("b")},
				{K: "c", V: []byte("d")},
				{K: "e", V: []byte("42")},
				{K: "f", V: []byte("false")},
			},
		},
		{
			name:           "some bad some good",
			headersJSONStr: `'{"a": "b", "c": 1, "d": true, "e": null, "f": [1, 2, 3], "g": {"h": "i"}}'`,
			expected: cdctest.Headers{
				{K: "a", V: []byte("b")},
				{K: "c", V: []byte("1")},
				{K: "d", V: []byte("true")},
				// e will be skipped since its value is null. f and g will be skipped since they're non-primitive types.
			},
			warn: "must be a JSON object with primitive values",
		},
		{
			name:           "not an object",
			headersJSONStr: `'[1,2,3]'`,
			warn:           "must be a JSON object",
		},
		// Both types of nulls are ok.
		{
			name:           "sql null",
			headersJSONStr: `null`,
		},
		{
			name:           "json null",
			headersJSONStr: `'null'`,
		},
	}

	for _, format := range []string{"json", "avro"} {
		t.Run(format, func(t *testing.T) {
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
						spy := &changefeedLogSpy{}
						cleanup := log.InterceptWith(context.Background(), spy)
						defer cleanup()

						sqlDB := sqlutils.MakeSQLRunner(s.DB)
						sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, headerz JSONB)`)
						// Using fmt.Sprintf because it's tricky to specify sql null vs json null with params.
						sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO foo VALUES (1, %s::jsonb)`, c.headersJSONStr))

						foo := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR foo WITH headers_json_column_name=headerz, format=%s`, format))
						defer closeFeed(t, foo)

						headersStr := c.expected.String()
						if c.warn != "" {
							defer func() {
								spy.Lock()
								defer spy.Unlock()

								for _, log := range spy.logs {
									if strings.Contains(log, c.warn) {
										return
									}
								}
								t.Errorf("expected warning %q not found in logs: %v", c.warn, spy.logs)
							}()
						}
						key := "[1]"
						val := `{"after": {"a": 1}}`
						if format == "avro" {
							key = `{"a":{"long":1}}`
							val = `{"after":{"foo":{"a":{"long":1}}}}`
						}
						assertPayloads(t, foo, []string{fmt.Sprintf(`foo: %s%s->%s`, key, headersStr, val)})
					}
					cdcTest(t, testFn, feedTestForceSink("kafka"))
				})
			}
		})
	}
}

func TestChangefeedProtectedTimestampUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	verifyFunc := func() {}
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		defer verifyFunc()
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		// Checkpoint and trigger potential protected timestamp updates frequently.
		// Make the protected timestamp lag long enough that it shouldn't be
		// immediately updated after a restart.
		changefeedbase.SpanCheckpointInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 10*time.Millisecond)
		changefeedbase.ProtectTimestampInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 10*time.Millisecond)
		changefeedbase.ProtectTimestampLag.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 10*time.Hour)

		sqlDB.Exec(t, `CREATE TABLE foo (id INT)`)

		registry := s.Server.JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics)
		createPtsCount, _ := metrics.AggMetrics.Timers.PTSCreate.WindowedSnapshot().Total()
		managePtsCount, _ := metrics.AggMetrics.Timers.PTSManage.WindowedSnapshot().Total()
		managePTSErrorCount, _ := metrics.AggMetrics.Timers.PTSManageError.WindowedSnapshot().Total()
		require.Equal(t, int64(0), createPtsCount)
		require.Equal(t, int64(0), managePtsCount)
		require.Equal(t, int64(0), managePTSErrorCount)

		createStmt := `CREATE CHANGEFEED FOR foo WITH resolved='10ms', no_initial_scan`
		testFeed := feed(t, f, createStmt)
		defer closeFeed(t, testFeed)

		createPtsCount, _ = metrics.AggMetrics.Timers.PTSCreate.WindowedSnapshot().Total()
		managePtsCount, _ = metrics.AggMetrics.Timers.PTSManage.WindowedSnapshot().Total()
		require.Equal(t, int64(1), createPtsCount)
		require.Equal(t, int64(0), managePtsCount)

		eFeed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		// Wait for the changefeed to checkpoint and update PTS at least once.
		var lastHWM hlc.Timestamp
		checkHWM := func() error {
			hwm, err := eFeed.HighWaterMark()
			if err == nil && !hwm.IsEmpty() && lastHWM.Less(hwm) {
				lastHWM = hwm
				return nil
			}
			return errors.New("waiting for high watermark to advance")
		}
		testutils.SucceedsSoon(t, checkHWM)

		// Get the PTS of this feed.
		p, err := eFeed.Progress()
		require.NoError(t, err)

		ptsQry := fmt.Sprintf(`SELECT ts FROM system.protected_ts_records WHERE id = '%s'`, p.ProtectedTimestampRecord)
		var ts, ts2 string
		sqlDB.QueryRow(t, ptsQry).Scan(&ts)
		require.NoError(t, err)

		// Force the changefeed to restart.
		require.NoError(t, eFeed.Pause())
		require.NoError(t, eFeed.Resume())

		// Wait for a new checkpoint.
		testutils.SucceedsSoon(t, checkHWM)

		// Check that the PTS was not updated after the resume.
		sqlDB.QueryRow(t, ptsQry).Scan(&ts2)
		require.NoError(t, err)
		require.Equal(t, ts, ts2)

		// Lower the PTS lag and check that it has been updated.
		changefeedbase.ProtectTimestampLag.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 10*time.Millisecond)

		// Ensure that the resolved timestamp advances at least once
		// since the PTS lag override.
		testutils.SucceedsSoon(t, checkHWM)
		testutils.SucceedsSoon(t, checkHWM)

		sqlDB.QueryRow(t, ptsQry).Scan(&ts2)
		require.NoError(t, err)
		require.Less(t, ts, ts2)

		managePtsCount, _ = metrics.AggMetrics.Timers.PTSManage.WindowedSnapshot().Total()
		managePTSErrorCount, _ = metrics.AggMetrics.Timers.PTSManageError.WindowedSnapshot().Total()
		require.GreaterOrEqual(t, managePtsCount, int64(2))
		require.Equal(t, int64(0), managePTSErrorCount)
	}

	withTxnRetries := withArgsFn(func(args *base.TestServerArgs) {
		requestFilter, vf := testutils.TestingRequestFilterRetryTxnWithPrefix(t, changefeedJobProgressTxnName, 1)
		args.Knobs.Store = &kvserver.StoreTestingKnobs{
			TestingRequestFilter: requestFilter,
		}
		verifyFunc = vf
	})

	cdcTest(t, testFn, feedTestForceSink("kafka"), withTxnRetries)
}

func TestChangefeedProtectedTimestampUpdateError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		// Checkpoint and trigger potential protected timestamp updates frequently.
		// Make the protected timestamp lag long enough that it shouldn't be
		// immediately updated after a restart.
		changefeedbase.SpanCheckpointInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 10*time.Millisecond)
		changefeedbase.ProtectTimestampInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 10*time.Millisecond)
		changefeedbase.ProtectTimestampLag.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 10*time.Hour)

		sqlDB.Exec(t, `CREATE TABLE foo (id INT)`)

		registry := s.Server.JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics)
		createPtsCount, _ := metrics.AggMetrics.Timers.PTSCreate.WindowedSnapshot().Total()
		managePtsCount, _ := metrics.AggMetrics.Timers.PTSManage.WindowedSnapshot().Total()
		managePTSErrorCount, _ := metrics.AggMetrics.Timers.PTSManageError.WindowedSnapshot().Total()
		require.Equal(t, int64(0), createPtsCount)
		require.Equal(t, int64(0), managePtsCount)
		require.Equal(t, int64(0), managePTSErrorCount)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		knobs.ManagePTSError = func() error {
			return errors.New("test error")
		}

		createStmt := `CREATE CHANGEFEED FOR foo WITH resolved='10ms', no_initial_scan`
		testFeed := feed(t, f, createStmt)
		defer closeFeed(t, testFeed)

		createPtsCount, _ = metrics.AggMetrics.Timers.PTSCreate.WindowedSnapshot().Total()
		require.Equal(t, int64(1), createPtsCount)
		managePTSErrorCount, _ = metrics.AggMetrics.Timers.PTSManageError.WindowedSnapshot().Total()
		require.Equal(t, int64(0), managePTSErrorCount)

		// Lower the PTS lag to trigger a PTS update.
		changefeedbase.ProtectTimestampLag.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 10*time.Millisecond)

		testutils.SucceedsSoon(t, func() error {
			managePTSErrorCount, _ = metrics.AggMetrics.Timers.PTSManageError.WindowedSnapshot().Total()
			if managePTSErrorCount > 0 {
				fmt.Println("manage protected timestamps test: manage pts error count", managePTSErrorCount)
				return nil
			}
			return errors.New("waiting for manage pts error")
		})
	}
	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestCDCQuerySelectSingleRow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	errCh := make(chan error, 1)
	knobsFn := func(knobs *base.TestingKnobs) {
		if knobs.DistSQL == nil {
			knobs.DistSQL = &execinfra.TestingKnobs{}
		}
		if knobs.DistSQL.(*execinfra.TestingKnobs).Changefeed == nil {
			knobs.DistSQL.(*execinfra.TestingKnobs).Changefeed = &TestingKnobs{}
		}
		cfKnobs := knobs.DistSQL.(*execinfra.TestingKnobs).Changefeed.(*TestingKnobs)
		cfKnobs.HandleDistChangefeedError = func(err error) error {
			// Only capture the first error -- that's enough for the test.
			select {
			case errCh <- err:
			default:
			}
			return err
		}
	}

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		db := sqlutils.MakeSQLRunner(s.DB)
		db.Exec(t, `CREATE TABLE foo (key INT PRIMARY KEY);`)
		db.Exec(t, `INSERT INTO foo VALUES (1), (2), (3);`)

		// initial_scan='only' is not required, but it makes testing this easier.
		foo := feed(t, f, `CREATE CHANGEFEED WITH initial_scan='only' AS SELECT * FROM foo WHERE key = 1`)
		defer closeFeed(t, foo)

		done := make(chan struct{})
		go func() {
			defer close(done)
			assertPayloads(t, foo, []string{`foo: [1]->{"key": 1}`})
		}()

		select {
		case err := <-errCh:
			// Ignore any error after the above assertion completed, because
			// it's likely just due to feed shutdown.
			select {
			case <-done:
			default:
				t.Fatalf("unexpected error: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("timed out")
		case <-done:
			return
		}
	}
	cdcTest(t, testFn, withKnobsFn(knobsFn))
}

// TestChangefeedAsSelectForEmptyTable verifies that a changefeed
// yields a proper user error on an empty table and in the same
// allows hidden columns to be selected.
func TestChangefeedAsSelectForEmptyTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE empty()`)
		sqlDB.Exec(t, `INSERT INTO empty DEFAULT VALUES`)
		// Should fail when no columns are selected.
		// Use expectErrCreatingFeed which handles sinkless feeds correctly by
		// attempting to read from the feed if no error occurs at creation time
		expectErrCreatingFeed(t, f, `CREATE CHANGEFEED AS SELECT * FROM empty`, `SELECT yields no columns`)

		// Should succeed when a rowid column is explicitly selected.
		feed, err := f.Feed(`CREATE CHANGEFEED AS SELECT rowid FROM empty`)
		require.NoError(t, err)
		defer closeFeed(t, feed)
	}

	cdcTest(t, testFn)
}

func TestCloudstorageParallelCompression(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test only provides value under race, as it's explicitly testing for
	// data races between feeds.
	skip.UnlessUnderRace(t)

	const numFeedsEach = 10

	testutils.RunValues(t, "compression", []string{"zstd", "gzip"}, func(t *testing.T, compression string) {
		opts := makeOptions(t)
		opts.externalIODir = t.TempDir()
		s, cleanup := makeServerWithOptions(t, opts)
		defer cleanup()

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT);`)
		sqlDB.Exec(t, `INSERT INTO foo (a) SELECT * FROM generate_series(1, 5000);`)

		t.Logf("inserted into table")

		var jobIDs []int
		for i := range numFeedsEach {
			var jobID int
			sqlDB.QueryRow(t, fmt.Sprintf(`CREATE CHANGEFEED FOR foo INTO 'nodelocal://1/%d-testout' WITH compression='%s', initial_scan='only', format='parquet';`, i, compression)).Scan(&jobID)
			jobIDs = append(jobIDs, jobID)
		}

		t.Logf("created changefeeds")

		const duration = 3 * time.Minute
		const checkStatusInterval = 10 * time.Second

		for start := timeutil.Now(); timeutil.Since(start) < duration; {
			// Check the statuses of the jobs.
			for _, jobID := range jobIDs {
				var status string
				sqlDB.QueryRow(t, `SELECT status FROM [SHOW JOBS] WHERE job_id = $1`, jobID).Scan(&status)
				if status != "succeeded" && status != "running" {
					t.Fatalf("job %d entered unknown state: %s", jobID, status)
				}
			}
			time.Sleep(checkStatusInterval)
		}
	})
}

func TestChangefeedAdditionalHeadersDoesntWorkWithV1KafkaSink(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.new_kafka_sink.enabled = false`)

		sqlDB.Exec(t, `CREATE TABLE foo (key INT PRIMARY KEY);`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1);`)

		_, err := f.Feed(`CREATE CHANGEFEED FOR foo WITH extra_headers='{"X-Someheader": "somevalue"}'`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "headers are not supported for the v1 kafka sink")
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestDatabaseRenameDuringDatabaseLevelChangefeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE DATABASE foo;`)
		sqlDB.Exec(t, `CREATE TABLE foo.bar (id INT PRIMARY KEY);`)
		sqlDB.Exec(t, `INSERT INTO foo.bar VALUES (1);`)
		expectedRows := []string{
			`bar: [1]->{"after": {"id": 1}}`,
		}
		feed1 := feed(t, f, `CREATE CHANGEFEED FOR DATABASE foo`)
		defer closeFeed(t, feed1)
		assertPayloads(t, feed1, expectedRows)

		sqlDB.Exec(t, `ALTER DATABASE foo RENAME TO bar;`)
		sqlDB.Exec(t, `INSERT INTO bar.bar VALUES (2);`)
		expectedRows = []string{
			`bar: [2]->{"after": {"id": 2}}`,
		}
		assertPayloads(t, feed1, expectedRows)
	}
	cdcTest(t, testFn)
}

func TestTableRenameDuringDatabaseLevelChangefeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE DATABASE foo;`)
		sqlDB.Exec(t, `CREATE TABLE foo.bar (id INT PRIMARY KEY);`)
		sqlDB.Exec(t, `INSERT INTO foo.bar VALUES (1);`)
		expectedRows := []string{
			`bar: [1]->{"after": {"id": 1}}`,
		}
		feed1 := feed(t, f, `CREATE CHANGEFEED FOR DATABASE foo`)
		defer closeFeed(t, feed1)
		assertPayloads(t, feed1, expectedRows)

		sqlDB.Exec(t, `ALTER TABLE foo.bar RENAME TO foo;`)
		sqlDB.Exec(t, `INSERT INTO foo.foo VALUES (2);`)
		expectedRows = []string{
			`bar: [2]->{"after": {"id": 2}}`,
		}
		assertPayloads(t, feed1, expectedRows)
	}
	cdcTest(t, testFn)
}
