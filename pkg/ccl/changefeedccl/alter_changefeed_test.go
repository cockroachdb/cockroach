// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestAlterChangefeedAddTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobStatus(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobStatus(sqlDB, t, feed.JobID(), `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES(1)`)
		assertPayloads(t, testFeed, []string{
			`foo: [1]->{"after": {"a": 1}}`,
		})

		sqlDB.Exec(t, `INSERT INTO bar VALUES(2)`)
		assertPayloads(t, testFeed, []string{
			`bar: [2]->{"after": {"a": 2}}`,
		})
	}

	t.Run(`kafka`, kafkaTest(testFn))
}

func TestAlterChangefeedDropTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobStatus(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d DROP bar`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobStatus(sqlDB, t, feed.JobID(), `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES(1)`)
		assertPayloads(t, testFeed, []string{
			`foo: [1]->{"after": {"a": 1}}`,
		})

		sqlDB.Exec(t, `INSERT INTO bar VALUES(2)`)
		assertPayloads(t, testFeed, nil)
	}

	t.Run(`kafka`, kafkaTest(testFn))
}

func TestAlterChangefeedSetDiffOption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobStatus(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET diff`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobStatus(sqlDB, t, feed.JobID(), `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		assertPayloads(t, testFeed, []string{
			`foo: [0]->{"after": {"a": 0, "b": "initial"}, "before": null}`,
		})
	}

	t.Run(`kafka`, kafkaTest(testFn))
}

func TestAlterChangefeedUnsetDiffOption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo WITH diff`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobStatus(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d UNSET diff`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobStatus(sqlDB, t, feed.JobID(), `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		assertPayloads(t, testFeed, []string{
			`foo: [0]->{"after": {"a": 0, "b": "initial"}}`,
		})
	}

	t.Run(`kafka`, kafkaTest(testFn))
}

func TestAlterChangefeedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.ExpectErr(t,
			`could not load job with job id -1`,
			`ALTER CHANGEFEED -1 ADD bar`,
		)

		sqlDB.Exec(t, `ALTER TABLE bar ADD COLUMN b INT`)
		var alterTableJobID jobspb.JobID
		sqlDB.QueryRow(t, `SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE'`).Scan(&alterTableJobID)
		sqlDB.ExpectErr(t,
			fmt.Sprintf(`job %d is not changefeed job`, alterTableJobID),
			fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar`, alterTableJobID),
		)

		sqlDB.ExpectErr(t,
			fmt.Sprintf(`job %d is not paused`, feed.JobID()),
			fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar`, feed.JobID()),
		)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobStatus(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.ExpectErr(t,
			`pq: target "baz" does not exist`,
			fmt.Sprintf(`ALTER CHANGEFEED %d ADD baz`, feed.JobID()),
		)
		sqlDB.ExpectErr(t,
			`pq: target "baz" does not exist`,
			fmt.Sprintf(`ALTER CHANGEFEED %d DROP baz`, feed.JobID()),
		)
		sqlDB.ExpectErr(t,
			`pq: invalid option "qux"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d SET qux`, feed.JobID()),
		)
		sqlDB.ExpectErr(t,
			`pq: cannot alter option "initial_scan"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d SET initial_scan`, feed.JobID()),
		)
		sqlDB.ExpectErr(t,
			`pq: invalid option "qux"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d UNSET qux`, feed.JobID()),
		)
		sqlDB.ExpectErr(t,
			`pq: cannot alter option "initial_scan"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d UNSET initial_scan`, feed.JobID()),
		)

		sqlDB.ExpectErr(t,
			`cannot unset option "sink"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d UNSET sink`, feed.JobID()),
		)

		sqlDB.ExpectErr(t,
			`pq: invalid option "diff"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar WITH diff`, feed.JobID()),
		)

		sqlDB.ExpectErr(t,
			`pq: cannot specify both "initial_scan" and "no_initial_scan"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar WITH initial_scan, no_initial_scan`, feed.JobID()),
		)
	}

	t.Run(`kafka`, kafkaTest(testFn))
}

func TestAlterChangefeedDropAllTargetsError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobStatus(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.ExpectErr(t,
			fmt.Sprintf(`cannot drop all targets for changefeed job %d`, feed.JobID()),
			fmt.Sprintf(`ALTER CHANGEFEED %d DROP foo, bar`, feed.JobID()),
		)
	}

	t.Run(`kafka`, kafkaTest(testFn))
}

// The purpose of this test is to ensure that the ALTER CHANGEFEED statement
// does not accidentally redact secret keys in the changefeed details
func TestAlterChangefeedPersistSinkURI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, rawSQLDB, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)
	registry := s.JobRegistry().(*jobs.Registry)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	query := `CREATE TABLE foo (a string)`
	sqlDB.Exec(t, query)

	query = `CREATE TABLE bar (b string)`
	sqlDB.Exec(t, query)

	query = `SET CLUSTER SETTING kv.rangefeed.enabled = true`
	sqlDB.Exec(t, query)

	var changefeedID jobspb.JobID

	doneCh := make(chan struct{})
	defer close(doneCh)
	registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
		jobspb.TypeChangefeed: func(raw jobs.Resumer) jobs.Resumer {
			r := fakeResumer{
				done: doneCh,
			}
			return &r
		},
	}

	query = `CREATE CHANGEFEED FOR TABLE foo, bar INTO
		's3://fake-bucket-name/fake/path?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456'`
	sqlDB.QueryRow(t, query).Scan(&changefeedID)

	sqlDB.Exec(t, `PAUSE JOB $1`, changefeedID)
	waitForJobStatus(sqlDB, t, changefeedID, `paused`)

	sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET diff`, changefeedID))

	sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, changefeedID))
	waitForJobStatus(sqlDB, t, changefeedID, `running`)

	job, err := registry.LoadJob(ctx, changefeedID)
	require.NoError(t, err)
	details, ok := job.Details().(jobspb.ChangefeedDetails)
	require.True(t, ok)

	require.Equal(t, details.SinkURI, `s3://fake-bucket-name/fake/path?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456`)
}

func TestAlterChangefeedChangeSinkTypeError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobStatus(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.ExpectErr(t,
			`pq: New sink type "s3" does not match original sink type "kafka". Altering the sink type of a changefeed is disallowed, consider creating a new changefeed instead.`,
			fmt.Sprintf(`ALTER CHANGEFEED %d SET sink = 's3://fake-bucket-name/fake/path?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456'`, feed.JobID()),
		)
	}

	t.Run(`kafka`, kafkaTest(testFn))
}

func TestAlterChangefeedChangeSinkURI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		registry := f.Server().JobRegistry().(*jobs.Registry)
		ctx := context.Background()

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobStatus(sqlDB, t, feed.JobID(), `paused`)

		newSinkURI := `kafka://new_kafka_uri`

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET sink = '%s'`, feed.JobID(), newSinkURI))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobStatus(sqlDB, t, feed.JobID(), `running`)

		job, err := registry.LoadJob(ctx, feed.JobID())
		require.NoError(t, err)
		details, ok := job.Details().(jobspb.ChangefeedDetails)
		require.True(t, ok)

		require.Equal(t, newSinkURI, details.SinkURI)
	}

	t.Run(`kafka`, kafkaTest(testFn))
}

func TestAlterChangefeedInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved = '1s'`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobStatus(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1), (2), (3)`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar WITH initial_scan`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobStatus(sqlDB, t, feed.JobID(), `running`)

		assertPayloads(t, testFeed, []string{
			`bar: [1]->{"after": {"a": 1}}`,
			`bar: [2]->{"after": {"a": 2}}`,
			`bar: [3]->{"after": {"a": 3}}`,
		})

		sqlDB.Exec(t, `INSERT INTO bar VALUES (4)`)
		assertPayloads(t, testFeed, []string{
			`bar: [4]->{"after": {"a": 4}}`,
		})
	}

	t.Run(`kafka`, kafkaTest(testFn))
}

func TestAlterChangefeedNoInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved = '1s'`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobStatus(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1), (2), (3)`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar WITH no_initial_scan`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobStatus(sqlDB, t, feed.JobID(), `running`)

		expectResolvedTimestamp(t, testFeed)

		sqlDB.Exec(t, `INSERT INTO bar VALUES (4)`)
		assertPayloads(t, testFeed, []string{
			`bar: [4]->{"after": {"a": 4}}`,
		})
	}

	t.Run(`kafka`, kafkaTest(testFn))
}

func TestAlterChangefeedAddTargetsDuringBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)

	rnd, _ := randutil.NewTestRand()
	var maxCheckpointSize int64 = 100

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo(val INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo (val) SELECT * FROM generate_series(0, 999)`)

		fooDesc := desctestutils.TestingGetPublicTableDescriptor(
			f.Server().DB(), keys.SystemSQLCodec, "d", "foo")
		fooTableSpan := fooDesc.PrimaryIndexSpan(keys.SystemSQLCodec)

		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// Ensure Scan Requests are always small enough that we receive multiple
		// resolvedFoo events during a backfill
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) {
			b.Header.MaxSpanRequestKeys = 1 + rnd.Int63n(100)
		}

		// Emit resolvedFoo events for majority of spans.  Be extra paranoid and ensure that
		// we have at least 1 span for which we don't emit resolvedFoo timestamp (to force checkpointing).
		haveGaps := false
		knobs.ShouldSkipResolved = func(r *jobspb.ResolvedSpan) bool {
			if r.Span.Equal(fooTableSpan) {
				// Do not emit resolvedFoo events for the entire table span.
				// We "simulate" large table by splitting single table span into many parts, so
				// we want to resolve those sub-spans instead of the entire table span.
				// However, we have to emit something -- otherwise the entire changefeed
				// machine would not work.
				r.Span.EndKey = fooTableSpan.Key.Next()
				return false
			}
			if haveGaps {
				return rnd.Intn(10) > 7
			}
			haveGaps = true
			return true
		}

		// Checkpoint progress frequently, and set the checkpoint size limit.
		changefeedbase.FrontierCheckpointFrequency.Override(
			context.Background(), &f.Server().ClusterSettings().SV, 1)
		changefeedbase.FrontierCheckpointMaxBytes.Override(
			context.Background(), &f.Server().ClusterSettings().SV, maxCheckpointSize)

		registry := f.Server().JobRegistry().(*jobs.Registry)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved = '100ms'`)

		// Kafka feeds are not buffered, so we have to consume messages.
		g := ctxgroup.WithContext(context.Background())
		g.Go(func() error {
			expectedValues := make([]string, 1000)
			for j := 0; j <= 999; j++ {
				expectedValues[j] = fmt.Sprintf(`foo: [%d]->{"after": {"val": %d}}`, j, j)
			}
			err := assertPayloadsBaseErr(testFeed, expectedValues, false, false)
			if err != nil {
				return err
			}

			for j := 0; j <= 999; j++ {
				expectedValues[j] = fmt.Sprintf(`bar: [%d]->{"after": {"val": %d}}`, j, j)
			}
			err = assertPayloadsBaseErr(testFeed, expectedValues, false, false)
			return err
		})

		defer func() {
			require.NoError(t, g.Wait())
			closeFeed(t, testFeed)
		}()

		jobFeed := testFeed.(cdctest.EnterpriseTestFeed)
		loadProgress := func() jobspb.Progress {
			jobID := jobFeed.JobID()
			job, err := registry.LoadJob(context.Background(), jobID)
			require.NoError(t, err)
			return job.Progress()
		}

		// Wait for non-nil checkpoint.
		testutils.SucceedsSoon(t, func() error {
			progress := loadProgress()
			if p := progress.GetChangefeed(); p != nil && p.Checkpoint != nil && len(p.Checkpoint.Spans) > 0 {
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

		jobCheckpoint := progress.GetChangefeed().Checkpoint
		require.Less(t, 0, len(jobCheckpoint.Spans))
		var checkpoint roachpb.SpanGroup
		checkpoint.Add(jobCheckpoint.Spans...)

		sqlDB.Exec(t, `CREATE TABLE bar(val INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO bar (val) SELECT * FROM generate_series(0, 999)`)

		sqlDB.Exec(t, `CREATE TABLE baz(val INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO baz (val) SELECT * FROM generate_series(0, 999)`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar WITH initial_scan ADD baz WITH no_initial_scan`, jobFeed.JobID()))

		// Collect spans we attempt to resolve after when we resume.
		var resolvedFoo []roachpb.Span
		knobs.ShouldSkipResolved = func(r *jobspb.ResolvedSpan) bool {
			if !r.Span.Equal(fooTableSpan) {
				resolvedFoo = append(resolvedFoo, r.Span)
			}
			return false
		}

		require.NoError(t, jobFeed.Resume())

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
		require.Equal(t, 0, len(progress.GetChangefeed().Checkpoint.Spans))

		// Verify that none of the resolvedFoo spans after resume were checkpointed.
		for _, sp := range resolvedFoo {
			require.Falsef(t, checkpoint.Contains(sp.Key), "span should not have been resolved: %s", sp)
		}
	}

	t.Run(`kafka`, kafkaTest(testFn, feedTestNoTenants))
}
