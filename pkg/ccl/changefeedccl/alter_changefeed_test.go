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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
