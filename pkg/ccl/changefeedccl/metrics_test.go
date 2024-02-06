// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestRunningAndTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)
		sliMetric, err := registry.MetricsStruct().Changefeed.(*Metrics).getSLIMetrics(defaultSLIScope)
		require.NoError(t, err)
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

		// Create a changefeed on one table.
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		waitForChangefeedRunning(t, sliMetric, 1)
		require.EqualValues(t, 1, sliMetric.WatchedTargets.Value())

		// Create a changefeed on the same table.
		foo2 := feed(t, f, `CREATE CHANGEFEED AS SELECT a FROM foo`)
		waitForChangefeedRunning(t, sliMetric, 2)
		require.EqualValues(t, 1, sliMetric.WatchedTargets.Value())

		// Create a changefeed on a different table.
		bar := feed(t, f, `CREATE CHANGEFEED FOR bar`)
		waitForChangefeedRunning(t, sliMetric, 3)
		require.EqualValues(t, 2, sliMetric.WatchedTargets.Value())

		// Close a changefeed on the same table as another. The number of tables
		// watched should remain the same.
		closeFeed(t, foo2)
		waitForChangefeedRunning(t, sliMetric, 2)
		require.EqualValues(t, 2, sliMetric.WatchedTargets.Value())

		// Close a changefeed.
		closeFeed(t, foo)
		waitForChangefeedRunning(t, sliMetric, 1)
		require.EqualValues(t, 1, sliMetric.WatchedTargets.Value())

		// Create a changefeed with two tables, one of which is already watched.
		foobar := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		waitForChangefeedRunning(t, sliMetric, 2)
		require.EqualValues(t, 2, sliMetric.WatchedTargets.Value())

		// Close the changefeed watching one table.
		closeFeed(t, bar)
		waitForChangefeedRunning(t, sliMetric, 1)
		require.EqualValues(t, 2, sliMetric.WatchedTargets.Value())

		// Close the changefeed watching two tables.
		closeFeed(t, foobar)
		waitForChangefeedRunning(t, sliMetric, 0)
		require.EqualValues(t, 0, sliMetric.WatchedTargets.Value())
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestRunningAndTablesPause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)
		sliMetric, err := registry.MetricsStruct().Changefeed.(*Metrics).getSLIMetrics(defaultSLIScope)
		require.NoError(t, err)
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

		foobar := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		waitForChangefeedRunning(t, sliMetric, 1)
		require.EqualValues(t, 2, sliMetric.WatchedTargets.Value())

		// The running count should be decremented when the job is paused, but the
		// number of tables watched remains the same.
		testfeed, ok := foobar.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)
		require.NoError(t, testfeed.Pause())
		require.EqualValues(t, 2, sliMetric.WatchedTargets.Value())

		// Resuming the job should increase the running count again.
		require.NoError(t, testfeed.Resume())
		waitForChangefeedRunning(t, sliMetric, 1)
		require.EqualValues(t, 2, sliMetric.WatchedTargets.Value())

		// Pause the job again and cancel the changefeed while paused.
		require.NoError(t, testfeed.Pause())
		require.EqualValues(t, 2, sliMetric.WatchedTargets.Value())
		require.EqualValues(t, 0, sliMetric.RunningCount.Value())
		closeFeed(t, foobar)
		waitForJobStatus(sqlDB, t, testfeed.JobID(), `canceled`)
		require.EqualValues(t, 0, sliMetric.WatchedTargets.Value())
		require.EqualValues(t, 0, sliMetric.RunningCount.Value())
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestTablesAlter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)
		sliMetric, err := registry.MetricsStruct().Changefeed.(*Metrics).getSLIMetrics(defaultSLIScope)
		require.NoError(t, err)
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

		// Create a changefeed on two tables.
		foobar := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		waitForChangefeedRunning(t, sliMetric, 1)
		require.EqualValues(t, 2, sliMetric.WatchedTargets.Value())

		// Remove one table from the changefeed.
		testfeed, ok := foobar.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)
		require.NoError(t, testfeed.Pause())
		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d DROP bar`, testfeed.JobID()))

		require.NoError(t, testfeed.Resume())
		waitForChangefeedRunning(t, sliMetric, 1)
		require.EqualValues(t, 1, sliMetric.WatchedTargets.Value())

		// Add one table to the changefeed.
		require.NoError(t, testfeed.Pause())
		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar`, testfeed.JobID()))
		require.NoError(t, testfeed.Resume())
		waitForChangefeedRunning(t, sliMetric, 1)
		require.EqualValues(t, 2, sliMetric.WatchedTargets.Value())

		// Rename a table in the changefeed.
		sqlDB.Exec(t, `ALTER TABLE bar RENAME TO baz`)
		require.NoError(t, testfeed.Resume())
		require.EqualValues(t, 2, sliMetric.WatchedTargets.Value())

		// Create a table with the old name. It should not affect the number of
		// tables watched.
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
		require.NoError(t, testfeed.Resume())
		require.EqualValues(t, 2, sliMetric.WatchedTargets.Value())

		// Cancel the feed.
		closeFeed(t, foobar)
		waitForJobStatus(sqlDB, t, testfeed.JobID(), `canceled`)
		require.EqualValues(t, 0, sliMetric.WatchedTargets.Value())
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}
