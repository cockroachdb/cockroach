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

func TestRunningCountAndWatchedTargets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics)
		aggMetrics := metrics.AggMetrics
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

		// Create a changefeed on one table.
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		waitForGaugeCount(t, aggMetrics.RunningCount, 1)
		require.EqualValues(t, 1, metrics.WatchedTargets.Value())

		// Create a changefeed on the same table.
		foo2 := feed(t, f, `CREATE CHANGEFEED AS SELECT a FROM foo`)
		waitForGaugeCount(t, aggMetrics.RunningCount, 2)
		require.EqualValues(t, 1, metrics.WatchedTargets.Value())

		// Create a changefeed on a different table.
		bar := feed(t, f, `CREATE CHANGEFEED FOR bar`)
		waitForGaugeCount(t, aggMetrics.RunningCount, 3)
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())

		// Close a changefeed on the same table as another. The number of tables
		// watched should remain the same.
		closeFeed(t, foo2)
		waitForGaugeCount(t, aggMetrics.RunningCount, 2)
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())

		// Close a changefeed.
		closeFeed(t, foo)
		waitForGaugeCount(t, aggMetrics.RunningCount, 1)
		require.EqualValues(t, 1, metrics.WatchedTargets.Value())

		// Create a changefeed with two tables, one of which is already watched.
		foobar := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		waitForGaugeCount(t, aggMetrics.RunningCount, 2)
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())

		// Close the changefeed watching one table.
		closeFeed(t, bar)
		waitForGaugeCount(t, aggMetrics.RunningCount, 1)
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())

		// Close the changefeed watching two tables.
		closeFeed(t, foobar)
		waitForGaugeCount(t, aggMetrics.RunningCount, 0)
		require.EqualValues(t, 0, metrics.WatchedTargets.Value())
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestRunningCountAndWatchedTargetsPause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics)
		aggMetrics := metrics.AggMetrics
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

		foobar := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		waitForGaugeCount(t, aggMetrics.RunningCount, 1)
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())

		// The running count should be decremented when the job is paused, but the
		// number of tables watched remains the same.
		testfeed, ok := foobar.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)
		require.NoError(t, testfeed.Pause())
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())
		require.EqualValues(t, 0, aggMetrics.RunningCount.Value())

		// Resuming the job should increase the running count again.
		require.NoError(t, testfeed.Resume())
		waitForGaugeCount(t, aggMetrics.RunningCount, 1)
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())

		// Pause the job again and cancel the changefeed while paused.
		require.NoError(t, testfeed.Pause())
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())
		require.EqualValues(t, 0, aggMetrics.RunningCount.Value())
		closeFeed(t, foobar)
		waitForJobStatus(sqlDB, t, testfeed.JobID(), `canceled`)
		require.EqualValues(t, 0, metrics.WatchedTargets.Value())
		require.EqualValues(t, 0, aggMetrics.RunningCount.Value())
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestWatchedTargetsAlter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics)
		aggMetrics := metrics.AggMetrics
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

		// Create a changefeed on two tables.
		foobar := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		waitForGaugeCount(t, aggMetrics.RunningCount, 1)
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())

		// Remove one table from the changefeed.
		testfeed, ok := foobar.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)
		require.NoError(t, testfeed.Pause())
		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d DROP bar`, testfeed.JobID()))

		require.NoError(t, testfeed.Resume())
		waitForGaugeCount(t, aggMetrics.RunningCount, 1)
		require.EqualValues(t, 1, metrics.WatchedTargets.Value())

		// Add one table to the changefeed.
		require.NoError(t, testfeed.Pause())
		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar`, testfeed.JobID()))
		require.NoError(t, testfeed.Resume())
		waitForGaugeCount(t, aggMetrics.RunningCount, 1)
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())

		// Rename a table in the changefeed.
		sqlDB.Exec(t, `ALTER TABLE bar RENAME TO baz`)
		require.NoError(t, testfeed.Resume())
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())

		// Create a table with the old name. It should not affect the number of
		// tables watched.
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
		require.NoError(t, testfeed.Resume())
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())

		// Cancel the feed.
		closeFeed(t, foobar)
		waitForJobStatus(sqlDB, t, testfeed.JobID(), `canceled`)
		require.EqualValues(t, 0, metrics.WatchedTargets.Value())
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestWatchedTargetsScope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics)
		aggMetrics := metrics.AggMetrics
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE baz (a INT PRIMARY KEY, b STRING)`)

		// Create a changefeed on two tables with label1.
		foobar := feed(t, f, `CREATE CHANGEFEED FOR foo, bar WITH metrics_label=label1`)
		waitForGaugeCount(t, aggMetrics.RunningCount, 1)
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())

		// Create a changefeed on one table with label2.
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH metrics_label=label2`)
		waitForGaugeCount(t, aggMetrics.RunningCount, 2)
		require.EqualValues(t, 2, metrics.WatchedTargets.Value())

		// Create a changefeed on one table with label2.
		baz := feed(t, f, `CREATE CHANGEFEED FOR baz WITH metrics_label=label2`)
		waitForGaugeCount(t, aggMetrics.RunningCount, 3)
		require.EqualValues(t, 3, metrics.WatchedTargets.Value())

		// Cancel the feeds.
		closeFeed(t, foo)
		waitForGaugeCount(t, aggMetrics.RunningCount, 2)
		require.EqualValues(t, 3, metrics.WatchedTargets.Value())

		closeFeed(t, foobar)
		waitForGaugeCount(t, aggMetrics.RunningCount, 1)
		require.EqualValues(t, 1, metrics.WatchedTargets.Value())

		closeFeed(t, baz)
		waitForGaugeCount(t, aggMetrics.RunningCount, 0)
		require.EqualValues(t, 0, metrics.WatchedTargets.Value())
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}
