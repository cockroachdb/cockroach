// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestChangefeedResolvedTablesRestoreDuringInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		ctx := context.Background()
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Disable span-level checkpointing to force per-table progress tracking.
		changefeedbase.SpanCheckpointInterval.Override(ctx, &s.Server.ClusterSettings().SV, 0)

		// Create two tables with data
		sqlDB.Exec(t, `CREATE TABLE foo (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (10, 'x'), (20, 'y')`)

		// Start changefeed
		feed := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		defer closeFeed(t, feed)

		// TODO make sure we write per-table resolved timestamps

		// TODO Pause and resume
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}
