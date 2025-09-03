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
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/progresspb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestChangefeedJobInfoResolvedTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		ctx := context.Background()
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Make sure per-table tracking is enabled.
		changefeedbase.TrackPerTableProgress.Override(ctx, &s.Server.ClusterSettings().SV, true)

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (x INT PRIMARY KEY, y STRING)`)
		feed := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		defer closeFeed(t, feed)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'one')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (10, 'ten')`)
		assertPayloads(t, feed, []string{
			`foo: [1]->{"after": {"a": 1, "b": "one"}}`,
			`bar: [10]->{"after": {"x": 10, "y": "ten"}}`,
		})

		// The ResolvedTables message should be persisted to the job_info table
		// at the same time as the highwater being set.
		enterpriseFeed := feed.(cdctest.EnterpriseTestFeed)
		waitForHighwater(t, enterpriseFeed, s.Server.JobRegistry().(*jobs.Registry))

		// Make sure the ResolvedTables message was persisted and can be decoded.
		var resolvedTables progresspb.ResolvedTables
		execCfg := s.Server.ExecutorConfig().(sql.ExecutorConfig)
		err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return readChangefeedJobInfo(ctx, resolvedTablesFilename, &resolvedTables, txn, enterpriseFeed.JobID())
		})
		require.NoError(t, err)
		require.Len(t, resolvedTables.Tables, 2)
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}
