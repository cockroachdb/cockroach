// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcprogresspb"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestChangefeedResolvedTablesRestoreDuringInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		ctx := context.Background()
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Disable span-level checkpointing to force per-table progress tracking.
		changefeedbase.TrackPerTableProgress.Override(ctx, &s.Server.ClusterSettings().SV, true)
		changefeedbase.SpanCheckpointInterval.Override(ctx, &s.Server.ClusterSettings().SV, 0)

		// Create two tables with data
		sqlDB.Exec(t, `CREATE TABLE foo (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (10, 'x'), (20, 'y')`)

		// Get table span for foo to block its checkpoints
		var fooTableID, barTableID uint32
		sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables WHERE name = 'foo'`).Scan(&fooTableID)
		sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables WHERE name = 'bar'`).Scan(&barTableID)
		fooTableSpan := s.Codec.TableSpan(fooTableID)

		// Modify aggregator progress to filter out foo's resolved spans
		knobs := s.TestingKnobs.DistSQL.(*execinfra.TestingKnobs).Changefeed.(*TestingKnobs)
		knobs.ChangeFrontierKnobs.OnAggregatorProgress = func(resolvedSpans *jobspb.ResolvedSpans) error {
			var filteredSpans []jobspb.ResolvedSpan
			for _, resolved := range resolvedSpans.ResolvedSpans {
				if fooTableSpan.Contains(resolved.Span) {
					fmt.Printf("ON AGGREGATOR PROGRESS: filtering out foo span %s\n", resolved.Span)
					continue // Skip foo's resolved spans
				}
				filteredSpans = append(filteredSpans, resolved)
			}
			resolvedSpans.ResolvedSpans = filteredSpans
			return nil
		}

		// Start changefeed with per-table tracking enabled
		feed := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		defer closeFeed(t, feed)

		// Consume initial scan data
		assertPayloads(t, feed, []string{
			`foo: [1]->{"after": {"id": 1, "name": "a"}}`,
			`foo: [2]->{"after": {"id": 2, "name": "b"}}`,
			`bar: [10]->{"after": {"id": 10, "name": "x"}}`,
			`bar: [20]->{"after": {"id": 20, "name": "y"}}`,
		})

		jobFeed := feed.(cdctest.EnterpriseTestFeed)

		// Read ResolvedTables from job_info to verify what was saved
		execCfg := s.Server.ExecutorConfig().(sql.ExecutorConfig)
		var resolvedTables cdcprogresspb.ResolvedTables

		// Retry reading until some progress is saved
		testutils.SucceedsSoon(t, func() error {
			err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				return readChangefeedJobInfo(ctx, resolvedTablesFilename, &resolvedTables, txn, jobFeed.JobID())
			})
			if err != nil {
				return err
			}

			// Check that some tables have progress saved
			if len(resolvedTables.Tables) == 0 {
				return errors.New("no table progress saved yet")
			}
			return nil
		})

		t.Logf("resolved tables: %v", resolvedTables)

		// Check that foo's timestamp is empty (no progress saved) and bar's is not empty (progress was saved)
		require.True(t, resolvedTables.Tables[descpb.ID(fooTableID)].IsEmpty(), "foo's timestamp should be empty")
		require.False(t, resolvedTables.Tables[descpb.ID(barTableID)].IsEmpty(), "bar's timestamp should not be empty")

		// TODO pause and resume
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}
