// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestInspectMetrics verifies that INSPECT job metrics are correctly updated
// when jobs run.
func TestInspectMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	runner := sqlutils.MakeSQLRunner(db)
	runner.Exec(t, `
		CREATE DATABASE db;
		CREATE TABLE db.t (
			id INT PRIMARY KEY,
			val INT
		);
		CREATE INDEX i1 on db.t (val);
		INSERT INTO db.t VALUES (1, 2), (2, 3);
	`)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	metrics := execCfg.JobRegistry.MetricsStruct().Inspect.(*InspectMetrics)

	initialRuns := metrics.Runs.Count()
	initialRunsWithIssues := metrics.RunsWithIssues.Count()
	initialIssuesFound := metrics.IssuesFound.Count()
	initialSpansProcessed := metrics.SpansProcessed.Count()

	// First run: no corruption, should succeed without issues
	runner.Exec(t, "INSPECT TABLE db.t")
	require.Equal(t, initialRuns+1, metrics.Runs.Count(), "Runs counter should increment")
	require.Equal(t, initialRunsWithIssues, metrics.RunsWithIssues.Count(), "RunsWithIssues should not increment")
	require.Equal(t, initialIssuesFound, metrics.IssuesFound.Count(), "IssuesFound should not increment")
	require.Equal(t, initialSpansProcessed+1, metrics.SpansProcessed.Count(),
		"SpansProcessed should increment by 1 (one secondary index i1)")

	// Create corruption: delete a secondary index entry for row (1, 2)
	// This creates a "missing_secondary_index_entry" issue - the primary key exists
	// but the corresponding secondary index entry is missing.
	kvDB := s.DB()
	codec := s.Codec()
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "db", "t")
	secIndex := tableDesc.PublicNonPrimaryIndexes()[0] // i1 index on (val)
	row := []tree.Datum{
		tree.NewDInt(1), // id
		tree.NewDInt(2), // val
	}
	err := deleteSecondaryIndexEntry(ctx, codec, row, kvDB, tableDesc, secIndex)
	require.NoError(t, err)

	// Second run: with corruption, should detect the missing index entry.
	_, err = db.Exec("INSPECT TABLE db.t")
	require.Error(t, err, "INSPECT should fail when corruption is detected")
	require.Contains(t, err.Error(), "INSPECT found inconsistencies")
	var pqErr *pq.Error
	require.True(t, errors.As(err, &pqErr), "expected pq.Error, got %T", err)
	require.NotEmpty(t, pqErr.Hint, "expected error to have a hint")
	require.Regexp(t, "SHOW INSPECT ERRORS FOR JOB [0-9]+ WITH DETAILS", pqErr.Hint)
	require.Equal(t, initialRuns+2, metrics.Runs.Count(),
		"Runs counter should increment for each job execution")
	require.Equal(t, initialRunsWithIssues+1, metrics.RunsWithIssues.Count(),
		"RunsWithIssues should increment when issues are found")
	require.Equal(t, initialIssuesFound+1, metrics.IssuesFound.Count(),
		"IssuesFound should increment when issues are detected")
	require.Equal(t, initialSpansProcessed+2, metrics.SpansProcessed.Count(),
		"SpansProcessed should increment by 2 total (1 span per INSPECT × 2 runs)")

	// Third run: on a different clean table to verify RunsWithIssues doesn't increment again.
	runner.Exec(t, `
		CREATE TABLE db.t2 (
			id INT PRIMARY KEY,
			val INT
		);
		CREATE INDEX i2 on db.t2 (val);
		INSERT INTO db.t2 VALUES (1, 2), (2, 3);
	`)
	runner.Exec(t, "INSPECT TABLE db.t2")
	require.Equal(t, initialRuns+3, metrics.Runs.Count(),
		"Runs counter should increment for third job execution")
	require.Equal(t, initialRunsWithIssues+1, metrics.RunsWithIssues.Count(),
		"RunsWithIssues should NOT increment for successful job")
	require.Equal(t, initialIssuesFound+1, metrics.IssuesFound.Count(),
		"IssuesFound should NOT increment for successful job")
	require.Equal(t, initialSpansProcessed+3, metrics.SpansProcessed.Count(),
		"SpansProcessed should increment by 3 total (1 span per INSPECT × 3 runs)")
}
