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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestRowCountCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	codec := s.ApplicationLayer().Codec()
	execCfg := s.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
	r := sqlutils.MakeSQLRunner(db)

	// Create a table and populate it with data
	r.ExecMultiple(t,
		`CREATE DATABASE test`,
		`CREATE TABLE test.t (id INT PRIMARY KEY)`,
		`INSERT INTO test.t SELECT * FROM generate_series(1, 100)`,
	)

	// Get the table descriptor
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "test", "t")

	// Create checks for the table with expected row count of 100
	expectedRowCount := uint64(101)
	checks, err := ChecksForTable(ctx, nil /* PlanHookState */, tableDesc, &expectedRowCount)
	require.NoError(t, err)
	require.Len(t, checks, 2)

	// Get a timestamp for AS OF SYSTEM TIME
	var timestampStr string
	r.QueryRow(t, "SELECT cluster_logical_timestamp()::STRING").Scan(&timestampStr)
	asOfTimestamp, err := hlc.ParseHLC(timestampStr)
	require.NoError(t, err)

	// Trigger the inspect job directly
	job, err := TriggerJob(
		ctx,
		"test row count check",
		&execCfg,
		checks,
		asOfTimestamp,
	)
	require.NoError(t, err)
	require.NotNil(t, job)

	// Wait for the job to complete
	err = job.AwaitCompletion(ctx)
	require.NoError(t, err, "inspect job should complete successfully")

	// Verify the job succeeded
	var jobStatus string
	var fractionCompleted float64
	r.QueryRow(t,
		`SELECT status, fraction_completed FROM [SHOW JOBS] WHERE job_type = 'INSPECT' ORDER BY created DESC LIMIT 1`,
	).Scan(&jobStatus, &fractionCompleted)
	require.Equal(t, "succeeded", jobStatus, "inspect job should succeed with matching row count")
	require.InEpsilon(t, 1.0, fractionCompleted, 0.01, "progress should reach 100%% on successful completion")
}
