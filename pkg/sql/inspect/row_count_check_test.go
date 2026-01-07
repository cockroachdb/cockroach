// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// mockRowCountCheck is a test implementation of inspectCheckRowCount that
// always returns a fixed row count value.
type mockRowCountCheck struct {
	mockInspectCheck
	rowCount uint64
}

var _ inspectCheck = &mockRowCountCheck{}
var _ inspectCheckRowCount = &mockRowCountCheck{}

// Rows implements the inspectCheckRowCount interface.
func (m *mockRowCountCheck) Rows() uint64 {
	return m.rowCount
}

func TestRowCountCheckSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testCases := []struct {
		desc          string
		checks        []inspectCheck
		expectedCount uint64
		expectError   bool
		errorPattern  string
	}{
		{
			desc: "single check returns row count",
			checks: []inspectCheck{
				&mockRowCountCheck{rowCount: 100},
			},
			expectedCount: 100,
			expectError:   false,
		},
		{
			desc: "multiple checks with same row count - returns first",
			checks: []inspectCheck{
				&mockRowCountCheck{rowCount: 200},
				&mockRowCountCheck{rowCount: 200},
			},
			expectedCount: 200,
			expectError:   false,
		},
		{
			desc: "conflicting row counts - should error",
			checks: []inspectCheck{
				&mockRowCountCheck{rowCount: 100},
				&mockRowCountCheck{rowCount: 200},
			},
			expectError:  true,
			errorPattern: "conflicting row counts",
		},
		{
			desc: "no inspectCheckRowCount implementation",
			checks: []inspectCheck{
				&mockInspectCheck{},
			},
			expectError:  true,
			errorPattern: "requires an inspectCheckRowCount",
		},
		{
			desc: "mixed checks - non-row-count check first, then row count",
			checks: []inspectCheck{
				&mockInspectCheck{},
				&mockRowCountCheck{rowCount: 300},
			},
			expectedCount: 300,
			expectError:   false,
		},
		{
			desc: "mixed checks - row count check first",
			checks: []inspectCheck{
				&mockRowCountCheck{rowCount: 300},
				&mockInspectCheck{},
			},
			expectedCount: 300,
			expectError:   false,
		},
		{
			desc: "three checks with same count",
			checks: []inspectCheck{
				&mockRowCountCheck{rowCount: 150},
				&mockRowCountCheck{rowCount: 150},
				&mockRowCountCheck{rowCount: 150},
			},
			expectedCount: 150,
			expectError:   false,
		},
		{
			desc: "zero row count",
			checks: []inspectCheck{
				&mockRowCountCheck{rowCount: 0},
			},
			expectedCount: 0,
			expectError:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			// Create a rowCountCheck
			check := &rowCountCheck{
				rowCountCheckApplicability: rowCountCheckApplicability{
					tableID: 1,
				},
			}

			// Register the checks
			err := check.RegisterChecksForSpan(tc.checks)
			if tc.errorPattern != "" && err != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errorPattern)
				return
			}
			require.NoError(t, err)

			// Create progress message
			msg := &jobspb.InspectProcessorProgress{
				SpanRowCount: 0,
			}

			// Call CheckSpan
			err = check.CheckSpan(ctx, nil /* logger */, msg)

			if tc.expectError {
				require.Error(t, err)
				if tc.errorPattern != "" {
					require.Contains(t, err.Error(), tc.errorPattern)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedCount, msg.SpanRowCount,
					"SpanRowCount should be set to the expected value")
			}
		})
	}
}

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
		`CREATE TABLE test.t (id INT PRIMARY KEY, INDEX idx (id))`,
		`INSERT INTO test.t SELECT * FROM generate_series(1, 100)`,
	)

	// Get the table descriptor
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "test", "t")

	// Create checks for the table with expected row count of 100
	expectedRowCount := uint64(105)
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
	require.Error(t, err, "inspect job should error on the row count mismatch")

	// Verify the job succeeded
	var jobStatus string
	var fractionCompleted float64
	r.QueryRow(t,
		`SELECT status, fraction_completed FROM [SHOW JOBS] WHERE job_type = 'INSPECT' ORDER BY created DESC LIMIT 1`,
	).Scan(&jobStatus, &fractionCompleted)
	require.Equal(t, "failed", jobStatus, "inspect job should fail with mismatching row count")
	require.InEpsilon(t, 1.0, fractionCompleted, 0.01, "progress should reach 100% on successful completion")

	// Query the inspect_errors table to verify a row count mismatch issue was reported
	// There should be exactly one error (row count checks produce one error per table)
	var errorType string
	var databaseName, schemaName, tableName, primaryKey, aost, details string
	var jobID int64
	var errorCount int
	r.QueryRow(t,
		fmt.Sprintf(`SELECT count(*) FROM [SHOW INSPECT ERRORS FOR JOB %d]`, job.ID()),
	).Scan(&errorCount)
	require.Equal(t, 1, errorCount, "should have exactly one row count mismatch error")

	r.QueryRow(t,
		fmt.Sprintf(`SHOW INSPECT ERRORS FOR JOB %d WITH DETAILS`, job.ID()),
	).Scan(&errorType, &databaseName, &schemaName, &tableName, &primaryKey, &jobID, &aost, &details)

	require.Equal(t, string(RowCountMismatch), errorType)
	require.Equal(t, "test", databaseName, "issue should reference the correct database")
	require.Equal(t, "public", schemaName, "issue should reference the correct schema")
	require.Equal(t, "t", tableName, "issue should reference the correct table")

	// Parse the details JSON to verify expected and actual counts
	var detailsMap map[string]interface{}
	err = json.Unmarshal([]byte(details), &detailsMap)
	require.NoError(t, err, "details should be valid JSON")
	require.Contains(t, detailsMap, "expected", "details should contain expected count")
	require.Contains(t, detailsMap, "actual", "details should contain actual count")
	require.Equal(t, float64(expectedRowCount), detailsMap["expected"], "expected count should match")
	require.Equal(t, float64(100), detailsMap["actual"], "actual count should be 100")
}
