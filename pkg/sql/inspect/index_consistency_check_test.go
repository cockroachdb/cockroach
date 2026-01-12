// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

const (
	expectedInspectFoundInconsistencies = "INSPECT found inconsistencies"
	expectedInspectInternalErrors       = "INSPECT encountered internal errors"
)

// requireCheckCountsMatch verifies that the job's total check count equals its completed check count.
// This is used to verify that progress tracking correctly counted all checks.
func requireCheckCountsMatch(t *testing.T, r *sqlutils.SQLRunner, jobID int64) {
	t.Helper()
	var totalChecks, completedChecks int64
	r.QueryRow(t, `
		SELECT
			(crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Progress', value)->'inspect'->>'jobTotalCheckCount')::INT,
			(crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Progress', value)->'inspect'->>'jobCompletedCheckCount')::INT
		FROM system.job_info
		WHERE job_id = $1 AND info_key = 'legacy_progress'
	`, jobID).Scan(&totalChecks, &completedChecks)
	require.Equal(t, totalChecks, completedChecks, "total checks should equal completed checks when job succeeds")
}

// encodeSecondaryIndexEntry encodes row data into a secondary index entry.
// The datums must be ordered according to the table's public columns.
// Returns the encoded index entry, expecting exactly one entry to be produced.
// Returns an error if the encoding fails or if multiple index entries are generated.
func encodeSecondaryIndexEntry(
	codec keys.SQLCodec, row []tree.Datum, tableDesc catalog.TableDescriptor, index catalog.Index,
) (rowenc.IndexEntry, error) {
	var colIDtoRowIndex catalog.TableColMap
	for i, c := range tableDesc.PublicColumns() {
		colIDtoRowIndex.Set(c.GetID(), i)
	}
	indexEntries, err := rowenc.EncodeSecondaryIndex(
		context.Background(), codec, tableDesc, index,
		colIDtoRowIndex, row, rowenc.EmptyVectorIndexEncodingHelper, true, /* includeEmpty */
	)
	if err != nil {
		return rowenc.IndexEntry{}, err
	}

	if len(indexEntries) != 1 {
		return rowenc.IndexEntry{}, errors.Newf("expected 1 index entry, got %d. got %#v", len(indexEntries), indexEntries)
	}
	return indexEntries[0], nil
}

// deleteSecondaryIndexEntry removes a secondary index entry for the given row data.
// The datums must be ordered according to the table's public columns.
// This function assumes the row generates exactly one index entry and will fail
// if multiple entries are produced.
func deleteSecondaryIndexEntry(
	ctx context.Context,
	codec keys.SQLCodec,
	row []tree.Datum,
	kvDB *kv.DB,
	tableDesc catalog.TableDescriptor,
	index catalog.Index,
) error {
	entry, err := encodeSecondaryIndexEntry(codec, row, tableDesc, index)
	if err != nil {
		return err
	}
	_, err = kvDB.Del(ctx, entry.Key)
	return err
}

// insertSecondaryIndexEntry adds a secondary index entry for the given row data.
// The datums must be ordered according to the table's public columns.
// This function assumes the row generates exactly one index entry and will fail
// if multiple entries are produced.
func insertSecondaryIndexEntry(
	ctx context.Context,
	codec keys.SQLCodec,
	row []tree.Datum,
	kvDB *kv.DB,
	tableDesc catalog.TableDescriptor,
	index catalog.Index,
) error {
	entry, err := encodeSecondaryIndexEntry(codec, row, tableDesc, index)
	if err != nil {
		return err
	}
	err = kvDB.Put(ctx, entry.Key, &entry.Value)
	return err
}

func TestDetectIndexConsistencyErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "slow test")

	issueLogger := &testIssueCollector{}
	ctx := context.Background()
	const numNodes = 3
	cl := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Inspect: &sql.InspectTestingKnobs{
					InspectIssueLogger: issueLogger,
					OnCheckComplete: func(check interface{}) error {
						if rowCountCheck, ok := check.(inspectCheckRowCount); ok {
							issueLogger.recordRowCount(rowCountCheck.RowCount())
						}
						return nil
					},
				},
				GCJob: &sql.GCJobTestingKnobs{
					SkipWaitingForMVCCGC: true,
				},
				SQLEvalContext: &eval.TestingKnobs{
					ForceProductionValues: true,
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	})
	defer cl.Stopper().Stop(ctx)
	s := cl.ApplicationLayer(0)

	db := s.SQLConn(t)
	kvDB := s.DB()
	codec := s.Codec()
	ie := s.InternalExecutor().(*sql.InternalExecutor)
	r := sqlutils.MakeSQLRunner(db)

	testCases := []struct {
		// desc is a description of the test case.
		desc string
		// splitRangeDDL is the DDL to split the table into multiple ranges. The
		// table will be populated via generate_series using values up to 1000.
		splitRangeDDL string
		// indexDDL is the DDL to create the indexes on the table.
		indexDDL []string
		// corruptionTargetIndex specifies which secondary index to corrupt (0-based position).
		// If not specified, defaults to 0 (first index).
		corruptionTargetIndex int
		// missingIndexEntrySelector defines a SQL predicate that selects rows
		// whose secondary index entries will be manually deleted to simulate
		// missing index entries (i.e., present in the primary index but not in the
		// secondary).
		missingIndexEntrySelector string
		// danglingIndexEntryInsertQuery is a full SQL SELECT expression that generates
		// rows to be inserted directly into the secondary index without corresponding
		// primary index entries, simulating dangling entries.
		danglingIndexEntryInsertQuery string
		// postIndexSQL is arbitrary SQL to execute after index creation, before the final
		// data insertion. Useful for deleting data or other test setup.
		postIndexSQL string
		// expectedIssues is the list of expected issues that should be found.
		expectedIssues []inspectIssue
		// expectedErrRegex is the regex pattern that the error message should match.
		// If empty then no error is expected.
		expectedErrRegex string
		// expectedInternalErrorPatterns contains patterns for validating internal error details.
		// Each element corresponds to the issue at the same index in expectedIssues.
		// For non-internal-error issues, the corresponding element should be nil.
		expectedInternalErrorPatterns []map[string]string
		// useTimestampBeforeCorruption uses a timestamp from before corruption is introduced
		useTimestampBeforeCorruption bool
		// expectedRowCount is the expected number of rows counted by the check.
		// If 0, defaults to 2000 (1000 initial + 1000 after index creation).
		expectedRowCount uint64
	}{
		{
			desc: "happy path sanity",
			indexDDL: []string{
				"CREATE INDEX idx_t_a ON test.t (b)",
			},
			missingIndexEntrySelector: "", /* nothing corrupted */
		},
		{
			desc:          "3 ranges, secondary index on a, 1 missing entry",
			splitRangeDDL: "ALTER TABLE test.t SPLIT AT VALUES (333),(666)",
			indexDDL: []string{
				"CREATE INDEX idx_t_a ON test.t (a)",
			},
			missingIndexEntrySelector: "a = 4",
			expectedIssues: []inspectIssue{
				{ErrorType: "missing_secondary_index_entry", PrimaryKey: "e'(4, \\'d_4\\')'"},
			},
			expectedErrRegex: expectedInspectFoundInconsistencies,
		},
		{
			desc:          "2 ranges, secondary index on 'b' with storing 'e', 1 missing entry",
			splitRangeDDL: "ALTER TABLE test.t SPLIT AT VALUES (500)",
			indexDDL: []string{
				"CREATE INDEX idx_t_a ON test.t (b) STORING (e)",
			},
			missingIndexEntrySelector: "a = 8",
			expectedIssues: []inspectIssue{
				{ErrorType: "missing_secondary_index_entry", PrimaryKey: "e'(8, \\'d_8\\')'"},
			},
			expectedErrRegex: expectedInspectFoundInconsistencies,
		},
		{
			desc:          "10 ranges, secondary index on c with storing 'f', many missing entry",
			splitRangeDDL: "ALTER TABLE test.t SPLIT AT VALUES (100),(200),(300),(400),(500),(600),(700),(800),(900)",
			indexDDL: []string{
				"CREATE INDEX idx_t_a ON test.t (c) STORING (f)",
			},
			missingIndexEntrySelector: "a BETWEEN 7 AND 10",
			expectedIssues: []inspectIssue{
				{ErrorType: "missing_secondary_index_entry", PrimaryKey: "e'(7, \\'d_7\\')'"},
				{ErrorType: "missing_secondary_index_entry", PrimaryKey: "e'(8, \\'d_8\\')'"},
				{ErrorType: "missing_secondary_index_entry", PrimaryKey: "e'(9, \\'d_9\\')'"},
				{ErrorType: "missing_secondary_index_entry", PrimaryKey: "e'(10, \\'d_10\\')'"},
			},
			expectedErrRegex: expectedInspectFoundInconsistencies,
		},
		{
			desc:          "2 ranges, secondary index on 'a', 1 dangling entry with internal error",
			splitRangeDDL: "ALTER TABLE test.t SPLIT AT VALUES (500)",
			indexDDL: []string{
				"CREATE INDEX idx_t_a ON test.t (a) STORING (f)",
			},
			danglingIndexEntryInsertQuery: "SELECT 3, 30, 300, 'd_3', 'e_3', -56.712",
			expectedIssues: []inspectIssue{
				{ErrorType: "internal_error"},
			},
			expectedErrRegex: expectedInspectInternalErrors,
			expectedInternalErrorPatterns: []map[string]string{
				{
					"error_message": "error decoding.*float64",
					"error_type":    "internal_query_error",
					"index_name":    "idx_t_a",
					"query":         "FROM.*table_",
				},
			},
		},
		{
			desc:          "2 ranges, secondary index on 'b' storing 'f', 1 dangling entry",
			splitRangeDDL: "ALTER TABLE test.t SPLIT AT VALUES (500)",
			indexDDL: []string{
				"CREATE INDEX idx_t_a ON test.t (b) STORING (c)",
			},
			danglingIndexEntryInsertQuery: "SELECT 15, 30, 300, 'corrupt', 'e_3', 300.5",
			expectedIssues: []inspectIssue{
				{ErrorType: "dangling_secondary_index_entry", PrimaryKey: "e'(15, \\'corrupt\\')'"},
			},
			expectedErrRegex: expectedInspectFoundInconsistencies,
		},
		{
			desc:          "2 ranges, all data deleted - no rows in spans",
			splitRangeDDL: "ALTER TABLE test.t SPLIT AT VALUES (500)",
			indexDDL: []string{
				"CREATE INDEX idx_t_a ON test.t (a)",
			},
			postIndexSQL:     "DELETE FROM test.t", /* delete all rows to test hasRows=false code path */
			expectedRowCount: 1000,                 // Only 1000 rows remain after deletion
		},
		{
			desc:          "timestamp before corruption - no issues found",
			splitRangeDDL: "ALTER TABLE test.t SPLIT AT VALUES (500)",
			indexDDL: []string{
				"CREATE INDEX idx_t_a ON test.t (a) STORING (c)",
			},
			danglingIndexEntryInsertQuery: "SELECT 15, 30, 300, 'corrupt', 'e_3', 300.5", // Add dangling entry after timestamp
			useTimestampBeforeCorruption:  true,                                          // Use timestamp from before corruption
			expectedRowCount:              1000,                                          // Only 1000 rows exist at the timestamp
		},
		{
			desc:          "2 indexes, corrupt second index, missing entry",
			splitRangeDDL: "ALTER TABLE test.t SPLIT AT VALUES (500)",
			indexDDL: []string{
				"CREATE INDEX idx_t_a ON test.t (a)",
				"CREATE INDEX idx_t_b ON test.t (b) STORING (e)",
			},
			corruptionTargetIndex:     1, // Target second index (idx_t_b)
			missingIndexEntrySelector: "a = 7",
			expectedIssues: []inspectIssue{
				{
					ErrorType:  "missing_secondary_index_entry",
					PrimaryKey: "e'(7, \\'d_7\\')'",
					Details: map[redact.RedactableString]interface{}{
						"index_name": "idx_t_b",
					},
				},
			},
			expectedErrRegex: expectedInspectFoundInconsistencies,
		},
		{
			desc:          "3 indexes, corrupt middle index, dangling entry",
			splitRangeDDL: "ALTER TABLE test.t SPLIT AT VALUES (333),(666)",
			indexDDL: []string{
				"CREATE INDEX idx_t_a ON test.t (a)",
				"CREATE INDEX idx_t_b ON test.t (b) STORING (c)",
				"CREATE INDEX idx_t_c ON test.t (c) STORING (f)",
			},
			corruptionTargetIndex:         1, // Target second index (middle one)
			danglingIndexEntryInsertQuery: "SELECT 25, 50, 500, 'corrupt_middle', 'e_25', 125.5",
			expectedIssues: []inspectIssue{
				{
					ErrorType:  "dangling_secondary_index_entry",
					PrimaryKey: "e'(25, \\'corrupt_middle\\')'",
					Details: map[redact.RedactableString]interface{}{
						"index_name": "idx_t_b",
					},
				},
			},
			expectedErrRegex: expectedInspectFoundInconsistencies,
		},
		{
			desc: "multiple indexes, no corruption - all should be checked",
			indexDDL: []string{
				"CREATE INDEX idx_t_a ON test.t (a)",
				"CREATE INDEX idx_t_b ON test.t (b)",
				"CREATE INDEX idx_t_c ON test.t (c)",
			},
			// No corruptionTargetIndex specified, no corruption
			missingIndexEntrySelector: "", // No corruption
		},
	}
	hashConfigs := []struct {
		name    string
		enabled bool
	}{
		{name: "hash-enabled", enabled: true},
		{name: "hash-disabled", enabled: false},
	}
	for _, hashCfg := range hashConfigs {
		t.Run(hashCfg.name, func(t *testing.T) {
			r.Exec(t, "SET CLUSTER SETTING sql.inspect.index_consistency_hash.enabled = $1", hashCfg.enabled)
			t.Cleanup(func() {
				r.Exec(t, "SET CLUSTER SETTING sql.inspect.index_consistency_hash.enabled = true")
			})

			for _, tc := range testCases {
				tc := tc
				t.Run(tc.desc, func(t *testing.T) {
					issueLogger.reset()
					r.ExecMultiple(t,
						`DROP DATABASE IF EXISTS test`,
						`CREATE DATABASE test`,
						`CREATE TABLE test.t (
					a INT,
					b INT,
					c INT NOT NULL,
				  d TEXT,
				  e TEXT NOT NULL,
				  f FLOAT,
				  PRIMARY KEY (a, d),
					FAMILY fam0 (a, b, c, d, e, f)
				)`,
						`INSERT INTO test.t (a, b, c, d, e, f)
				SELECT
					gs1 AS a,
					gs1 * 10 AS b,
					gs1 * 100 AS c,
					'd_' || gs1::STRING AS d,
					'e_' || gs1::STRING AS e,
					gs1 * 1.5 AS f
				FROM generate_series(1, 1000) AS gs1;`,
					)

					// Split the values and relocate leases so that the INSPECT job ends up
					// running in parallel across multiple nodes.
					r.ExecMultiple(t, tc.splitRangeDDL)
					ranges, err := db.Query(`
				WITH r AS (SHOW RANGES FROM TABLE test.t WITH DETAILS)
        SELECT range_id FROM r ORDER BY 1`)
					require.NoError(t, err)
					t.Cleanup(func() {
						require.NoError(t, ranges.Close())
					})
					for i := 0; ranges.Next(); i++ {
						var rangeID int
						err = ranges.Scan(&rangeID)
						require.NoError(t, err)
						relocate := fmt.Sprintf("ALTER RANGE %d RELOCATE LEASE TO %d", rangeID, (i%numNodes)+1)
						r.Exec(t, relocate)
					}

					r.ExecMultiple(t, tc.indexDDL...)

					// Execute any post-index SQL
					if tc.postIndexSQL != "" {
						r.Exec(t, tc.postIndexSQL)
					}

					// Get timestamp before corruption if needed
					var expectedASOFTime time.Time

					if tc.useTimestampBeforeCorruption {
						// Get timestamp before corruption
						r.QueryRow(t, "SELECT now()::timestamp").Scan(&expectedASOFTime)
						expectedASOFTime = expectedASOFTime.UTC()

						// Sleep for 1 millisecond to ensure corruption happens after timestamp
						// This should be long enough to guarantee a different timestamp
						time.Sleep(1 * time.Millisecond)
					}

					tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "test", "t")

					// Select target index based on corruptionTargetIndex with bounds checking
					indexes := tableDesc.PublicNonPrimaryIndexes()
					targetIndexPos := tc.corruptionTargetIndex
					if targetIndexPos < 0 || targetIndexPos >= len(indexes) {
						targetIndexPos = 0 // Default to first index for safety/backward compatibility
					}
					secIndex := indexes[targetIndexPos]

					// Apply test-specific corruption based on configured selectors:
					// - If missingIndexEntrySelector is set, we delete the secondary index entries
					//   for all rows matching the predicate. This simulates missing index entries
					//   (i.e., primary rows with no corresponding secondary index key).
					// - If danglingIndexEntryInsertQuery is set, we evaluate the query to produce
					//   synthetic rows and manually insert their secondary index entries without
					//   inserting matching primary rows. This simulates dangling entries
					//   (i.e., secondary index keys pointing to non-existent primary keys).
					if tc.missingIndexEntrySelector != "" {
						rows, err := ie.QueryBufferedEx(ctx, "missing-index-entry-query-filter", nil /* no txn */, sessiondata.NodeUserSessionDataOverride,
							`SELECT * FROM test.t WHERE `+tc.missingIndexEntrySelector)
						require.NoError(t, err)
						require.Greater(t, len(rows), 0,
							"filter '%s' matched no rows - check that values exist in table data",
							tc.missingIndexEntrySelector)
						t.Logf("Corrupting %d rows that match filter by deleting secondary index keys: %s", len(rows), tc.missingIndexEntrySelector)
						for _, row := range rows {
							err = deleteSecondaryIndexEntry(ctx, codec, row, kvDB, tableDesc, secIndex)
							require.NoError(t, err)
						}
					}
					if tc.danglingIndexEntryInsertQuery != "" {
						rows, err := ie.QueryBufferedEx(ctx, "dangling-index-insert-query", nil, /* no txn */
							sessiondata.NodeUserSessionDataOverride, tc.danglingIndexEntryInsertQuery)
						require.NoError(t, err)
						require.Greater(t, len(rows), 0,
							"danglingIndexEntryInsertQuery '%s' returned no rows - check query syntax",
							tc.danglingIndexEntryInsertQuery)
						t.Logf("Corrupting %d rows by inserting keys into secondary index returned by this query: %s",
							len(rows), tc.danglingIndexEntryInsertQuery)
						for _, row := range rows {
							err = insertSecondaryIndexEntry(ctx, codec, row, kvDB, tableDesc, secIndex)
							require.NoError(t, err)
						}
					}
					r.Exec(t,
						`INSERT INTO test.t (a, b, c, d, e, f)
				SELECT
					gs1 AS a,
					gs1 * 10 AS b,
					gs1 * 100 AS c,
					'd_' || gs1::STRING AS d,
					'e_' || gs1::STRING AS e,
					gs1 * 1.5 AS f
				FROM generate_series(1001, 2000) AS gs1;`,
					)

					// If not using timestamp before corruption, get current timestamp
					if !tc.useTimestampBeforeCorruption {
						// Convert relative timestamp to absolute timestamp using CRDB
						r.QueryRow(t, "SELECT (now() + '-1us')::timestamp").Scan(&expectedASOFTime)
						expectedASOFTime = expectedASOFTime.UTC()
					}

					// Use the absolute timestamp in nanoseconds for inspect command
					absoluteTimestamp := fmt.Sprintf("'%d'", expectedASOFTime.UnixNano())
					inspectQuery := fmt.Sprintf(`INSPECT TABLE test.t AS OF SYSTEM TIME %s WITH OPTIONS INDEX ALL`, absoluteTimestamp)
					_, err = db.Query(inspectQuery)
					if tc.expectedErrRegex == "" {
						require.NoError(t, err)
						require.Equal(t, 0, issueLogger.numIssuesFound())

						// Verify that row count was captured for successful checks when hash is enabled.
						// Row counts are only populated during the hash precheck, so we only verify them
						// in the hash-enabled test configuration.
						if hashCfg.enabled {
							rowCount, ok := issueLogger.getRowCount()
							require.True(t, ok, "expected row count to be captured")
							// The test inserts 2000 rows total (1000 before index creation + 1000 after),
							// unless otherwise specified by expectedRowCount.
							expectedRowCount := tc.expectedRowCount
							if expectedRowCount == 0 {
								expectedRowCount = 2000
							}
							require.Equal(t, expectedRowCount, rowCount, "expected row count to match")
						}
						return
					}

					require.Error(t, err)
					require.Regexp(t, tc.expectedErrRegex, err.Error())
					var pqErr *pq.Error
					require.True(t, errors.As(err, &pqErr), "expected pq.Error, got %T", err)
					require.NotEmpty(t, pqErr.Hint, "expected error to have a hint")
					require.Regexp(t, "SHOW INSPECT ERRORS FOR JOB [0-9]+ WITH DETAILS", pqErr.Hint)

					numExpected := len(tc.expectedIssues)
					numFound := issueLogger.numIssuesFound()

					dumpAllFoundIssues := func() {
						t.Log("Dumping all found issues:")
						for i := 0; i < numFound; i++ {
							t.Logf("  [%d] %s", i, issueLogger.issue(i))
						}
					}

					// The number of expected and actual issues must match exactly.
					// If they don't, dump all found issues for debugging and fail.
					if numExpected != numFound {
						t.Logf("Mismatch in number of issues: expected %d, found %d", numExpected, numFound)
						dumpAllFoundIssues()
						t.Fatalf("expected %d issues, but found %d", numExpected, numFound)
					}

					// For each expected issue, ensure it was found.
					for i, expectedIssue := range tc.expectedIssues {
						foundIssue := issueLogger.findIssue(expectedIssue.ErrorType, expectedIssue.PrimaryKey)
						if foundIssue == nil {
							t.Logf("Expected issue not found: %q", expectedIssue)
							dumpAllFoundIssues()
							t.Fatalf("expected issue %d (%q) not found", i, expectedIssue)
						}
						require.NotEqual(t, 0, foundIssue.DatabaseID, "expected issue to have a database ID: %s", expectedIssue)
						require.NotEqual(t, 0, foundIssue.SchemaID, "expected issue to have a schema ID: %s", expectedIssue)
						require.NotEqual(t, 0, foundIssue.ObjectID, "expected issue to have an object ID: %s", expectedIssue)
						require.Equal(t, expectedASOFTime, foundIssue.AOST.UTC())

						// Additional validation for internal errors
						if foundIssue.ErrorType == "internal_error" {
							require.NotNil(t, foundIssue.Details, "internal error should have details")

							// Validate patterns if provided for this specific issue
							if tc.expectedInternalErrorPatterns != nil && i < len(tc.expectedInternalErrorPatterns) &&
								tc.expectedInternalErrorPatterns[i] != nil {
								expectedPatterns := tc.expectedInternalErrorPatterns[i]

								// Validate each expected pattern
								for detailKey, expectedPattern := range expectedPatterns {
									redactableKey := redact.RedactableString(detailKey)
									require.Contains(t, foundIssue.Details, redactableKey, "internal error should contain detail key: %s", detailKey)

									detailValue, ok := foundIssue.Details[redactableKey].(string)
									require.True(t, ok, "detail value for key %s should be a string", detailKey)
									require.Regexp(t, expectedPattern, detailValue,
										"detail %s should match pattern %s, got: %s", detailKey, expectedPattern, detailValue)
								}
							}
						}

						// Validate Details if provided in expected issue
						if expectedIssue.Details != nil {
							require.NotNil(t, foundIssue.Details, "issue should have details when expected")

							// Check that all expected detail keys and values match
							for expectedKey, expectedValue := range expectedIssue.Details {
								require.Contains(t, foundIssue.Details, expectedKey,
									"issue should contain detail key: %s", expectedKey)

								actualValue := foundIssue.Details[expectedKey]
								require.Equal(t, expectedValue, actualValue,
									"detail %s should be %v, got %v", expectedKey, expectedValue, actualValue)
							}
						}
					}

					// Validate job status matches expected outcome
					var jobID int64
					var jobStatus string
					var fractionCompleted float64
					r.QueryRow(t, `SELECT job_id, status, fraction_completed FROM [SHOW JOBS] WHERE job_type = 'INSPECT' ORDER BY job_id DESC LIMIT 1`).Scan(&jobID, &jobStatus, &fractionCompleted)

					if tc.expectedErrRegex == "" {
						require.Equal(t, "succeeded", jobStatus, "expected job to succeed when no issues found")
						require.InEpsilon(t, 1.0, fractionCompleted, 0.01, "progress should reach 100%% on successful completion")
						requireCheckCountsMatch(t, r, jobID)
					} else {
						require.Equal(t, "failed", jobStatus, "expected job to fail when inconsistencies found")
					}
				})
			}
		})
	}
}

// TestDanglingIndexEntryInEmptyTable verifies that INSPECT can detect dangling
// secondary index entries even when the primary index is completely empty.
func TestDanglingIndexEntryInEmptyTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer s.Stopper().Stop(ctx)
	codec := s.ApplicationLayer().Codec()

	// Create an empty table with a secondary index.
	_, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
CREATE INDEX secondary ON t.test (v);
`)
	require.NoError(t, err)

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "t", "test")
	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]

	// Manually insert a dangling secondary index entry for a non-existent primary key.
	// This creates corruption: a secondary index entry pointing to primary key (10)
	// which doesn't exist in the primary index.
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(314)}
	err = insertSecondaryIndexEntry(ctx, codec, values, kvDB, tableDesc, secondaryIndex)
	require.NoError(t, err)

	// Run INSPECT and expect it to find the dangling index entry.
	// INSPECT should return an error when inconsistencies are found.
	_, err = db.Query(`INSPECT TABLE t.test AS OF SYSTEM TIME '-1us' WITH OPTIONS INDEX ALL`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "INSPECT found inconsistencies")

	// Verify the error details using SHOW INSPECT ERRORS.
	rows, err := db.Query(`SHOW INSPECT ERRORS FOR TABLE t.test WITH DETAILS`)
	require.NoError(t, err)
	defer rows.Close()

	var errorType, dbName, schemaName, tableName, primaryKey, aost, details string
	var jobID int64
	found := false
	for rows.Next() {
		err = rows.Scan(&errorType, &dbName, &schemaName, &tableName, &primaryKey, &jobID, &aost, &details)
		require.NoError(t, err)
		found = true

		// Verify the error is a dangling secondary index entry.
		require.Equal(t, "dangling_secondary_index_entry", errorType)
		require.Equal(t, "t", dbName)
		require.Equal(t, "test", tableName)
		require.Equal(t, "'(10)'", primaryKey)
		// Verify details contain the expected column value.
		require.Contains(t, details, `"v": "314"`)
	}

	require.True(t, found, "expected to find at least one inspect error, but found none")
}

func TestIndexConsistencyWithReservedWordColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	issueLogger := &testIssueCollector{}
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer s.Stopper().Stop(ctx)
	r := sqlutils.MakeSQLRunner(db)

	// Test with a table that has reserved word column names
	issueLogger.reset()
	r.ExecMultiple(t,
		`DROP DATABASE IF EXISTS test`,
		`CREATE DATABASE test`,
		`CREATE TABLE test.reserved_table (
			"select" INT,
			"from" INT,
			"where" INT NOT NULL,
			"order" TEXT,
			"group" TEXT NOT NULL,
			"having" FLOAT,
			PRIMARY KEY ("select", "order"),
			FAMILY fam0 ("select", "from", "where", "order", "group", "having")
		)`,
		`INSERT INTO test.reserved_table ("select", "from", "where", "order", "group", "having")
		SELECT
			gs AS "select",
			gs * 10 AS "from",
			gs * 100 AS "where",
			'order_' || gs::STRING AS "order",
			'group_' || gs::STRING AS "group",
			gs * 1.5 AS "having"
		FROM generate_series(1, 100) AS gs`,
		`CREATE INDEX idx_where ON test.reserved_table ("where")`,
		`CREATE INDEX idx_from_group ON test.reserved_table ("from") STORING ("group")`,
		`CREATE INDEX idx_having ON test.reserved_table ("having", "group")`,
	)

	_, err := db.Query(`INSPECT TABLE test.reserved_table WITH OPTIONS INDEX ALL`)
	require.NoError(t, err, "should succeed on table with reserved word column names")
	require.Equal(t, 0, issueLogger.numIssuesFound(), "No issues should be found in happy path test")

	// Verify job succeeded and progress reached 100%
	var jobID int64
	var jobStatus string
	var fractionCompleted float64
	r.QueryRow(t, `SELECT job_id, status, fraction_completed FROM [SHOW JOBS] WHERE job_type = 'INSPECT' ORDER BY job_id DESC LIMIT 1`).Scan(&jobID, &jobStatus, &fractionCompleted)
	require.Equal(t, "succeeded", jobStatus, "INSPECT job should succeed")
	require.InEpsilon(t, 1.0, fractionCompleted, 0.01, "progress should reach 100%% on successful completion")
	requireCheckCountsMatch(t, r, jobID)
}

// TestMissingIndexEntryWithHistoricalQuery verifies that INSPECT can detect
// missing index entries when querying at a historical timestamp (AS OF SYSTEM
// TIME), including cases where data has since been deleted.
//
// This test overlaps with TestDetectIndexConsistencyErrors but is kept to
// expand coverage. It exercises scenarios that TestDetectIndexConsistencyErrors
// missed and specifically validates INSPECT’s handling of AS OF SYSTEM TIME and
// mixed-case table names.
func TestMissingIndexEntryWithHistoricalQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer s.Stopper().Stop(ctx)
	r := sqlutils.MakeSQLRunner(db)
	codec := s.ApplicationLayer().Codec()

	// Create the table and the row entry.
	// We use a table with mixed case as a regression case for #38184.
	r.ExecMultiple(t,
		`CREATE DATABASE t`,
		`CREATE TABLE t."tEst" ("K" INT PRIMARY KEY, v INT)`,
		`CREATE INDEX secondary ON t."tEst" (v)`,
		`INSERT INTO t."tEst" VALUES (10, 20)`,
	)

	// Construct datums for our row values (k, v).
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(20)}
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "t", "tEst")
	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]

	// Delete the secondary index entry to create corruption.
	err := deleteSecondaryIndexEntry(ctx, codec, values, kvDB, tableDesc, secondaryIndex)
	require.NoError(t, err)

	// Run INSPECT and expect it to find the missing index entry.
	// INSPECT returns an error when inconsistencies are found.
	_, err = db.Query(`INSPECT TABLE t."tEst" WITH OPTIONS INDEX ALL`)
	require.Error(t, err, "expected INSPECT to return an error when inconsistencies are found")
	require.Contains(t, err.Error(), "INSPECT found inconsistencies")

	// Run again with AS OF SYSTEM TIME.
	time.Sleep(1 * time.Millisecond)
	_, err = db.Query(`INSPECT TABLE t."tEst" AS OF SYSTEM TIME '-1ms' WITH OPTIONS INDEX ALL`)
	require.Error(t, err, "expected INSPECT to return an error when inconsistencies are found")
	require.Contains(t, err.Error(), "INSPECT found inconsistencies")

	// Verify that AS OF SYSTEM TIME actually operates in the past by:
	// 1. Getting a timestamp before we delete the row
	// 2. Deleting the entire row
	// 3. Running INSPECT at the historical timestamp
	// At that historical timestamp, the row existed and was corrupted, so INSPECT
	// should still find the corruption even though the row no longer exists.
	ts := r.QueryStr(t, `SELECT cluster_logical_timestamp()`)[0][0]
	r.Exec(t, `DELETE FROM t."tEst"`)

	_, err = db.Query(fmt.Sprintf(
		`INSPECT TABLE t."tEst" AS OF SYSTEM TIME '%s' WITH OPTIONS INDEX ALL`, ts,
	))
	require.Error(t, err, "expected INSPECT to find corruption at historical timestamp")
	require.Contains(t, err.Error(), "INSPECT found inconsistencies",
		"INSPECT should detect the missing index entry that existed at the historical timestamp")
}

func TestInspectWithoutASOFSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	unblockCh := make(chan struct{})
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Inspect: &sql.InspectTestingKnobs{
				OnInspectJobStart: func() error {
					// Block the INSPECT job from starting until the DROP INDEX completes.
					const maxWait = 30 * time.Second
					select {
					case <-unblockCh:
						return nil
					case <-time.After(maxWait):
						return errors.New("timed out waiting for DROP INDEX to complete")
					}
				},
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer s.Stopper().Stop(ctx)
	r := sqlutils.MakeSQLRunner(db)

	r.ExecMultiple(t,
		`SET CLUSTER SETTING jobs.registry.interval.adopt = '500ms'`,
		`CREATE DATABASE t`,
		`CREATE TABLE t.pk_swap (
                       old_pk INT PRIMARY KEY,
                       new_pk INT UNIQUE NOT NULL,
                       payload INT NOT NULL,
                       INDEX payload_idx (payload)
              )`,
		`INSERT INTO t.pk_swap SELECT i, i+100, i+200 FROM generate_series(1, 5) AS g(i)`,
	)

	r.Exec(t, `INSPECT TABLE t.pk_swap WITH OPTIONS INDEX ALL, DETACHED`)
	r.Exec(t, `DROP INDEX t.pk_swap@payload_idx`)
	// Unblock the INSPECT job now that the DROP INDEX has completed.
	close(unblockCh)

	var jobID int64
	require.Eventually(t, func() bool {
		row := db.QueryRow(`SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'INSPECT' ORDER BY created DESC LIMIT 1`)
		return row.Scan(&jobID) == nil
	}, 10*time.Second, 100*time.Millisecond)

	var (
		status string
		jobErr gosql.NullString
	)
	require.Eventually(t, func() bool {
		row := db.QueryRow(`SELECT status, error FROM [SHOW JOBS] WHERE job_id = $1`, jobID)
		if err := row.Scan(&status, &jobErr); err != nil {
			t.Logf("error polling job status: %v", err)
			return false
		}
		return status == "failed"
	}, 60*time.Second, time.Second)

	require.Equal(t, "failed", status, "detached job should fail after schema change")
	require.Regexp(t, `table pk_swap \[\d+\] has had a schema change since the job has started at .+\nHINT: use AS OF SYSTEM TIME to avoid schema changes during inspection`, jobErr.String)

	rows, err := db.Query(`SHOW INSPECT ERRORS FOR TABLE t.pk_swap WITH DETAILS`)
	require.NoError(t, err)
	defer rows.Close()
	require.False(t, rows.Next(), "no inspect errors are recorded when the job aborts before running checks")
}

// TestInspectASOFAfterPrimaryKeySwapFails runs an INSPECT ASOF statement while
// doing a schema change after the ASOF time.
func TestInspectASOFAfterPrimaryKeySwap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer s.Stopper().Stop(ctx)
	r := sqlutils.MakeSQLRunner(db)

	r.ExecMultiple(t,
		`CREATE DATABASE t`,
		`CREATE TABLE t.pk_swap (
			old_pk INT PRIMARY KEY,
			new_pk INT UNIQUE NOT NULL,
			payload INT NOT NULL,
			INDEX payload_idx (payload)
		)`,
		`INSERT INTO t.pk_swap SELECT i, i+100, i+200 FROM generate_series(1, 5) AS g(i)`,
	)

	var asOf string
	r.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&asOf)

	// Rewrite the table descriptors by swapping the primary key.
	r.Exec(t, `ALTER TABLE t.pk_swap ALTER PRIMARY KEY USING COLUMNS (new_pk)`)

	// Helper function to wait for the latest INSPECT job to complete successfully.
	waitForInspectJob := func() {
		var jobID int64
		var status string
		var fractionCompleted float64
		testutils.SucceedsSoon(t, func() error {
			row := db.QueryRow(`SELECT job_id, status, fraction_completed FROM [SHOW JOBS] WHERE job_type = 'INSPECT' ORDER BY job_id DESC LIMIT 1`)
			if err := row.Scan(&jobID, &status, &fractionCompleted); err != nil {
				return err
			}
			if status == "succeeded" || status == "failed" {
				return nil
			}
			return errors.Newf("job is not in the succeeded or failed state: %q", status)
		})
		require.Equal(t, "succeeded", status, "INSPECT job should succeed")
		require.InEpsilon(t, 1.0, fractionCompleted, 0.01, "progress should reach 100%% on successful completion")
		requireCheckCountsMatch(t, r, jobID)
	}

	t.Run("non-detached", func(t *testing.T) {
		r.Exec(t, fmt.Sprintf(`INSPECT TABLE t.pk_swap AS OF SYSTEM TIME '%s' WITH OPTIONS INDEX ALL`, asOf))
		waitForInspectJob()
	})

	t.Run("detached with implicit transaction", func(t *testing.T) {
		r.Exec(t, fmt.Sprintf(`INSPECT TABLE t.pk_swap AS OF SYSTEM TIME '%s' WITH OPTIONS INDEX ALL, DETACHED`, asOf))
		waitForInspectJob()
	})

	t.Run("detached with explicit transaction", func(t *testing.T) {
		_, err := db.Exec("BEGIN AS OF SYSTEM TIME '" + asOf + "'")
		require.NoError(t, err)
		_, err = db.Exec(fmt.Sprintf(`INSPECT TABLE t.pk_swap AS OF SYSTEM TIME '%s' WITH OPTIONS INDEX ALL, DETACHED`, asOf))
		require.NoError(t, err)
		_, err = db.Exec("COMMIT")
		// Expecting the commit to fail due to the schema change after ASOF time.
		require.Contains(t, err.Error(), "TransactionRetryError")
	})
}
