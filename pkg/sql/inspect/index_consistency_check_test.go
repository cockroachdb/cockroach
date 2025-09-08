// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

const expectedInspectFoundInconsistencies = "INSPECT found inconsistencies"

// encodeSecondaryIndexEntry encodes row data into a secondary index entry.
// The datums must be ordered according to the table's public columns.
// Returns the encoded index entry, expecting exactly one entry to be produced.
// Returns an error if the encoding fails or if multiple index entries are generated.
func encodeSecondaryIndexEntry(
	row []tree.Datum, tableDesc catalog.TableDescriptor, index catalog.Index,
) (rowenc.IndexEntry, error) {
	var colIDtoRowIndex catalog.TableColMap
	for i, c := range tableDesc.PublicColumns() {
		colIDtoRowIndex.Set(c.GetID(), i)
	}
	indexEntries, err := rowenc.EncodeSecondaryIndex(
		context.Background(), keys.SystemSQLCodec, tableDesc, index,
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
	row []tree.Datum,
	kvDB *kv.DB,
	tableDesc catalog.TableDescriptor,
	index catalog.Index,
) error {
	entry, err := encodeSecondaryIndexEntry(row, tableDesc, index)
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
	row []tree.Datum,
	kvDB *kv.DB,
	tableDesc catalog.TableDescriptor,
	index catalog.Index,
) error {
	entry, err := encodeSecondaryIndexEntry(row, tableDesc, index)
	if err != nil {
		return err
	}
	err = kvDB.Put(ctx, entry.Key, &entry.Value)
	return err
}

func TestDetectIndexConsistencyErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	issueLogger := &testIssueCollector{}
	ctx := context.Background()
	const numNodes = 3
	cl := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			Knobs: base.TestingKnobs{
				Inspect: &sql.InspectTestingKnobs{
					InspectIssueLogger: issueLogger,
				},
			},
		},
	})
	defer cl.Stopper().Stop(ctx)

	db := cl.ServerConn(0)
	kvDB := cl.Server(0).DB()
	codec := cl.Server(0).Codec()
	ie := cl.Server(0).InternalExecutor().(*sql.InternalExecutor)
	r := sqlutils.MakeSQLRunner(db)

	for _, tc := range []struct {
		// desc is a description of the test case.
		desc string
		// splitRangeDDL is the DDL to split the table into multiple ranges. The
		// table will be populated via generate_series using values up to 1000.
		splitRangeDDL string
		// indexDDL is the DDL to create the indexes on the table.
		indexDDL []string
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
			expectedErrRegex: expectedInspectFoundInconsistencies,
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
			postIndexSQL: "DELETE FROM test.t", /* delete all rows to test hasRows=false code path */
		},
	} {
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

			tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "test", "t")
			secIndex := tableDesc.PublicNonPrimaryIndexes()[0]

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
					err = deleteSecondaryIndexEntry(ctx, row, kvDB, tableDesc, secIndex)
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
					err = insertSecondaryIndexEntry(ctx, row, kvDB, tableDesc, secIndex)
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

			// TODO(148365): Run INSPECT instead of SCRUB.
			_, err = db.Exec(`SET enable_scrub_job=true`)
			require.NoError(t, err)
			_, err = db.Query(`EXPERIMENTAL SCRUB TABLE test.t WITH OPTIONS INDEX ALL`)
			if tc.expectedErrRegex == "" {
				require.NoError(t, err)
				require.Equal(t, 0, issueLogger.numIssuesFound())
				return
			}

			require.Error(t, err)
			require.Regexp(t, tc.expectedErrRegex, err.Error())

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
				require.NotEqual(t, time.Time{}, foundIssue.AOST, "expected issue to have an AOST time: %s", expectedIssue)

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
			}

			// Validate job status matches expected outcome
			var jobStatus string
			r.QueryRow(t, `SELECT status FROM [SHOW JOBS] WHERE job_type = 'INSPECT' ORDER BY job_id DESC LIMIT 1`).Scan(&jobStatus)

			if tc.expectedErrRegex == "" {
				require.Equal(t, "succeeded", jobStatus, "expected job to succeed when no issues found")
			} else {
				require.Equal(t, "failed", jobStatus, "expected job to fail when inconsistencies found")
			}
		})
	}
}

func TestIndexConsistencyWithReservedWordColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	issueLogger := &testIssueCollector{}
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
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

	// TODO(148365): Run INSPECT instead of SCRUB.
	_, err := db.Exec(`SET enable_scrub_job=true`)
	require.NoError(t, err)
	_, err = db.Query(`EXPERIMENTAL SCRUB TABLE test.reserved_table WITH OPTIONS INDEX ALL`)
	require.NoError(t, err, "should succeed on table with reserved word column names")
	require.Equal(t, 0, issueLogger.numIssuesFound(), "No issues should be found in happy path test")

	// Verify job succeeded
	var jobStatus string
	r.QueryRow(t, `SELECT status FROM [SHOW JOBS] WHERE job_type = 'INSPECT' ORDER BY job_id DESC LIMIT 1`).Scan(&jobStatus)
	require.Equal(t, "succeeded", jobStatus, "INSPECT job should succeed")
}
