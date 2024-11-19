// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colfetcher_test

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

type scanBatchSizeTestCase struct {
	tableRowCount       int
	skipStatsCollection bool
	query               string
	expectedKVRowsRead  int
	expectedBatches     int
}

var scanBatchSizeTestCases = []scanBatchSizeTestCase{
	// Uses the hard limit.
	{
		tableRowCount:       511,
		skipStatsCollection: true,
		query:               "SELECT * FROM t LIMIT 511",
		expectedKVRowsRead:  511,
		expectedBatches:     1,
	},
	// Uses the estimated row count.
	{
		tableRowCount:      511,
		query:              "SELECT * FROM t",
		expectedKVRowsRead: 511,
		expectedBatches:    1,
	},
	// Uses the soft limit.
	{
		tableRowCount: 511,
		query:         "SELECT * FROM t WHERE b <= 256 LIMIT 1",
		// We have a soft limit of 2 calculated by the optimizer given the
		// selectivity of the filter (511 / 256).
		expectedKVRowsRead: 2,
		expectedBatches:    1,
	},
	// Uses the limit to not fill the output batch to its capacity.
	{
		tableRowCount:      2000,
		query:              "SELECT * FROM t LIMIT 1500",
		expectedKVRowsRead: 1500,
		expectedBatches:    2,
	},
}

// TestScanBatchSize tests that the cFetcher's dynamic batch size algorithm uses
// the limit hint or the optimizer's estimated row count for its initial batch
// size as well as when to not fill the whole batch. This test confirms that
// cFetcher returns the expected number of batches but also checks that the
// expected number of KV rows were read. See the test cases above for more
// details.
func TestScanBatchSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderMetamorphic(t, "This test doesn't work with metamorphic batch sizes.")

	ctx := context.Background()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Until we propagate the estimated row count hint in the KV projection
	// pushdown case, this test is expected to fail if the direct scans are
	// used (#94850).
	_, err := conn.ExecContext(ctx, `SET direct_columnar_scans_enabled = false`)
	assert.NoError(t, err)

	// Disable automatic table stats collection so that we can control whether
	// they are present or not.
	_, err = conn.ExecContext(ctx, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false;`)
	assert.NoError(t, err)
	for _, testCase := range scanBatchSizeTestCases {
		t.Run(testCase.query, func(t *testing.T) {
			sqlDB.Exec(t, "DROP TABLE IF EXISTS t;")
			sqlDB.Exec(t, "CREATE TABLE t (a PRIMARY KEY, b) AS SELECT i, i FROM generate_series(1, $1) AS g(i)", testCase.tableRowCount)
			if !testCase.skipStatsCollection {
				// This test needs the stats, so analyze the table.
				sqlDB.Exec(t, "ANALYZE t")
			}

			// Allow for commas in the numbers that exceed 1000.
			kvRowsReadRegex := regexp.MustCompile(`KV rows decoded: ([\d,]+)`)
			batchCountRegex := regexp.MustCompile(`vectorized batch count: (\d+)`)
			mvccStepCountRegex := regexp.MustCompile(`MVCC step count \(ext/int\): ([\d,]+)/\d+`)
			testutils.SucceedsSoon(t, func() error {
				rows, err := conn.QueryContext(ctx, `EXPLAIN ANALYZE (VERBOSE) `+testCase.query)
				assert.NoError(t, err)
				foundKVRowsRead, foundBatches, foundMVCCSteps := -1, -1, -1
				var sb strings.Builder
				for rows.Next() {
					var res string
					assert.NoError(t, rows.Scan(&res))
					sb.WriteString(res)
					sb.WriteByte('\n')
					if matches := kvRowsReadRegex.FindStringSubmatch(res); len(matches) > 0 {
						foundKVRowsRead, err = strconv.Atoi(strings.ReplaceAll(matches[1], ",", ""))
						assert.NoError(t, err)
					} else if matches = batchCountRegex.FindStringSubmatch(res); len(matches) > 0 {
						foundBatches, err = strconv.Atoi(matches[1])
						assert.NoError(t, err)
					} else if matches = mvccStepCountRegex.FindStringSubmatch(res); len(matches) > 0 {
						foundMVCCSteps, err = strconv.Atoi(strings.ReplaceAll(matches[1], ",", ""))
						assert.NoError(t, err)
					}
				}
				if foundKVRowsRead != testCase.expectedKVRowsRead {
					return fmt.Errorf("expected to scan %d rows, found %d:\n%s", testCase.expectedKVRowsRead, foundKVRowsRead, sb.String())
				}
				if foundBatches != testCase.expectedBatches {
					return fmt.Errorf("should use %d batches to scan rows, found %d:\n%s", testCase.expectedBatches, foundBatches, sb.String())
				}
				if foundMVCCSteps != testCase.expectedKVRowsRead {
					return fmt.Errorf("expected to do %d MVCC steps, found %d", testCase.expectedKVRowsRead, foundMVCCSteps)
				}
				return nil
			})
		})
	}
}

// TestCFetcherLimitsOutputBatch verifies that cFetcher limits its output batch
// based on the memory footprint.
func TestCFetcherLimitsOutputBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderMetamorphic(t, "This test doesn't work with metamorphic batch sizes.")

	ctx := context.Background()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Until we propagate the estimated row count hint in the KV projection
	// pushdown case, this test is expected to fail if the direct scans are
	// used (#94850).
	_, err := conn.ExecContext(ctx, `SET direct_columnar_scans_enabled = false`)
	assert.NoError(t, err)

	// Lower the distsql_workmem session variable to 128KiB to speed up the
	// test.
	_, err = conn.ExecContext(ctx, `SET distsql_workmem='128KiB';`)
	assert.NoError(t, err)

	for _, tc := range []struct {
		// numRows and rowSize must be of the same length, with each index
		// specifying the number of rows of the corresponding size (in bytes) to
		// be inserted.
		numRows []int
		rowSize []int
		// batchCountLowerBound is a hard lower bound on the number of
		// vectorized batches used to return all rows inserted in the current
		// iteration.
		batchCountLowerBound int
	}{
		// Set up a table with 50 rows that take up 16KiB each. We will also
		// collect the stats on the table so that we had an estimated row count
		// of 50 for the scan. With such setup the cFetcher will allocate an
		// output batch of capacity 50, yet after setting the 7th or so row the
		// footprint of the batch will exceed the memory limit. As a result, we
		// will get around 7 batches.
		{
			numRows: []int{50},
			rowSize: []int{16 * 1024},
			// There is a bit of non-determinism (namely, in how data in
			// coldata.Bytes is appended), so we require that we get at least 7
			// batches. If we get more, that's still ok since it means that the
			// batches were even smaller.
			batchCountLowerBound: 7,
		},
		// Test case when the data is of variable size. The cFetcher is happy to
		// put first 1024 rows that are small into a single batch, so it'll
		// attempt to do the same for the large rows; however, after setting
		// about 128 rows, the memory limit is exceeded, so no more rows will be
		// added.
		{
			numRows: []int{1024, 1024},
			rowSize: []int{1, 1024},
			// We expect first 1024 rows to be returned in a single batch, but
			// second 1024 rows are split into batches of 128, so we'll at least
			// get 9 batches, maybe more.
			batchCountLowerBound: 9,
		},
	} {
		_, err = conn.ExecContext(ctx, `DROP TABLE IF EXISTS t`)
		assert.NoError(t, err)
		_, err = conn.ExecContext(ctx, `CREATE TABLE t (k INT PRIMARY KEY, v STRING)`)
		assert.NoError(t, err)
		startIdx := 1
		for i := range tc.numRows {
			_, err = conn.ExecContext(ctx,
				"INSERT INTO t SELECT i, repeat('a', $1) FROM generate_series($2, $3) AS g(i);",
				tc.rowSize[i], startIdx, startIdx+tc.numRows[i]-1,
			)
			assert.NoError(t, err)
			startIdx += tc.numRows[i]
		}
		_, err = conn.ExecContext(ctx, `ANALYZE t`)
		assert.NoError(t, err)

		batchCountRegex := regexp.MustCompile(`vectorized batch count: (\d+)`)
		rows, err := conn.QueryContext(ctx, `EXPLAIN ANALYZE (VERBOSE) SELECT * FROM t`)
		assert.NoError(t, err)
		foundBatches := -1
		for rows.Next() {
			var res string
			assert.NoError(t, rows.Scan(&res))
			if matches := batchCountRegex.FindStringSubmatch(res); len(matches) > 0 {
				foundBatches, err = strconv.Atoi(matches[1])
				assert.NoError(t, err)
			}
		}
		assert.GreaterOrEqual(t, foundBatches, tc.batchCountLowerBound)
	}
}
