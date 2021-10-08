// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

type scanBatchSizeTestCase struct {
	needsStats         bool
	query              string
	expectedKVRowsRead int
}

var scanBatchSizeTestCases = []scanBatchSizeTestCase{
	// Uses the hard limit.
	{
		needsStats:         false,
		query:              "SELECT * FROM t LIMIT 511",
		expectedKVRowsRead: 511,
	},
	// Uses the estimated row count.
	{
		needsStats:         true,
		query:              "SELECT * FROM t",
		expectedKVRowsRead: 511,
	},
	// Uses the soft limit.
	{
		needsStats: true,
		query:      "SELECT * FROM t WHERE b <= 256 LIMIT 1",
		// We have a soft limit of 2 calculated by the optimizer given the
		// selectivity of the filter (511 / 256).
		expectedKVRowsRead: 2,
	},
}

// TestScanBatchSize tests that the the cFetcher's dynamic batch size algorithm
// uses the limit hint or the optimizer's estimated row count for its initial
// batch size. This test confirms that cFetcher returns a single batch but also
// checks that the expected number of KV rows were read. See the test cases
// above for more details.
func TestScanBatchSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderMetamorphic(t, "This test doesn't work with metamorphic batch sizes.")

	for _, testCase := range scanBatchSizeTestCases {
		t.Run(testCase.query, func(t *testing.T) {
			testClusterArgs := base.TestClusterArgs{
				ReplicationMode: base.ReplicationAuto,
			}
			tc := testcluster.StartTestCluster(t, 1, testClusterArgs)
			ctx := context.Background()
			defer tc.Stopper().Stop(ctx)

			conn := tc.Conns[0]

			// Create the table with disabled automatic table stats collection (so
			// that we can control whether they are present or not).
			_, err := conn.ExecContext(ctx, `
SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false;
CREATE TABLE t (a PRIMARY KEY, b) AS SELECT i, i FROM generate_series(1, 511) AS g(i)
`)
			assert.NoError(t, err)

			if testCase.needsStats {
				// This test needs the stats info, so analyze the table.
				_, err := conn.ExecContext(ctx, `ANALYZE t`)
				assert.NoError(t, err)
			}

			kvRowsReadRegex := regexp.MustCompile(`KV rows read: (\d+)`)
			batchCountRegex := regexp.MustCompile(`vectorized batch count: (\d+)`)
			testutils.SucceedsSoon(t, func() error {
				rows, err := conn.QueryContext(ctx, `EXPLAIN ANALYZE (VERBOSE, DISTSQL) `+testCase.query)
				assert.NoError(t, err)
				foundKVRowsRead, foundBatches := -1, -1
				var sb strings.Builder
				for rows.Next() {
					var res string
					assert.NoError(t, rows.Scan(&res))
					sb.WriteString(res)
					sb.WriteByte('\n')
					if matches := kvRowsReadRegex.FindStringSubmatch(res); len(matches) > 0 {
						foundKVRowsRead, err = strconv.Atoi(matches[1])
						assert.NoError(t, err)
					} else if matches = batchCountRegex.FindStringSubmatch(res); len(matches) > 0 {
						foundBatches, err = strconv.Atoi(matches[1])
						assert.NoError(t, err)
					}
				}
				if foundKVRowsRead != testCase.expectedKVRowsRead {
					return fmt.Errorf("expected to scan %d rows, found %d:\n%s", testCase.expectedKVRowsRead, foundKVRowsRead, sb.String())
				}
				if foundBatches != 1 {
					return fmt.Errorf("should use just 1 batch to scan rows, found %d:\n%s", foundBatches, sb.String())
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

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ReplicationMode: base.ReplicationAuto})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]

	// We set up a table with 50 rows that take up 16KiB each and we lower the
	// distsql_workmem session variable to 128KiB. We also collect the stats on
	// the table so that we had an estimated row count of 50 for the scan. With
	// such setup the cFetcher will allocate an output batch of capacity 50, yet
	// after setting the 7th or so row the footprint of the batch will exceed
	// the memory limit. As a result, we will get around 7 batches.
	_, err := conn.ExecContext(ctx, `
SET distsql_workmem='128KiB';
CREATE TABLE t (a PRIMARY KEY, b) AS SELECT i, repeat('a', 16 * 1024) FROM generate_series(1, 50) AS g(i);
ANALYZE t
`)
	assert.NoError(t, err)

	batchCountRegex := regexp.MustCompile(`vectorized batch count: (\d+)`)
	rows, err := conn.QueryContext(ctx, `EXPLAIN ANALYZE (VERBOSE, DISTSQL) SELECT * FROM t`)
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

	// There is a bit of non-determinism (namely, in how data in coldata.Bytes
	// is appended), so we require that we get at least 7 batches. If we get
	// more, that's still ok since it means that the batches were even smaller.
	assert.GreaterOrEqual(t, foundBatches, 7)
}
