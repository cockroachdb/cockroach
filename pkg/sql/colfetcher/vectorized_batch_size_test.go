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

// TestScanBatchSize tests that the the cfetcher's dynamic batch size algorithm
// uses the optimizer's estimated row count for its initial batch size. This
// test sets up a scan against a table with a known row count, and makes sure
// that the optimizer uses its statistics to produce an estimated row count that
// is equal to the number of rows in the table, allowing the fetcher to create
// a single batch for the scan.
func TestScanBatchSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderMetamorphic(t, "This test doesn't work with metamorphic batch sizes.")

	testClusterArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
	}
	tc := testcluster.StartTestCluster(t, 1, testClusterArgs)
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	conn := tc.Conns[0]

	_, err := conn.ExecContext(ctx, `CREATE TABLE t (a PRIMARY KEY) AS SELECT generate_series(1, 511); ANALYZE t`)
	assert.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		rows, err := conn.QueryContext(ctx, `EXPLAIN ANALYZE (VERBOSE, DISTSQL) SELECT * FROM t`)
		assert.NoError(t, err)
		batchCountRegex := regexp.MustCompile(`vectorized batch count: (\d+)`)
		var found, failed bool
		var foundBatches int
		var sb strings.Builder
		for rows.Next() {
			var res string
			assert.NoError(t, rows.Scan(&res))
			sb.WriteString(res)
			sb.WriteByte('\n')
			matches := batchCountRegex.FindStringSubmatch(res)
			if len(matches) == 0 {
				continue
			}
			foundBatches, err = strconv.Atoi(matches[1])
			assert.NoError(t, err)
			if foundBatches != 1 {
				failed = true
			}
			found = true
		}
		if failed {
			return fmt.Errorf("should use just 1 batch to scan 511 rows, found %d:\n%s", foundBatches, sb.String())
		}
		if !found {
			t.Fatalf("expected to find a vectorized batch count; found nothing. text:\n%s", sb.String())
		}
		return nil
	})
}
