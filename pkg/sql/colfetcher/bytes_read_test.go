// Copyright 2022 The Cockroach Authors.
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
	"regexp"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestBytesRead verifies that the ColBatchScan and the ColIndexJoin correctly
// report the number of bytes read.
func TestBytesRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testClusterArgs := base.TestClusterArgs{ReplicationMode: base.ReplicationAuto}
	tc := testcluster.StartTestCluster(t, 1, testClusterArgs)
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	conn := tc.Conns[0]

	// Create the table with disabled automatic table stats collection. The
	// stats collection is disabled so that the ColBatchScan would read the
	// first row in one batch and then the second row in another batch.
	_, err := conn.ExecContext(ctx, `
SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false;
`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `
CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT, INDEX(b));
INSERT INTO t VALUES (1, 1, 1), (2, 2, 2);
`)
	require.NoError(t, err)

	// Run the query that reads from the secondary index and then performs an
	// index join against the primary index.
	query := "EXPLAIN ANALYZE SELECT * FROM t@t_b_idx"
	kvBytesReadRegex := regexp.MustCompile(`KV bytes read: (\d+) B`)
	matchIdx := 0
	rows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err)
	for rows.Next() {
		var res string
		require.NoError(t, rows.Scan(&res))
		if matches := kvBytesReadRegex.FindStringSubmatch(res); len(matches) > 0 {
			bytesRead, err := strconv.Atoi(matches[1])
			require.NoError(t, err)
			// We're only interested in 'bytes read' statistic being non-zero.
			require.Greater(
				t, bytesRead, 0, "expected bytes read to be greater than zero",
			)
			matchIdx++
		}
	}
}
