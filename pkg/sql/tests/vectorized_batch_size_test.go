// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

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

	_, err := conn.ExecContext(ctx, `CREATE TABLE t (a PRIMARY KEY) AS SELECT generate_series(1, 511)`)
	assert.NoError(t, err)

	rows, err := conn.QueryContext(ctx, `EXPLAIN ANALYZE (DISTSQL) SELECT * FROM t`)
	assert.NoError(t, err)
	batchCountRegex := regexp.MustCompile(`vectorized batch count: (\d+)`)
	var found bool
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
		foundBatches, err := strconv.Atoi(matches[1])
		assert.NoError(t, err)
		assert.Equal(t, 1, foundBatches, "should use just 1 batch to scan 511 rows")
		found = true
		break
	}
	if !found {
		t.Fatalf("expected to find a vectorized batch count; found nothing. text:\n%s", sb.String())
	}
}
