// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colflow_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestVectorizedPlanning verifies some assumptions about the vectorized flow
// setup.
func TestVectorizedPlanning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	t.Run("no columnarizer-materializer", func(t *testing.T) {
		if !buildutil.CrdbTestBuild {
			// The expected output below assumes that the invariants checkers
			// are present which are planned only when buildutil.CrdbTestBuild is
			// true; if it isn't, we skip this test.
			return
		}
		// Disable the direct columnar scans to make the output below
		// deterministic.
		_, err := conn.ExecContext(ctx, `SET direct_columnar_scans_enabled = false`)
		require.NoError(t, err)
		// Check that there is no columnarizer-materializer pair on top of the
		// root of the execution tree if the root is a wrapped row-execution
		// processor.
		_, err = conn.ExecContext(ctx, `CREATE TABLE t (id INT PRIMARY KEY, val INT)`)
		require.NoError(t, err)
		rows, err := conn.QueryContext(ctx, `EXPLAIN (VEC, VERBOSE) SELECT * FROM t AS t1 INNER LOOKUP JOIN t AS t2 ON t1.val = t2.id`)
		require.NoError(t, err)
		expectedOutput := []string{
			"│",
			"└ Node 1",
			"  └ *colflow.FlowCoordinator",
			"    └ *rowexec.joinReader",
			"      └ *colexec.Materializer",
			"        └ *colexec.invariantsChecker",
			"          └ *colexecutils.CancelChecker",
			"            └ *colexec.invariantsChecker",
			"              └ *colfetcher.ColBatchScan",
		}
		for rows.Next() {
			var actual string
			require.NoError(t, rows.Scan(&actual))
			expected := expectedOutput[0]
			expectedOutput = expectedOutput[1:]
			require.Equal(t, expected, actual)
		}
	})
}
