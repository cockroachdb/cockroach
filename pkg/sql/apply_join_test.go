// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestApplyJoinError is a regression test for not closing the subqueries in the
// apply join if they hit an error (#54166). The underlying error is returned
// because of a known limitation of apply joins with subqueries (#39433).
func TestApplyJoinError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testClusterArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
	}
	tc := testcluster.StartTestCluster(t, 1, testClusterArgs)
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	conn := tc.Conns[0]

	for _, vectorizeOption := range []string{"on", "off"} {
		_, err := conn.Exec("SET vectorize=$1", vectorizeOption)
		require.NoError(t, err)
		_, err = conn.Exec(`
SELECT
  (
    SELECT
      tab_4.col_4
    FROM
      (VALUES (1)) AS tab_1 (col_1)
      JOIN (
          VALUES
            (
              (
                SELECT
                  1
                FROM
                  (SELECT 1)
                WHERE
                  EXISTS(SELECT 1 / (SELECT 0))
              )
            )
        )
          AS tab_6 (col_6) ON (tab_1.col_1) = (tab_6.col_6)
  )
FROM
  (VALUES (NULL)) AS tab_4 (col_4),
  (VALUES (NULL), (NULL)) AS tab_5 (col_5)
`)
		// We expect that an internal error is returned.
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "internal error"))
	}
}

// TestApplyJoinPanic is a regression test for not closing the planNode tree in
// the apply join that resulted in a panic that was caught by the vectorized
// engine (#59871). At the moment of writing it's unclear why the type mismatch
// causing the panic in the first place occurs.
func TestApplyJoinPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testClusterArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
	}
	tc := testcluster.StartTestCluster(t, 1, testClusterArgs)
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	conn := tc.Conns[0]

	_, err := conn.Exec(`
WITH
  with_2675 AS (SELECT * FROM (VALUES ('', (SELECT ''))) AS tab_8229 (col_14424, col_14425))
SELECT
  (
    SELECT
      cte_ref_793.col_14424
    WHERE
      EXISTS(
        SELECT
          NULL
        WHERE
          ''
          IN (
              (
                SELECT
                  NULL
                FROM
                  (VALUES (NULL), ((SELECT B'101100110111011110011100101001110000111001110' FROM (VALUES (0)) AS tab_8238 (col_14434))))
                    AS tab_8239 (col_14436)
                ORDER BY
                  tab_8239.col_14436 DESC
                LIMIT
                  1
              ),
              ''
            )
      )
  ),
  cte_ref_793.col_14425
FROM
  with_2675 AS cte_ref_793;
`)
	// We expect that an internal error is returned.
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "invalid datum type given"))
}
