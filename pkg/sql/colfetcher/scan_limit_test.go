// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colfetcher_test

import (
	"context"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestScanLimitKVPairsRead is a regression test for fetching an extra KV batch
// beyond the limit when the index has more than one column family and doesn't
// include the max column family. It verifies that the correct number of KV
// pairs are read as reported in EXPLAIN ANALYZE output.
func TestScanLimitKVPairsRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{SQLEvalContext: &eval.TestingKnobs{ForceProductionValues: true}},
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	checkRows := func(rows [][]string) {
		// Look for "KV pairs read" in the output.
		kvPairsReadRE := regexp.MustCompile(`KV pairs read: ([\d,]+)`)
		var kvPairsCount string
		for _, row := range rows {
			if len(row) != 1 {
				t.Fatalf("expected one column in EXPLAIN ANALYZE output")
			}
			if matches := kvPairsReadRE.FindStringSubmatch(row[0]); len(matches) > 0 {
				kvPairsCount = matches[1]
				break
			}
		}
		require.Equal(t, "15", kvPairsCount,
			"expected exactly 15 KV pairs to be read for LIMIT 5 with 3 column families",
		)
	}

	sqlDB.Exec(t, `
  CREATE TABLE t_multi_cf (
    k INT, a INT, b INT, c INT,
    INDEX (k) STORING (a, b),
    FAMILY (k), FAMILY (a), FAMILY (b), FAMILY (c)
  )`)

	// Case with no NULL values.
	sqlDB.Exec(t, `INSERT INTO t_multi_cf (SELECT t, t%19, t*7, t//3 FROM generate_series(1, 100) g(t))`)
	rows := sqlDB.QueryStr(t, `EXPLAIN ANALYZE SELECT a, b FROM t_multi_cf LIMIT 5`)
	checkRows(rows)

	// Case with some NULL values in the stored columns.
	sqlDB.Exec(t, `UPDATE t_multi_cf SET a = NULL WHERE k % 10 = 0`)
	rows = sqlDB.QueryStr(t, `EXPLAIN ANALYZE SELECT a, b FROM t_multi_cf LIMIT 5`)
	checkRows(rows)
}
