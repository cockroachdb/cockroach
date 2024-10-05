// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package idxrecommendations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/idxrecommendations"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/indexrec"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestIndexRecommendationsStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testServer, sqlConn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer testServer.Stopper().Stop(ctx)

	testConn := sqlutils.MakeSQLRunner(sqlConn)
	testConn.Exec(t, "CREATE DATABASE idxrectest")
	testConn.Exec(t, "USE idxrectest")
	minExecutions := 5
	var recommendations string

	t.Run("index recommendations generated", func(t *testing.T) {
		testConn.Exec(t, "CREATE TABLE t ( k INT PRIMARY KEY, v INT, FAMILY \"primary\" (k, v))")
		testConn.Exec(t, "CREATE TABLE t1 (k INT, i INT, f FLOAT, s STRING)")
		testConn.Exec(t, "CREATE TABLE t2 (k INT, i INT, s STRING)")
		testConn.Exec(t, "CREATE UNIQUE INDEX existing_t1_i ON t1(i)")

		testCases := []struct {
			stmt            string
			fingerprint     string
			recommendations string
		}{
			{
				stmt:            "SELECT * FROM t WHERE v > 123",
				fingerprint:     "SELECT * FROM t WHERE v > _",
				recommendations: "{\"creation : CREATE INDEX ON idxrectest.public.t (v);\"}",
			},
			{
				stmt:        "SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE t1.i > 3 AND t2.i > 3",
				fingerprint: "SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE (t1.i > _) AND (t2.i > _)",
				recommendations: "{\"replacement : CREATE UNIQUE INDEX ON idxrectest.public.t1 (i) STORING (k); DROP INDEX idxrectest.public.t1@existing_t1_i;\"," +
					"\"creation : CREATE INDEX ON idxrectest.public.t2 (i) STORING (k);\"}",
			},
		}

		for i := 0; i < (minExecutions + 2); i++ {
			for _, tc := range testCases {
				testConn.Exec(t, tc.stmt)
				rows := testConn.QueryRow(t, "SELECT index_recommendations FROM CRDB_INTERNAL.STATEMENT_STATISTICS "+
					" WHERE metadata ->> 'db' = 'idxrectest' AND metadata ->> 'query'=$1", tc.fingerprint)
				rows.Scan(&recommendations)

				expected := tc.recommendations
				if i < minExecutions {
					expected = "{}"
				}
				require.Equal(t, expected, recommendations)
			}
		}
	})

	t.Run("index recommendations not generated for one-phase commit "+
		"optimization statement", func(t *testing.T) {
		testConn.Exec(t, "CREATE TABLE t3 (k INT PRIMARY KEY)")

		for i := 0; i < (minExecutions + 2); i++ {
			testConn.Exec(t, `INSERT INTO t3 VALUES($1)`, i)
			rows := testConn.QueryRow(t, "SELECT index_recommendations FROM CRDB_INTERNAL.STATEMENT_STATISTICS "+
				" WHERE metadata ->> 'db' = 'idxrectest' AND metadata ->> 'query' = 'INSERT INTO t3 VALUES (_)'")
			rows.Scan(&recommendations)

			require.Equal(t, "{}", recommendations)
		}

	})
}

func TestFormatIdxRecommendations(t *testing.T) {
	testCases := []struct {
		title         string
		indexRecs     []indexrec.Rec
		formattedInfo []string
	}{
		{
			title:         "nil recommendation",
			indexRecs:     nil,
			formattedInfo: nil,
		},
		{
			title:         "empty recommendation",
			indexRecs:     []indexrec.Rec{},
			formattedInfo: nil,
		},
		{
			title: "single recommendation with one command",
			indexRecs: []indexrec.Rec{{
				SQL: "CREATE INDEX ON t2 (i) STORING (k);",
			}},
			formattedInfo: []string{"creation : CREATE INDEX ON t2 (i) STORING (k);"},
		},
		{
			title: "single recommendation with multiple commands",
			indexRecs: []indexrec.Rec{{
				SQL:     "CREATE UNIQUE INDEX ON t1 (i) STORING (k); DROP INDEX t1@existing_t1_i;",
				RecType: indexrec.TypeReplaceIndex,
			}},
			formattedInfo: []string{"replacement : CREATE UNIQUE INDEX ON t1 (i) STORING (k); DROP INDEX t1@existing_t1_i;"},
		},
		{
			title: "multiple recommendations",
			indexRecs: []indexrec.Rec{{
				SQL:     "CREATE UNIQUE INDEX ON t1 (i) STORING (k); DROP INDEX t1@existing_t1_i;",
				RecType: indexrec.TypeReplaceIndex,
			}, {
				SQL: "CREATE INDEX ON t2 (i) STORING (k);",
			}},
			formattedInfo: []string{
				"replacement : CREATE UNIQUE INDEX ON t1 (i) STORING (k); DROP INDEX t1@existing_t1_i;",
				"creation : CREATE INDEX ON t2 (i) STORING (k);",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			actual := idxrecommendations.FormatIdxRecommendations(tc.indexRecs)
			require.Equal(t, tc.formattedInfo, actual)
		})
	}
}
