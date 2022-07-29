// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idxrecommendations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/idxrecommendations"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
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
	params, _ := tests.CreateTestServerParams()
	testServer, sqlConn, _ := serverutils.StartServer(t, params)
	defer func() {
		require.NoError(t, sqlConn.Close())
		testServer.Stopper().Stop(ctx)
	}()

	testConn := sqlutils.MakeSQLRunner(sqlConn)
	testConn.Exec(t, "CREATE DATABASE idxrectest")
	testConn.Exec(t, "USE idxrectest")
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
			recommendations: "{\"creation : CREATE INDEX ON t (v);\"}",
		},
		{
			stmt:        "SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE t1.i > 3 AND t2.i > 3",
			fingerprint: "SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k WHERE (t1.i > _) AND (t2.i > _)",
			recommendations: "{\"replacement : CREATE UNIQUE INDEX ON t1 (i) STORING (k); DROP INDEX t1@existing_t1_i;\"," +
				"\"creation : CREATE INDEX ON t2 (i) STORING (k);\"}",
		},
	}

	var recommendations string
	for i := 0; i < 8; i++ {
		for _, tc := range testCases {
			testConn.Exec(t, tc.stmt)
			rows := testConn.QueryRow(t, "SELECT index_recommendations FROM CRDB_INTERNAL.STATEMENT_STATISTICS "+
				" WHERE metadata ->> 'db' = 'idxrectest' AND metadata ->> 'query'=$1", tc.fingerprint)
			rows.Scan(&recommendations)

			expected := tc.recommendations
			if i < 5 {
				expected = "{}"
			}
			require.Equal(t, expected, recommendations)
		}
	}
}

func TestFormatIdxRecommendations(t *testing.T) {
	testCases := []struct {
		title         string
		fullRecInfo   []string
		formattedInfo []string
	}{
		{
			title:         "empty recommendation",
			fullRecInfo:   []string{},
			formattedInfo: []string{},
		},
		{
			title: "single recommendation with one command",
			fullRecInfo: []string{
				"index recommendations: 1",
				"   1. type: index creation",
				"   SQL command: CREATE INDEX ON t2 (i) STORING (k);",
			},
			formattedInfo: []string{"creation : CREATE INDEX ON t2 (i) STORING (k);"},
		},
		{
			title: "single recommendation with multiple commands",
			fullRecInfo: []string{
				"index recommendations: 1",
				"   1. type: index replacement",
				"   SQL commands: CREATE UNIQUE INDEX ON t1 (i) STORING (k); DROP INDEX t1@existing_t1_i;",
			},
			formattedInfo: []string{"replacement : CREATE UNIQUE INDEX ON t1 (i) STORING (k); DROP INDEX t1@existing_t1_i;"},
		},
		{
			title: "multiple recommendations",
			fullRecInfo: []string{
				"index recommendations: 2",
				"   1. type: index replacement",
				"   SQL commands: CREATE UNIQUE INDEX ON t1 (i) STORING (k); DROP INDEX t1@existing_t1_i;",
				"   2. type: index creation",
				"   SQL command: CREATE INDEX ON t2 (i) STORING (k);",
			},
			formattedInfo: []string{
				"replacement : CREATE UNIQUE INDEX ON t1 (i) STORING (k); DROP INDEX t1@existing_t1_i;",
				"creation : CREATE INDEX ON t2 (i) STORING (k);",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			actual := idxrecommendations.FormatIdxRecommendations(tc.fullRecInfo)
			require.Equal(t, tc.formattedInfo, actual)
		})
	}
}
