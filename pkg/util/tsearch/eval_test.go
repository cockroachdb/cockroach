// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tsearch

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEval(t *testing.T) {
	tcs := []struct {
		query    string
		vector   string
		expected bool
	}{
		{`a`, `a:10`, true},
		{`a`, ``, false},
		{`b`, `a:10`, false},
		{`!a`, `a:10`, false},
		{`!b`, `a:10`, true},
		{`!b`, ``, true},
		{`c`, `a:10 b:3 c:7`, true},
		{`a|b`, `a:10 b:3 c:7`, true},
		{`c|d`, `a:10 b:3 c:7`, true},
		{`d|e`, `a:10 b:3 c:7`, false},
		{`a&b`, `a:10 b:3 c:7`, true},
		{`c&d`, `a:10 b:3 c:7`, false},
		{`d&e`, `a:10 b:3 c:7`, false},
		{`a&!b`, `a:10 b:3 c:7`, false},
		{`c&!d`, `a:10 b:3 c:7`, true},
		{`d&!e`, `a:10 b:3 c:7`, false},
		{`!d&!e`, `a:10 b:3 c:7`, true},

		// Tests for prefix matching.
		{`a:*`, `ar:10`, true},
		{`ar:*`, `ar:10`, true},
		{`arg:*`, `ar:10`, false},
		{`b:*`, `ar:10`, false},
		{`a:* & ar:*`, `ar:10`, true},
		{`a:* & ar:* & arg:*`, `ar:10`, false},

		// Tests for followed-by.
		{`a <-> b`, `a:1 b:2`, true},
		{`a <-> b`, `a:2 b:1`, false},
		{`a <-> b`, `a:1 b:3`, false},
		{`a <-> b <-> c`, `a:1 b:2 c:3`, true},
		{`(a <-> b) <-> c`, `a:1 b:2 c:3`, true},
		{`a <2> b`, `a:1 b:3`, true},
		{`a:* <0> ab:*`, `abba:1`, true},
		{`a:* <0> ab:*`, `a:1`, false},
		{`a:* <1> abc:*`, `a:1 abd:2`, false},

		// Negations.
		{`a <-> !b`, `a:1 b:2`, false},
		{`a <-> !b`, `a:1 c:2`, true},
		{`a <-> !b`, `a:1,3 c:2 b:4`, true},
		{`a <-> !b`, `a:1,3 b:2,4`, false},
		{`a <-> !b`, `b:2 a:3`, true},
		{`!a <-> b`, `a:1 b:2`, false},
		{`!a <-> b`, `a:1 c:2`, false},
		{`!a <-> b`, `a:1,3 c:2 b:4,5`, true},
		{`!a <-> b`, `a:1,3 b:2,4`, false},
		{`!a <-> b`, `b:2 a:3`, true},
		{`!a <-> !b`, `b:2 a:3`, true},
		{`!a <-> !b`, `a:3 b:4`, true},
		{`!a <-> !b`, `a:3`, true},

		// Or on the RHS of a follows-by.
		{`a <-> (b|c)`, `a:1 b:2`, true},
		{`a <-> (b|c)`, `a:1 c:2`, true},
		{`a <-> (b|c)`, `a:1 d:2`, false},
		{`a <-> (!b|c)`, `a:1 b:2`, false},
		{`a <-> (!b|c)`, `a:1 c:2`, true},
		{`a <-> (!b|c)`, `a:1 d:2`, true},
		{`a <-> (b|!c)`, `a:1 b:2`, true},
		{`a <-> (b|!c)`, `a:1 c:2`, false},
		{`a <-> (b|!c)`, `a:1 d:2`, true},
		{`a <-> (!b|!c)`, `a:1 b:2`, true},
		{`a <-> (!b|!c)`, `a:1 c:2`, true},
		{`a <-> (!b|!c)`, `a:1 d:2`, true},
		{`a <-> ((b <-> c) | d)`, `a:1 b:2 c:3 d:4`, true},
		{`a <-> (b | (c <-> d))`, `a:1 b:2 c:3 d:4`, true},
		// And on the RHS of a follows-by.
		{`a <-> (b&c)`, `a:1 b:2`, false},
		{`a <-> (b&c)`, `a:1 c:2`, false},
		{`a <-> (b&c)`, `a:1 d:2`, false},
		{`a <-> (!b&c)`, `a:1 b:2`, false},
		{`a <-> (!b&c)`, `a:1 c:2`, true},
		{`a <-> (!b&c)`, `a:1 d:2`, false},
		{`a <-> (b&!c)`, `a:1 b:2`, true},
		{`a <-> (b&!c)`, `a:1 c:2`, false},
		{`a <-> (b&!c)`, `a:1 d:2`, false},
		{`a <-> (!b&!c)`, `a:1 b:2`, false},
		{`a <-> (!b&!c)`, `a:1 c:2`, false},
		{`a <-> (!b&!c)`, `a:1 d:2`, true},
		{`a <-> ((b <-> c) & d)`, `a:1 b:2 c:3 d:4`, false},
		{`a <-> (b & (c <-> d))`, `a:1 b:2 c:3 d:4`, false},
	}
	for _, tc := range tcs {
		t.Log(tc)
		q, err := ParseTSQuery(tc.query)
		require.NoError(t, err)
		v, err := ParseTSVector(tc.vector)
		require.NoError(t, err)
		eval, err := EvalTSQuery(q, v)
		require.NoError(t, err)

		assert.Equal(t, tc.expected, eval)
	}

	// This subtest runs all the test cases against PG to ensure the behavior is
	// the same.
	t.Run("ComparePG", func(t *testing.T) {
		// This test can be manually run by pointing it to a local Postgres. There
		// are no requirements for the contents of the local Postgres - it just runs
		// expressions. The test validates that all of the test cases in this test
		// file work the same in Postgres as they do this package.
		skip.IgnoreLint(t, "need to manually enable")
		conn, err := pgx.Connect(context.Background(), "postgresql://jordan@localhost:5432")
		require.NoError(t, err)
		for _, tc := range tcs {
			t.Log(tc)

			var actual bool
			row := conn.QueryRow(context.Background(), "SELECT $1::TSQuery @@ $2::TSVector",
				tc.query, tc.vector,
			)
			require.NoError(t, row.Scan(&actual))
			assert.Equal(t, tc.expected, actual)
		}
	})
}
