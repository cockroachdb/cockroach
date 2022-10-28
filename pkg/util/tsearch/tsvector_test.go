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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTSVector(t *testing.T) {
	tcs := []struct {
		input       string
		expectedStr string
	}{
		{``, ``},
		{`foo`, `'foo'`},
		{`foo bar`, `'bar' 'foo'`},
		{`foo   bar`, `'bar' 'foo'`},
		{`foo  
bar   `, `'bar' 'foo'`},
		{`foo` + "\t" + `bar  `, `'bar' 'foo'`},
		{`foo ` + "\t" + `bar  `, `'bar' 'foo'`},
		{`foo:3`, `'foo':3`},
		{`foo:03`, `'foo':3`},
		{`foo:3a`, `'foo':3A`},
		{`foo:3A`, `'foo':3A`},
		{`foo:3D`, `'foo':3`},
		{`foo:30A`, `'foo':30A`},
		{`foo:20,30b,1a`, `'foo':1A,20,30B`},
		{`foo:3 bar`, `'bar' 'foo':3`},
		{`foo:3a bar`, `'bar' 'foo':3A`},
		{`foo:3a bar:100`, `'bar':100 'foo':3A`},
		{`:foo`, `':foo'`},
		{`:`, `':'`},
		{`\:`, `':'`},
		{`'\:'`, `':'`},
		{`'\ '`, `' '`},
		{`\ `, `' '`},
		{`\\`, `'\'`},
		{`:3`, `':3'`},
		{`::3`, `':':3`},
		{`:3:3`, `':3':3`},

		{`blah'blah`, `'blah''blah'`},
		{`blah'`, `'blah'''`},
		{`blah''`, `'blah'''''`},
		{`blah'':3`, `'blah''''':3`},
		{`blah\'':3`, `'blah''''':3`},
		{`b\:lah\::3`, `'b:lah:':3`},
		{`b\ lah\ :3`, `'b lah ':3`},
		{`b\lah:3`, `'blah':3`},
		{`b\lah\:`, `'blah:'`},

		{`'blah'`, `'blah'`},
		{`'blah':3`, `'blah':3`},
		{`'bla\'h'`, `'bla''h'`},
		{`'bla\h'`, `'blah'`},
		{`'bla\ h'`, `'bla h'`},
		{`'bla h'`, `'bla h'`},
		{`'blah'arg:3`, `'arg':3 'blah'`},
		{`'blah'arg`, `'arg' 'blah'`},

		// Test that we sort and de-duplicate inputs (both lexemes and position
		// lists).
		{`foo:3,2,1`, `'foo':1,2,3`},
		{`foo:3,1`, `'foo':1,3`},
		{`foo:3,2,1 foo:1,2,3`, `'foo':1,2,3`},
		{`a:3 b:2 a:1`, `'a':1,3 'b':2`},
	}
	for _, tc := range tcs {
		t.Log(tc.input)
		vec, err := ParseTSVector(tc.input)
		require.NoError(t, err)
		actual := vec.String()
		assert.Equal(t, tc.expectedStr, actual)
	}

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

			var actual string
			row := conn.QueryRow(context.Background(), "SELECT $1::TSVector", tc.input)
			require.NoError(t, row.Scan(&actual))
			// To make sure that our tests behave as expected, we need to remove
			// doubled backslashes from Postgres's output. Confusingly, even in the
			// protocol that pgx uses, the actual text returned for a string
			// containing a backslash doubles all backslash literals.
			actual = strings.ReplaceAll(actual, `\\`, `\`)
			assert.Equal(t, tc.expectedStr, actual)
		}
	})
}

func TestParseTSVectorError(t *testing.T) {
	for _, tc := range []string{
		`foo:`,
		`foo:a`,
		`foo:ab`,
		`foo:0`,
		`foo:0,3`,
		`foo:1f`,
		`foo:1f blah`,
		`foo:1,`,
		`foo:1, a`,
		`foo:1, `,
		`foo:\1`,
		`foo:-1`,
		`'foo`,
	} {
		t.Log(tc)
		_, err := ParseTSVector(tc)
		assert.Error(t, err)
	}
}

func TestParseTSRandom(t *testing.T) {
	r, _ := randutil.NewTestRand()
	for i := 0; i < 10000; i++ {
		b := randutil.RandBytes(r, randutil.RandIntInRange(r, 0, 50))
		// Just make sure we don't panic
		_, _ = ParseTSVector(string(b))
		_, _ = ParseTSQuery(string(b))
	}
}
