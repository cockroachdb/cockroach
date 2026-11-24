// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tsearch

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTSVector(t *testing.T) {
	var inputLarge, expectedLarge string
	var sb strings.Builder
	sb.WriteString("'hi':")
	for i := 1; i < 258; i++ {
		if i > 1 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.Itoa(i))
		if i == 256 {
			expectedLarge = sb.String()
		}
	}
	inputLarge = sb.String()

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

		// Test large positions are truncated.
		{`foo:100000000`, `'foo':16383`},
		{inputLarge, expectedLarge},
	}
	for _, tc := range tcs {
		t.Log(tc.input)
		vec, err := ParseTSVector(tc.input)
		require.NoError(t, err)
		actual := vec.String()
		assert.Equal(t, tc.expectedStr, actual)

		// Test serialization.
		serialized, err := EncodeTSVector(nil, vec)
		require.NoError(t, err)
		roundtripped, err := DecodeTSVector(serialized)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedStr, roundtripped.String())

		serialized, err = EncodeTSVectorPGBinary(nil, vec)
		require.NoError(t, err)
		roundtripped, err = DecodeTSVectorPGBinary(serialized)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedStr, roundtripped.String())

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
	longLexeme := strings.Repeat("a", 2047)
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
		longLexeme,
	} {
		t.Log(tc)
		_, err := ParseTSVector(tc)
		assert.Error(t, err)
	}
}

func TestParseTSRandom(t *testing.T) {
	r, _ := randutil.NewTestRand()
	for i := 0; i < 100; i++ {
		b := randutil.RandBytes(r, randutil.RandIntInRange(r, 0, 2047))
		// Just make sure we don't panic
		_, _ = ParseTSVector(string(b))
		_, _ = ParseTSQuery(string(b))
	}
	for i := 0; i < 1000; i++ {
		v := RandomTSVector(r)
		text := v.String()
		v2, err := ParseTSVector(text)
		assert.NoError(t, err)
		assert.Equal(t, v, v2)
	}
}

func TestTSVectorStringSize(t *testing.T) {
	r, _ := randutil.NewTestRand()
	for i := 0; i < 1000; i++ {
		v := RandomTSVector(r)
		require.Equal(t, len(v.String()), v.StringSize())
	}
}

func BenchmarkTSVector(b *testing.B) {
	r, _ := randutil.NewTestRand()
	tsVectors := make([]TSVector, 10000)
	for i := range tsVectors {
		tsVectors[i] = RandomTSVector(r)
	}
	b.ResetTimer()
	b.Run("String", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, v := range tsVectors {
				_ = v.String()
			}
		}
	})
	b.Run("StringSize", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, v := range tsVectors {
				_ = v.StringSize()
			}
		}
	})
}
