// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tsearch

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanTSQuery(t *testing.T) {
	for _, tc := range []struct {
		input       string
		expectedStr string
	}{
		{`foo&bar`, `'foo' & 'bar'`},

		{``, ``},
		{`foo`, `'foo'`},
		{`foo bar`, `'foo' 'bar'`},
		{`foo   bar`, `'foo' 'bar'`},
		{`foo  
bar   `, `'foo' 'bar'`},
		{`foo` + "\t" + `bar  `, `'foo' 'bar'`},
		{`foo ` + "\t" + `bar  `, `'foo' 'bar'`},

		{`foo:a`, `'foo':A`},
		{`foo:ab`, `'foo':AB`},
		{`foo:ba`, `'foo':AB`},
		{`foo:dbaba`, `'foo':ABD`},
		{`foo:*`, `'foo':*`},
		{`foo:cab*cccdba`, `'foo':*ABCD`},

		{`\:`, `':'`},
		{`'\:'`, `':'`},
		{`'\ '`, `' '`},
		{`\ `, `' '`},
		{`\\`, `'\'`},

		{`blah'blah`, `'blah''blah'`},
		{`blah'`, `'blah'''`},
		{`blah''`, `'blah'''''`},
		{`b\lah\:`, `'blah:'`},

		{`'blah'`, `'blah'`},
		{`'bla\'h'`, `'bla''h'`},
		{`'bla\h'`, `'blah'`},
		{`'bla\ h'`, `'bla h'`},
		{`'bla h'`, `'bla h'`},
		{`'blah'arg`, `'blah' 'arg'`},

		{`foo&bar`, `'foo' & 'bar'`},
		{`foo\&bar`, `'foo&bar'`},
		{`\&bar`, `'&bar'`},
		{`'b&ar'`, `'b&ar'`},
		{`foo   |bar`, `'foo' | 'bar'`},
		{`foo!bar`, `'foo' ! 'bar'`},
		{`foo<->bar`, `'foo' <-> 'bar'`},
		{`foo<1>bar`, `'foo' <-> 'bar'`},
		{`foo<2>bar`, `'foo' <2> 'bar'`},
		{`foo<0>bar`, `'foo' <0> 'bar'`},
		{`foo<20>bar`, `'foo' <20> 'bar'`},
		{`foo<020>bar`, `'foo' <20> 'bar'`},
		{`foo<16384>bar`, `'foo' <16384> 'bar'`},

		{`()`, `( )`},
		{`f(a)b`, `'f' ( 'a' ) 'b'`},
		{`'(' ')'`, `'(' ')'`},
	} {
		t.Log(tc.input)
		vec, err := lexTSQuery(tc.input)
		require.NoError(t, err)
		actual := vec.String()
		assert.Equal(t, tc.expectedStr, actual)
	}
}

func TestScanTSQueryError(t *testing.T) {
	for _, tc := range []string{
		`foo:3`,
		`foo:3A`,
		`foo:3D`,
		`foo:3D`,
		`foo:30A`,
		`foo:30A,20,1a`,
		`foo:3 bar`,
		`foo:3a bar`,
		`foo:3a bar:100`,

		`:`,
		`:3`,
		`::3`,
		`:3:3`,
		`:foo`,

		`blah'':3`,
		`blah\'':3`,
		`b\:lah\::3`,
		`b\ lah\ :3`,
		`b\lah:3`,
		`'blah':3`,
		`'blah'arg:3`,

		`a<`,
		`a<-x`,
		`a<-0`,
		`a<b`,
		`a<-b`,
		`a<0b`,
		`a<01b`,
		`a<01-`,
		`a<>`,
		`a <16385> b`,
	} {
		t.Log(tc)
		_, err := lexTSQuery(tc)
		assert.Error(t, err)
	}
}

func TestParseTSQuery(t *testing.T) {
	tcs := []struct {
		input       string
		expectedStr string
	}{
		{`foo`, `'foo'`},

		{`foo:a`, `'foo':A`},
		{`foo:ab`, `'foo':AB`},
		{`foo:ba`, `'foo':AB`},
		{`foo:dbaba`, `'foo':ABD`},
		{`foo:*`, `'foo':*`},
		{`foo:cab*cccdba`, `'foo':*ABCD`},

		{`\:`, `':'`},
		{`'\:'`, `':'`},
		{`'\ '`, `' '`},
		{`\ `, `' '`},
		{`\\`, `'\'`},

		{`blah'blah`, `'blah''blah'`},
		{`blah'`, `'blah'''`},
		{`blah''`, `'blah'''''`},
		{`b\lah\:`, `'blah:'`},

		{`'blah'`, `'blah'`},
		{`'bla\'h'`, `'bla''h'`},
		{`'bla\h'`, `'blah'`},
		{`'bla\ h'`, `'bla h'`},
		{`'bla h'`, `'bla h'`},

		{`foo&bar`, `'foo' & 'bar'`},
		{`foo\&bar`, `'foo&bar'`},
		{`\&bar`, `'&bar'`},
		{`'b&ar'`, `'b&ar'`},
		{`foo   |bar`, `'foo' | 'bar'`},
		{`foo<->bar`, `'foo' <-> 'bar'`},
		{`foo<1>bar`, `'foo' <-> 'bar'`},
		{`foo<2>bar`, `'foo' <2> 'bar'`},
		{`foo<0>bar`, `'foo' <0> 'bar'`},
		{`foo<20>bar`, `'foo' <20> 'bar'`},
		{`foo<020>bar`, `'foo' <20> 'bar'`},
		{`foo<->bar<->baz`, `'foo' <-> 'bar' <-> 'baz'`},
		{`foo<->bar&baz|qux`, `'foo' <-> 'bar' & 'baz' | 'qux'`},
		{`(foo)`, `'foo'`},
		{`((foo))`, `'foo'`},

		{`!(a | !b)`, `!( 'a' | !'b' )`},
		{`!(a | (b & c))`, `!( 'a' | 'b' & 'c' )`},
		{`a & b & c`, `'a' & 'b' & 'c'`},
		{`a & b & !c`, `'a' & 'b' & !'c'`},
		{`d <0> !x | y`, `'d' <0> !'x' | 'y'`},
		{`a <-> (d <0> !x | y)`, `'a' <-> ( 'd' <0> !'x' | 'y' )`},
		{`(a) | b`, `'a' | 'b'`},
		{`!a | b`, `!'a' | 'b'`},
		{`!!a`, `!!'a'`},
		{`!!(a & b)`, `!!( 'a' & 'b' )`},
		{`!!a & b`, `!!'a' & 'b'`},

		{`(b & c) <-> (d <0> !x | y)`, `( 'b' & 'c' ) <-> ( 'd' <0> !'x' | 'y' )`},
		{`a | (b & c) <-> (d <0> !x | y)`, `'a' | ( 'b' & 'c' ) <-> ( 'd' <0> !'x' | 'y' )`},
		{`!(a | (b & c) <-> (d <0> !x | y))`, `!( 'a' | ( 'b' & 'c' ) <-> ( 'd' <0> !'x' | 'y' ) )`},
	}
	for _, tc := range tcs {
		t.Run(tc.input, func(t *testing.T) {
			query, err := ParseTSQuery(tc.input)
			assert.NoError(t, err)
			actual := query.String()
			assert.Equal(t, tc.expectedStr, actual)

			// Test serialization.
			serialized, err := EncodeTSQuery(nil, query)
			require.NoError(t, err)
			roundtripped, err := DecodeTSQuery(serialized)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedStr, roundtripped.String())

			serialized = EncodeTSQueryPGBinary(nil, query)
			roundtripped, err = DecodeTSQueryPGBinary(serialized)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedStr, roundtripped.String())
		})
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
			row := conn.QueryRow(context.Background(), "SELECT $1::TSQuery", tc.input)
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

func TestParseTSQueryAssociativity(t *testing.T) {
	for _, tc := range []struct {
		input       string
		expectedStr string
	}{
		{`a<->b<->c`, `[a<->[b<->c]]`},
		{`a|b|c`, `[a|[b|c]]`},
		{`a&b&c`, `[a&[b&c]]`},
		{`a<->b&c|d`, `[[[a<->b]&c]|d]`},
		{`a&b<->c`, `[a&[b<->c]]`},
		{`a|b&c<->d`, `[a|[b&[c<->d]]]`},
		{`a|b<->c&d`, `[a|[[b<->c]&d]]`},
	} {
		t.Log(tc.input)
		query, err := ParseTSQuery(tc.input)
		assert.NoError(t, err)
		actual := query.root.UnambiguousString()
		assert.Equal(t, tc.expectedStr, actual)
	}
}

func TestParseTSQueryError(t *testing.T) {
	longLexeme := strings.Repeat("a", 2047)
	for _, tc := range []string{
		``,
		`       `,
		` | `,
		`&`,
		`<->`,
		`<0>`,
		`!`,
		`(`,
		`)`,
		`(foo))`,
		`(foo`,
		`foo )`,
		`foo bar`,
		`foo(bar)`,
		`foo!bar`,
		`&foo`,
		`|foo`,
		`<->foo`,
		`foo&`,
		`foo|`,
		`foo!`,
		`foo<->`,
		`()`,
		`(foo bar)`,
		`f(a)b`,
		longLexeme,
	} {
		t.Log(tc)
		_, err := ParseTSQuery(tc)
		assert.Error(t, err)
	}
}
