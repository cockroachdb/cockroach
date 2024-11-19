// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scanner

import (
	"fmt"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestHasMultipleStatements(t *testing.T) {
	testCases := []struct {
		in       string
		expected bool
	}{
		{`a b c`, false},
		{`a; b c`, true},
		{`a b; b c`, true},
		{`a b; b c;`, true},
		{`a b;`, false},
		{`SELECT 123; SELECT 123`, true},
		{`SELECT 123; SELECT 123;`, true},
	}

	for _, tc := range testCases {
		actual, err := HasMultipleStatements(tc.in)
		if err != nil {
			t.Error(err)
		}

		if actual != tc.expected {
			t.Errorf("%q: expected %v, got %v", tc.in, tc.expected, actual)
		}
	}
}

func TestFirstLexicalToken(t *testing.T) {
	tests := []struct {
		s   string
		res int
	}{
		{
			s:   "",
			res: 0,
		},
		{
			s:   " /* comment */ ",
			res: 0,
		},
		{
			s:   "SELECT",
			res: lexbase.SELECT,
		},
		{
			s:   "SELECT 1",
			res: lexbase.SELECT,
		},
		{
			s:   "SELECT 1;",
			res: lexbase.SELECT,
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			tok := FirstLexicalToken(tc.s)
			require.Equal(t, tc.res, tok)
		})
	}
}

func TestLastLexicalToken(t *testing.T) {
	tests := []struct {
		s   string
		res int
	}{
		{
			s:   "",
			res: 0,
		},
		{
			s:   " /* comment */ ",
			res: 0,
		},
		{
			s:   "SELECT",
			res: lexbase.SELECT,
		},
		{
			s:   "SELECT 1",
			res: lexbase.ICONST,
		},
		{
			s:   "SELECT 1;",
			res: ';',
		},
		{
			s:   "SELECT 1; /* comment */",
			res: ';',
		},
		{
			s: `SELECT 1;
			    -- comment`,
			res: ';',
		},
		{
			s: `
				--SELECT 1, 2, 3;
				SELECT 4, 5
				--blah`,
			res: lexbase.ICONST,
		},
		{
			s: `
				--SELECT 1, 2, 3;
				SELECT 4, 5;
				--blah`,
			res: ';',
		},
		{
			s:   `SELECT 'unfinished`,
			res: lexbase.ERROR,
		},
		{
			s:   `SELECT e'\xaa';`, // invalid token but last token is semicolon
			res: ';',
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			tok, ok := LastLexicalToken(tc.s)
			if !ok && tok != 0 {
				t.Fatalf("!ok but nonzero tok")
			}
			if tc.res != tok {
				t.Errorf("expected %d but got %d", tc.res, tok)
			}
		})
	}
}

func TestScannerBuffer(t *testing.T) {
	scanner := makeScanner("pretty long initial query string")

	// get one buffer and return it
	initialBuffer := scanner.buffer()
	b := append(initialBuffer, []byte("abc")...)
	s := scanner.finishString(b)
	require.Equal(t, "abc", s)

	// append some bytes with allocBytes()
	b = scanner.allocBytes(4)
	copy(b, []byte("defg"))
	require.Equal(t, []byte("abcdefg"), initialBuffer[:7])

	// append other bytes with buffer()+finishString()
	b = scanner.buffer()
	b = append(b, []byte("hi")...)
	s = scanner.finishString(b)
	require.Equal(t, "hi", s)
	require.Equal(t, []byte("abcdefghi"), initialBuffer[:9])
}

func makeScanner(str string) Scanner {
	var s Scanner
	s.Init(str)
	return s
}

func TestInspect(t *testing.T) {
	datadriven.RunTest(t, "testdata/inspect", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "inspect":
			sql := strings.TrimSuffix(td.Input, "🛇")
			tokens := Inspect(sql)
			var buf strings.Builder
			fmt.Fprintf(&buf, "input: %q\n", td.Input)
			tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
			for _, tok := range tokens {
				fmt.Fprintf(tw, "%d\t%d\t%s\t%s\t%s\t%q\t%s\n",
					tok.Start, tok.End,
					qMarker(tok.Quoted),
					tokname(tok.ID), tokname(tok.MaybeID),
					tok.Str,
					prefix(td.Input, tok.Start, tok.End),
				)
			}
			_ = tw.Flush()
			return buf.String()
		default:
			t.Fatalf("%s: unrecognized command", td.Pos)
		}
		return ""
	})
}

func tokname(id int32) string {
	if s, ok := tokenNames[int(id)]; ok {
		return s
	}
	if id == 0 {
		return "EOF"
	}
	return fmt.Sprint(id)
}

func qMarker(q bool) string {
	if q {
		return "q"
	}
	return " "
}
func prefix(s string, b, e int32) string {
	s = s[b:]
	if len(s) > int(e-b) {
		s = s[:e-b]
	}
	if len(s) > 5 {
		s = s[:5] + "…"
	}
	if len(s) == 0 {
		s = "🛇"
	}
	return s
}
