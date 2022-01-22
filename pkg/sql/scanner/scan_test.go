// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scanner

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, "abc", s)

	// append some bytes with allocBytes()
	b = scanner.allocBytes(4)
	copy(b, []byte("defg"))
	assert.Equal(t, []byte("abcdefg"), initialBuffer[:7])

	// append other bytes with buffer()+finishString()
	b = scanner.buffer()
	b = append(b, []byte("hi")...)
	s = scanner.finishString(b)
	assert.Equal(t, "hi", s)
	assert.Equal(t, []byte("abcdefghi"), initialBuffer[:9])
}

func makeScanner(str string) Scanner {
	var s Scanner
	s.Init(str)
	return s
}
