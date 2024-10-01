// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlshell

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestCheckInputComplete(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		// In the tests below, a '@' character in the test input
		// marks the position of the input cursor.
		t        string
		expected bool
	}{
		// An empty line is considered complete when the enter key is
		// pressed. This forces re-display of a prompt and the key binding
		// help.
		{`@`, true},
		// The 'exit' keyword on its own is considered a complete input,
		// no matter where the cursor is.
		{`@   exit  `, true},
		{`  @exit  `, true},
		{`   exit @ `, true},
		// Ditto 'help' and 'quit'.
		{`@   help  `, true},
		{`   help  @`, true},
		{`@  quit  `, true},
		{`   quit  @`, true},
		// If the input starts with a client-side command, no matter how
		// many lines and where the cursor is, enter considers it to be a
		// complete input.
		{`\d@`, true},
		{`@\d`, true},
		{`@\d
secondline`, true},
		{`\d@
secondline`, true},
		{`\d
secondline@`, true},
		// Only lines that _start_ with '\' are considered to be
		// client-side commands.
		{`  \d@`, false},
		// For everything else, we look at the sql syntax.
		{`@select 123`, false},
		{`select 123@`, false},
		{`select@123`, false},
		{`select@
123`, false},
		{`@select 123;`, true},
		{`select 123;@`, true},
		{`select@123;`, true},
		{`select@
123;`, true},
		// If the lexical structure is incorrect, the input is not
		// considered to be complete.
		{`select '123;`, false},
		// The semicolon must be at the end; if we have more than one statement
		// there must be a semicolon after the last.
		{`select 123; select 123@`, false},
		{`select 123;@ select 123`, false},
		{`select 123; select 123;@`, true},
		{`select 123;@ select 123;`, true},
	}

	for _, tc := range testData {
		v, line, col := torunes(tc.t)
		actual := checkInputComplete(v, line, col)
		if actual != tc.expected {
			t.Errorf("%q: expected %v, got %v", tc.t, tc.expected, actual)
		}
	}
}

func torunes(s string) (v [][]rune, lnum, cnum int) {
	lines := strings.Split(s, "\n")
	for l, line := range lines {
		if c := strings.IndexByte(line, '@'); c >= 0 {
			lnum = l
			cnum = c
			line = strings.ReplaceAll(line, "@", "")
		}
		v = append(v, []rune(line))
	}
	return v, lnum, cnum
}
