// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package parser

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func (tn *TableName) resetRepr() {
	tn.dbNameOriginallyOmitted = false
}

func TestNormalizeName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		in, expected string
	}{
		{"HELLO", "hello"},                            // Lowercase is the norm
		{"ıİ", "ii"},                                  // Turkish/Azeri special cases
		{"no\u0308rmalization", "n\u00f6rmalization"}, // NFD -> NFC.
	}
	for _, test := range testCases {
		s := ReNormalizeName(test.in)
		if test.expected != s {
			t.Errorf("%s: expected %s, but found %s", test.in, test.expected, s)
		}
	}
}

func TestNormalizeTableName(t *testing.T) {
	testCases := []struct {
		in, out string
		db      string
		err     string
	}{
		{`foo`, `test.foo`, `test`, ``},
		{`test.foo`, `test.foo`, ``, ``},
		{`bar.foo`, `bar.foo`, `test`, ``},

		{`""`, ``, ``, `empty table name`},
		{`foo`, ``, ``, `no database specified`},
		{`foo@bar`, ``, ``, `syntax error`},
		{`test.foo.bar`, ``, ``, `invalid table name: "test.foo.bar"`},
		{`test.foo[bar]`, ``, ``, `invalid table name: "test.foo\[bar\]"`},
		{`test.foo.bar[blah]`, ``, ``, `invalid table name: "test.foo.bar\[blah\]"`},
		{`test.*`, ``, ``, `invalid table name: "test.*"`},
		{`test[blah]`, ``, ``, `invalid table name: "test\[blah\]"`},
	}

	for _, tc := range testCases {
		tn, err := ParseTableNameTraditional(tc.in)
		if err == nil {
			err = tn.QualifyWithDatabase(tc.db)
		}
		if tc.err != "" {
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %v", tc.in, tc.err, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: expected success, but found %v", tc.in, err)
		}
		tn.resetRepr()
		if out := tn.String(); tc.out != out {
			t.Errorf("%s: expected %s, but found %s", tc.in, tc.out, out)
		}
	}
}
