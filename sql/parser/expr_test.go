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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package parser

import (
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
)

// TestQualifiedNameString tests the string representation of QualifiedName.
func TestQualifiedNameString(t *testing.T) {
	testCases := []struct {
		in, out string
	}{
		{"*", `"*"`},
		// Keyword.
		{"DATABASE", `"DATABASE"`},
		{"dAtAbAse", `"dAtAbAse"`},
		// Ident format: starts with [a-zA-Z_] or extended ascii,
		// and is then followed by [a-zA-Z0-9$_] or extended ascii.
		{"foo$09", "foo$09"},
		{"_Ab10", "_Ab10"},
		// Everything else quotes the string and escapes double quotes.
		{".foobar", `".foobar"`},
		{`".foobar"`, `""".foobar"""`},
		{`\".foobar\"`, `"\"".foobar\"""`},
	}

	for _, tc := range testCases {
		q := &QualifiedName{Base: Name(tc.in)}
		if q.String() != tc.out {
			t.Errorf("expected q.String() == %q, got %q", tc.out, q.String())
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
		{`foo@bar`, `test.foo@bar`, `test`, ``},
		{`test.foo@bar`, `test.foo@bar`, ``, ``},

		{`""`, ``, ``, `empty table name`},
		{`foo`, ``, ``, `no database specified`},
		{`foo@bar`, ``, ``, `no database specified`},
		{`test.foo.bar`, ``, ``, `invalid table name: test.foo.bar`},
		{`test.foo[bar]`, ``, ``, `invalid table name: test.foo\[bar\]`},
		{`test.foo.bar[blah]`, ``, ``, `invalid table name: test.foo.bar\[blah\]`},
	}

	for _, tc := range testCases {
		stmts, err := Parse("SELECT " + tc.in)
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		q := stmts[0].(*Select).Exprs[0].Expr.(*QualifiedName)
		err = q.NormalizeTableName(tc.db)
		if tc.err != "" {
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %s", tc.in, tc.err, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: expected success, but found %v", tc.in, err)
		}
		if out := q.String(); tc.out != out {
			t.Errorf("%s: expected %s, but found %s", tc.in, tc.out, out)
		}
	}
}

func TestNormalizeColumnName(t *testing.T) {
	testCases := []struct {
		in, out string
		err     string
	}{
		{`foo`, `"".foo`, ``},
		{`"".foo`, `"".foo`, ``},
		{`"".*`, `"".*`, ``},
		{`foo.bar`, `foo.bar`, ``},
		{`foo.*`, `foo.*`, ``},
		{`foo.bar[blah]`, `foo.bar[blah]`, ``},
		{`foo[bar]`, `"".foo[bar]`, ``},

		{`""`, ``, `empty column name`},
		{`test.foo.bar`, ``, `invalid column name: test.foo.bar`},
		{`test.foo.*`, ``, `invalid column name: test.foo.*`},
		{`test.foo@bar`, ``, `invalid column name: test.foo@bar`},
		{`test.foo.bar@blah`, ``, `invalid column name: test.foo.bar@blah`},
	}

	for _, tc := range testCases {
		stmts, err := Parse("SELECT " + tc.in)
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		q := stmts[0].(*Select).Exprs[0].Expr.(*QualifiedName)
		err = q.NormalizeColumnName()
		if tc.err != "" {
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %s", tc.in, tc.err, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: expected success, but found %v", tc.in, err)
		}
		if out := q.String(); tc.out != out {
			t.Errorf("%s: expected %s, but found %s", tc.in, tc.out, out)
		}
	}
}
