// Copyright 2016 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/testutils"
)

func TestNormalizeFunctionName(t *testing.T) {
	testCases := []struct {
		in, out string
		err     string
	}{
		{`foo`, `foo`, ``},
		{`foo.bar`, `foo.bar`, ``},
		{`foo.*.baz`, `foo.*.baz`, ``},
		{`foo[bar].baz`, `foo[bar].baz`, ``},

		{`""`, ``, `empty function name`},
		{`foo.*`, ``, `invalid function name: "foo.*"`},
		{`foo[bar]`, ``, `invalid function name: "foo\[bar\]"`},
	}

	for _, tc := range testCases {
		stmt, err := ParseOneTraditional("SELECT " + tc.in + "(1)")
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		f, ok := stmt.(*Select).Select.(*SelectClause).Exprs[0].Expr.(*FuncExpr)
		if !ok {
			t.Fatalf("%s does not parse to a FuncExpr", tc.in)
		}
		q := f.Name
		_, err = q.Normalize()
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
