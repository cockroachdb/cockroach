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

	"github.com/cockroachdb/cockroach/pkg/testutils"
)

func TestResolveFunction(t *testing.T) {
	testCases := []struct {
		in, out string
		err     string
	}{
		{`count`, `count`, ``},
		{`pg_catalog.pg_typeof`, `pg_typeof`, ``},

		{`foo`, ``, `unknown function: foo`},
		{`""`, ``, `invalid function name: ""`},
		{`foo.*.baz`, ``, `invalid function name: foo.*.baz`},
		{`foo.*`, ``, `invalid function name: foo.*`},
	}

	searchPath := []string{"pg_catalog"}
	for _, tc := range testCases {
		stmt, err := ParseOneTraditional("SELECT " + tc.in + "(1)")
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		f, ok := stmt.(*Select).Select.(*SelectClause).Exprs[0].Expr.(*FuncExpr)
		if !ok {
			t.Fatalf("%s does not parse to a FuncExpr", tc.in)
		}
		q := f.Func
		_, err = q.Resolve(searchPath)
		if tc.err != "" {
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %v", tc.in, tc.err, err)
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
