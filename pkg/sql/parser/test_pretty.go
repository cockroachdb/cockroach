// Copyright 2018 The Cockroach Authors.
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

package parser

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// VerifyStatementPrettyRoundtrip verifies that the SQL statements in s
// correctly round trip through the pretty printer.
func VerifyStatementPrettyRoundtrip(t *testing.T, sql string) {
	t.Helper()

	stmts, err := Parse(sql)
	if err != nil {
		t.Fatalf("%s: %s", err, sql)
	}
	for _, origStmt := range stmts {
		prettyStmt := tree.Pretty(origStmt)
		parsedPretty, err := ParseOne(prettyStmt)
		if err != nil {
			t.Fatal(err)
		}
		if a, e := parsedPretty.String(), origStmt.String(); a != e {
			// See #26942: sometimes .Format doesn't round trip itself, so have it parse
			// its own output.
			reparsedStmt, err := ParseOne(e)
			if err != nil {
				t.Fatal(err)
			}
			e = reparsedStmt.String()
			if a != e {
				t.Fatalf("\norig SQL: %q\norig formatted: %q\npretty printed: %s\npretty formatted: %q",
					sql,
					e,
					prettyStmt,
					a,
				)
			}
		}
	}
}
