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

package sqlutils

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// VerifyStatementPrettyRoundtrip verifies that the SQL statements in s
// correctly round trip through the pretty printer.
func VerifyStatementPrettyRoundtrip(t *testing.T, sql string) {
	t.Helper()

	stmts, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("%s: %s", err, sql)
	}
	cfg := tree.DefaultPrettyCfg()
	// Be careful to not simplify otherwise the tests won't round trip.
	cfg.Simplify = false
	for i := range stmts {
		origStmt := stmts[i].AST
		// Be careful to not simplify otherwise the tests won't round trip.
		prettyStmt := cfg.Pretty(origStmt)
		parsedPretty, err := parser.ParseOne(prettyStmt)
		if err != nil {
			t.Fatalf("%s: %s", err, prettyStmt)
		}
		prettyFormatted := tree.AsStringWithFlags(parsedPretty, tree.FmtSimple)
		origFormatted := tree.AsStringWithFlags(origStmt, tree.FmtParsable)
		if prettyFormatted != origFormatted {
			// Type annotations and unicode strings don't round trip well. Sometimes we
			// need to reparse the original formatted output and format that for these
			// to match.
			reparsedStmt, err := parser.ParseOne(origFormatted)
			if err != nil {
				t.Fatal(err)
			}
			origFormatted = tree.AsStringWithFlags(reparsedStmt, tree.FmtParsable)
			if prettyFormatted != origFormatted {
				t.Fatalf("orig formatted != pretty formatted\norig SQL: %q\norig formatted: %q\npretty printed: %s\npretty formatted: %q",
					sql,
					origFormatted,
					prettyStmt,
					prettyFormatted,
				)
			}
		}
	}
}
