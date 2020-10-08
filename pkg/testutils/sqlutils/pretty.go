// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
		// Dataflow of the statement through these checks:
		//
		//             sql (from test file)
		//              |
		//          (parser.Parse)
		//              v
		//           origStmt
		//          /         \
		//    (cfg.Pretty)    (AsStringWithFlags,FmtParsable)
		//       v                          |
		//    prettyStmt                    |
		//       |                          |
		// (parser.ParseOne)                |
		//       v                          |
		//   parsedPretty                   |
		//       |                          |
		// (AsStringWithFlags,FmtSimple)    |
		//       v                          v
		//   prettyFormatted          origFormatted
		//
		// == Check 1: prettyFormatted == origFormatted
		// If false:
		//
		//       |                          |
		//       |                   (parser.ParseOne)
		//       |                          v
		//       |                    reparsedStmt
		//       |                          |
		//       |                 (AsStringWithFlags,FmtParsable)
		//       v                          v
		//   prettyFormatted           origFormatted
		//
		// == Check 2: prettyFormatted == origFormatted
		//
		origStmt := stmts[i].AST
		// Be careful to not simplify otherwise the tests won't round trip.
		prettyStmt := cfg.Pretty(origStmt)
		parsedPretty, err := parser.ParseOne(prettyStmt)
		if err != nil {
			t.Fatalf("%s: %s", err, prettyStmt)
		}
		prettyFormatted := tree.AsStringWithFlags(parsedPretty.AST, tree.FmtSimple)
		origFormatted := tree.AsStringWithFlags(origStmt, tree.FmtParsable)
		if prettyFormatted != origFormatted {
			// Type annotations and unicode strings don't round trip well. Sometimes we
			// need to reparse the original formatted output and format that for these
			// to match.
			reparsedStmt, err := parser.ParseOne(origFormatted)
			if err != nil {
				t.Fatal(err)
			}
			origFormatted = tree.AsStringWithFlags(reparsedStmt.AST, tree.FmtParsable)
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
