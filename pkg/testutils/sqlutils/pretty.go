// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	for i := range stmts {
		origStmt := stmts[i].AST
		verifyStatementPrettyRoundTrip(t, sql, origStmt, SQL)

		// Verify that the AST can be walked.
		if _, err := tree.SimpleStmtVisit(
			origStmt,
			func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) { return },
		); err != nil {
			t.Fatalf("cannot walk stmt %s %v", stmts[i].SQL, err)
		}

	}
}

// verifyStatementPrettyRoundTrip verifies that a SQL or PL/pgSQL statement
// correctly round trips through the pretty printer.
func verifyStatementPrettyRoundTrip(
	t *testing.T, sql string, origStmt tree.NodeFormatter, p Parser,
) {
	t.Helper()
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
	cfg := tree.DefaultPrettyCfg()
	// Be careful to not simplify otherwise the tests won't round trip.
	cfg.Simplify = false
	prettyStmt, err := cfg.Pretty(origStmt)
	if err != nil {
		t.Fatalf("%s: %s", err, prettyStmt)
	}
	parsedPretty, err := parseOne(t, prettyStmt, p)
	if err != nil {
		t.Fatalf("%s: %s", err, prettyStmt)
	}
	prettyFormatted := tree.AsStringWithFlags(parsedPretty, tree.FmtSimple)
	origFormatted := tree.AsStringWithFlags(origStmt, tree.FmtParsable)
	if prettyFormatted != origFormatted {
		// Type annotations and unicode strings don't round trip well. Sometimes we
		// need to reparse the original formatted output and format that for these
		// to match.
		reparsedStmt, err := parseOne(t, origFormatted, p)
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
