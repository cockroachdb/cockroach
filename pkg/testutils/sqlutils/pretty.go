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
	stmts, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("%s: %s", err, sql)
	}
	for i := range stmts {
		origStmt := stmts[i].AST

		if containsCreateRoutine(origStmt) {
			// The Format round-trip check doesn't work for CreateRoutine
			// because Doc re-parses and reformats the routine body, while
			// Format outputs it as a raw string. Use the idempotent check
			// instead, which also verifies the output is parseable.
			//
			// TODO(yang): Run verifyPrettyIdempotent for all statement types.
			// Currently some AST nodes (e.g. FuncExpr with reserved keyword
			// names like "overlaps") have idempotency issues that we need to
			// fix first.
			verifyPrettyIdempotent(t, sql, origStmt, SQL)
		} else {
			verifyStatementPrettyRoundTrip(t, sql, origStmt, SQL)
		}

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
// correctly round trips through the pretty printer by checking
// Format(Parse(Pretty(AST))) == Format(AST). If that fails, it retries with
// Format(Parse(Pretty(AST))) == Format(Parse(Format(AST))).
func verifyStatementPrettyRoundTrip(
	t *testing.T, sql string, origStmt tree.NodeFormatter, p Parser,
) {
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

// verifyPrettyIdempotent checks that Pretty(Parse(Pretty(AST))) == Pretty(AST).
// This check also doubles as a check that the pretty-printed version parses.
func verifyPrettyIdempotent(t *testing.T, sql string, origStmt tree.NodeFormatter, p Parser) {
	cfg := tree.DefaultPrettyCfg()
	cfg.Simplify = false
	pretty1, err := cfg.Pretty(origStmt)
	if err != nil {
		t.Fatalf("%s: %s", err, sql)
	}
	reparsed, err := parseOne(t, pretty1, p)
	if err != nil {
		t.Fatalf("cannot re-parse pretty output: %s\npretty: %s", err, pretty1)
	}
	pretty2, err := cfg.Pretty(reparsed)
	if err != nil {
		t.Fatalf("%s: %s", err, pretty1)
	}
	if pretty1 != pretty2 {
		t.Fatalf("pretty-printing is not idempotent\nfirst:\n%s\nsecond:\n%s",
			pretty1, pretty2)
	}
}

// containsCreateRoutine returns true if the statement is or wraps a
// CreateRoutine (e.g. EXPLAIN CREATE FUNCTION).
func containsCreateRoutine(stmt tree.Statement) bool {
	switch s := stmt.(type) {
	case *tree.CreateRoutine:
		return true
	case *tree.Explain:
		return containsCreateRoutine(s.Statement)
	case *tree.ExplainAnalyze:
		return containsCreateRoutine(s.Statement)
	case *tree.Prepare:
		return containsCreateRoutine(s.Statement)
	default:
		return false
	}
}
