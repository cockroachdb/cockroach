// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlutils

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	plpgsqlparser "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type Parser int

const (
	SQL Parser = iota
	PLpgSQL
)

func parse(t *testing.T, input string, p Parser) (statements.ParsedStmts, error) {
	t.Helper()
	switch p {
	case SQL:
		return parser.Parse(input)
	case PLpgSQL:
		return plpgsqlparser.Parse(input)
	default:
		t.Fatalf("unexpected parser type: %v", p)
	}
	return nil, fmt.Errorf("unreachable code")
}

func parseOne(t *testing.T, input string, p Parser) (tree.NodeFormatter, error) {
	t.Helper()
	switch p {
	case SQL:
		stmt, err := parser.ParseOne(input)
		if err != nil {
			return nil, err
		}
		return stmt.AST, err
	case PLpgSQL:
		stmt, err := plpgsqlparser.Parse(input)
		if err != nil {
			return nil, err
		}
		return stmt.AST, err
	default:
		t.Fatalf("unexpected parser type: %v", p)
	}
	return nil, fmt.Errorf("unreachable code")
}

// VerifyParseFormat is used in the SQL and PL/pgSQL datadriven parser tests to
// check that a successfully parsed expression round trips and correctly handles
// formatting flags.
//
// -reParseWithoutLiterals indicates whether the statement should be re-parsed
// after constants are removed. This can be needed to handle cases where quotes
// are formatted differently depending on the string content.
func VerifyParseFormat(
	t *testing.T, input, pos string, p Parser, reParseWithoutLiterals bool,
) string {
	t.Helper()

	// Check parse.
	stmts, err := parse(t, input, p)
	if err != nil {
		t.Fatalf("%s\nunexpected parse error: %v", pos, err)
	}

	// Check pretty-print roundtrip.
	switch p {
	case SQL:
		VerifyStatementPrettyRoundtrip(t, input)
	case PLpgSQL:
		plStmt := stmts.(statements.PLpgStatement).AST
		verifyStatementPrettyRoundTrip(t, input, plStmt, PLpgSQL)
	default:
		t.Fatalf("unexpected parser type: %v", p)
	}

	ref := stmts.StringWithFlags(tree.FmtSimple)
	note := ""
	if ref != input {
		note = " -- normalized!"
	}

	// Check roundtrip and formatting with flags.
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s%s\n", ref, note)
	fmt.Fprintln(&buf, stmts.StringWithFlags(tree.FmtAlwaysGroupExprs), "-- fully parenthesized")
	constantsHidden := stmts.StringWithFlags(tree.FmtHideConstants)
	fmt.Fprintln(&buf, constantsHidden, "-- literals removed")

	if reParseWithoutLiterals {
		// As of this writing, the SQL statement stats proceed as follows:
		// first the literals are removed from statement to form a stat key,
		// then the stat key is re-parsed, to undergo the anonymization stage.
		// We also want to check the re-parsing is fine.
		reparsedStmts, err := parse(t, constantsHidden, p)
		if err != nil {
			t.Fatalf("%s\nunexpected error when reparsing without literals: %+v", pos, err)
		} else {
			reparsedStmtsS := reparsedStmts.String()
			if reparsedStmtsS != constantsHidden {
				t.Fatalf(
					"%s\nmismatched AST when reparsing without literals:\noriginal: %s\nexpected: %s\nactual:   %s",
					pos, input, constantsHidden, reparsedStmtsS,
				)
			}
		}
	}

	fmt.Fprintln(&buf, stmts.StringWithFlags(tree.FmtAnonymize), "-- identifiers removed")
	if strings.Contains(ref, tree.PasswordSubstitution) {
		fmt.Fprintln(&buf, stmts.StringWithFlags(tree.FmtShowPasswords), "-- passwords exposed")
	}

	return buf.String()
}

var issueLinkRE = regexp.MustCompile("https://go.crdb.dev/issue-v/([0-9]+)/.*")

// VerifyParseError is used in the SQL and PL/pgSQL datadriven parser tests to
// check that an unsuccessfully parsed expression returns an expected error.
func VerifyParseError(err error) string {
	pgerr := pgerror.Flatten(err)
	msg := pgerr.Message
	if pgerr.Detail != "" {
		msg += "\nDETAIL: " + pgerr.Detail
	}
	if pgerr.Hint != "" {
		msg += "\nHINT: " + pgerr.Hint
	}
	msg = issueLinkRE.ReplaceAllString(msg, "https://go.crdb.dev/issue-v/$1/")
	return msg
}
