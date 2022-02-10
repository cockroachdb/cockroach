// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parser_test

import (
	"bytes"
	"fmt"
	"go/constant"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

// TestParseDataDriven verifies that we can parse the supplied SQL and regenerate the SQL
// string from the syntax tree.
func TestParseDatadriven(t *testing.T) {
	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "parse":
				// Check parse.
				stmts, err := parser.Parse(d.Input)
				if err != nil {
					d.Fatalf(t, "unexpected parse error: %v", err)
				}

				// Check pretty-print roundtrip via the sqlfmt logic.
				sqlutils.VerifyStatementPrettyRoundtrip(t, d.Input)

				ref := stmts.String()
				note := ""
				if ref != d.Input {
					note = " -- normalized!"
				}

				// Check roundtrip and formatting with flags.
				var buf bytes.Buffer
				fmt.Fprintf(&buf, "%s%s\n", ref, note)
				fmt.Fprintln(&buf, stmts.StringWithFlags(tree.FmtAlwaysGroupExprs), "-- fully parenthesized")
				constantsHidden := stmts.StringWithFlags(tree.FmtHideConstants)
				fmt.Fprintln(&buf, constantsHidden, "-- literals removed")

				// As of this writing, the SQL statement stats proceed as follows:
				// first the literals are removed from statement to form a stat key,
				// then the stat key is re-parsed, to undergo the anonymization stage.
				// We also want to check the re-parsing is fine.
				reparsedStmts, err := parser.Parse(constantsHidden)
				if err != nil {
					d.Fatalf(t, "unexpected error when reparsing without literals: %+v", err)
				} else {
					reparsedStmtsS := reparsedStmts.String()
					if reparsedStmtsS != constantsHidden {
						d.Fatalf(t,
							"mismatched AST when reparsing without literals:\noriginal: %s\nexpected: %s\nactual:   %s",
							d.Input, constantsHidden, reparsedStmtsS,
						)
					}
				}

				fmt.Fprintln(&buf, stmts.StringWithFlags(tree.FmtAnonymize), "-- identifiers removed")
				if strings.Contains(ref, tree.PasswordSubstitution) {
					fmt.Fprintln(&buf, stmts.StringWithFlags(tree.FmtShowPasswords), "-- passwords exposed")
				}

				return buf.String()

			case "error":
				_, err := parser.Parse(d.Input)
				if err == nil {
					return ""
				}
				pgerr := pgerror.Flatten(err)
				msg := pgerr.Message
				if pgerr.Detail != "" {
					msg += "\nDETAIL: " + pgerr.Detail
				}
				if pgerr.Hint != "" {
					msg += "\nHINT: " + pgerr.Hint
				}
				return msg
			}
			d.Fatalf(t, "unsupported command: %s", d.Cmd)
			return ""
		})
	})
}

// TestParseTree checks that the implicit grouping done by the grammar
// is properly reflected in the parse tree.
func TestParseTree(t *testing.T) {
	testData := []struct {
		sql      string
		expected string
	}{
		{`SELECT 1`, `SELECT (1)`},
		{`SELECT -1+2`, `SELECT ((-1) + (2))`},
		{`SELECT -1:::INT8`, `SELECT (-((1):::INT8))`},
		{`SELECT 1 = 2::INT8`, `SELECT ((1) = ((2)::INT8))`},
		{`SELECT 1 = ANY 2::INT8`, `SELECT ((1) = ANY ((2)::INT8))`},
		{`SELECT 1 = ANY ARRAY[1]:::INT8`, `SELECT ((1) = ANY ((ARRAY[(1)]):::INT8))`},
	}

	for _, d := range testData {
		t.Run(d.sql, func(t *testing.T) {
			stmts, err := parser.Parse(d.sql)
			if err != nil {
				t.Errorf("%s: expected success, but found %s", d.sql, err)
				return
			}
			s := stmts.StringWithFlags(tree.FmtAlwaysGroupExprs)
			if d.expected != s {
				t.Errorf("%s: expected %s, but found (%d statements): %s", d.sql, d.expected, len(stmts), s)
			}
			if _, err := parser.Parse(s); err != nil {
				t.Errorf("expected string found, but not parsable: %s:\n%s", err, s)
			}
			sqlutils.VerifyStatementPrettyRoundtrip(t, d.expected)
		})
	}
}

// TestParseSyntax verifies that parsing succeeds, though the syntax tree
// likely differs. All of the test cases here should eventually be moved
// elsewhere.
func TestParseSyntax(t *testing.T) {
	testData := []struct {
		sql string
	}{
		{`SELECT '\0' FROM a`},
		{`SELECT ((1)) FROM t WHERE ((a)) IN (((1))) AND ((a, b)) IN ((((1, 1))), ((2, 2)))`},
		{`SELECT e'\'\"\b\n\r\t\\' FROM t`},
		{`SELECT '\x' FROM t`},
	}
	for _, d := range testData {
		t.Run(d.sql, func(t *testing.T) {
			if _, err := parser.Parse(d.sql); err != nil {
				t.Fatalf("%s: expected success, but not parsable %s", d.sql, err)
			}
			sqlutils.VerifyStatementPrettyRoundtrip(t, d.sql)
		})
	}
}

func TestParsePanic(t *testing.T) {
	// Replicates #1801.
	defer func() {
		if r := recover(); r != nil {
			t.Fatal(r)
		}
	}()
	s := "SELECT(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(T" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F(F" +
		"(F(F(F(F(F(F(F(F(F((" +
		"F(0"
	_, err := parser.Parse(s)
	expected := `at or near "EOF": syntax error`
	if !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}
}

func TestParsePrecedence(t *testing.T) {
	// Precedence levels (highest first):
	//   0: - ~
	//   1: * / // %
	//   2: + -
	//   3: << >>
	//   4: &
	//   5: ^
	//   6: |
	//   7: = != > >= < <=
	//   8: NOT
	//   9: AND
	//  10: OR

	unary := func(op tree.UnaryOperatorSymbol, expr tree.Expr) tree.Expr {
		return &tree.UnaryExpr{Operator: tree.MakeUnaryOperator(op), Expr: expr}
	}
	binary := func(op treebin.BinaryOperatorSymbol, left, right tree.Expr) tree.Expr {
		return &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(op), Left: left, Right: right}
	}
	cmp := func(op treecmp.ComparisonOperatorSymbol, left, right tree.Expr) tree.Expr {
		return &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(op), Left: left, Right: right}
	}
	not := func(expr tree.Expr) tree.Expr {
		return &tree.NotExpr{Expr: expr}
	}
	and := func(left, right tree.Expr) tree.Expr {
		return &tree.AndExpr{Left: left, Right: right}
	}
	or := func(left, right tree.Expr) tree.Expr {
		return &tree.OrExpr{Left: left, Right: right}
	}
	concat := func(left, right tree.Expr) tree.Expr {
		return &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Concat), Left: left, Right: right}
	}
	regmatch := func(left, right tree.Expr) tree.Expr {
		return &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.RegMatch), Left: left, Right: right}
	}
	regimatch := func(left, right tree.Expr) tree.Expr {
		return &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.RegIMatch), Left: left, Right: right}
	}

	one := tree.NewNumVal(constant.MakeInt64(1), "1", false /* negative */)
	minusone := tree.NewNumVal(constant.MakeInt64(1), "1", true /* negative */)
	two := tree.NewNumVal(constant.MakeInt64(2), "2", false /* negative */)
	minustwo := tree.NewNumVal(constant.MakeInt64(2), "2", true /* negative */)
	three := tree.NewNumVal(constant.MakeInt64(3), "3", false /* negative */)
	a := tree.NewStrVal("a")
	b := tree.NewStrVal("b")
	c := tree.NewStrVal("c")

	testData := []struct {
		sql      string
		expected tree.Expr
	}{
		// Unary plus and complement.
		{`~-1`, unary(tree.UnaryComplement, minusone)},
		{`-~1`, unary(tree.UnaryMinus, unary(tree.UnaryComplement, one))},

		// Mul, div, floordiv, mod combined with higher precedence.
		{`-1*2`, binary(treebin.Mult, minusone, two)},
		{`1*-2`, binary(treebin.Mult, one, minustwo)},
		{`-1/2`, binary(treebin.Div, minusone, two)},
		{`1/-2`, binary(treebin.Div, one, minustwo)},
		{`-1//2`, binary(treebin.FloorDiv, minusone, two)},
		{`1//-2`, binary(treebin.FloorDiv, one, minustwo)},
		{`-1%2`, binary(treebin.Mod, minusone, two)},
		{`1%-2`, binary(treebin.Mod, one, minustwo)},

		// Mul, div, floordiv, mod combined with self (left associative).
		{`1*2*3`, binary(treebin.Mult, binary(treebin.Mult, one, two), three)},
		{`1*2/3`, binary(treebin.Div, binary(treebin.Mult, one, two), three)},
		{`1/2*3`, binary(treebin.Mult, binary(treebin.Div, one, two), three)},
		{`1*2//3`, binary(treebin.FloorDiv, binary(treebin.Mult, one, two), three)},
		{`1//2*3`, binary(treebin.Mult, binary(treebin.FloorDiv, one, two), three)},
		{`1*2%3`, binary(treebin.Mod, binary(treebin.Mult, one, two), three)},
		{`1%2*3`, binary(treebin.Mult, binary(treebin.Mod, one, two), three)},
		{`1/2/3`, binary(treebin.Div, binary(treebin.Div, one, two), three)},
		{`1/2//3`, binary(treebin.FloorDiv, binary(treebin.Div, one, two), three)},
		{`1//2/3`, binary(treebin.Div, binary(treebin.FloorDiv, one, two), three)},
		{`1/2%3`, binary(treebin.Mod, binary(treebin.Div, one, two), three)},
		{`1%2/3`, binary(treebin.Div, binary(treebin.Mod, one, two), three)},
		{`1//2//3`, binary(treebin.FloorDiv, binary(treebin.FloorDiv, one, two), three)},
		{`1//2%3`, binary(treebin.Mod, binary(treebin.FloorDiv, one, two), three)},
		{`1%2//3`, binary(treebin.FloorDiv, binary(treebin.Mod, one, two), three)},
		{`1%2%3`, binary(treebin.Mod, binary(treebin.Mod, one, two), three)},

		// Binary plus and minus combined with higher precedence.
		{`1*2+3`, binary(treebin.Plus, binary(treebin.Mult, one, two), three)},
		{`1+2*3`, binary(treebin.Plus, one, binary(treebin.Mult, two, three))},
		{`1*2-3`, binary(treebin.Minus, binary(treebin.Mult, one, two), three)},
		{`1-2*3`, binary(treebin.Minus, one, binary(treebin.Mult, two, three))},

		// Binary plus and minus combined with self (left associative).
		{`1+2-3`, binary(treebin.Minus, binary(treebin.Plus, one, two), three)},
		{`1-2+3`, binary(treebin.Plus, binary(treebin.Minus, one, two), three)},

		// Left and right shift combined with higher precedence.
		{`1<<2+3`, binary(treebin.LShift, one, binary(treebin.Plus, two, three))},
		{`1+2<<3`, binary(treebin.LShift, binary(treebin.Plus, one, two), three)},
		{`1>>2+3`, binary(treebin.RShift, one, binary(treebin.Plus, two, three))},
		{`1+2>>3`, binary(treebin.RShift, binary(treebin.Plus, one, two), three)},

		// Left and right shift combined with self (left associative).
		{`1<<2<<3`, binary(treebin.LShift, binary(treebin.LShift, one, two), three)},
		{`1<<2>>3`, binary(treebin.RShift, binary(treebin.LShift, one, two), three)},
		{`1>>2<<3`, binary(treebin.LShift, binary(treebin.RShift, one, two), three)},
		{`1>>2>>3`, binary(treebin.RShift, binary(treebin.RShift, one, two), three)},

		// Power combined with lower precedence.
		{`1*2^3`, binary(treebin.Mult, one, binary(treebin.Pow, two, three))},
		{`1^2*3`, binary(treebin.Mult, binary(treebin.Pow, one, two), three)},

		// Bit-and combined with higher precedence.
		{`1&2<<3`, binary(treebin.Bitand, one, binary(treebin.LShift, two, three))},
		{`1<<2&3`, binary(treebin.Bitand, binary(treebin.LShift, one, two), three)},

		// Bit-and combined with self (left associative)
		{`1&2&3`, binary(treebin.Bitand, binary(treebin.Bitand, one, two), three)},

		// Bit-xor combined with higher precedence.
		{`1#2&3`, binary(treebin.Bitxor, one, binary(treebin.Bitand, two, three))},
		{`1&2#3`, binary(treebin.Bitxor, binary(treebin.Bitand, one, two), three)},

		// Bit-xor combined with self (left associative)
		{`1#2#3`, binary(treebin.Bitxor, binary(treebin.Bitxor, one, two), three)},

		// Bit-or combined with higher precedence.
		{`1|2#3`, binary(treebin.Bitor, one, binary(treebin.Bitxor, two, three))},
		{`1#2|3`, binary(treebin.Bitor, binary(treebin.Bitxor, one, two), three)},

		// Bit-or combined with self (left associative)
		{`1|2|3`, binary(treebin.Bitor, binary(treebin.Bitor, one, two), three)},

		// Equals, not-equals, greater-than, greater-than equals, less-than and
		// less-than equals combined with higher precedence.
		{`1 = 2|3`, cmp(treecmp.EQ, one, binary(treebin.Bitor, two, three))},
		{`1|2 = 3`, cmp(treecmp.EQ, binary(treebin.Bitor, one, two), three)},
		{`1 != 2|3`, cmp(treecmp.NE, one, binary(treebin.Bitor, two, three))},
		{`1|2 != 3`, cmp(treecmp.NE, binary(treebin.Bitor, one, two), three)},
		{`1 > 2|3`, cmp(treecmp.GT, one, binary(treebin.Bitor, two, three))},
		{`1|2 > 3`, cmp(treecmp.GT, binary(treebin.Bitor, one, two), three)},
		{`1 >= 2|3`, cmp(treecmp.GE, one, binary(treebin.Bitor, two, three))},
		{`1|2 >= 3`, cmp(treecmp.GE, binary(treebin.Bitor, one, two), three)},
		{`1 < 2|3`, cmp(treecmp.LT, one, binary(treebin.Bitor, two, three))},
		{`1|2 < 3`, cmp(treecmp.LT, binary(treebin.Bitor, one, two), three)},
		{`1 <= 2|3`, cmp(treecmp.LE, one, binary(treebin.Bitor, two, three))},
		{`1|2 <= 3`, cmp(treecmp.LE, binary(treebin.Bitor, one, two), three)},

		// NOT combined with higher precedence.
		{`NOT 1 = 2`, not(cmp(treecmp.EQ, one, two))},
		{`NOT 1 = NOT 2 = 3`, not(cmp(treecmp.EQ, one, not(cmp(treecmp.EQ, two, three))))},

		// NOT combined with self.
		{`NOT NOT 1 = 2`, not(not(cmp(treecmp.EQ, one, two)))},

		// AND combined with higher precedence.
		{`NOT 1 AND 2`, and(not(one), two)},
		{`1 AND NOT 2`, and(one, not(two))},

		// AND combined with self (left associative).
		{`1 AND 2 AND 3`, and(and(one, two), three)},

		// OR combined with higher precedence.
		{`1 AND 2 OR 3`, or(and(one, two), three)},
		{`1 OR 2 AND 3`, or(one, and(two, three))},

		// OR combined with self (left associative).
		{`1 OR 2 OR 3`, or(or(one, two), three)},

		// ~ and ~* should both be lower than ||.
		{`'a' || 'b' ~ 'c'`, regmatch(concat(a, b), c)},
		{`'a' || 'b' ~* 'c'`, regimatch(concat(a, b), c)},

		// Unary ~ should have highest precedence.
		{`~1+2`, binary(treebin.Plus, unary(tree.UnaryComplement, one), two)},

		// OPERATOR(pg_catalog.~) should not be error (#66861).
		{
			`'a' OPERATOR(pg_catalog.~) 'b'`,
			&tree.ComparisonExpr{Operator: treecmp.ComparisonOperator{Symbol: treecmp.RegMatch, IsExplicitOperator: true}, Left: a, Right: b},
		},
	}
	for _, d := range testData {
		t.Run(d.sql, func(t *testing.T) {
			expr, err := parser.ParseExpr(d.sql)
			if err != nil {
				t.Fatalf("%s: %v", d.sql, err)
			}
			if !reflect.DeepEqual(d.expected, expr) {
				t.Fatalf("%s: expected %s, but found %s", d.sql, d.expected, expr)
			}
		})
	}
}

func TestUnimplementedSyntax(t *testing.T) {
	testData := []struct {
		sql      string
		issue    int
		expected string
		hint     string
	}{
		{`ALTER TABLE a ALTER CONSTRAINT foo`, 31632, `alter constraint`, ``},
		{`ALTER TABLE a ADD CONSTRAINT foo EXCLUDE USING gist (bar WITH =)`, 46657, `add constraint exclude using`, ``},
		{`ALTER TABLE a INHERITS b`, 22456, `alter table inherits`, ``},
		{`ALTER TABLE a NO INHERITS b`, 22456, `alter table no inherits`, ``},

		{`CREATE ACCESS METHOD a`, 0, `create access method`, ``},

		{`COMMENT ON EXTENSION a`, 74777, `comment on extension`, ``},
		{`COMMENT ON FUNCTION f() is 'f'`, 17511, ``, ``},
		{`COPY x FROM STDIN WHERE a = b`, 54580, ``, ``},

		{`ALTER AGGREGATE a`, 74775, `alter aggregate`, ``},
		{`ALTER FUNCTION a`, 17511, `alter function`, ``},

		{`CREATE AGGREGATE a`, 74775, `create aggregate`, ``},
		{`CREATE CAST a`, 0, `create cast`, ``},
		{`CREATE CONSTRAINT TRIGGER a`, 28296, `create constraint`, ``},
		{`CREATE CONVERSION a`, 0, `create conversion`, ``},
		{`CREATE DEFAULT CONVERSION a`, 0, `create def conv`, ``},
		{`CREATE EXTENSION a WITH schema = 'public'`, 74777, `create extension with`, ``},
		{`CREATE EXTENSION IF NOT EXISTS a WITH schema = 'public'`, 74777, `create extension if not exists with`, ``},
		{`CREATE FOREIGN DATA WRAPPER a`, 0, `create fdw`, ``},
		{`CREATE FOREIGN TABLE a`, 0, `create foreign table`, ``},
		{`CREATE FUNCTION a`, 17511, `create`, ``},
		{`CREATE OR REPLACE FUNCTION a`, 17511, `create`, ``},
		{`CREATE LANGUAGE a`, 17511, `create language a`, ``},
		{`CREATE OPERATOR a`, 65017, ``, ``},
		{`CREATE PUBLICATION a`, 0, `create publication`, ``},
		{`CREATE RULE a`, 0, `create rule`, ``},
		{`CREATE SERVER a`, 0, `create server`, ``},
		{`CREATE SUBSCRIPTION a`, 0, `create subscription`, ``},
		{`CREATE TABLESPACE a`, 54113, `create tablespace`, ``},
		{`CREATE TEXT SEARCH a`, 7821, `create text`, ``},
		{`CREATE TRIGGER a`, 28296, `create`, ``},

		{`DROP ACCESS METHOD a`, 0, `drop access method`, ``},
		{`DROP AGGREGATE a`, 74775, `drop aggregate`, ``},
		{`DROP CAST a`, 0, `drop cast`, ``},
		{`DROP COLLATION a`, 0, `drop collation`, ``},
		{`DROP CONVERSION a`, 0, `drop conversion`, ``},
		{`DROP DOMAIN a`, 27796, `drop`, ``},
		{`DROP EXTENSION a`, 74777, `drop extension`, ``},
		{`DROP EXTENSION IF EXISTS a`, 74777, `drop extension if exists`, ``},
		{`DROP FOREIGN TABLE a`, 0, `drop foreign table`, ``},
		{`DROP FOREIGN DATA WRAPPER a`, 0, `drop fdw`, ``},
		{`DROP FUNCTION a`, 17511, `drop `, ``},
		{`DROP LANGUAGE a`, 17511, `drop language a`, ``},
		{`DROP OPERATOR a`, 0, `drop operator`, ``},
		{`DROP PUBLICATION a`, 0, `drop publication`, ``},
		{`DROP RULE a`, 0, `drop rule`, ``},
		{`DROP SERVER a`, 0, `drop server`, ``},
		{`DROP SUBSCRIPTION a`, 0, `drop subscription`, ``},
		{`DROP TEXT SEARCH a`, 7821, `drop text`, ``},
		{`DROP TRIGGER a`, 28296, `drop`, ``},

		{`DISCARD PLANS`, 0, `discard plans`, ``},
		{`DISCARD SEQUENCES`, 0, `discard sequences`, ``},
		{`DISCARD TEMP`, 0, `discard temp`, ``},
		{`DISCARD TEMPORARY`, 0, `discard temp`, ``},

		{`SET CONSTRAINTS foo`, 0, `set constraints`, ``},
		{`SET foo FROM CURRENT`, 0, `set from current`, ``},

		{`CREATE MATERIALIZED VIEW a AS SELECT 1 WITH NO DATA`, 74083, ``, ``},

		{`CREATE TABLE a(x INT[][])`, 32552, ``, ``},
		{`CREATE TABLE a(x INT[1][2])`, 32552, ``, ``},
		{`CREATE TABLE a(x INT ARRAY[1][2])`, 32552, ``, ``},

		{`CREATE TABLE a(b INT8) WITH OIDS`, 0, `create table with oids`, ``},

		{`CREATE TABLE a AS SELECT b WITH NO DATA`, 0, `create table as with no data`, ``},

		{`CREATE TABLE a(b INT8 REFERENCES c(x) MATCH PARTIAL`, 20305, `match partial`, ``},
		{`CREATE TABLE a(b INT8, FOREIGN KEY (b) REFERENCES c(x) MATCH PARTIAL)`, 20305, `match partial`, ``},

		{`CREATE TABLE a(b INT8, FOREIGN KEY (b) REFERENCES c(x) DEFERRABLE)`, 31632, `deferrable`, ``},
		{`CREATE TABLE a(b INT8, FOREIGN KEY (b) REFERENCES c(x) INITIALLY DEFERRED)`, 31632, `initially deferred`, ``},
		{`CREATE TABLE a(b INT8, FOREIGN KEY (b) REFERENCES c(x) INITIALLY IMMEDIATE)`, 31632, `initially immediate`, ``},
		{`CREATE TABLE a(b INT8, FOREIGN KEY (b) REFERENCES c(x) DEFERRABLE INITIALLY DEFERRED)`, 31632, `initially deferred`, ``},
		{`CREATE TABLE a(b INT8, FOREIGN KEY (b) REFERENCES c(x) DEFERRABLE INITIALLY IMMEDIATE)`, 31632, `initially immediate`, ``},
		{`CREATE TABLE a(b INT8, UNIQUE (b) DEFERRABLE)`, 31632, `deferrable`, ``},
		{`CREATE TABLE a(b INT8, CHECK (b > 0) DEFERRABLE)`, 31632, `deferrable`, ``},

		{`CREATE TABLE a (LIKE b INCLUDING COMMENTS)`, 47071, `like table`, ``},
		{`CREATE TABLE a (LIKE b INCLUDING IDENTITY)`, 47071, `like table`, ``},
		{`CREATE TABLE a (LIKE b INCLUDING STATISTICS)`, 47071, `like table`, ``},
		{`CREATE TABLE a (LIKE b INCLUDING STORAGE)`, 47071, `like table`, ``},

		{`CREATE TABLE a () INHERITS b`, 22456, `create table inherit`, ``},

		{`CREATE TEMP TABLE a (a int) ON COMMIT DROP`, 46556, `drop`, ``},
		{`CREATE TEMP TABLE a (a int) ON COMMIT DELETE ROWS`, 46556, `delete rows`, ``},
		{`CREATE TEMP TABLE IF NOT EXISTS a (a int) ON COMMIT DROP`, 46556, `drop`, ``},
		{`CREATE TEMP TABLE IF NOT EXISTS a (a int) ON COMMIT DELETE ROWS`, 46556, `delete rows`, ``},
		{`CREATE TEMP TABLE b AS SELECT a FROM a ON COMMIT DROP`, 46556, `drop`, ``},
		{`CREATE TEMP TABLE b AS SELECT a FROM a ON COMMIT DELETE ROWS`, 46556, `delete rows`, ``},
		{`CREATE TEMP TABLE IF NOT EXISTS b AS SELECT a FROM a ON COMMIT DROP`, 46556, `drop`, ``},
		{`CREATE TEMP TABLE IF NOT EXISTS b AS SELECT a FROM a ON COMMIT DELETE ROWS`, 46556, `delete rows`, ``},

		{`CREATE RECURSIVE VIEW a AS SELECT b`, 0, `create recursive view`, ``},

		{`CREATE TYPE a AS (b)`, 27792, ``, ``},
		{`CREATE TYPE a AS RANGE b`, 27791, ``, ``},
		{`CREATE TYPE a (b)`, 27793, `base`, ``},
		{`CREATE TYPE a`, 27793, `shell`, ``},
		{`CREATE DOMAIN a`, 27796, `create`, ``},

		{`ALTER TYPE db.t RENAME ATTRIBUTE foo TO bar`, 48701, `ALTER TYPE ATTRIBUTE`, ``},
		{`ALTER TYPE db.s.t ADD ATTRIBUTE foo bar`, 48701, `ALTER TYPE ATTRIBUTE`, ``},
		{`ALTER TYPE db.s.t ADD ATTRIBUTE foo bar COLLATE hello`, 48701, `ALTER TYPE ATTRIBUTE`, ``},
		{`ALTER TYPE db.s.t ADD ATTRIBUTE foo bar RESTRICT`, 48701, `ALTER TYPE ATTRIBUTE`, ``},
		{`ALTER TYPE db.s.t ADD ATTRIBUTE foo bar CASCADE`, 48701, `ALTER TYPE ATTRIBUTE`, ``},
		{`ALTER TYPE db.s.t DROP ATTRIBUTE foo`, 48701, `ALTER TYPE ATTRIBUTE`, ``},
		{`ALTER TYPE db.s.t DROP ATTRIBUTE foo RESTRICT`, 48701, `ALTER TYPE ATTRIBUTE`, ``},
		{`ALTER TYPE db.s.t DROP ATTRIBUTE foo CASCADE`, 48701, `ALTER TYPE ATTRIBUTE`, ``},
		{`ALTER TYPE db.s.t ALTER ATTRIBUTE foo TYPE typ`, 48701, `ALTER TYPE ATTRIBUTE`, ``},
		{`ALTER TYPE db.s.t ALTER ATTRIBUTE foo TYPE typ COLLATE en`, 48701, `ALTER TYPE ATTRIBUTE`, ``},
		{`ALTER TYPE db.s.t ALTER ATTRIBUTE foo TYPE typ COLLATE en CASCADE`, 48701, `ALTER TYPE ATTRIBUTE`, ``},
		{`ALTER TYPE db.s.t ALTER ATTRIBUTE foo SET DATA TYPE typ COLLATE en RESTRICT`, 48701, `ALTER TYPE ATTRIBUTE`, ``},
		{`ALTER TYPE db.s.t ADD ATTRIBUTE foo bar RESTRICT, DROP ATTRIBUTE foo`, 48701, `ALTER TYPE ATTRIBUTE`, ``},

		{`CREATE INDEX a ON b USING HASH (c)`, 0, `index using hash`, ``},
		{`CREATE INDEX a ON b USING SPGIST (c)`, 0, `index using spgist`, ``},
		{`CREATE INDEX a ON b USING BRIN (c)`, 0, `index using brin`, ``},

		{`CREATE INDEX a ON b(c gin_trgm_ops)`, 41285, `index using gin_trgm_ops`, ``},
		{`CREATE INDEX a ON b(c gist_trgm_ops)`, 41285, `index using gist_trgm_ops`, ``},
		{`CREATE INDEX a ON b(c bobby)`, 47420, ``, ``},
		{`CREATE INDEX a ON b(a NULLS LAST)`, 6224, ``, ``},
		{`CREATE INDEX a ON b(a ASC NULLS LAST)`, 6224, ``, ``},
		{`CREATE INDEX a ON b(a DESC NULLS FIRST)`, 6224, ``, ``},

		{`INSERT INTO foo(a, a.b) VALUES (1,2)`, 27792, ``, ``},

		{`SELECT * FROM ROWS FROM (a(b) AS (d))`, 0, `ROWS FROM with col_def_list`, ``},

		{`SELECT a(b) 'c'`, 0, `a(...) SCONST`, ``},
		{`SELECT (a,b) OVERLAPS (c,d)`, 0, `overlaps`, ``},
		{`SELECT UNIQUE (SELECT b)`, 0, `UNIQUE predicate`, ``},
		{`SELECT GROUPING (a,b,c)`, 0, `d_expr grouping`, ``},
		{`SELECT a(VARIADIC b)`, 0, `variadic`, ``},
		{`SELECT a(b, c, VARIADIC b)`, 0, `variadic`, ``},
		{`SELECT TREAT (a AS INT8)`, 0, `treat`, ``},

		{`SELECT 1 FROM t GROUP BY ROLLUP (b)`, 46280, `rollup`, ``},
		{`SELECT 1 FROM t GROUP BY a, ROLLUP (b)`, 46280, `rollup`, ``},
		{`SELECT 1 FROM t GROUP BY CUBE (b)`, 46280, `cube`, ``},
		{`SELECT 1 FROM t GROUP BY GROUPING SETS (b)`, 46280, `grouping sets`, ``},

		{`CREATE TABLE a(b BOX)`, 21286, `box`, ``},
		{`CREATE TABLE a(b CIDR)`, 18846, `cidr`, ``},
		{`CREATE TABLE a(b CIRCLE)`, 21286, `circle`, ``},
		{`CREATE TABLE a(b JSONPATH)`, 22513, `jsonpath`, ``},
		{`CREATE TABLE a(b LINE)`, 21286, `line`, ``},
		{`CREATE TABLE a(b LSEG)`, 21286, `lseg`, ``},
		{`CREATE TABLE a(b MACADDR)`, 45813, `macaddr`, ``},
		{`CREATE TABLE a(b MACADDR8)`, 45813, `macaddr8`, ``},
		{`CREATE TABLE a(b MONEY)`, 41578, `money`, ``},
		{`CREATE TABLE a(b PATH)`, 21286, `path`, ``},
		{`CREATE TABLE a(b PG_LSN)`, 0, `pg_lsn`, ``},
		{`CREATE TABLE a(b POINT)`, 21286, `point`, ``},
		{`CREATE TABLE a(b POLYGON)`, 21286, `polygon`, ``},
		{`CREATE TABLE a(b TSQUERY)`, 7821, `tsquery`, ``},
		{`CREATE TABLE a(b TSVECTOR)`, 7821, `tsvector`, ``},
		{`CREATE TABLE a(b TXID_SNAPSHOT)`, 0, `txid_snapshot`, ``},
		{`CREATE TABLE a(b XML)`, 43355, `xml`, ``},

		{`CREATE TABLE a(a INT, PRIMARY KEY (a) NOT VALID)`, 0, `table constraint`,
			`PRIMARY KEY constraints cannot be marked NOT VALID`},
		{`CREATE TABLE a(a INT, UNIQUE (a) NOT VALID)`, 0, `table constraint`,
			`UNIQUE constraints cannot be marked NOT VALID`},

		{`UPDATE foo SET (a, a.b) = (1, 2)`, 27792, ``, ``},
		{`UPDATE foo SET a.b = 1`, 27792, ``, ``},
		{`UPDATE Foo SET x.y = z`, 27792, ``, ``},

		{`GRANT SELECT ON SEQUENCE a`, 74780, `grant privileges on sequence`, ``},

		{`REINDEX INDEX a`, 0, `reindex index`, `CockroachDB does not require reindexing.`},
		{`REINDEX INDEX CONCURRENTLY a`, 0, `reindex index`, `CockroachDB does not require reindexing.`},
		{`REINDEX TABLE a`, 0, `reindex table`, `CockroachDB does not require reindexing.`},
		{`REINDEX SCHEMA a`, 0, `reindex schema`, `CockroachDB does not require reindexing.`},
		{`REINDEX DATABASE a`, 0, `reindex database`, `CockroachDB does not require reindexing.`},
		{`REINDEX SYSTEM a`, 0, `reindex system`, `CockroachDB does not require reindexing.`},

		{`UPSERT INTO foo(a, a.b) VALUES (1,2)`, 27792, ``, ``},

		{`SELECT 1 OPERATOR(public.+) 2`, 65017, ``, ``},
	}
	for _, d := range testData {
		t.Run(d.sql, func(t *testing.T) {
			_, err := parser.Parse(d.sql)
			if err == nil {
				t.Errorf("%s: expected error, got nil", d.sql)
				return
			}
			if errMsg := err.Error(); !strings.Contains(errMsg, "unimplemented: this syntax") {
				t.Errorf("%s: expected unimplemented in message, got %q", d.sql, errMsg)
			}
			tkeys := errors.GetTelemetryKeys(err)
			if len(tkeys) == 0 {
				t.Errorf("%s: expected telemetry key set", d.sql)
			} else {
				found := false
				for _, tk := range tkeys {
					if strings.Contains(tk, d.expected) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("%s: expected %q in telemetry keys, got %+v", d.sql, d.expected, tkeys)
				}
			}
			if d.hint != "" {
				hints := errors.GetAllHints(err)
				assert.Contains(t, hints, d.hint)
			}
			if d.issue != 0 {
				exp := fmt.Sprintf("syntax.#%d", d.issue)
				found := false
				for _, tk := range tkeys {
					if strings.HasPrefix(tk, exp) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("%s: expected %q in telemetry keys, got %+v", d.sql, exp, tkeys)
				}

				exp2 := fmt.Sprintf("issue-v/%d", d.issue)
				found = false
				hints := errors.GetAllHints(err)
				for _, h := range hints {
					if strings.Contains(h, exp2) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("%s: expected %q at in hint, got %+v", d.sql, exp2, hints)
				}
			}
		})
	}
}

// TestParseSQL verifies that Statement.SQL is set correctly.
func TestParseSQL(t *testing.T) {
	testData := []struct {
		in  string
		exp []string
	}{
		{in: ``, exp: nil},
		{in: `SELECT 1`, exp: []string{`SELECT 1`}},
		{in: `SELECT 1;`, exp: []string{`SELECT 1`}},
		{in: `SELECT 1 /* comment */`, exp: []string{`SELECT 1`}},
		{in: `SELECT 1;SELECT 2`, exp: []string{`SELECT 1`, `SELECT 2`}},
		{in: `SELECT 1 /* comment */ ;SELECT 2`, exp: []string{`SELECT 1`, `SELECT 2`}},
		{in: `SELECT 1 /* comment */ ; /* comment */ SELECT 2`, exp: []string{`SELECT 1`, `SELECT 2`}},
	}
	var p parser.Parser // Verify that the same parser can be reused.
	for _, d := range testData {
		t.Run(d.in, func(t *testing.T) {
			stmts, err := p.Parse(d.in)
			if err != nil {
				t.Fatalf("expected success, but found %s", err)
			}
			var res []string
			for i := range stmts {
				res = append(res, stmts[i].SQL)
			}
			if !reflect.DeepEqual(res, d.exp) {
				t.Errorf("expected \n%v\n, but found %v", res, d.exp)
			}
		})
	}
}

// TestParseNumPlaceholders verifies that Statement.NumPlaceholders is set
// correctly.
func TestParseNumPlaceholders(t *testing.T) {
	testData := []struct {
		in  string
		exp []int
	}{
		{in: ``, exp: nil},

		{in: `SELECT 1`, exp: []int{0}},
		{in: `SELECT $1`, exp: []int{1}},
		{in: `SELECT $1 + $1`, exp: []int{1}},
		{in: `SELECT $1 + $2`, exp: []int{2}},
		{in: `SELECT $1 + $2 + $1 + $2`, exp: []int{2}},
		{in: `SELECT $2`, exp: []int{2}},
		{in: `SELECT $1, $1 + $2, $1 + $2 + $3`, exp: []int{3}},

		{in: `SELECT $1; SELECT $1`, exp: []int{1, 1}},
		{in: `SELECT $1; SELECT $1 + $2 + $3; SELECT $1 + $2`, exp: []int{1, 3, 2}},
	}

	var p parser.Parser // Verify that the same parser can be reused.
	for _, d := range testData {
		t.Run(d.in, func(t *testing.T) {
			stmts, err := p.Parse(d.in)
			if err != nil {
				t.Fatalf("expected success, but found %s", err)
			}
			var res []int
			for i := range stmts {
				res = append(res, stmts[i].NumPlaceholders)
			}
			if !reflect.DeepEqual(res, d.exp) {
				t.Errorf("expected \n%v\n, but found %v", res, d.exp)
			}
		})
	}
}

func TestParseOne(t *testing.T) {
	_, err := parser.ParseOne("SELECT 1; SELECT 2")
	if !testutils.IsError(err, "expected 1 statement") {
		t.Errorf("unexpected error %s", err)
	}
}

func BenchmarkParse(b *testing.B) {
	testCases := []struct {
		name, query string
	}{
		{
			"simple",
			`SELECT a FROM t WHERE a = 1`,
		},
		{
			"string",
			`SELECT a FROM t WHERE a = 'some-string' AND b = 'some-other-string'`,
		},
		{
			"tpcc-delivery",
			`SELECT no_o_id FROM new_order WHERE no_w_id = $1 AND no_d_id = $2 ORDER BY no_o_id ASC LIMIT 1`,
		},
		{
			"account",
			`BEGIN;
			 UPDATE pgbench_accounts SET abalance = abalance + 77 WHERE aid = 5;
			 SELECT abalance FROM pgbench_accounts WHERE aid = 5;
			 INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (1, 2, 5, 77, CURRENT_TIMESTAMP);
			 END`,
		},
	}
	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := parser.Parse(tc.query); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
