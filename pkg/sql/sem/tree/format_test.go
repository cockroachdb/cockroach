// Copyright 2017 The Cockroach Authors.
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

package tree_test

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/internal/rsg"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestFormatStatement(t *testing.T) {
	testData := []struct {
		stmt     string
		f        tree.FmtFlags
		expected string
	}{
		{`CREATE USER foo WITH PASSWORD 'bar'`, tree.FmtSimple,
			`CREATE USER 'foo' WITH PASSWORD *****`},
		{`CREATE USER foo WITH PASSWORD 'bar'`, tree.FmtShowPasswords,
			`CREATE USER 'foo' WITH PASSWORD 'bar'`},

		{`CREATE TABLE foo (x INT)`, tree.FmtAnonymize,
			`CREATE TABLE _ (_ INT)`},
		{`INSERT INTO foo(x) TABLE bar`, tree.FmtAnonymize,
			`INSERT INTO _(_) TABLE _`},
		{`UPDATE foo SET x = y`, tree.FmtAnonymize,
			`UPDATE _ SET _ = _`},
		{`DELETE FROM foo`, tree.FmtAnonymize,
			`DELETE FROM _`},
		{`TRUNCATE foo`, tree.FmtAnonymize,
			`TRUNCATE TABLE _`},
		{`ALTER TABLE foo RENAME TO bar`, tree.FmtAnonymize,
			`ALTER TABLE _ RENAME TO _`},
		{`SHOW COLUMNS FROM foo`, tree.FmtAnonymize,
			`SHOW COLUMNS FROM _`},
		{`SHOW CREATE TABLE foo`, tree.FmtAnonymize,
			`SHOW CREATE _`},
		{`GRANT SELECT ON bar TO foo`, tree.FmtAnonymize,
			`GRANT SELECT ON TABLE _ TO _`},

		{`INSERT INTO a VALUES (-2, +3)`,
			tree.FmtHideConstants,
			`INSERT INTO a VALUES (_, _)`},

		{`INSERT INTO a VALUES (0), (0), (0), (0), (0), (0)`,
			tree.FmtHideConstants,
			`INSERT INTO a VALUES (_), (__more5__)`},
		{`INSERT INTO a VALUES (0, 0, 0, 0, 0, 0)`,
			tree.FmtHideConstants,
			`INSERT INTO a VALUES (_, _, __more4__)`},
		{`INSERT INTO a VALUES (ARRAY[0, 0, 0, 0, 0, 0, 0])`,
			tree.FmtHideConstants,
			`INSERT INTO a VALUES (ARRAY[_, _, __more5__])`},
		{`INSERT INTO a VALUES (ARRAY[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ` +
			`0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ` +
			`0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])`,
			tree.FmtHideConstants,
			`INSERT INTO a VALUES (ARRAY[_, _, __more30__])`},

		{`SELECT 1+COALESCE(NULL, 'a', x)-ARRAY[3.14]`, tree.FmtHideConstants,
			`SELECT (_ + COALESCE(_, _, x)) - ARRAY[_]`},

		// This here checks encodeSQLString on non-tree.DString strings also
		// calls encodeSQLString with the right formatter.
		// See TestFormatExprs below for the test on DStrings.
		{`CREATE DATABASE foo TEMPLATE = 'bar-baz'`, tree.FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = bar-baz`},
		{`CREATE DATABASE foo TEMPLATE = 'bar baz'`, tree.FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = 'bar baz'`},
		{`CREATE DATABASE foo TEMPLATE = 'bar,baz'`, tree.FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = 'bar,baz'`},
		{`CREATE DATABASE foo TEMPLATE = 'bar{baz}'`, tree.FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = 'bar{baz}'`},

		{`SET "time zone" = UTC`, tree.FmtSimple,
			`SET "time zone" = utc`},
		{`SET "time zone" = UTC`, tree.FmtBareIdentifiers,
			`SET time zone = utc`},
		{`SET "time zone" = UTC`, tree.FmtBareStrings,
			`SET "time zone" = utc`},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.stmt), func(t *testing.T) {
			stmt, err := parser.ParseOne(test.stmt)
			if err != nil {
				t.Fatal(err)
			}
			stmtStr := tree.AsStringWithFlags(stmt, test.f)
			if stmtStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, stmtStr)
			}
		})
	}
}

func TestFormatTableName(t *testing.T) {
	testData := []struct {
		stmt     string
		expected string
	}{
		{`CREATE TABLE foo (x INT)`,
			`CREATE TABLE xoxoxo (x INT)`},
		{`INSERT INTO foo(x) TABLE bar`,
			`INSERT INTO xoxoxo(x) TABLE xoxoxo`},
		{`UPDATE foo SET x = y`,
			`UPDATE xoxoxo SET x = y`},
		{`DELETE FROM foo`,
			`DELETE FROM xoxoxo`},
		{`ALTER TABLE foo RENAME TO bar`,
			`ALTER TABLE xoxoxo RENAME TO xoxoxo`},
		{`SHOW COLUMNS FROM foo`,
			`SHOW COLUMNS FROM xoxoxo`},
		{`SHOW CREATE TABLE foo`,
			`SHOW CREATE xoxoxo`},
		// TODO(knz): TRUNCATE and GRANT table names are removed by
		// tree.FmtAnonymize but not processed by table formatters.
		//
		// {`TRUNCATE foo`,
		// `TRUNCATE TABLE xoxoxo`},
		// {`GRANT SELECT ON bar TO foo`,
		// `GRANT SELECT ON xoxoxo TO foo`},
	}

	f := tree.NewFmtCtxWithBuf(tree.FmtSimple)
	defer f.Close()
	f.WithReformatTableNames(func(ctx *tree.FmtCtx, _ *tree.NormalizableTableName) {
		ctx.WriteString("xoxoxo")
	})

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.stmt), func(t *testing.T) {
			stmt, err := parser.ParseOne(test.stmt)
			if err != nil {
				t.Fatal(err)
			}
			f.Reset()
			f.FormatNode(stmt)
			stmtStr := f.String()
			if stmtStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, stmtStr)
			}
		})
	}
}

func TestFormatExpr(t *testing.T) {
	testData := []struct {
		expr     string
		f        tree.FmtFlags
		expected string
	}{
		{`null`, tree.FmtShowTypes,
			`(NULL)[unknown]`},
		{`true`, tree.FmtShowTypes,
			`(true)[bool]`},
		{`123`, tree.FmtShowTypes,
			`(123)[int]`},
		{`123.456`, tree.FmtShowTypes,
			`(123.456)[decimal]`},
		{`'abc'`, tree.FmtShowTypes,
			`('abc')[string]`},
		{`b'abc'`, tree.FmtShowTypes,
			`('\x616263')[bytes]`},
		{`B'10010'`, tree.FmtShowTypes,
			`(B'10010')[varbit]`},
		{`interval '3s'`, tree.FmtShowTypes,
			`('3s')[interval]`},
		{`date '2003-01-01'`, tree.FmtShowTypes,
			`('2003-01-01')[date]`},
		{`timestamp '2003-01-01 00:00:00'`, tree.FmtShowTypes,
			`('2003-01-01 00:00:00+00:00')[timestamp]`},
		{`timestamptz '2003-01-01 00:00:00+03'`, tree.FmtShowTypes,
			`('2003-01-01 00:00:00+03:00')[timestamptz]`},
		{`greatest(unique_rowid(), 12)`, tree.FmtShowTypes,
			`(greatest((unique_rowid())[int], (12)[int]))[int]`},

		// While TestFormatStmt above checks StrVals, this here
		// checks DStrings.
		{`ARRAY['a','b c','d,e','f{g','h}i']`, tree.FmtBareStrings,
			`ARRAY[a, 'b c', 'd,e', 'f{g', 'h}i']`},
		// TODO(jordan): pg does *not* quote strings merely containing hex
		// escapes when included in array values. #16487
		// {`ARRAY[e'j\x10k']`, tree.FmtBareStrings,
		//	 `ARRAY[j\x10k]`},

		{`(-1):::INT`, tree.FmtParsableNumerics, "(-1)"},
		{`'NaN':::FLOAT`, tree.FmtParsableNumerics, "'NaN'"},
		{`'-Infinity':::FLOAT`, tree.FmtParsableNumerics, "'-Inf'"},
		{`'Infinity':::FLOAT`, tree.FmtParsableNumerics, "'+Inf'"},
		{`3.00:::FLOAT`, tree.FmtParsableNumerics, "3.0"},
		{`(-3.00):::FLOAT`, tree.FmtParsableNumerics, "(-3.0)"},
		{`'NaN':::DECIMAL`, tree.FmtParsableNumerics, "'NaN'"},
		{`'-Infinity':::DECIMAL`, tree.FmtParsableNumerics, "'-Infinity'"},
		{`'Infinity':::DECIMAL`, tree.FmtParsableNumerics, "'Infinity'"},
		{`3.00:::DECIMAL`, tree.FmtParsableNumerics, "3.00"},
		{`(-3.00):::DECIMAL`, tree.FmtParsableNumerics, "(-3.00)"},

		{`1`, tree.FmtParsable, "1:::INT"},
		{`9223372036854775807`, tree.FmtParsable, "9223372036854775807:::INT"},
		{`9223372036854775808`, tree.FmtParsable, "9223372036854775808:::DECIMAL"},
		{`-1`, tree.FmtParsable, "(-1):::INT"},
		{`-9223372036854775808`, tree.FmtParsable, "(-9223372036854775808):::INT"},
		{`-9223372036854775809`, tree.FmtParsable, "(-9223372036854775809):::DECIMAL"},
		{`(-92233.1):::FLOAT`, tree.FmtParsable, "(-92233.1):::FLOAT8"},
		{`92233.00:::DECIMAL`, tree.FmtParsable, "92233.00:::DECIMAL"},

		{`B'00100'`, tree.FmtParsable, "B'00100'"},

		{`unique_rowid() + 123`, tree.FmtParsable,
			`unique_rowid() + 123:::INT`},
		{`sqrt(123.0) + 456`, tree.FmtParsable,
			`sqrt(123.0:::DECIMAL) + 456:::DECIMAL`},
		{`ROW()`, tree.FmtParsable, `()`},
		{`now() + interval '3s'`, tree.FmtSimple,
			`now() + '3s'`},
		{`now() + interval '3s'`, tree.FmtParsable,
			`now():::TIMESTAMPTZ + '3s':::INTERVAL`},
		{`current_date() - date '2003-01-01'`, tree.FmtSimple,
			`current_date() - '2003-01-01'`},
		{`current_date() - date '2003-01-01'`, tree.FmtParsable,
			`current_date() - '2003-01-01':::DATE`},
		{`now() - timestamp '2003-01-01'`, tree.FmtSimple,
			`now() - '2003-01-01 00:00:00+00:00'`},
		{`now() - timestamp '2003-01-01'`, tree.FmtParsable,
			`now():::TIMESTAMPTZ - '2003-01-01 00:00:00+00:00':::TIMESTAMP`},
		{`'+Inf':::DECIMAL + '-Inf':::DECIMAL + 'NaN':::DECIMAL`, tree.FmtParsable,
			`('Infinity':::DECIMAL + '-Infinity':::DECIMAL) + 'NaN':::DECIMAL`},
		{`'+Inf':::FLOAT8 + '-Inf':::FLOAT8 + 'NaN':::FLOAT8`, tree.FmtParsable,
			`('+Inf':::FLOAT8 + '-Inf':::FLOAT8) + 'NaN':::FLOAT8`},
		{`'12:00:00':::TIME`, tree.FmtParsable,
			`'12:00:00':::TIME`},
		{`'63616665-6630-3064-6465-616462656562':::UUID`, tree.FmtParsable,
			`'63616665-6630-3064-6465-616462656562':::UUID`},

		{`(123:::INT, 123:::DECIMAL)`, tree.FmtCheckEquivalence,
			`(123:::INT, 123:::DECIMAL)`},

		{`(1, COALESCE(NULL, 123), ARRAY[45.6])`, tree.FmtHideConstants,
			`(_, COALESCE(_, _), ARRAY[_])`},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			expr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			ctx := tree.MakeSemaContext(false)
			typeChecked, err := tree.TypeCheck(expr, &ctx, types.Any)
			if err != nil {
				t.Fatal(err)
			}
			exprStr := tree.AsStringWithFlags(typeChecked, test.f)
			if exprStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, exprStr)
			}
		})
	}
}

func TestFormatExpr2(t *testing.T) {
	// This tests formatting from an expr AST. Suitable for use if your input
	// isn't easily creatable from a string without running an Eval.
	testData := []struct {
		expr     tree.Expr
		f        tree.FmtFlags
		expected string
	}{
		{tree.NewDOidWithName(tree.DInt(10), coltypes.RegClass, "foo"),
			tree.FmtParsable, `crdb_internal.create_REGCLASS(10,'foo'):::REGCLASS`},
		{tree.NewDOidWithName(tree.DInt(10), coltypes.RegProc, "foo"),
			tree.FmtParsable, `crdb_internal.create_REGPROC(10,'foo'):::REGPROC`},
		{tree.NewDOidWithName(tree.DInt(10), coltypes.RegType, "foo"),
			tree.FmtParsable, `crdb_internal.create_REGTYPE(10,'foo'):::REGTYPE`},
		{tree.NewDOidWithName(tree.DInt(10), coltypes.RegNamespace, "foo"),
			tree.FmtParsable, `crdb_internal.create_REGNAMESPACE(10,'foo'):::REGNAMESPACE`},

		// Ensure that nulls get properly type annotated when printed in an
		// enclosing tuple that has a type for their position within the tuple.
		{tree.NewDTuple(
			types.TTuple{
				Types: []types.T{
					types.Int,
					types.String,
				},
			}, tree.DNull, tree.NewDString("foo")),
			tree.FmtParsable,
			`(NULL::INT, 'foo':::STRING)`,
		},
		{tree.NewDTuple(
			types.TTuple{
				Types: []types.T{
					types.Unknown,
					types.String,
				},
			}, tree.DNull, tree.NewDString("foo")),
			tree.FmtParsable,
			`(NULL, 'foo':::STRING)`,
		},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			ctx := tree.MakeSemaContext(false)
			typeChecked, err := tree.TypeCheck(test.expr, &ctx, types.Any)
			if err != nil {
				t.Fatal(err)
			}
			exprStr := tree.AsStringWithFlags(typeChecked, test.f)
			if exprStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, exprStr)
			}
		})
	}
}

func TestFormatPgwireText(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`true`, `t`},
		{`false`, `f`},
		{`ROW(1)`, `(1)`},
		{`ROW(1, NULL)`, `(1,)`},
		{`ROW(1, true, 3)`, `(1,t,3)`},
		{`ROW(1, (2, 3))`, `(1,"(2,3)")`},
		{`ROW(1, (2, 'a b'))`, `(1,"(2,""a b"")")`},
		{`ROW(1, (2, 'a"b'))`, `(1,"(2,""a""""b"")")`},
		{`ROW(1, 2, ARRAY[1,2,3])`, `(1,2,"{1,2,3}")`},
		{`ROW(1, 2, ARRAY[1,NULL,3])`, `(1,2,"{1,NULL,3}")`},
		{`ROW(1, 2, ARRAY['a','b','c'])`, `(1,2,"{""a"",""b"",""c""}")`},
		{`ROW(1, 2, ARRAY[true,false,true])`, `(1,2,"{t,f,t}")`},
		{`ARRAY[(1,2),(3,4)]`, `{"(1,2)","(3,4)"}`},
		{`ARRAY[(false,'a'),(true,'b')]`, `{"(f,\"a\")","(t,\"b\")"}`},
		{`ARRAY[(1,ARRAY[2,NULL])]`, `{"(1,\"{2,NULL}\")"}`},
		{`ARRAY[(1,(1,2)),(2,(3,4))]`, `{"(1,\"(1,2)\")","(2,\"(3,4)\")"}`},

		{`(((1, 'a b', 3), (4, 'c d'), ROW(6)), (7, 8), ROW('e f'))`,
			`("(""(1,""""a b"""",3)"",""(4,""""c d"""")"",""(6)"")","(7,8)","(""e f"")")`},

		{`(((1, '2', 3), (4, '5'), ROW(6)), (7, 8), ROW('9'))`,
			// TODO(knz): if/when we change the sub-string formatter
			// to omit double quotes when not needed, the reference results
			// needs to become:
			// ("(""(1,2,3)"",""(4,5)"",""(6)"")","(7,8)","(9)")
			`("(""(1,""""2"""",3)"",""(4,""""5"""")"",""(6)"")","(7,8)","(""9"")")`},

		{`ARRAY[('a b',ARRAY['c d','e f']), ('g h',ARRAY['i j','k l'])]`,
			`{"(\"a b\",\"{\"\"c d\"\",\"\"e f\"\"}\")","(\"g h\",\"{\"\"i j\"\",\"\"k l\"\"}\")"}`},

		{`ARRAY[('1',ARRAY['2','3']), ('4',ARRAY['5','6'])]`,
			// TODO(knz): if/when we change the sub-string formatter
			// to omit double quotes when not needed, the reference results
			// needs to become:
			// {"(1,\"{2,3}\")","(4,\"{5,6}\")"}
			`{"(\"1\",\"{\"\"2\"\",\"\"3\"\"}\")","(\"4\",\"{\"\"5\"\",\"\"6\"\"}\")"}`},

		{`ARRAY[e'\U00002001☃']`, `{" ☃"}`},
	}
	var evalCtx tree.EvalContext
	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			expr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			ctx := tree.MakeSemaContext(false)
			typeChecked, err := tree.TypeCheck(expr, &ctx, types.Any)
			if err != nil {
				t.Fatal(err)
			}
			typeChecked, err = evalCtx.NormalizeExpr(typeChecked)
			if err != nil {
				t.Fatal(err)
			}
			exprStr := tree.AsStringWithFlags(typeChecked, tree.FmtPgwireText)
			if exprStr != test.expected {
				t.Fatalf("expected %s, got %s", test.expected, exprStr)
			}
		})
	}
}

// BenchmarkFormatRandomStatements measures the time needed to format
// 1000 random statements.
func BenchmarkFormatRandomStatements(b *testing.B) {
	// Generate a bunch of random statements.
	yBytes, err := ioutil.ReadFile(filepath.Join("..", "..", "parser", "sql.y"))
	if err != nil {
		b.Fatalf("error reading grammar: %v", err)
	}
	r, err := rsg.NewRSG(timeutil.Now().UnixNano(), string(yBytes), false)
	if err != nil {
		b.Fatalf("error instantiating RSG: %v", err)
	}
	strs := make([]string, 1000)
	stmts := make(tree.StatementList, 1000)
	for i := 0; i < 1000; {
		rdm := r.Generate("stmt", 20)
		stmt, err := parser.ParseOne(rdm)
		if err != nil {
			// Some statements (e.g. those containing error terminals) do
			// not parse.  It's all right. Just ignore this and continue
			// until we have all we want.
			continue
		}
		strs[i] = rdm
		stmts[i] = stmt
		i++
	}

	// Benchmark the parses.
	b.Run("parse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, sql := range strs {
				_, err := parser.ParseOne(sql)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	// Benchmark the formats.
	b.Run("format", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for i, stmt := range stmts {
				f := tree.NewFmtCtxWithBuf(tree.FmtSimple)
				f.FormatNode(stmt)
				strs[i] = f.CloseAndGetString()
			}
		}
	})
}
