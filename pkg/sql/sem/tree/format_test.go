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
			`SHOW CREATE TABLE _`},
		{`GRANT SELECT ON bar TO foo`, tree.FmtAnonymize,
			`GRANT SELECT ON _ TO _`},

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
			`SHOW CREATE TABLE xoxoxo`},
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

		{`1`, tree.FmtParsable, "1:::INT"},
		{`9223372036854775807`, tree.FmtParsable, "9223372036854775807:::INT"},
		{`9223372036854775808`, tree.FmtParsable, "9223372036854775808:::DECIMAL"},
		{`-1`, tree.FmtParsable, "(-1):::INT"},
		{`-9223372036854775808`, tree.FmtParsable, "(-9223372036854775808):::INT"},
		{`-9223372036854775809`, tree.FmtParsable, "-9223372036854775809:::DECIMAL"},

		{`unique_rowid() + 123`, tree.FmtParsable,
			`unique_rowid() + 123:::INT`},
		{`sqrt(123.0) + 456`, tree.FmtParsable,
			`sqrt(123.0:::DECIMAL) + 456:::DECIMAL`},
		{`now() + interval '3s'`, tree.FmtSimple,
			`now() + '3s'`},
		{`now() + interval '3s'`, tree.FmtParsable,
			`now() + '3s':::INTERVAL`},
		{`current_date() - date '2003-01-01'`, tree.FmtSimple,
			`current_date() - '2003-01-01'`},
		{`current_date() - date '2003-01-01'`, tree.FmtParsable,
			`current_date() - '2003-01-01':::DATE`},
		{`now() - timestamp '2003-01-01'`, tree.FmtSimple,
			`now() - '2003-01-01 00:00:00+00:00'`},
		{`now() - timestamp '2003-01-01'`, tree.FmtParsable,
			`now() - '2003-01-01 00:00:00+00:00':::TIMESTAMP`},
		{`'+Inf':::DECIMAL + '-Inf':::DECIMAL + 'NaN':::DECIMAL`, tree.FmtParsable,
			`('Infinity':::DECIMAL + '-Infinity':::DECIMAL) + 'NaN':::DECIMAL`},
		{`'+Inf':::FLOAT + '-Inf':::FLOAT + 'NaN':::FLOAT`, tree.FmtParsable,
			`('+Inf':::FLOAT + '-Inf':::FLOAT) + 'NaN':::FLOAT`},

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

// BenchmarkFormatRandomStatements measures the time needed to format
// 1000 random statements.
func BenchmarkFormatRandomStatements(b *testing.B) {
	// Generate a bunch of random statements.
	yBytes, err := ioutil.ReadFile(filepath.Join("..", "..", "parser", "sql.y"))
	if err != nil {
		b.Fatalf("error reading grammar: %v", err)
	}
	r, err := rsg.NewRSG(timeutil.Now().UnixNano(), string(yBytes))
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
