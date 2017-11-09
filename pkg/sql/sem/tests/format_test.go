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

package tests

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func TestFormatStatement(t *testing.T) {
	tableFormatter := parser.FmtReformatTableNames(parser.FmtSimple,
		func(_ *parser.NormalizableTableName, buf *bytes.Buffer, _ parser.FmtFlags) {
			buf.WriteString("xoxoxo")
		})

	testData := []struct {
		stmt     string
		f        parser.FmtFlags
		expected string
	}{
		{`CREATE USER foo WITH PASSWORD 'bar'`, parser.FmtSimple,
			`CREATE USER 'foo' WITH PASSWORD *****`},
		{`CREATE USER foo WITH PASSWORD 'bar'`, parser.FmtSimpleWithPasswords,
			`CREATE USER 'foo' WITH PASSWORD 'bar'`},

		{`CREATE TABLE foo (x INT)`, tableFormatter,
			`CREATE TABLE xoxoxo (x INT)`},
		{`INSERT INTO foo(x) TABLE bar`, tableFormatter,
			`INSERT INTO xoxoxo(x) TABLE xoxoxo`},
		{`UPDATE foo SET x = y`, tableFormatter,
			`UPDATE xoxoxo SET x = y`},
		{`DELETE FROM foo`, tableFormatter,
			`DELETE FROM xoxoxo`},
		{`ALTER TABLE foo RENAME TO bar`, tableFormatter,
			`ALTER TABLE xoxoxo RENAME TO xoxoxo`},
		{`SHOW COLUMNS FROM foo`, tableFormatter,
			`SHOW COLUMNS FROM xoxoxo`},
		{`SHOW CREATE TABLE foo`, tableFormatter,
			`SHOW CREATE TABLE xoxoxo`},
		// TODO(knz): TRUNCATE and GRANT table names are removed by
		// parser.FmtAnonymize but not processed by table formatters.
		//
		// {`TRUNCATE foo`, tableFormatter,
		// `TRUNCATE TABLE xoxoxo`},
		// {`GRANT SELECT ON bar TO foo`, tableFormatter,
		// `GRANT SELECT ON xoxoxo TO foo`},

		{`CREATE TABLE foo (x INT)`, parser.FmtAnonymize,
			`CREATE TABLE _ (_ INT)`},
		{`INSERT INTO foo(x) TABLE bar`, parser.FmtAnonymize,
			`INSERT INTO _(_) TABLE _`},
		{`UPDATE foo SET x = y`, parser.FmtAnonymize,
			`UPDATE _ SET _ = _`},
		{`DELETE FROM foo`, parser.FmtAnonymize,
			`DELETE FROM _`},
		{`TRUNCATE foo`, parser.FmtAnonymize,
			`TRUNCATE TABLE _`},
		{`ALTER TABLE foo RENAME TO bar`, parser.FmtAnonymize,
			`ALTER TABLE _ RENAME TO _`},
		{`SHOW COLUMNS FROM foo`, parser.FmtAnonymize,
			`SHOW COLUMNS FROM _`},
		{`SHOW CREATE TABLE foo`, parser.FmtAnonymize,
			`SHOW CREATE TABLE _`},
		{`GRANT SELECT ON bar TO foo`, parser.FmtAnonymize,
			`GRANT SELECT ON _ TO _`},

		{`SELECT 1+COALESCE(NULL, 'a', x)-ARRAY[3.14]`, parser.FmtHideConstants,
			`SELECT (_ + COALESCE(_, _, x)) - ARRAY[_]`},

		// This here checks encodeSQLString on non-parser.DString strings also
		// calls encodeSQLString with the right formatter.
		// See TestFormatExprs below for the test on DStrings.
		{`CREATE DATABASE foo TEMPLATE = 'bar-baz'`, parser.FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = bar-baz`},
		{`CREATE DATABASE foo TEMPLATE = 'bar baz'`, parser.FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = 'bar baz'`},
		{`CREATE DATABASE foo TEMPLATE = 'bar,baz'`, parser.FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = 'bar,baz'`},
		{`CREATE DATABASE foo TEMPLATE = 'bar{baz}'`, parser.FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = 'bar{baz}'`},

		{`SET "time zone" = UTC`, parser.FmtSimple,
			`SET "time zone" = utc`},
		{`SET "time zone" = UTC`, parser.FmtBareIdentifiers,
			`SET time zone = utc`},
		{`SET "time zone" = UTC`, parser.FmtBareStrings,
			`SET "time zone" = utc`},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.stmt), func(t *testing.T) {
			stmt, err := parser.ParseOne(test.stmt)
			if err != nil {
				t.Fatal(err)
			}
			stmtStr := parser.AsStringWithFlags(stmt, test.f)
			if stmtStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, stmtStr)
			}
		})
	}
}

func TestFormatExpr(t *testing.T) {
	testData := []struct {
		expr     string
		f        parser.FmtFlags
		expected string
	}{
		{`null`, parser.FmtShowTypes,
			`(NULL)[NULL]`},
		{`true`, parser.FmtShowTypes,
			`(true)[bool]`},
		{`123`, parser.FmtShowTypes,
			`(123)[int]`},
		{`123.456`, parser.FmtShowTypes,
			`(123.456)[decimal]`},
		{`'abc'`, parser.FmtShowTypes,
			`('abc')[string]`},
		{`b'abc'`, parser.FmtShowTypes,
			`('\x616263')[bytes]`},
		{`interval '3s'`, parser.FmtShowTypes,
			`('3s')[interval]`},
		{`date '2003-01-01'`, parser.FmtShowTypes,
			`('2003-01-01')[date]`},
		{`timestamp '2003-01-01 00:00:00'`, parser.FmtShowTypes,
			`('2003-01-01 00:00:00+00:00')[timestamp]`},
		{`timestamptz '2003-01-01 00:00:00+03'`, parser.FmtShowTypes,
			`('2003-01-01 00:00:00+03:00')[timestamptz]`},
		{`greatest(unique_rowid(), 12)`, parser.FmtShowTypes,
			`(greatest((unique_rowid())[int], (12)[int]))[int]`},

		// While TestFormatStmt above checks StrVals, this here
		// checks DStrings.
		{`ARRAY['a','b c','d,e','f{g','h}i']`, parser.FmtBareStrings,
			`ARRAY[a, 'b c', 'd,e', 'f{g', 'h}i']`},
		// TODO(jordan): pg does *not* quote strings merely containing hex
		// escapes when included in array values. #16487
		// {`ARRAY[e'j\x10k']`, parser.FmtBareStrings,
		//	 `ARRAY[j\x10k]`},

		{`1`, parser.FmtParsable, "1:::INT"},
		{`9223372036854775807`, parser.FmtParsable, "9223372036854775807:::INT"},
		{`9223372036854775808`, parser.FmtParsable, "9223372036854775808:::DECIMAL"},
		{`-1`, parser.FmtParsable, "(-1):::INT"},
		{`-9223372036854775808`, parser.FmtParsable, "(-9223372036854775808):::INT"},
		{`-9223372036854775809`, parser.FmtParsable, "-9223372036854775809:::DECIMAL"},

		{`unique_rowid() + 123`, parser.FmtParsable,
			`unique_rowid() + 123:::INT`},
		{`sqrt(123.0) + 456`, parser.FmtParsable,
			`sqrt(123.0:::DECIMAL) + 456:::DECIMAL`},
		{`now() + interval '3s'`, parser.FmtSimple,
			`now() + '3s'`},
		{`now() + interval '3s'`, parser.FmtParsable,
			`now() + '3s':::INTERVAL`},
		{`current_date() - date '2003-01-01'`, parser.FmtSimple,
			`current_date() - '2003-01-01'`},
		{`current_date() - date '2003-01-01'`, parser.FmtParsable,
			`current_date() - '2003-01-01':::DATE`},
		{`now() - timestamp '2003-01-01'`, parser.FmtSimple,
			`now() - '2003-01-01 00:00:00+00:00'`},
		{`now() - timestamp '2003-01-01'`, parser.FmtParsable,
			`now() - '2003-01-01 00:00:00+00:00':::TIMESTAMP`},
		{`'+Inf':::DECIMAL + '-Inf':::DECIMAL + 'NaN':::DECIMAL`, parser.FmtParsable,
			`('Infinity':::DECIMAL + '-Infinity':::DECIMAL) + 'NaN':::DECIMAL`},
		{`'+Inf':::FLOAT + '-Inf':::FLOAT + 'NaN':::FLOAT`, parser.FmtParsable,
			`('+Inf':::FLOAT + '-Inf':::FLOAT) + 'NaN':::FLOAT`},

		{`(123:::INT, 123:::DECIMAL)`, parser.FmtCheckEquivalence,
			`(123:::INT, 123:::DECIMAL)`},

		{`(1, COALESCE(NULL, 123), ARRAY[45.6])`, parser.FmtHideConstants,
			`(_, COALESCE(_, _), ARRAY[_])`},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			expr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			ctx := parser.MakeSemaContext(false)
			typeChecked, err := parser.TypeCheck(expr, &ctx, types.Any)
			if err != nil {
				t.Fatal(err)
			}
			exprStr := parser.AsStringWithFlags(typeChecked, test.f)
			if exprStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, exprStr)
			}
		})
	}
}
