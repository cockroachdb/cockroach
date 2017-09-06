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

package parser

import (
	"bytes"
	"fmt"
	"testing"
)

func TestFormatStatement(t *testing.T) {
	tableFormatter := FmtReformatTableNames(FmtSimple,
		func(_ *NormalizableTableName, buf *bytes.Buffer, _ FmtFlags) {
			buf.WriteString("xoxoxo")
		})

	testData := []struct {
		stmt     string
		f        FmtFlags
		expected string
	}{
		{`CREATE USER foo WITH PASSWORD 'bar'`, FmtSimple,
			`CREATE USER foo WITH PASSWORD *****`},
		{`CREATE USER foo WITH PASSWORD 'bar'`, FmtSimpleWithPasswords,
			`CREATE USER foo WITH PASSWORD 'bar'`},

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
		// FmtAnonymize but not processed by table formatters.
		//
		// {`TRUNCATE foo`, tableFormatter,
		// `TRUNCATE TABLE xoxoxo`},
		// {`GRANT SELECT ON bar TO foo`, tableFormatter,
		// `GRANT SELECT ON xoxoxo TO foo`},

		{`CREATE TABLE foo (x INT)`, FmtAnonymize,
			`CREATE TABLE _ (_ INT)`},
		{`INSERT INTO foo(x) TABLE bar`, FmtAnonymize,
			`INSERT INTO _(_) TABLE _`},
		{`UPDATE foo SET x = y`, FmtAnonymize,
			`UPDATE _ SET _ = _`},
		{`DELETE FROM foo`, FmtAnonymize,
			`DELETE FROM _`},
		{`TRUNCATE foo`, FmtAnonymize,
			`TRUNCATE TABLE _`},
		{`ALTER TABLE foo RENAME TO bar`, FmtAnonymize,
			`ALTER TABLE _ RENAME TO _`},
		{`SHOW COLUMNS FROM foo`, FmtAnonymize,
			`SHOW COLUMNS FROM _`},
		{`SHOW CREATE TABLE foo`, FmtAnonymize,
			`SHOW CREATE TABLE _`},
		{`GRANT SELECT ON bar TO foo`, FmtAnonymize,
			`GRANT SELECT ON _ TO _`},

		{`SELECT 1+COALESCE(NULL, 'a', x)-ARRAY[3.14]`, FmtHideConstants,
			`SELECT (_ + COALESCE(_, _, x)) - ARRAY[_]`},

		// This here checks encodeSQLString on non-DString strings also
		// calls encodeSQLString with the right formatter.
		// See TestFormatExprs below for the test on DStrings.
		{`CREATE DATABASE foo TEMPLATE = 'bar-baz'`, FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = bar-baz`},
		{`CREATE DATABASE foo TEMPLATE = 'bar baz'`, FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = 'bar baz'`},
		{`CREATE DATABASE foo TEMPLATE = 'bar,baz'`, FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = 'bar,baz'`},
		{`CREATE DATABASE foo TEMPLATE = 'bar{baz}'`, FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = 'bar{baz}'`},

		{`SET "time zone" = UTC`, FmtSimple,
			`SET "time zone" = utc`},
		{`SET "time zone" = UTC`, FmtBareIdentifiers,
			`SET time zone = utc`},
		{`SET "time zone" = UTC`, FmtBareStrings,
			`SET "time zone" = utc`},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.stmt), func(t *testing.T) {
			stmt, err := ParseOne(test.stmt)
			if err != nil {
				t.Fatal(err)
			}
			stmtStr := AsStringWithFlags(stmt, test.f)
			if stmtStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, stmtStr)
			}
		})
	}
}

func TestFormatExpr(t *testing.T) {
	testData := []struct {
		expr     string
		f        FmtFlags
		expected string
	}{
		{`null`, FmtShowTypes,
			`(NULL)[NULL]`},
		{`true`, FmtShowTypes,
			`(true)[bool]`},
		{`123`, FmtShowTypes,
			`(123)[int]`},
		{`123.456`, FmtShowTypes,
			`(123.456)[decimal]`},
		{`'abc'`, FmtShowTypes,
			`('abc')[string]`},
		{`b'abc'`, FmtShowTypes,
			`(b'abc')[bytes]`},
		{`interval '3s'`, FmtShowTypes,
			`('3s')[interval]`},
		{`date '2003-01-01'`, FmtShowTypes,
			`('2003-01-01')[date]`},
		{`greatest(unique_rowid(), 12)`, FmtShowTypes,
			`(greatest((unique_rowid())[int], (12)[int]))[int]`},

		// While TestFormatStmt above checks StrVals, this here
		// checks DStrings.
		{`ARRAY['a','b c','d,e','f{g','h}i']`, FmtBareStrings,
			`ARRAY[a, 'b c', 'd,e', 'f{g', 'h}i']`},
		// TODO(jordan): pg does *not* quote strings merely containing hex
		// escapes when included in array values. #16487
		// {`ARRAY[e'j\x10k']`, FmtBareStrings,
		//	 `ARRAY[j\x10k]`},

		{`1`, FmtParsable, "1:::INT"},
		{`9223372036854775807`, FmtParsable, "9223372036854775807:::INT"},
		{`9223372036854775808`, FmtParsable, "9223372036854775808:::DECIMAL"},
		{`-1`, FmtParsable, "(-1):::INT"},
		{`-9223372036854775808`, FmtParsable, "(-9223372036854775808):::INT"},
		{`-9223372036854775809`, FmtParsable, "-9223372036854775809:::DECIMAL"},

		{`unique_rowid() + 123`, FmtParsable,
			`unique_rowid() + 123:::INT`},
		{`sqrt(123.0) + 456`, FmtParsable,
			`sqrt(123.0:::DECIMAL) + 456:::DECIMAL`},
		{`now() + interval '3s'`, FmtSimple,
			`now() + '3s'`},
		{`now() + interval '3s'`, FmtParsable,
			`now() + '3s':::INTERVAL`},
		{`current_date() - date '2003-01-01'`, FmtSimple,
			`current_date() - '2003-01-01'`},
		{`current_date() - date '2003-01-01'`, FmtParsable,
			`current_date() - '2003-01-01':::DATE`},
		{`now() - timestamp '2003-01-01'`, FmtSimple,
			`now() - '2003-01-01 00:00:00+00:00'`},
		{`now() - timestamp '2003-01-01'`, FmtParsable,
			`now() - '2003-01-01 00:00:00+00:00':::TIMESTAMP`},
		{`'+Inf':::DECIMAL + '-Inf':::DECIMAL + 'NaN':::DECIMAL`, FmtParsable,
			`('Infinity':::DECIMAL + '-Infinity':::DECIMAL) + 'NaN':::DECIMAL`},
		{`'+Inf':::FLOAT + '-Inf':::FLOAT + 'NaN':::FLOAT`, FmtParsable,
			`('+Inf':::FLOAT + '-Inf':::FLOAT) + 'NaN':::FLOAT`},

		{`(123:::INT, 123:::DECIMAL)`, FmtCheckEquivalence,
			`(123:::INT, 123:::DECIMAL)`},

		{`(1, COALESCE(NULL, 123), ARRAY[45.6])`, FmtHideConstants,
			`(_, COALESCE(_, _), ARRAY[_])`},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			expr, err := ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			ctx := MakeSemaContext(false)
			typeChecked, err := TypeCheck(expr, &ctx, TypeAny)
			if err != nil {
				t.Fatal(err)
			}
			exprStr := AsStringWithFlags(typeChecked, test.f)
			if exprStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, exprStr)
			}
		})
	}
}
