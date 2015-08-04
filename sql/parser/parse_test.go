// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
	_ "github.com/cockroachdb/cockroach/util/log" // for flags
)

// TestParse verifies that we can parse the supplied SQL and regenerate the SQL
// string from the syntax tree.
func TestParse(t *testing.T) {
	testData := []struct {
		sql string
	}{
		{``},
		{`VALUES ("")`},

		{`CREATE DATABASE a`},
		{`CREATE DATABASE IF NOT EXISTS a`},
		{`CREATE TABLE a ()`},
		{`CREATE TABLE a (b INT)`},
		{`CREATE TABLE a (b INT, c INT)`},
		{`CREATE TABLE a (b CHAR)`},
		{`CREATE TABLE a (b CHAR(3))`},
		{`CREATE TABLE a (b FLOAT)`},
		{`CREATE TABLE a (b INT NULL)`},
		{`CREATE TABLE a (b INT NOT NULL)`},
		{`CREATE TABLE a (b INT PRIMARY KEY)`},
		{`CREATE TABLE a (b INT UNIQUE)`},
		{`CREATE TABLE a (b INT NULL PRIMARY KEY)`},
		// "0" lost quotes previously.
		{`CREATE TABLE a (b INT, c TEXT, PRIMARY KEY (b, c, "0"))`},
		{`CREATE TABLE a (b INT, c TEXT, INDEX (b, c))`},
		{`CREATE TABLE a (b INT, c TEXT, CONSTRAINT d INDEX (b, c))`},
		{`CREATE TABLE a (b INT, UNIQUE (b))`},
		{`CREATE TABLE a.b (b INT)`},
		{`CREATE TABLE IF NOT EXISTS a (b INT)`},

		{`DELETE FROM a`},
		{`DELETE FROM a.b`},
		{`DELETE FROM a WHERE a = b`},

		{`DROP DATABASE a`},
		{`DROP DATABASE IF EXISTS a`},
		{`DROP TABLE a`},
		{`DROP TABLE a.b`},
		{`DROP TABLE a, b`},
		{`DROP TABLE IF EXISTS a`},

		{`SHOW DATABASES`},
		{`SHOW TABLES`},
		{`SHOW TABLES FROM a`},
		{`SHOW TABLES FROM a.b.c`},
		{`SHOW COLUMNS FROM a`},
		{`SHOW COLUMNS FROM a.b.c`},
		{`SHOW INDEX FROM a`},
		{`SHOW INDEX FROM a.b.c`},
		{`SHOW TABLES FROM a; SHOW COLUMNS FROM b`},

		{`GRANT READ ON DATABASE foo TO root`},
		{`GRANT ALL ON DATABASE foo TO root, test`},
		{`GRANT READ, WRITE ON DATABASE bar TO foo, bar, baz`},
		{`GRANT READ, WRITE ON DATABASE db1, db2 TO foo, bar, baz`},

		{`REVOKE READ ON DATABASE foo FROM root`},
		{`REVOKE ALL ON DATABASE foo FROM root, test`},
		{`REVOKE READ, WRITE ON DATABASE bar FROM foo, bar, baz`},
		{`REVOKE READ, WRITE ON DATABASE db1, db2 FROM foo, bar, baz`},

		{`INSERT INTO a VALUES (1)`},
		{`INSERT INTO a.b VALUES (1)`},
		{`INSERT INTO a VALUES (1, 2)`},
		{`INSERT INTO a VALUES (1, 2), (3, 4)`},
		{`INSERT INTO a VALUES (a + 1, 2 * 3)`},
		{`INSERT INTO a(a, b) VALUES (1, 2)`},
		{`INSERT INTO a(a, a.b) VALUES (1, 2)`},
		{`INSERT INTO a SELECT b, c FROM d`},
		{`INSERT INTO a DEFAULT VALUES`},

		{`SELECT 1 + 1`},
		{`SELECT - - 5`},
		{`SELECT - 1`},
		{`SELECT + 1`},
		{`SELECT .1`},
		{`SELECT 1.2e1`},
		{`SELECT 1.2e+1`},
		{`SELECT 1.2e-1`},
		{`SELECT true AND false`},
		{`SELECT true AND NULL`},
		{`SELECT true = false`},
		{`SELECT (true = false)`},
		{`SELECT (SELECT 1)`},
		{`SELECT ((SELECT 1))`},
		{`SELECT ((((VALUES (1)))))`},
		{`SELECT EXISTS (SELECT 1)`},
		{`SELECT (VALUES (1))`},
		{`SELECT (1, 2, 3)`},
		{`SELECT (ROW(1, 2, 3))`},
		{`SELECT (ROW())`},
		{`SELECT (TABLE a)`},

		{`SELECT FROM t`},
		{`SELECT 1 FROM t`},
		{`SELECT 1, 2 FROM t`},
		{`SELECT * FROM t`},
		{`SELECT "*" FROM t`},
		{`SELECT a, b FROM t`},
		{`SELECT a AS b FROM t`},
		{`SELECT a.* FROM t`},
		{`SELECT a = b FROM t`},
		{`SELECT $1 FROM t`},
		{`SELECT $1, $2 FROM t`},
		{`SELECT NULL FROM t`},
		{`SELECT 0.1 FROM t`},
		{`SELECT a FROM t`},
		{`SELECT a.b FROM t`},
		{`SELECT a.b.* FROM t`},
		{`SELECT a.b[1] FROM t`},
		{`SELECT a.b[1 + 1:4][3] FROM t`},
		{`SELECT 'a' FROM t`},

		{`SELECT 'a' AS "12345"`},
		{`SELECT 'a' AS clnm`},

		// Escaping may change since the scanning process loses information
		// (you can write e'\'' or ''''), but these are the idempotent cases.
		// Generally, anything that needs to escape plus \ and ' leads to an
		// escaped string.
		{`SELECT e'a\'a' FROM t`},
		{`SELECT e'a\\\\na' FROM t`},
		{`SELECT e'\\\\n' FROM t`},
		{`SELECT "a""a" FROM t`},
		{`SELECT a FROM "t\n"`}, // no escaping in sql identifiers
		{`SELECT a FROM "t"""`}, // no escaping in sql identifiers

		{`SELECT "FROM" FROM t`},
		{`SELECT CAST(1 AS TEXT)`},
		{`SELECT FROM t AS bar`},
		{`SELECT FROM (SELECT 1 FROM t)`},
		{`SELECT FROM (SELECT 1 FROM t) AS bar`},
		{`SELECT FROM t1, t2`},
		{`SELECT FROM t AS t1`},
		{`SELECT FROM s.t`},

		{`SELECT DISTINCT 1 FROM t`},
		{`SELECT COUNT(DISTINCT a) FROM t`},

		{`SELECT FROM t WHERE b = - 2`},
		{`SELECT FROM t WHERE a = b`},
		{`SELECT FROM t WHERE a = b AND a = c`},
		{`SELECT FROM t WHERE a = b OR a = c`},
		{`SELECT FROM t WHERE NOT a = b`},
		{`SELECT FROM t WHERE EXISTS (SELECT 1 FROM t)`},
		{`SELECT FROM t WHERE NOT (a = b)`},
		{`SELECT FROM t WHERE NOT true`},
		{`SELECT FROM t WHERE NOT false`},
		{`SELECT FROM t WHERE a IN (b)`},
		{`SELECT FROM t WHERE a IN (b, c)`},
		{`SELECT FROM t WHERE a IN (SELECT FROM t)`},
		{`SELECT FROM t WHERE a NOT IN (b, c)`},
		{`SELECT FROM t WHERE a LIKE b`},
		{`SELECT FROM t WHERE a NOT LIKE b`},
		{`SELECT FROM t WHERE a BETWEEN b AND c`},
		{`SELECT FROM t WHERE a NOT BETWEEN b AND c`},
		{`SELECT FROM t WHERE a IS NULL`},
		{`SELECT FROM t WHERE a IS NOT NULL`},
		{`SELECT FROM t WHERE a < b`},
		{`SELECT FROM t WHERE a <= b`},
		{`SELECT FROM t WHERE a >= b`},
		{`SELECT FROM t WHERE a != b`},
		{`SELECT FROM t WHERE a = (SELECT a FROM t)`},
		{`SELECT FROM t WHERE a = (b)`},
		{`SELECT FROM t WHERE a = b & c`},
		{`SELECT FROM t WHERE a = b | c`},
		{`SELECT FROM t WHERE a = b ^ c`},
		{`SELECT FROM t WHERE a = b + c`},
		{`SELECT FROM t WHERE a = b - c`},
		{`SELECT FROM t WHERE a = b * c`},
		{`SELECT FROM t WHERE a = b / c`},
		{`SELECT FROM t WHERE a = b % c`},
		{`SELECT FROM t WHERE a = b # c`},
		{`SELECT FROM t WHERE a = b || c`},
		{`SELECT FROM t WHERE a = + b`},
		{`SELECT FROM t WHERE a = - b`},
		{`SELECT FROM t WHERE a = ~ b`},
		{`SELECT FROM t WHERE CASE WHEN a = b THEN c END`},
		{`SELECT FROM t WHERE CASE WHEN a = b THEN c ELSE d END`},
		{`SELECT FROM t WHERE CASE WHEN a = b THEN c WHEN b = d THEN d ELSE d END`},
		{`SELECT FROM t WHERE CASE aa WHEN a = b THEN c END`},
		{`SELECT FROM t WHERE a = B()`},
		{`SELECT FROM t WHERE a = B(c)`},
		{`SELECT FROM t WHERE a = B(c, d)`},
		{`SELECT FROM t WHERE a = COUNT(*)`},
		{`SELECT (a.b) FROM t WHERE (b.c) = 2`},

		{`SELECT FROM t HAVING a = b`},

		{`SELECT FROM t UNION SELECT 1 FROM t`},
		{`SELECT FROM t UNION SELECT 1 FROM t UNION SELECT 1 FROM t`},
		{`SELECT FROM t EXCEPT SELECT 1 FROM t`},
		{`SELECT FROM t INTERSECT SELECT 1 FROM t`},

		{`SELECT FROM t1 JOIN t2 ON a = b`},
		{`SELECT FROM t1 JOIN t2 USING (a)`},
		{`SELECT FROM t1 LEFT JOIN t2 ON a = b`},
		{`SELECT FROM t1 RIGHT JOIN t2 ON a = b`},
		{`SELECT FROM t1 INNER JOIN t2 ON a = b`},
		{`SELECT FROM t1 CROSS JOIN t2`},
		{`SELECT FROM t1 NATURAL JOIN t2`},
		{`SELECT FROM t1 INNER JOIN t2 USING (a)`},
		{`SELECT FROM t1 FULL JOIN t2 USING (a)`},

		{`SELECT FROM t LIMIT a`},
		{`SELECT FROM t OFFSET b`},
		{`SELECT FROM t LIMIT a OFFSET b`},

		{`SET a = 3`},
		{`SET a = 3, 4`},
		{`SET a = '3'`},
		{`SET a = 3.0`},

		// TODO(pmattis): Is this a postgres extension?
		{`TABLE a`}, // Shorthand for: SELECT * FROM a

		{`TRUNCATE TABLE a`},
		{`TRUNCATE TABLE a, b.c`},

		{`UPDATE a SET b = 3`},
		{`UPDATE a.b SET b = 3`},
		{`UPDATE a SET b.c = 3`},
		{`UPDATE a SET b = 3, c = 4`},
		{`UPDATE a SET b = 3 + 4`},
		{`UPDATE a SET b = 3 WHERE a = b`},
		{`UPDATE T AS "0" SET K = ''`},                 // "0" lost its quotes
		{`SELECT * FROM "0" JOIN "0" USING (id, "0")`}, // last "0" lost its quotes.
	}
	for _, d := range testData {
		stmts, err := Parse(d.sql)
		if err != nil {
			t.Fatalf("%s: expected success, but found %s", d.sql, err)
		}
		s := stmts.String()
		if d.sql != s {
			t.Errorf("expected %s, but found %s", d.sql, s)
		}
	}
}

// TestParse2 verifies that we can parse the supplied SQL and regenerate the
// expected SQL string from the syntax tree. Note that if the input and output
// SQL strings are the same, the test case should go in TestParse instead.
func TestParse2(t *testing.T) {
	testData := []struct {
		sql      string
		expected string
	}{
		// TODO(pmattis): Handle octal and hexadecimal numbers.
		// {`SELECT 010 FROM t`, ``},
		// {`SELECT 0xf0 FROM t`, ``},
		// {`SELECT 0xF0 FROM t`, ``},
		// Escaped string literals are not always escaped the same because
		// '''' and e'\'' scan to the same token. It's more convenient to
		// prefer escaping ' and \, so we do that.
		{`SELECT 'a''a'`,
			`SELECT e'a\'a'`},
		{`SELECT 'a\a'`,
			`SELECT e'a\\a'`},
		{`SELECT 'a\n'`,
			`SELECT e'a\\n'`},
		{"SELECT '\n'",
			`SELECT e'\n'`},
		{"SELECT '\n\\'",
			`SELECT e'\n\\'`},
		{`SELECT "a'a" FROM t`,
			`SELECT "a'a" FROM t`},
		// Comments are stripped.
		{`SELECT 1 FROM t -- hello world`,
			`SELECT 1 FROM t`},
		{`SELECT /* hello world */ 1 FROM t`,
			`SELECT 1 FROM t`},
		{`SELECT /* hello */ 1 FROM /* world */ t`,
			`SELECT 1 FROM t`},
		// Alias expressions are always output using AS.
		{`SELECT 1 FROM t t1`,
			`SELECT 1 FROM t AS t1`},
		// Alternate not-equal operator.
		{`SELECT FROM t WHERE a <> b`,
			`SELECT FROM t WHERE a != b`},
		// OUTER is syntactic sugar.
		{`SELECT FROM t1 LEFT OUTER JOIN t2 ON a = b`,
			`SELECT FROM t1 LEFT JOIN t2 ON a = b`},
		{`SELECT FROM t1 RIGHT OUTER JOIN t2 ON a = b`,
			`SELECT FROM t1 RIGHT JOIN t2 ON a = b`},
		// TODO(pmattis): Handle UNION ALL.
		{`SELECT FROM t UNION ALL SELECT 1 FROM t`,
			`SELECT FROM t UNION SELECT 1 FROM t`},
		// We allow OFFSET before LIMIT, but always output LIMIT first.
		{`SELECT FROM t OFFSET a LIMIT b`,
			`SELECT FROM t LIMIT b OFFSET a`},
		// Shorthand type cast.
		{`SELECT '1'::INT`,
			`SELECT CAST('1' AS INT)`},
		// Double negation. See #1800.
		{`SELECT *,-/* comment */-5`,
			`SELECT *, - - 5`},
		{"SELECT -\n-5",
			`SELECT - - 5`},
		{`SELECT -0.-/*test*/-1`,
			`SELECT - 0. - - 1`,
		},
	}
	for _, d := range testData {
		stmts, err := Parse(d.sql)
		if err != nil {
			t.Fatalf("%s: expected success, but found %s", d.sql, err)
		}
		s := stmts.String()
		if d.expected != s {
			t.Errorf("expected %s, but found %s", d.expected, s)
		}
		if _, err := Parse(s); err != nil {
			t.Errorf("expected string found, but not parsable: %s:\n%s", err, s)
		}
	}
}

// TestParseSyntax verifieds that parsing succeeds, though the syntax tree
// likely differs. All of the test cases here should eventually be moved
// elsewhere.
func TestParseSyntax(t *testing.T) {
	testData := []struct {
		sql string
	}{
		{`SELECT '\0' FROM a`},
		{`SELECT 1 FROM t FOR READ ONLY`},
		{`SELECT 1 FROM t FOR UPDATE`},
		{`SELECT 1 FROM t FOR SHARE`},
		{`SELECT 1 FROM t FOR KEY SHARE`},
		{`SELECT ((1)) FROM t WHERE ((a)) IN (((1))) AND ((a, b)) IN ((((1, 1))), ((2, 2)))`},
		{`SELECT e'\'\"\b\n\r\t\\' FROM t`},
		{`SELECT '\x' FROM t`},
		{`SELECT 1 FROM t GROUP BY a`},
		{`SELECT 1 FROM t ORDER BY a`},
		{`SELECT 1 FROM t ORDER BY a ASC`},
		{`SELECT 1 FROM t ORDER BY a DESC`},
		{`CREATE INDEX a ON b (c)`},
		{`CREATE INDEX a ON b.c (d)`},
		{`CREATE INDEX ON a (b)`},
		{`CREATE UNIQUE INDEX a ON b (c)`},
		{`CREATE UNIQUE INDEX a ON b.c (d)`},
		{`CREATE UNIQUE INDEX a ON b USING foo (c)`},
		{`DROP INDEX a`},
		{`DROP INDEX IF EXISTS a`},
	}
	for _, d := range testData {
		if _, err := Parse(d.sql); err != nil {
			t.Fatalf("%s: expected success, but not parsable %s", d.sql, err)
		}
	}
}

func TestParseError(t *testing.T) {
	testData := []struct {
		sql      string
		expected string
	}{
		{`SELECT2 1`, `syntax error at or near "SELECT2"
SELECT2 1
^
`},
		{`SELECT 1 FROM (t)`, `syntax error at or near ")"
SELECT 1 FROM (t)
                ^
`},
		{`SET a = 1, b = 2`, `syntax error at or near "="
SET a = 1, b = 2
             ^
`},
		{`SET a = 1,
b = 2`, `syntax error at or near "="
SET a = 1,
b = 2
  ^
`},
		{`SELECT 1 /* hello`, `unterminated comment
SELECT 1 /* hello
         ^
`},
		{`SELECT '1`, `unterminated string
SELECT '1
       ^
`},
		{`CREATE TABLE test (
  INDEX foo (bar)
)`, `syntax error at or near "foo"
CREATE TABLE test (
  INDEX foo (bar)
        ^
`},
		{`CREATE DATABASE a b`,
			`syntax error at or near "b"
CREATE DATABASE a b
                  ^
`},
		{`CREATE DATABASE a b c`,
			`syntax error at or near "b"
CREATE DATABASE a b c
                  ^
`},
		{"SELECT 1e-\n-1",
			`invalid floating point constant
SELECT 1e-
       ^
`},
		{"SELECT foo''",
			`syntax error at or near ""
SELECT foo''
          ^
`},
	}
	for _, d := range testData {
		_, err := Parse(d.sql)
		if err == nil || err.Error() != d.expected {
			t.Fatalf("%s: expected\n%s, but found\n%v", d.sql, d.expected, err)
		}
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
	_, err := Parse(s)
	expected := `syntax error at or near "EOF"`
	if !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}
}
