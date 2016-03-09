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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"testing"

	"github.com/cockroachdb/cockroach/util/log"
)

// TestFillArgs tests both FillArgs and WalkExpr.
func TestFillArgs(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
		args     MapArgs
	}{
		{`$1`, `'a'`, MapArgs{`1`: DString(`a`)}},
		{`($1, $1, $1)`, `('a', 'a', 'a')`, MapArgs{`1`: DString(`a`)}},
		{`$1 & $2`, `1 & 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 | $2`, `1 | 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 ^ $2`, `1 ^ 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 + $2`, `1 + 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 - $2`, `1 - 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 * $2`, `1 * 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 % $2`, `1 % 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 / $2`, `1 / 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 / $2`, `1 / 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 + $2 + ($3 * $4)`, `1 + 2 + (3 * 4)`,
			MapArgs{`1`: DInt(1), `2`: DInt(2), `3`: DInt(3), `4`: DInt(4)}},
		{`$1 || $2`, `'a' || 'b'`, MapArgs{`1`: DString("a"), `2`: DString("b")}},
		{`$1 OR $2`, `true OR false`, MapArgs{`1`: DBool(true), `2`: DBool(false)}},
		{`$1 AND $2`, `true AND false`, MapArgs{`1`: DBool(true), `2`: DBool(false)}},
		{`$1 = $2`, `1 = 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 != $2`, `1 != 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 <> $2`, `1 != 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 < $2`, `1 < 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 <= $2`, `1 <= 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 > $2`, `1 > 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 >= $2`, `1 >= 2`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`$1 IS NULL`, `1 IS NULL`, MapArgs{`1`: DInt(1)}},
		{`$1 IS NOT NULL`, `1 IS NOT NULL`, MapArgs{`1`: DInt(1)}},
		{`$1 BETWEEN $2 AND $3`, `1 BETWEEN 2 AND 3`, MapArgs{`1`: DInt(1), `2`: DInt(2), `3`: DInt(3)}},
		{`$1 NOT BETWEEN $2 AND $3`, `1 NOT BETWEEN 2 AND 3`,
			MapArgs{`1`: DInt(1), `2`: DInt(2), `3`: DInt(3)}},
		{`CASE WHEN $1 THEN $2 END`, `CASE WHEN 1 THEN 2 END`, MapArgs{`1`: DInt(1), `2`: DInt(2)}},
		{`CASE WHEN $1 THEN $2 ELSE $3 END`, `CASE WHEN 1 THEN 2 ELSE 3 END`,
			MapArgs{`1`: DInt(1), `2`: DInt(2), `3`: DInt(3)}},
		{`CASE $1 WHEN $2 THEN $3 ELSE $4 END`, `CASE 1 WHEN 2 THEN 3 ELSE 4 END`,
			MapArgs{`1`: DInt(1), `2`: DInt(2), `3`: DInt(3), `4`: DInt(4)}},
		{`($1, $2) = ($3, $4)`, `(1, 2) = (3, 4)`,
			MapArgs{`1`: DInt(1), `2`: DInt(2), `3`: DInt(3), `4`: DInt(4)}},
		{`$1 IN ($2, $3)`, `1 IN (2, 3)`,
			MapArgs{`1`: DInt(1), `2`: DInt(2), `3`: DInt(3)}},
		{`$1 NOT IN ($2, $3)`, `1 NOT IN (2, 3)`,
			MapArgs{`1`: DInt(1), `2`: DInt(2), `3`: DInt(3)}},
		{`length($1)`, `length('a')`, MapArgs{`1`: DString("a")}},
		{`length($1, $2)`, `length('a', 'b')`, MapArgs{`1`: DString("a"), `2`: DString("b")}},
		{`CAST($1 AS INT)`, `CAST(1.1 AS INT)`, MapArgs{`1`: DFloat(1.1)}},
		{`ROW($1, $2, $3)`, `ROW(1, 2, '3')`, MapArgs{`1`: DInt(1), `2`: DInt(2), `3`: DString("3")}},
		{`(SELECT $1)`, `(SELECT 'a')`, MapArgs{`1`: DString("a")}},
		{`EXISTS (SELECT $1)`, `EXISTS (SELECT 'a')`, MapArgs{`1`: DString("a")}},
	}

	for _, d := range testData {
		q, err := ParseOneTraditional("SELECT " + d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		q, err = FillArgs(q, d.args)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if s := q.(*Select).Select.(*SelectClause).Exprs[0].Expr.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestFillArgsError(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
		args     MapArgs
	}{
		{`$1`, `arg $1 not found`, MapArgs{}},
		{`$2 AND $1`, `arg $2 not found`, MapArgs{}},
	}
	for _, d := range testData {
		q, err := ParseOneTraditional("SELECT " + d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if _, err := FillArgs(q, d.args); err == nil {
			t.Fatalf("%s: expected failure, but found success", d.expr)
		} else if d.expected != err.Error() {
			t.Fatalf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}

func TestWalkStmt(t *testing.T) {
	testData := []struct {
		sql      string
		expected string
		args     MapArgs
	}{
		{`DELETE FROM db.table WHERE k IN ($1, $2)`,
			`DELETE FROM db.table WHERE k IN ('a', 'c')`,
			MapArgs{`1`: DString(`a`), `2`: DString(`c`)}},
		{`INSERT INTO db.table (k, v) VALUES (1, 2), ($1, $2)`,
			`INSERT INTO db.table (k, v) VALUES (1, 2), (3, 4)`,
			MapArgs{`1`: DInt(3), `2`: DInt(4)}},
		{`SELECT $1, $2 FROM db.table ORDER BY $1 DESC LIMIT $3 OFFSET $4`,
			`SELECT 'a', 'b' FROM db.table ORDER BY 'a' DESC LIMIT 5 OFFSET 2`,
			MapArgs{`1`: DString(`a`), `2`: DString(`b`), `3`: DInt(5), `4`: DInt(2)}},
		{`SELECT $1, $2 FROM db.table WHERE c in ($3, 2 * $4) GROUP BY $1 HAVING COUNT($5) > $6`,
			`SELECT 'a', 'b' FROM db.table WHERE c in (1.1, 2 * 6.5) GROUP BY 'a' HAVING COUNT('d') > 6`,
			MapArgs{`1`: DString(`a`), `2`: DString(`b`), `3`: DFloat(1.1), `4`: DFloat(6.5), `5`: DString('d'), `6`: DInt(6)}},
		{`UPDATE db.table SET v = $3 WHERE k IN ($1, $2)`,
			`UPDATE db.table SET v = 2 WHERE k IN ('a', 'b')`,
			MapArgs{`1`: DString(`a`), `2`: DString(`b`), `3`: DInt(2)}},
	}
	for _, d := range testData {
		q, err := ParseOneTraditional(d.sql)
		if err != nil {
			t.Fatalf("%s: %v", d.sql, err)
		}
		qOrig := q
		qOrigStr := q.String()
		// FillArgs is where the walk happens, using argVisitor.
		q, err = FillArgs(q, d.args)
		if err != nil {
			t.Fatalf("%s: %v", d.sql, err)
		}
		e, err := ParseOneTraditional(d.expected)
		if err != nil {
			t.Fatalf("%s: %v", d.expected, err)
		}
		// Verify that all expressions match up.
		if q.String() != e.String() {
			log.Fatalf("%s not eq expected: %s", q.String(), e.String())
		}
		// The original expression should be unchanged.
		if qOrig.String() != qOrigStr {
			t.Fatalf("Original expression `%s` changed to `%s`", qOrigStr, qOrigStr)
		}
	}
}
