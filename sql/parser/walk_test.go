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
	"log"
	"testing"
)

type mapArgs map[int]Datum

func (m mapArgs) Arg(i int) (Datum, bool) {
	d, ok := m[i]
	return d, ok
}

// TestFillArgs tests both FillArgs and WalkExpr.
func TestFillArgs(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
		args     mapArgs
	}{
		{`$1`, `'a'`, mapArgs{1: DString(`a`)}},
		{`($1, $1, $1)`, `('a', 'a', 'a')`, mapArgs{1: DString(`a`)}},
		{`$1 & $2`, `1 & 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 | $2`, `1 | 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 # $2`, `1 # 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 + $2`, `1 + 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 - $2`, `1 - 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 * $2`, `1 * 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 % $2`, `1 % 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 / $2`, `1 / 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 / $2`, `1 / 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 + $2 + ($3 * $4)`, `1 + 2 + (3 * 4)`,
			mapArgs{1: DInt(1), 2: DInt(2), 3: DInt(3), 4: DInt(4)}},
		{`$1 || $2`, `'a' || 'b'`, mapArgs{1: DString("a"), 2: DString("b")}},
		{`$1 OR $2`, `true OR false`, mapArgs{1: DBool(true), 2: DBool(false)}},
		{`$1 AND $2`, `true AND false`, mapArgs{1: DBool(true), 2: DBool(false)}},
		{`$1 = $2`, `1 = 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 != $2`, `1 != 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 <> $2`, `1 != 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 < $2`, `1 < 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 <= $2`, `1 <= 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 > $2`, `1 > 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 >= $2`, `1 >= 2`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`$1 IS NULL`, `1 IS NULL`, mapArgs{1: DInt(1)}},
		{`$1 IS NOT NULL`, `1 IS NOT NULL`, mapArgs{1: DInt(1)}},
		{`$1 BETWEEN $2 AND $3`, `1 BETWEEN 2 AND 3`, mapArgs{1: DInt(1), 2: DInt(2), 3: DInt(3)}},
		{`$1 NOT BETWEEN $2 AND $3`, `1 NOT BETWEEN 2 AND 3`,
			mapArgs{1: DInt(1), 2: DInt(2), 3: DInt(3)}},
		{`CASE WHEN $1 THEN $2 END`, `CASE WHEN 1 THEN 2 END`, mapArgs{1: DInt(1), 2: DInt(2)}},
		{`CASE WHEN $1 THEN $2 ELSE $3 END`, `CASE WHEN 1 THEN 2 ELSE 3 END`,
			mapArgs{1: DInt(1), 2: DInt(2), 3: DInt(3)}},
		{`CASE $1 WHEN $2 THEN $3 ELSE $4 END`, `CASE 1 WHEN 2 THEN 3 ELSE 4 END`,
			mapArgs{1: DInt(1), 2: DInt(2), 3: DInt(3), 4: DInt(4)}},
		{`($1, $2) = ($3, $4)`, `(1, 2) = (3, 4)`,
			mapArgs{1: DInt(1), 2: DInt(2), 3: DInt(3), 4: DInt(4)}},
		{`$1 IN ($2, $3)`, `1 IN (2, 3)`,
			mapArgs{1: DInt(1), 2: DInt(2), 3: DInt(3)}},
		{`$1 NOT IN ($2, $3)`, `1 NOT IN (2, 3)`,
			mapArgs{1: DInt(1), 2: DInt(2), 3: DInt(3)}},
		{`length($1)`, `length('a')`, mapArgs{1: DString("a")}},
		{`length($1, $2)`, `length('a', 'b')`, mapArgs{1: DString("a"), 2: DString("b")}},
		{`CAST($1 AS INT)`, `CAST(1.1 AS INT)`, mapArgs{1: DFloat(1.1)}},
	}

	for _, d := range testData {
		q, err := Parse("SELECT " + d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if err := FillArgs(q[0], d.args); err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if s := q[0].(*Select).Exprs[0].(*NonStarExpr).Expr.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestFillArgsError(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
		args     mapArgs
	}{
		{`$1`, `arg $1 not found`, mapArgs{}},
		{`$2 AND $1`, `arg $2 not found`, mapArgs{}},
	}
	for _, d := range testData {
		q, err := Parse("SELECT " + d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if err := FillArgs(q[0], d.args); err == nil {
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
		args     mapArgs
	}{
		{`SELECT $1`, `SELECT 'a'`, mapArgs{1: DString(`a`)}},
		{`SELECT $1, $2 FROM db.table WHERE c in ($3, 2 * $4)`,
			`SELECT 'a', 'b' FROM db.table WHERE c in (1.1, 2 * 6.5)`,
			mapArgs{1: DString(`a`), 2: DString(`b`), 3: DFloat(1.1), 4: DFloat(6.5)}},
		{`INSERT INTO db.table (k, v) VALUES (1, 2), ($1, $2)`,
			`INSERT INTO db.table (k, v) VALUES (1, 2), (3, 4)`,
			mapArgs{1: DInt(3), 2: DInt(4)}},
	}
	for _, d := range testData {
		q, err := Parse(d.sql)
		if err != nil {
			t.Fatalf("%s: %v", d.sql, err)
		}
		if err := FillArgs(q[0], d.args); err != nil {
			t.Fatalf("%s: %v", d.sql, err)
		}
		e, err := Parse(d.expected)
		if err != nil {
			t.Fatalf("%s: %v", d.expected, err)
		}
		// Verify that all expressions match up
		if q[0].String() != e[0].String() {
			log.Fatalf("%s not eq expected: %s", q[0].String(), e[0].String())
		}
	}
}
