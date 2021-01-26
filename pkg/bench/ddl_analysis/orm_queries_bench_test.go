// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bench

import "testing"

func BenchmarkORMQueries(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			name:  "django table introspection 1 table",
			setup: `CREATE TABLE t1(a int primary key, b int);`,
			stmt: `SELECT
    a.attname AS column_name,
    NOT (a.attnotnull OR ((t.typtype = 'd') AND t.typnotnull)) AS is_nullable,
    pg_get_expr(ad.adbin, ad.adrelid) AS column_default
FROM pg_attribute AS a
LEFT JOIN pg_attrdef AS ad ON (a.attrelid = ad.adrelid) AND (a.attnum = ad.adnum)
JOIN pg_type AS t ON a.atttypid = t.oid JOIN pg_class AS c ON a.attrelid = c.oid
JOIN pg_namespace AS n ON c.relnamespace = n.oid
WHERE (
    (
        (c.relkind IN ('f', 'm', 'p', 'r', 'v')) AND
        (c.relname = '<target table>')
    ) AND (n.nspname NOT IN ('pg_catalog', 'pg_toast'))
) AND pg_table_is_visible(c.oid)`,
		},

		{
			name: "django table introspection 4 tables",
			setup: `CREATE TABLE t1(a int primary key, b int);
CREATE TABLE t2(a int primary key, b int);
CREATE TABLE t3(a int primary key, b int);
CREATE TABLE t4(a int primary key, b int);`,
			stmt: `SELECT
    a.attname AS column_name,
    NOT (a.attnotnull OR ((t.typtype = 'd') AND t.typnotnull)) AS is_nullable,
    pg_get_expr(ad.adbin, ad.adrelid) AS column_default
FROM pg_attribute AS a
LEFT JOIN pg_attrdef AS ad ON (a.attrelid = ad.adrelid) AND (a.attnum = ad.adnum)
JOIN pg_type AS t ON a.atttypid = t.oid JOIN pg_class AS c ON a.attrelid = c.oid
JOIN pg_namespace AS n ON c.relnamespace = n.oid
WHERE (
    (
        (c.relkind IN ('f', 'm', 'p', 'r', 'v')) AND
        (c.relname = '<target table>')
    ) AND (n.nspname NOT IN ('pg_catalog', 'pg_toast'))
) AND pg_table_is_visible(c.oid)`,
		},

		{
			name: "django table introspection 8 tables",
			setup: `CREATE TABLE t1(a int primary key, b int);
CREATE TABLE t2(a int primary key, b int);
CREATE TABLE t3(a int primary key, b int);
CREATE TABLE t4(a int primary key, b int);
CREATE TABLE t5(a int primary key, b int);
CREATE TABLE t6(a int primary key, b int);
CREATE TABLE t7(a int primary key, b int);
CREATE TABLE t8(a int primary key, b int);`,
			stmt: `SELECT
    a.attname AS column_name,
    NOT (a.attnotnull OR ((t.typtype = 'd') AND t.typnotnull)) AS is_nullable,
    pg_get_expr(ad.adbin, ad.adrelid) AS column_default
FROM pg_attribute AS a
LEFT JOIN pg_attrdef AS ad ON (a.attrelid = ad.adrelid) AND (a.attnum = ad.adnum)
JOIN pg_type AS t ON a.atttypid = t.oid JOIN pg_class AS c ON a.attrelid = c.oid
JOIN pg_namespace AS n ON c.relnamespace = n.oid
WHERE (
    (
        (c.relkind IN ('f', 'm', 'p', 'r', 'v')) AND
        (c.relname = '<target table>')
    ) AND (n.nspname NOT IN ('pg_catalog', 'pg_toast'))
) AND pg_table_is_visible(c.oid)`,
		},
	}

	RunRoundTripBenchmark(b, tests)
}
