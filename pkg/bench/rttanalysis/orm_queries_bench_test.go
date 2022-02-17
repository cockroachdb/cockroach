// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rttanalysis

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkORMQueries(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("ORMQueries", []RoundTripBenchTestCase{
		{
			Name:  "django table introspection 1 table",
			Setup: `CREATE TABLE t1(a int primary key, b int);`,
			Stmt: `SELECT
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
			Name: "django table introspection 4 tables",
			Setup: `CREATE TABLE t1(a int primary key, b int);
CREATE TABLE t2(a int primary key, b int);
CREATE TABLE t3(a int primary key, b int);
CREATE TABLE t4(a int primary key, b int);`,
			Stmt: `SELECT
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
			Name: "django table introspection 8 tables",
			Setup: `CREATE TABLE t1(a int primary key, b int);
CREATE TABLE t2(a int primary key, b int);
CREATE TABLE t3(a int primary key, b int);
CREATE TABLE t4(a int primary key, b int);
CREATE TABLE t5(a int primary key, b int);
CREATE TABLE t6(a int primary key, b int);
CREATE TABLE t7(a int primary key, b int);
CREATE TABLE t8(a int primary key, b int);`,
			Stmt: `SELECT
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
			Name: "activerecord type introspection query",
			Stmt: `SELECT
  t.oid, t.typname, t.typelem, t.typdelim, t.typinput, r.rngsubtype, t.typtype, t.typbasetype
FROM
  pg_type AS t LEFT JOIN pg_range AS r ON oid = rngtypid
WHERE
  t.typname
  IN (
      'int2',
      'int4',
      'int8',
      'oid',
      'float4',
      'float8',
      'text',
      'varchar',
      'char',
      'Name',
      'bpchar',
      'bool',
      'bit',
      'varbit',
      'timestamptz',
      'date',
      'money',
      'bytea',
      'point',
      'hstore',
      'json',
      'jsonb',
      'cidr',
      'inet',
      'uuid',
      'xml',
      'tsvector',
      'macaddr',
      'citext',
      'ltree',
      'line',
      'lseg',
      'box',
      'path',
      'polygon',
      'circle',
      'interval',
      'time',
      'timestamp',
      'numeric'
    )
  OR t.typtype IN ('r', 'e', 'd')
  OR t.typinput = 'array_in(cstring,oid,integer)'::REGPROCEDURE
  OR t.typelem != 0`,
		},

		{
			Name:  "pg_type",
			Setup: `CREATE TABLE t1(a int primary key, b int);`,
			Stmt:  `SELECT * FROM pg_type`,
		},

		{
			Name:  "pg_class",
			Setup: `CREATE TABLE t1(a int primary key, b int);`,
			Stmt:  `SELECT * FROM pg_class`,
		},

		{
			Name:  "pg_namespace",
			Setup: `CREATE TABLE t1(a int primary key, b int);`,
			Stmt:  `SELECT * FROM pg_namespace`,
		},

		{
			Name:  "pg_attribute",
			Setup: `CREATE TABLE t1(a int primary key, b int);`,
			Stmt:  `SELECT * FROM pg_attribute`,
		},

		{
			Name:  "has_schema_privilege 1",
			Setup: `CREATE SCHEMA s`,
			Stmt:  `SELECT has_schema_privilege('s', 'CREATE')`,
		},

		{
			Name:  "has_schema_privilege 3",
			Setup: repeat("CREATE SCHEMA s%d_3", 3, "; "),
			Stmt:  "SELECT " + repeat("has_schema_privilege('s%d_3', 'CREATE')", 3, ", "),
		},

		{
			Name:  "has_schema_privilege 5",
			Setup: repeat("CREATE SCHEMA s%d_5", 5, "; "),
			Stmt:  "SELECT " + repeat("has_schema_privilege('s%d_5', 'CREATE')", 5, ", "),
		},

		{
			Name:  "has_sequence_privilege 1",
			Setup: `CREATE SEQUENCE seq`,
			Stmt:  `SELECT has_sequence_privilege('seq', 'SELECT')`,
		},

		{
			Name:  "has_sequence_privilege 3",
			Setup: repeat("CREATE SEQUENCE seq%d_3", 3, "; "),
			Stmt:  "SELECT " + repeat("has_sequence_privilege('seq%d_3', 'SELECT')", 3, ", "),
		},

		{
			Name:  "has_sequence_privilege 5",
			Setup: repeat("CREATE SEQUENCE seq%d_5", 5, ";"),
			Stmt:  "SELECT " + repeat("has_sequence_privilege('seq%d_5', 'SELECT')", 5, ", "),
		},

		{
			Name:  "has_table_privilege 1",
			Setup: `CREATE TABLE t(a int primary key, b int)`,
			Stmt:  `SELECT has_table_privilege('t', 'SELECT')`,
		},

		{
			Name:  "has_table_privilege 3",
			Setup: repeat("CREATE TABLE t%d_3(a int primary key, b int)", 3, "; "),
			Stmt:  "SELECT " + repeat("has_table_privilege('t%d_3', 'SELECT')", 3, ", "),
		},

		{
			Name:  "has_table_privilege 5",
			Setup: repeat("CREATE TABLE t%d_5(a int primary key, b int)", 5, "; "),
			Stmt:  "SELECT " + repeat("has_table_privilege('t%d_5', 'SELECT')", 5, ", "),
		},

		{
			Name:  "has_column_privilege using attnum",
			Setup: `CREATE TABLE t(a int primary key, b int)`,
			Stmt:  `SELECT has_column_privilege('t', 1, 'INSERT')`,
		},

		{
			Name:  "has_column_privilege using column name",
			Setup: `CREATE TABLE t(a int primary key, b int)`,
			Stmt:  `SELECT has_column_privilege('t', 'a', 'INSERT')`,
		},

		{
			Name: "pg_my_temp_schema",
			Setup: `SET experimental_enable_temp_tables = true;
              CREATE TEMP TABLE t(a int primary key, b int)`,
			Stmt: `SELECT pg_my_temp_schema()`,
		},

		{
			Name: "pg_my_temp_schema multiple times",
			Setup: `SET experimental_enable_temp_tables = true;
              CREATE TEMP TABLE t(a int primary key, b int)`,
			Stmt: `SELECT pg_my_temp_schema() FROM generate_series(1, 10)`,
		},

		{
			Name: "pg_is_other_temp_schema",
			Setup: `SET experimental_enable_temp_tables = true;
              CREATE TEMP TABLE t(a int primary key, b int)`,
			Stmt: `SELECT nspname, pg_is_other_temp_schema(oid) FROM
               (SELECT * FROM pg_namespace WHERE nspname = 'public') n`,
		},

		{
			Name: "pg_is_other_temp_schema multiple times",
			Setup: `SET experimental_enable_temp_tables = true;
              CREATE TEMP TABLE t(a int primary key, b int)`,
			Stmt: `SELECT nspname, pg_is_other_temp_schema(oid) FROM
               (SELECT * FROM pg_namespace LIMIT 5) n`,
		},

		{
			Name: "information_schema._pg_index_position",
			Setup: `CREATE TABLE indexed (
  a INT PRIMARY KEY,
  b INT,
  c INT,
  d INT,
  INDEX (b, d),
  INDEX (c, a)
);
CREATE VIEW indexes AS
  SELECT i.relname, indkey::INT2[], indexrelid
    FROM pg_catalog.pg_index
    JOIN pg_catalog.pg_class AS t ON indrelid   = t.oid
    JOIN pg_catalog.pg_class AS i ON indexrelid = i.oid
   WHERE t.relname = 'indexed'
ORDER BY i.relname`,
			Stmt: `SELECT relname,
	indkey,
	generate_series(1, 4) input,
	information_schema._pg_index_position(indexrelid, generate_series(1, 4))
FROM indexes
ORDER BY relname DESC, input`,
		},
	})
}

func repeat(format string, times int, sep string) string {
	formattedStrings := make([]string, times)
	for i := 0; i < times; i++ {
		formattedStrings[i] = fmt.Sprintf(format, i)
	}
	return strings.Join(formattedStrings, sep)
}
