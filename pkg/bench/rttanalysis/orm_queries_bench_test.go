// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkORMQueries(b *testing.B) { reg.Run(b) }
func init() {
	liquibaseSetup, liquibaseReset := buildNDatabasesWithMTables(15, 40)
	reg.Register("ORMQueries", []RoundTripBenchTestCase{
		{
			Name:  "django column introspection 1 table",
			Setup: buildNTables(1),
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
			Name:  "django column introspection 4 tables",
			Setup: buildNTables(4),
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
			Name:  "django column introspection 8 tables",
			Setup: buildNTables(8),
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
			Name:  "django table introspection 1 table",
			Setup: buildNTables(1),
			Stmt: `SELECT
    c.relname,
    CASE
        WHEN c.relispartition THEN 'p'
        WHEN c.relkind IN ('m', 'v') THEN 'v'
        ELSE 't'
    END,
    obj_description(c.oid)
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('f', 'm', 'p', 'r', 'v')
    AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
    AND pg_catalog.pg_table_is_visible(c.oid)`,
		},

		{
			Name:  "django table introspection 8 tables",
			Setup: buildNTables(8),
			Stmt: `SELECT
    c.relname,
    CASE
        WHEN c.relispartition THEN 'p'
        WHEN c.relkind IN ('m', 'v') THEN 'v'
        ELSE 't'
    END,
    obj_description(c.oid)
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('f', 'm', 'p', 'r', 'v')
    AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
    AND pg_catalog.pg_table_is_visible(c.oid)`,
		},

		{
			Name: "django comment introspection with comments",
			Setup: `CREATE TABLE t1(a int primary key, b int);
CREATE TABLE t2(a int primary key, b int);
CREATE TABLE t3(a int primary key, b int);
COMMENT ON TABLE t1 is 't1';
COMMENT ON TABLE t2 is 't2';
COMMENT ON TABLE t3 is 't1';
`,
			Stmt: `SELECT
                c.relname,
                CASE
                    WHEN c.relispartition THEN 'p'
                    WHEN c.relkind IN ('m', 'v') THEN 'v'
                    ELSE 't'
                END,
                obj_description(c.oid, 'pg_class')
            FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('f', 'm', 'p', 'r', 'v')
                AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
                AND pg_catalog.pg_table_is_visible(c.oid);`,
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
			Name:  "introspection description join",
			Setup: `CREATE TABLE t1(a int primary key, b int);`,
			Stmt: `SELECT
  n.nspname, relname, d.description
FROM
  pg_description AS d
  INNER JOIN pg_class AS c ON d.objoid = c.oid
  INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE
  d.objsubid = 0
  AND n.nspname
    NOT IN (
        'gp_toolkit':::STRING:::NAME,
        'information_schema':::STRING:::NAME,
        'pgagent':::STRING:::NAME,
        'bench':::STRING:::NAME
      )
  AND n.nspname NOT LIKE 'pg_%';`,
		},

		{
			Name:  "has_schema_privilege",
			Setup: `CREATE SCHEMA s`,
			// Force a lease on s.
			SetupEx: []string{"create table s.foo()", "select 1 from s.foo", "drop table s.foo"},
			Stmt:    `SELECT has_schema_privilege('s', 'CREATE')`,
		},

		{
			Name:    "has_sequence_privilege",
			Setup:   `CREATE SEQUENCE seq`,
			SetupEx: []string{`SELECT nextval('seq')`}, // lease seq early so we don't measure the leasing later
			Stmt:    `SELECT has_sequence_privilege('seq', 'SELECT')`,
		},

		{
			Name:    "has_table_privilege",
			Setup:   `CREATE TABLE t(a int primary key, b int); SELECT 1 FROM t;`,
			SetupEx: []string{`SELECT 1 FROM t`}, // Lease t's descriptor.
			Stmt:    `SELECT has_table_privilege('t', 'SELECT')`,
		},

		{
			Name:    "has_column_privilege using attnum",
			Setup:   `CREATE TABLE t(a int primary key, b int)`,
			SetupEx: []string{`SELECT 1 FROM t`}, // lease t early so we don't measure the leasing later
			Stmt:    `SELECT has_column_privilege('t', 1, 'INSERT')`,
		},

		{
			Name:    "has_column_privilege using column name",
			Setup:   `CREATE TABLE t(a int primary key, b int)`,
			SetupEx: []string{`SELECT 1 FROM t`}, // lease t early so we don't measure the leasing later
			Stmt:    `SELECT has_column_privilege('t', 'a', 'INSERT')`,
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
			SetupEx: []string{`select 1 from indexed; select 1 from indexes;`},
			Stmt: `SELECT relname,
	indkey,
	generate_series(1, 4) input,
	information_schema._pg_index_position(indexrelid, generate_series(1, 4))
FROM indexes
ORDER BY relname DESC, input`,
		},

		{
			Name:  "hasura column descriptions",
			Setup: buildNTables(1),
			Stmt: `WITH
  "tabletable" as ( SELECT "table".oid,
           "table".relkind,
           "table".relname AS "table_name",
           "schema".nspname AS "table_schema"
      FROM pg_catalog.pg_class "table"
      JOIN pg_catalog.pg_namespace "schema"
          ON schema.oid = "table".relnamespace
      WHERE "table".relkind IN ('r', 't', 'v', 'm', 'f', 'p')
        AND "schema".nspname NOT LIKE 'pg_%'
        AND "schema".nspname NOT IN ('information_schema', 'hdb_catalog', 'hdb_lib', '_timescaledb_internal', 'crdb_internal')
  )
SELECT
  "table".table_schema,
  "table".table_name,
  coalesce(columns.description, '[]') as columns
FROM "tabletable" "table"

LEFT JOIN LATERAL
  ( SELECT
      pg_catalog.col_description("table".oid, "column".attnum) as description
    FROM pg_catalog.pg_attribute "column"
    WHERE "column".attrelid = "table".oid
  ) columns ON true;`,
		},

		{
			Name:  "hasura column descriptions 8 tables",
			Setup: buildNTables(8),
			Stmt: `WITH
  "tabletable" as ( SELECT "table".oid,
           "table".relkind,
           "table".relname AS "table_name",
           "schema".nspname AS "table_schema"
      FROM pg_catalog.pg_class "table"
      JOIN pg_catalog.pg_namespace "schema"
          ON schema.oid = "table".relnamespace
      WHERE "table".relkind IN ('r', 't', 'v', 'm', 'f', 'p')
        AND "schema".nspname NOT LIKE 'pg_%'
        AND "schema".nspname NOT IN ('information_schema', 'hdb_catalog', 'hdb_lib', '_timescaledb_internal', 'crdb_internal')
  )
SELECT
  "table".table_schema,
  "table".table_name,
  coalesce(columns.description, '[]') as columns
FROM "tabletable" "table"

LEFT JOIN LATERAL
  ( SELECT
      pg_catalog.col_description("table".oid, "column".attnum) as description
    FROM pg_catalog.pg_attribute "column"
    WHERE "column".attrelid = "table".oid
  ) columns ON true;`,
		},

		{
			Name:  "hasura column descriptions modified",
			Setup: "CREATE TABLE t(a INT PRIMARY KEY)",
			Stmt: `WITH
  "tabletable" as ( SELECT "table".oid,
           "table".relkind,
           "table".relname AS "table_name",
           "schema".nspname AS "table_schema"
      FROM pg_catalog.pg_class "table"
      JOIN pg_catalog.pg_namespace "schema"
          ON schema.oid = "table".relnamespace
      WHERE "table".relkind IN ('r', 't', 'v', 'm', 'f', 'p')
        AND "schema".nspname NOT LIKE 'pg_%'
        AND "schema".nspname NOT IN ('information_schema', 'hdb_catalog', 'hdb_lib', '_timescaledb_internal', 'crdb_internal')
  )
SELECT
  "table".table_schema,
  "table".table_name,
  coalesce(columns.description, '[]') as columns
FROM "tabletable" "table"

LEFT JOIN LATERAL
  ( SELECT
      pg_catalog.col_description("column".attrelid, "column".attnum) as description
    FROM pg_catalog.pg_attribute "column"
    WHERE "column".attrelid = "table".oid
  ) columns ON true;`,
		},

		{
			Name:  "column descriptions json agg",
			Setup: "CREATE TABLE t(a INT PRIMARY KEY)",
			Stmt: `SELECT
	jsonb_build_object(
		'oid', "table".oid::INT8,
		'columns', COALESCE(columns.info, '[]')
	)::JSONB AS info
FROM
	pg_catalog.pg_class AS "table"
	JOIN pg_catalog.pg_namespace AS schema ON schema.oid = "table".relnamespace
	-- description
	LEFT JOIN pg_catalog.pg_description AS description ON
			description.classoid = 'pg_catalog.pg_class'::REGCLASS
			AND description.objoid = "table".oid
			AND description.objsubid = 0
	-- columns
	LEFT JOIN LATERAL (
			SELECT
				jsonb_agg(
					jsonb_build_object(
						'description', pg_catalog.col_description("table".oid, "column".attnum)
					)
				)
					AS info
			FROM
				pg_catalog.pg_attribute AS "column"
				LEFT JOIN pg_catalog.pg_type AS type ON type.oid = "column".atttypid
				LEFT JOIN pg_catalog.pg_type AS base_type ON
						type.typtype = 'd' AND base_type.oid = type.typbasetype
			WHERE
				"column".attrelid = "table".oid
				-- columns where attnum <= 0 are special, system-defined columns
				AND "column".attnum > 0
				-- dropped columns still exist in the system catalog as "zombie"columns, so ignore those
				AND NOT "column".attisdropped
		)
			AS columns ON true
WHERE
	"table".relkind IN ('r')`,
		},

		{
			Name:  "prisma column descriptions",
			Setup: buildNTables(20),
			Stmt: `SELECT
  oid.namespace,
  info.table_name,
  info.column_name,
  format_type(att.atttypid, att.atttypmod) AS formatted_type,
  info.numeric_precision,
  info.numeric_scale,
  info.numeric_precision_radix,
  info.datetime_precision,
  info.data_type,
  info.udt_schema AS type_schema_name,
  info.udt_name AS full_data_type,
  pg_get_expr(attdef.adbin, attdef.adrelid) AS column_default,
  info.is_nullable,
  info.is_identity,
  info.character_maximum_length,
  description.description
FROM
  information_schema.columns AS info
  JOIN pg_attribute AS att ON att.attname = info.column_name
  JOIN (
      SELECT
        pg_class.oid, relname, pg_namespace.nspname AS namespace
      FROM
        pg_class
        JOIN pg_namespace ON
            pg_namespace.oid = pg_class.relnamespace AND pg_namespace.nspname = ANY (ARRAY['public'])
    )
      AS oid ON
      oid.oid = att.attrelid AND relname = info.table_name AND namespace = info.table_schema
  LEFT JOIN pg_attrdef AS attdef ON
      attdef.adrelid = att.attrelid AND attdef.adnum = att.attnum AND table_schema = namespace
  LEFT JOIN pg_description AS description ON
      description.objoid = att.attrelid AND description.objsubid = ordinal_position
WHERE
  table_schema = ANY (ARRAY['public']) AND info.is_hidden = 'NO'
ORDER BY
  namespace, table_name, ordinal_position`,
		},
		{
			// The query below was modified to alter the pg_class query to
			// add a filter on reltype > 0, which avoids scanning the secondary
			// partial index on oid for pg_class. Misses on virtual partial indexes
			// force the entire table result set to be re-generated per-row which
			// is super expensive.
			//See: Prisma: https://github.com/prisma/prisma-engines/issues/4250
			Name:  "prisma column descriptions updated",
			Setup: buildNTables(20),
			Stmt: `SELECT
  oid.namespace,
  info.table_name,
  info.column_name,
  format_type(att.atttypid, att.atttypmod) AS formatted_type,
  info.numeric_precision,
  info.numeric_scale,
  info.numeric_precision_radix,
  info.datetime_precision,
  info.data_type,
  info.udt_schema AS type_schema_name,
  info.udt_name AS full_data_type,
  pg_get_expr(attdef.adbin, attdef.adrelid) AS column_default,
  info.is_nullable,
  info.is_identity,
  info.character_maximum_length,
  description.description
FROM
  information_schema.columns AS info
  JOIN pg_attribute AS att ON att.attname = info.column_name
  JOIN (
      SELECT
        pg_class.oid, relname, pg_namespace.nspname AS namespace
      FROM
        pg_class
        JOIN pg_namespace ON
            pg_namespace.oid = pg_class.relnamespace AND pg_namespace.nspname = ANY (ARRAY['public'])
      	WHERE reltype > 0
    )
      AS oid ON
      oid.oid = att.attrelid AND relname = info.table_name AND namespace = info.table_schema
  LEFT JOIN pg_attrdef AS attdef ON
      attdef.adrelid = att.attrelid AND attdef.adnum = att.attnum AND table_schema = namespace
  LEFT JOIN pg_description AS description ON
      description.objoid = att.attrelid AND description.objsubid = ordinal_position
WHERE
  table_schema = ANY (ARRAY['public']) AND info.is_hidden = 'NO'
ORDER BY
  namespace, table_name, ordinal_position`,
		},

		{
			Name: "prisma types 4",
			Setup: func() string {
				const count = 4
				sb := strings.Builder{}
				sb.WriteString(buildNTypes(count))
				sb.WriteString("\n")
				sb.WriteString(buildNTables(count))
				sb.WriteString("\n")
				for i := range count {
					// Indexes also appear in pg_class, so creating some here will
					// make the JOIN do more work.
					sb.WriteString(fmt.Sprintf("CREATE INDEX idx%d ON tab%d (b);\n", i, i))
				}
				return sb.String()
			}(),
			Stmt: `
SELECT
  ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid
FROM
  (
    SELECT
      typ.oid,
      typ.typnamespace,
      typ.typname,
      typ.typtype,
      typ.typrelid,
      typ.typnotnull,
      typ.relkind,
      elemtyp.oid AS elemtypoid,
      elemtyp.typname AS elemtypname,
      elemcls.relkind AS elemrelkind,
      CASE WHEN elemproc.proname = 'array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype
    FROM
      (
        SELECT
          typ.oid,
          typnamespace,
          typname,
          typrelid,
          typnotnull,
          relkind,
          typelem AS elemoid,
          CASE WHEN proc.proname = 'array_recv' THEN 'a' ELSE typ.typtype END AS typtype,
          CASE
          WHEN proc.proname = 'array_recv' THEN typ.typelem
          WHEN typ.typtype = 'r' THEN rngsubtype
          WHEN typ.typtype = 'd' THEN typ.typbasetype
          END
            AS elemtypoid
        FROM
          pg_type AS typ
          LEFT JOIN pg_class AS cls ON cls.oid = typ.typrelid
          LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive
          LEFT JOIN pg_range ON pg_range.rngtypid = typ.oid
      )
        AS typ
      LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid
      LEFT JOIN pg_class AS elemcls ON elemcls.oid = elemtyp.typrelid
      LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive
  )
    AS t
  JOIN pg_namespace AS ns ON ns.oid = typnamespace
WHERE
  typtype IN ('b':::STRING, 'r':::STRING, 'm':::STRING, 'e':::STRING, 'd':::STRING)
  OR (typtype = 'c' AND relkind = 'c')
  OR (typtype = 'p' AND typname IN ('record':::STRING:::NAME, 'void':::STRING:::NAME))
  OR (
      typtype = 'a'
      AND (
          elemtyptype IN ('b':::STRING, 'r':::STRING, 'm':::STRING, 'e':::STRING, 'd':::STRING)
          OR (
              elemtyptype = 'p'
              AND elemtypname IN ('record':::STRING:::NAME, 'void':::STRING:::NAME)
            )
          OR (elemtyptype = 'c' AND elemrelkind = 'c')
        )
    )
ORDER BY
  CASE
  WHEN typtype IN ('b':::STRING, 'e':::STRING, 'p':::STRING) THEN 0
  WHEN typtype = 'r' THEN 1
  WHEN typtype = 'm' THEN 2
  WHEN typtype = 'c' THEN 3
  WHEN typtype = 'd' AND elemtyptype != 'a' THEN 4
  WHEN typtype = 'a' THEN 5
  WHEN typtype = 'd' AND elemtyptype = 'a' THEN 6
  END;
`,
		},

		{
			Name: "prisma types 16",
			Setup: func() string {
				const count = 16
				sb := strings.Builder{}
				sb.WriteString(buildNTypes(count))
				sb.WriteString("\n")
				sb.WriteString(buildNTables(count))
				sb.WriteString("\n")
				for i := range count {
					// Indexes also appear in pg_class, so creating some here will
					// make the JOIN do more work.
					sb.WriteString(fmt.Sprintf("CREATE INDEX idx%d ON tab%d (b);\n", i, i))
				}
				return sb.String()
			}(),
			Stmt: `
SELECT
  ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid
FROM
  (
    SELECT
      typ.oid,
      typ.typnamespace,
      typ.typname,
      typ.typtype,
      typ.typrelid,
      typ.typnotnull,
      typ.relkind,
      elemtyp.oid AS elemtypoid,
      elemtyp.typname AS elemtypname,
      elemcls.relkind AS elemrelkind,
      CASE WHEN elemproc.proname = 'array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype
    FROM
      (
        SELECT
          typ.oid,
          typnamespace,
          typname,
          typrelid,
          typnotnull,
          relkind,
          typelem AS elemoid,
          CASE WHEN proc.proname = 'array_recv' THEN 'a' ELSE typ.typtype END AS typtype,
          CASE
          WHEN proc.proname = 'array_recv' THEN typ.typelem
          WHEN typ.typtype = 'r' THEN rngsubtype
          WHEN typ.typtype = 'd' THEN typ.typbasetype
          END
            AS elemtypoid
        FROM
          pg_type AS typ
          LEFT JOIN pg_class AS cls ON cls.oid = typ.typrelid
          LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive
          LEFT JOIN pg_range ON pg_range.rngtypid = typ.oid
      )
        AS typ
      LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid
      LEFT JOIN pg_class AS elemcls ON elemcls.oid = elemtyp.typrelid
      LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive
  )
    AS t
  JOIN pg_namespace AS ns ON ns.oid = typnamespace
WHERE
  typtype IN ('b':::STRING, 'r':::STRING, 'm':::STRING, 'e':::STRING, 'd':::STRING)
  OR (typtype = 'c' AND relkind = 'c')
  OR (typtype = 'p' AND typname IN ('record':::STRING:::NAME, 'void':::STRING:::NAME))
  OR (
      typtype = 'a'
      AND (
          elemtyptype IN ('b':::STRING, 'r':::STRING, 'm':::STRING, 'e':::STRING, 'd':::STRING)
          OR (
              elemtyptype = 'p'
              AND elemtypname IN ('record':::STRING:::NAME, 'void':::STRING:::NAME)
            )
          OR (elemtyptype = 'c' AND elemrelkind = 'c')
        )
    )
ORDER BY
  CASE
  WHEN typtype IN ('b':::STRING, 'e':::STRING, 'p':::STRING) THEN 0
  WHEN typtype = 'r' THEN 1
  WHEN typtype = 'm' THEN 2
  WHEN typtype = 'c' THEN 3
  WHEN typtype = 'd' AND elemtyptype != 'a' THEN 4
  WHEN typtype = 'a' THEN 5
  WHEN typtype = 'd' AND elemtyptype = 'a' THEN 6
  END;
`,
		},

		{
			Name:  "npgsql types",
			Setup: buildNTables(8),
			Stmt: `SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid
FROM (
    -- Arrays have typtype=b - this subquery identifies them by their typreceive and converts their typtype to a
    -- We first do this for the type (innerest-most subquery), and then for its element type
    -- This also returns the array element, range subtype and domain base type as elemtypoid
    SELECT
        typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind,
        elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,
        CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype
    FROM (
        SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid,
            CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype,
            CASE
                WHEN proc.proname='array_recv' THEN typ.typelem
                WHEN typ.typtype='r' THEN rngsubtype
                WHEN typ.typtype='d' THEN typ.typbasetype
            END AS elemtypoid
        FROM pg_type AS typ
        LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
        LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive
        LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)
    ) AS typ
    LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid
    LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)
    LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive
) AS t
JOIN pg_namespace AS ns ON (ns.oid = typnamespace)
WHERE
    typtype IN ('b', 'r', 'm', 'e', 'd') OR -- Base, range, multirange, enum, domain
    (typtype = 'c' AND relkind='c') OR -- User-defined free-standing composites (not table composites) by default
    (typtype = 'p' AND typname IN ('record', 'void')) OR -- Some special supported pseudo-types
    (typtype = 'a' AND (  -- Array of...
        elemtyptype IN ('b', 'r', 'm', 'e', 'd') OR -- Array of base, range, multirange, enum, domain
        (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR -- Arrays of special supported pseudo-types
        (elemtyptype = 'c' AND elemrelkind='c') -- Array of user-defined free-standing composites (not table composites) by default
    ))
ORDER BY CASE
       WHEN typtype IN ('b', 'e', 'p') THEN 0           -- First base types, enums, pseudo-types
       WHEN typtype = 'r' THEN 1                        -- Ranges after
       WHEN typtype = 'm' THEN 2                        -- Multiranges after
       WHEN typtype = 'c' THEN 3                        -- Composites after
       WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 4 -- Domains over non-arrays after
       WHEN typtype = 'a' THEN 5                        -- Arrays after
       WHEN typtype = 'd' AND elemtyptype = 'a' THEN 6  -- Domains over arrays last
END;
`,
		},

		{
			Name:  "npgsql fields",
			Setup: buildNTables(8),
			Stmt: `SELECT typ.oid, att.attname, att.atttypid
		FROM pg_type AS typ
		JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)
		JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
		JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)
		WHERE
		  (typ.typtype = 'c' AND cls.relkind='c') AND
		  attnum > 0 AND     -- Don't load system attributes
		  NOT attisdropped
		ORDER BY typ.oid, att.attnum;
		`,
		},

		{
			Name:  "asyncpg types",
			Setup: buildNTypes(8),
			Stmt: `
        SELECT
            t.oid                           AS oid,
            ns.nspname                      AS ns,
            t.typname                       AS name,
            t.typtype                       AS kind,
            (CASE WHEN t.typtype = 'd' THEN
                (WITH RECURSIVE typebases(oid, depth) AS (
                    SELECT
                        t2.typbasetype      AS oid,
                        0                   AS depth
                    FROM
                        pg_type t2
                    WHERE
                        t2.oid = t.oid

                    UNION ALL

                    SELECT
                        t2.typbasetype      AS oid,
                        tb.depth + 1        AS depth
                    FROM
                        pg_type t2,
                        typebases tb
                    WHERE
                       tb.oid = t2.oid
                       AND t2.typbasetype != 0
               ) SELECT oid FROM typebases ORDER BY depth DESC LIMIT 1)

               ELSE NULL
            END)                            AS basetype,
            t.typelem                       AS elemtype,
            elem_t.typdelim                 AS elemdelim,
            range_t.rngsubtype              AS range_subtype,
            (CASE WHEN t.typtype = 'c' THEN
                (SELECT
                    array_agg(ia.atttypid ORDER BY ia.attnum)
                FROM
                    pg_attribute ia
                    INNER JOIN pg_class c
                        ON (ia.attrelid = c.oid)
                WHERE
                    ia.attnum > 0 AND NOT ia.attisdropped
                    AND c.reltype = t.oid)

                ELSE NULL
            END)                            AS attrtypoids,
            (CASE WHEN t.typtype = 'c' THEN
                (SELECT
                    array_agg(ia.attname::text ORDER BY ia.attnum)
                FROM
                    pg_attribute ia
                    INNER JOIN pg_class c
                        ON (ia.attrelid = c.oid)
                WHERE
                    ia.attnum > 0 AND NOT ia.attisdropped
                    AND c.reltype = t.oid)

                ELSE NULL
            END)                            AS attrnames
        FROM
            pg_catalog.pg_type AS t
            INNER JOIN pg_catalog.pg_namespace ns ON (
                ns.oid = t.typnamespace)
            LEFT JOIN pg_type elem_t ON (
                t.typlen = -1 AND
                t.typelem != 0 AND
                t.typelem = elem_t.oid
            )
            LEFT JOIN pg_range range_t ON (
                t.oid = range_t.rngtypid
            )`,
		},

		{
			Name:  `liquibase migrations`,
			Setup: buildNTables(40),
			Stmt: `SELECT
  NULL AS table_cat,
  n.nspname AS table_schem,
  c.relname AS table_name,
  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'
  WHEN true
  THEN CASE
  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema'
  THEN CASE c.relkind
  WHEN 'r' THEN 'SYSTEM TABLE'
  WHEN 'v' THEN 'SYSTEM VIEW'
  WHEN 'i' THEN 'SYSTEM INDEX'
  ELSE NULL
  END
  WHEN n.nspname = 'pg_toast'
  THEN CASE c.relkind
  WHEN 'r' THEN 'SYSTEM TOAST TABLE'
  WHEN 'i' THEN 'SYSTEM TOAST INDEX'
  ELSE NULL
  END
  ELSE CASE c.relkind
  WHEN 'r' THEN 'TEMPORARY TABLE'
  WHEN 'p' THEN 'TEMPORARY TABLE'
  WHEN 'i' THEN 'TEMPORARY INDEX'
  WHEN 'S' THEN 'TEMPORARY SEQUENCE'
  WHEN 'v' THEN 'TEMPORARY VIEW'
  ELSE NULL
  END
  END
  WHEN false
  THEN CASE c.relkind
  WHEN 'r' THEN 'TABLE'
  WHEN 'p' THEN 'PARTITIONED TABLE'
  WHEN 'i' THEN 'INDEX'
  WHEN 'P' THEN 'PARTITIONED INDEX'
  WHEN 'S' THEN 'SEQUENCE'
  WHEN 'v' THEN 'VIEW'
  WHEN 'c' THEN 'TYPE'
  WHEN 'f' THEN 'FOREIGN TABLE'
  WHEN 'm' THEN 'MATERIALIZED VIEW'
  ELSE NULL
  END
  ELSE NULL
  END
    AS table_type,
  d.description AS remarks,
  '' AS type_cat,
  '' AS type_schem,
  '' AS type_name,
  '' AS self_referencing_col_name,
  '' AS ref_generation
FROM
  pg_catalog.pg_namespace AS n,
  pg_catalog.pg_class AS c
  LEFT JOIN pg_catalog.pg_description AS d ON
      c.oid = d.objoid AND d.objsubid = 0 AND d.classoid = 'pg_class':::STRING::REGCLASS
WHERE
  c.relnamespace = n.oid
  AND n.nspname LIKE 'reporting'
  AND c.relname LIKE 'databasechangelog'
  AND (
      false
      OR (c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname != 'information_schema')
      OR (c.relkind = 'p' AND n.nspname !~ '^pg_' AND n.nspname != 'information_schema')
    )
ORDER BY
  table_type, table_schem, table_name`,
		},

		{
			Name: `liquibase migrations on multiple dbs`,
			// 15 databases, each with 40 tables.
			Setup: liquibaseSetup,
			Reset: liquibaseReset,
			Stmt: `SELECT
  NULL AS table_cat,
  n.nspname AS table_schem,
  c.relname AS table_name,
  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'
  WHEN true
  THEN CASE
  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema'
  THEN CASE c.relkind
  WHEN 'r' THEN 'SYSTEM TABLE'
  WHEN 'v' THEN 'SYSTEM VIEW'
  WHEN 'i' THEN 'SYSTEM INDEX'
  ELSE NULL
  END
  WHEN n.nspname = 'pg_toast'
  THEN CASE c.relkind
  WHEN 'r' THEN 'SYSTEM TOAST TABLE'
  WHEN 'i' THEN 'SYSTEM TOAST INDEX'
  ELSE NULL
  END
  ELSE CASE c.relkind
  WHEN 'r' THEN 'TEMPORARY TABLE'
  WHEN 'p' THEN 'TEMPORARY TABLE'
  WHEN 'i' THEN 'TEMPORARY INDEX'
  WHEN 'S' THEN 'TEMPORARY SEQUENCE'
  WHEN 'v' THEN 'TEMPORARY VIEW'
  ELSE NULL
  END
  END
  WHEN false
  THEN CASE c.relkind
  WHEN 'r' THEN 'TABLE'
  WHEN 'p' THEN 'PARTITIONED TABLE'
  WHEN 'i' THEN 'INDEX'
  WHEN 'P' THEN 'PARTITIONED INDEX'
  WHEN 'S' THEN 'SEQUENCE'
  WHEN 'v' THEN 'VIEW'
  WHEN 'c' THEN 'TYPE'
  WHEN 'f' THEN 'FOREIGN TABLE'
  WHEN 'm' THEN 'MATERIALIZED VIEW'
  ELSE NULL
  END
  ELSE NULL
  END
    AS table_type,
  d.description AS remarks,
  '' AS type_cat,
  '' AS type_schem,
  '' AS type_name,
  '' AS self_referencing_col_name,
  '' AS ref_generation
FROM
  pg_catalog.pg_namespace AS n,
  pg_catalog.pg_class AS c
  LEFT JOIN pg_catalog.pg_description AS d ON
      c.oid = d.objoid AND d.objsubid = 0 AND d.classoid = 'pg_class':::STRING::REGCLASS
WHERE
  c.relnamespace = n.oid
  AND n.nspname LIKE 'reporting'
  AND c.relname LIKE 'databasechangelog'
  AND (
      false
      OR (c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname != 'information_schema')
      OR (c.relkind = 'p' AND n.nspname !~ '^pg_' AND n.nspname != 'information_schema')
    )
ORDER BY
  table_type, table_schem, table_name`,
		},
	})
}

func buildNTables(n int) string {
	b := strings.Builder{}
	for i := 0; i < n; i++ {
		b.WriteString(fmt.Sprintf("CREATE TABLE tab%d(a int primary key, b int);\n", i))
	}
	return b.String()
}

func buildNTypes(n int) string {
	b := strings.Builder{}
	for i := 0; i < n; i++ {
		b.WriteString(fmt.Sprintf("CREATE TYPE typ%d AS (a int, b int);\n", i))
	}
	return b.String()
}

func buildNDatabasesWithMTables(amtDbs int, amtTbls int) (string, string) {
	b := strings.Builder{}
	reset := strings.Builder{}
	tbls := buildNTables(amtTbls)
	for i := 0; i < amtDbs; i++ {
		db := fmt.Sprintf("d%d", i)
		b.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;\n", db))
		reset.WriteString(fmt.Sprintf("DROP DATABASE %s;\n", db))
		b.WriteString(fmt.Sprintf("USE %s;\n", db))
		b.WriteString(tbls)
	}
	return b.String(), reset.String()
}
