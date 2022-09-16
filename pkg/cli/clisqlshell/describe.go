// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlshell

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/errors"
)

type dkey struct {
	prefix string
	nargs  int
}

var dcmds = map[dkey]func(bool, bool) string{
	{`l`, 0}:  func(p, s bool) string { return listAllDbs(false, p) },
	{`l`, 1}:  func(p, s bool) string { return listAllDbs(true, p) },
	{`dn`, 0}: func(p, s bool) string { return listSchemas(false, p, s) },
	{`dn`, 1}: func(p, s bool) string { return listSchemas(true, p, s) },
	{`d`, 0}:  func(p, s bool) string { return listTables("tvmsE", false, p, s) },
	{`di`, 0}: func(p, s bool) string { return listTables("i", false, p, s) },
	{`di`, 1}: func(p, s bool) string { return listTables("i", true, p, s) },
	{`dm`, 0}: func(p, s bool) string { return listTables("m", false, p, s) },
	{`dm`, 1}: func(p, s bool) string { return listTables("m", true, p, s) },
	{`dE`, 0}: func(p, s bool) string { return listTables("E", false, p, s) },
	{`dE`, 1}: func(p, s bool) string { return listTables("E", true, p, s) },
	{`ds`, 0}: func(p, s bool) string { return listTables("s", false, p, s) },
	{`ds`, 1}: func(p, s bool) string { return listTables("s", true, p, s) },
	{`dt`, 0}: func(p, s bool) string { return listTables("t", false, p, s) },
	{`dt`, 1}: func(p, s bool) string { return listTables("t", true, p, s) },
	{`dv`, 0}: func(p, s bool) string { return listTables("v", false, p, s) },
	{`dv`, 1}: func(p, s bool) string { return listTables("v", true, p, s) },
	{`dC`, 0}: func(p, s bool) string { return listCasts(false, p) },
	{`dC`, 1}: func(p, s bool) string { return listCasts(true, p) },
	{`dT`, 0}: func(p, s bool) string { return describeTypes(false, p, s) },
	{`dT`, 1}: func(p, s bool) string { return describeTypes(true, p, s) },
	{`dg`, 0}: func(p, s bool) string { return describeRoles(false, p, s) },
	{`dg`, 1}: func(p, s bool) string { return describeRoles(true, p, s) },
	{`du`, 0}: func(p, s bool) string { return describeRoles(false, p, s) },
	{`du`, 1}: func(p, s bool) string { return describeRoles(true, p, s) },
	{`dd`, 0}: func(p, s bool) string { return objectDescription(false, s) },
	{`dd`, 1}: func(p, s bool) string { return objectDescription(true, s) },
}

func pgInspect(
	args []string,
) (
	sql string,
	qargs []interface{},
	foreach func([]string) (string, string, []interface{}),
	err error,
) {
	origCmd := args[0]
	args = args[1:]
	// Strip the leading `\`.
	cmd := origCmd[1:]

	plus := strings.Contains(cmd, "+")
	inclSystem := strings.Contains(cmd, "S")
	// Remove the characters "S" and "+" from the describe command.
	cmd = strings.TrimRight(cmd, "S+")

	for _, a := range args {
		qargs = append(qargs, lexbase.EscapeSQLString(a))
	}

	if cmd == `d` && len(args) == 1 {
		return describeTableDetails(), qargs, describeOneTableDetails(plus), nil
	}

	key := dkey{cmd, len(args)}
	fn := dcmds[key]
	if fn == nil {
		return "", nil, nil, errors.WithHint(
			errors.Newf("unsupported command: %s with %d arguments", origCmd, len(args)),
			"Use the SQL SHOW statement to inspect your schema.")
	}

	return fn(plus, inclSystem), qargs, nil, nil
}

// listAllDbs is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func listAllDbs(hasPattern bool, verbose bool) string {
	var buf strings.Builder

	buf.WriteString(`SELECT d.datname AS "Name",
pg_catalog.pg_get_userbyid(d.datdba) AS "Owner"`)
	if verbose {
		buf.WriteString(`,
pg_catalog.pg_encoding_to_char(d.encoding) AS "Encoding",
d.datcollate AS "Collate",
d.datctype AS "Ctype",`)
		printACLColumn(&buf, "d.datacl")
		buf.WriteString(`,
CASE
WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT')
THEN IF(d.datconnlimit < 0, 'Unlimited', d.datconnlimit::STRING)
ELSE 'No Access'
END AS "Connections",
COALESCE(pg_catalog.shobj_description(d.oid, 'pg_database'), '') AS "Description"`)
	}
	buf.WriteString(`
FROM pg_catalog.pg_database d`)

	if hasPattern {
		buf.WriteString(`
WHERE d.datname LIKE %[1]s`)
	}

	buf.WriteString(`
ORDER BY 1`)

	return buf.String()
}

// listSchemas is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func listSchemas(hasPattern bool, verbose, showSystem bool) string {
	var buf strings.Builder

	buf.WriteString(`SELECT n.nspname AS "Name",
pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"`)
	if verbose {
		buf.WriteByte(',')
		printACLColumn(&buf, "n.nspacl")
		buf.WriteString(`,
COALESCE(pg_catalog.obj_description(n.oid, 'pg_namespace'), '') AS "Description"`)
	}
	buf.WriteString(`
FROM pg_catalog.pg_namespace n`)

	if !showSystem && !hasPattern {
		buf.WriteString(`
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'crdb_internal'
  AND n.nspname <> 'information_schema'`)
	}
	if hasPattern {
		buf.WriteString(`
WHERE n.nspname LIKE %[1]s`)
	}

	buf.WriteString(`
ORDER BY 1`)

	return buf.String()
}

// objectDescription is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func objectDescription(hasPattern bool, showSystem bool) string {
	var buf strings.Builder

	buf.WriteString(`SELECT DISTINCT tt.nspname AS "Schema",
tt.name AS "Name",
tt.object AS "Object",
d.description AS "Description"
FROM (`)

	// Table constraint descriptions.
	buf.WriteString(`
  SELECT pgc.oid as oid, pgc.conrelid AS tableoid,
  n.nspname as nspname,
  pgc.conname::text as name,
  'table constraint'::text as object
FROM pg_catalog.pg_constraint pgc
JOIN pg_catalog.pg_class c ON c.oid = pgc.conrelid
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace`)

	if !showSystem && !hasPattern {
		buf.WriteString(`
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'crdb_internal'
  AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)`)
	}
	if hasPattern {
		buf.WriteString(`
WHERE pgc.conname LIKE %[1]s`)
	}

	// Domain constraint descriptions.
	buf.WriteString(`
UNION ALL
  SELECT pgc.oid as oid, pgc.conrelid AS tableoid,
 n.nspname as nspname,
 pgc.conname::text AS name,
 'domain constraint'::text AS object
  FROM pg_catalog.pg_constraint pgc
  JOIN pg_catalog.pg_type t ON t.oid = pgc.contypid
  LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace`)
	if !showSystem && !hasPattern {
		buf.WriteString(`
  WHERE n.nspname !~ '^pg_'
    AND n.nspname <> 'crdb_internal'
    AND n.nspname <> 'information_schema'
    AND pg_catalog.pg_type_is_visible(t.oid)`)
	}
	if hasPattern {
		buf.WriteString(`
  WHERE pgc.conname LIKE %[1]s`)
	}

	buf.WriteString(`) AS tt
JOIN pg_catalog.pg_description d
  ON (tt.oid = d.objoid AND tt.tableoid = d.classoid AND d.objsubid = 0)
ORDER BY 1,2,3`)

	return buf.String()
}

// listTable is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func listTables(tabTypes string, hasPattern bool, verbose, showSystem bool) string {
	showTables := strings.IndexByte(tabTypes, 't') >= 0
	showIndexes := strings.IndexByte(tabTypes, 'i') >= 0
	showViews := strings.IndexByte(tabTypes, 'v') >= 0
	showMatViews := strings.IndexByte(tabTypes, 'm') >= 0
	showSeq := strings.IndexByte(tabTypes, 's') >= 0
	showForeign := strings.IndexByte(tabTypes, 'E') >= 0

	if !(showTables || showIndexes || showViews || showMatViews || showSeq || showForeign) {
		showTables = true
		showIndexes = true
		showViews = true
		showMatViews = true
		showSeq = true
		showForeign = true
	}

	var buf strings.Builder
	buf.WriteString(`SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind
 WHEN 'r' THEN 'table'
 WHEN 'v' THEN 'view'
 WHEN 'm' THEN 'materialized view'
 WHEN 'i' THEN 'index'
 WHEN 'S' THEN 'sequence'
 WHEN 's' THEN 'special'
 WHEN 't' THEN 'TOAST table'
 WHEN 'f' THEN 'foreign table'
 WHEN 'p' THEN 'partitioned table'
 WHEN 'I' THEN 'partitioned index'
 END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"`)

	if showIndexes {
		buf.WriteString(`,
c2.relname AS "Table"`)
	}

	if verbose {
		buf.WriteString(`,
CASE c.relpersistence
WHEN 'p' THEN 'permanent'
WHEN 't' THEN 'temporary'
WHEN 'u' THEN 'unlogged' END AS "Persistence"`)

		if showTables || showMatViews || showIndexes {
			buf.WriteString(`,
am.amname AS "Access Method"`)
		}

		buf.WriteString(`,
COALESCE(pg_catalog.obj_description(c.oid, 'pg_class'),'') as "Description"`)
	}

	buf.WriteString(`
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n on n.oid = c.relnamespace`)

	if showTables || showMatViews || showIndexes {
		buf.WriteString(`
LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam`)
	}
	if showIndexes {
		buf.WriteString(`
LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid`)
	}

	buf.WriteString(`
WHERE c.relkind IN (`)

	if showTables {
		buf.WriteString(`'r','P',`)
		if showSystem || hasPattern {
			buf.WriteString(`'t',`)
		}
	}

	if showViews {
		buf.WriteString(`'v',`)
	}
	if showMatViews {
		buf.WriteString(`'m',`)
	}
	if showIndexes {
		buf.WriteString(`'i',`)
	}
	if showSeq {
		buf.WriteString(`'S',`)
	}
	if showSystem || hasPattern {
		buf.WriteString(`'s',`)
	}
	if showForeign {
		buf.WriteString(`'f',`)
	}
	buf.WriteString(`''`) // dummy
	buf.WriteString(`)`)

	if !showSystem && !hasPattern {
		buf.WriteString(`
AND n.nspname !~ '^pg_'
AND n.nspname <> 'information_schema'
AND n.nspname <> 'crdb_internal'`)
	}

	if hasPattern {
		// TODO(knz): translate pattern to filter on schema name.
		buf.WriteString(`
AND c.relname LIKE %[1]s`)
	} else {
		// Only show visible tables.
		// NB: we should not need to check relkind here, but
		// pg_table_is_visible in crdb contains a bug.
		buf.WriteString(`
AND (c.relkind = 'i' OR pg_catalog.pg_table_is_visible(c.oid))`)
	}

	buf.WriteString(`
ORDER BY 1,2`)

	return buf.String()
}

// listCasts is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func listCasts(hasPattern bool, verbose bool) string {
	var buf strings.Builder

	buf.WriteString(`SELECT
 pg_catalog.format_type(castsource, NULL) AS "Source type",
 pg_catalog.format_type(casttarget, NULL) AS "Target type",
 CASE WHEN c.castmethod = 'b' THEN '(binary coercible)'
      WHEN c.castmethod = 'i' THEN '(with inout)'
      ELSE p.proname
 END AS "Function",
 CASE WHEN c.castcontext = 'e' THEN 'no'
      WHEN c.castcontext = 'a' THEN 'in assignment'
      ELSE 'yes'
 END AS "Implicit?"`)

	if verbose {
		buf.WriteString(`,
  d.description AS "Description"`)
	}

	/*
	 * We need a left join to pg_proc for binary casts; the others are just
	 * paranoia.
	 */
	buf.WriteString(`
FROM pg_catalog.pg_cast c LEFT JOIN pg_catalog.pg_proc p
   ON c.castfunc = p.oid
   LEFT JOIN pg_catalog.pg_type ts
   ON c.castsource = ts.oid
   LEFT JOIN pg_catalog.pg_namespace ns
   ON ns.oid = ts.typnamespace
   LEFT JOIN pg_catalog.pg_type tt
   ON c.casttarget = tt.oid
   LEFT JOIN pg_catalog.pg_namespace nt
   ON nt.oid = tt.typnamespace`)

	if verbose {
		buf.WriteString(`
      LEFT JOIN pg_catalog.pg_description d
      ON d.classoid = c.tableoid AND d.objoid =  c.oid AND d.objsubid = 0`)
	}

	buf.WriteString(`
WHERE ((true `)

	if hasPattern {
		buf.WriteString(`AND (ts.typname LIKE %[1]s
OR pg_catalog.format_type(ts.oid, NULL) LIKE %[1]s)`)
	} else {
		buf.WriteString(`AND pg_catalog.pg_type_is_visible(ts.oid)`)
	}

	buf.WriteString(`) OR (true `)

	if hasPattern {
		buf.WriteString(`AND (tt.typname LIKE %[1]s
OR pg_catalog.format_type(tt.oid, NULL) LIKE %[1]s)`)
	} else {
		buf.WriteString(`AND pg_catalog.pg_type_is_visible(tt.oid)`)
	}

	buf.WriteString(`))
ORDER BY 1, 2`)

	return buf.String()
}

// describeTypes is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func describeTypes(hasPattern bool, verbose, showSystem bool) string {
	var buf strings.Builder

	buf.WriteString(`SELECT
 n.nspname AS "Schema",
 pg_catalog.format_type(t.oid, NULL) AS "Name",`)

	if verbose {
		buf.WriteString(`
 t.typname AS "Internal name",
 CASE WHEN t.typrelid != 0
     THEN CAST('tuple' AS pg_catalog.text)
   WHEN t.typlen < 0
     THEN CAST('var' AS pg_catalog.text)
   ELSE CAST(t.typlen AS pg_catalog.text)
 END AS "Size",
 pg_catalog.array_to_string(
     ARRAY(
         SELECT e.enumlabel
         FROM pg_catalog.pg_enum e
         WHERE e.enumtypid = t.oid
         ORDER BY e.enumsortorder),
     e'\n') AS "Elements",
  pg_catalog.pg_get_userbyid(t.typowner) AS "Owner",`)
		printACLColumn(&buf, "t.typacl")
		buf.WriteByte(',')
	}

	buf.WriteString(`
 COALESCE(pg_catalog.obj_description(t.oid, 'pg_type'),'') AS "Description"
FROM pg_catalog.pg_type t
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace`)

	// do not include complex types (typrelid!=0) unless they are standalone
	// composite types
	buf.WriteString(`
WHERE (t.typrelid = 0
OR (SELECT c.relkind = 'c'
FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))`)

	// do not include array types unless the pattern contains [].
	buf.WriteString(`
AND (`)
	if hasPattern {
		buf.WriteString(`%[1]s LIKE '%%[]%%' OR `)
	}
	buf.WriteString(`NOT EXISTS(
SELECT 1
FROM pg_catalog.pg_type el
WHERE el.oid = t.typelem AND el.typarray = t.oid))`)

	if !showSystem && !hasPattern {
		// Special CockroachDB affordance: also exclude crdb_internal.
		buf.WriteString(`
AND n.nspname !~ '^pg_'
AND n.nspname <> 'information_schema'
AND n.nspname <> 'crdb_internal'`)
	}

	if hasPattern {
		buf.WriteString(`
AND (t.typname LIKE %[1]s
OR pg_catalog.format_type(t.oid, NULL) LIKE %[1]s)`)
	} else {
		buf.WriteString(`
AND pg_catalog.pg_type_is_visible(t.oid)`)
	}

	buf.WriteString(`
ORDER BY 1, 2`)

	return buf.String()
}

func printACLColumn(buf *strings.Builder, colname string) {
	buf.WriteString(`
COALESCE(pg_catalog.array_to_string(`)
	buf.WriteString(colname)
	buf.WriteString(`, e'\n'), '') AS "Access privileges"`)
}

// describeRoles is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func describeRoles(hasPattern bool, verbose, showSystem bool) string {
	var buf strings.Builder

	buf.WriteString(`WITH roles AS (
SELECT r.rolname, r.rolsuper, r.rolinherit,
 r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,
 r.rolconnlimit, r.rolvaliduntil,
 ARRAY(SELECT b.rolname
       FROM pg_catalog.pg_auth_members m
       JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
       WHERE m.member = r.oid) as memberof`)

	if verbose {
		buf.WriteString(`,
pg_catalog.shobj_description(r.oid, 'pg_authid') AS description`)
	}

	buf.WriteString(`,
 r.rolreplication, r.rolbypassrls
FROM pg_catalog.pg_roles r`)

	if !showSystem && !hasPattern {
		buf.WriteString(`
WHERE r.rolname !~ '^pg_'`)
	}

	if hasPattern {
		buf.WriteString(`
WHERE r.rolname LIKE %[1]s`)
	}

	// Presentation.
	buf.WriteString(`)
SELECT
  rolname AS "Role name",
  array_to_string(ARRAY(
SELECT a FROM (VALUES
 (IF(rolsuper, 'Superuser', NULL)),
 (IF(NOT rolinherit, 'No inheritance', NULL)),
 (IF(rolcreaterole, 'Create role', NULL)),
 (IF(rolcreatedb, 'Create DB', NULL)),
 (IF(NOT rolcanlogin, 'Cannot login', NULL)),
 (IF(rolconnlimit = 0, 'No connections',
IF(rolconnlimit > 0,
   rolconnlimit::STRING || ' connection' || IF(rolconnlimit>1, 's',''),
   NULL))),
 (IF(rolreplication, 'Replication', NULL)),
 (IF(rolbypassrls, 'Bypass RLS', NULL)),
 ('Password valid until ' || rolvaliduntil)
) AS v(a) WHERE v.a IS NOT NULL), ', ') AS "Attributes",
memberof AS "Member of"`)

	if verbose {
		buf.WriteString(`,
COALESCE(description, '') AS "Description"`)
	}

	buf.WriteString(`
		FROM roles`)
	return buf.String()
}

// describeTableDetails is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func describeTableDetails() string {
	var buf strings.Builder

	buf.WriteString(`SELECT c.oid,
  n.nspname,
  c.relname,
  c.relkind,
  c.relpersistence,
  c.relchecks > 0,
  c.relhasindex
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname LIKE %[1]s
ORDER BY 2,3`)

	return buf.String()
}

// describeOneTableDetails is adapted from the function of the same
// name in the PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func describeOneTableDetails(verbose bool) func([]string) (title, sql string, qargs []interface{}) {
	return func(selectedTable []string) (title, sql string, qargs []interface{}) {
		oid := selectedTable[0]
		qargs = []interface{}{oid}
		scName := selectedTable[1]
		tName := selectedTable[2]
		relkind := selectedTable[3]
		relpersistence := selectedTable[4]
		//relhaschecks := selectedTable[5]
		//relhasindex := selectedTable[6]

		var buf strings.Builder

		// Common details.
		buf.WriteString(`WITH obj AS (
SELECT c.oid, c.relchecks, c.relkind, c.relhasindex, c.relhasrules,
c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity,
false AS relhasoids, c.relispartition,
c.reltablespace,
CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END,
c.relpersistence, c.relreplident, am.amname
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
WHERE c.oid = %[1]s)`)

		switch relkind {
		case "S": // Sequence.
			title = fmt.Sprintf(`Sequence "%s.%s"`, scName, tName)
			buf.WriteString(`
SELECT pg_catalog.format_type(seqtypid, NULL) AS "Type",
       seqstart AS "Start",
       seqmin AS "Minimum",
       seqmax AS "Maximum",
       seqincrement AS "Increment",
       CASE WHEN seqcycle THEN 'yes' ELSE 'no' END AS "Cycles?",
       seqcache AS "Cache"
FROM pg_catalog.pg_sequence s
WHERE s.reqrelid = %[1]s`)

		default:
			showColDetails := false
			switch relkind {
			case "r", "v", "m", "f", "c", "p":
				showColDetails = true
			}

			buf.WriteString(`, cols AS (
SELECT a.attname,
 pg_catalog.format_type(a.atttypid, a.atttypmod) AS typname`)

			if showColDetails {
				buf.WriteString(`,
(SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
   FROM pg_catalog.pg_attrdef d
  WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) AS defexpr,
a.attnotnull,
(SELECT c.collname
   FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
  WHERE c.oid = a.attcollation
    AND t.oid = a.atttypid
    AND a.attcollation <> t.typcollation) AS attcollation,
a.attidentity,
a.attgenerated`)
			}

			if relkind == "i" || relkind == "I" {
				// Index.
				buf.WriteString(`,
CASE WHEN a.attnum <= (
  SELECT i.indnkeyatts
    FROM pg_catalog.pg_index i
   WHERE i.indexrelid = %[1]s) THEN 'yes' ELSE 'no' END AS is_key,
pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE) AS indexdef`)
			}

			hasDesc := false
			if verbose {
				switch relkind {
				case "r", "v", "m", "f", "c", "p":
					hasDesc = true
					buf.WriteString(`,
pg_catalog.col_description(a.attrelid, a.attnum) AS description`)
				}
			}

			buf.WriteString(`
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = %[1]s AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum)`)

			// Select title.
			prefix := ""
			switch relkind {
			case "r":
				if relpersistence == "u" {
					prefix = "Unlogged table"
				} else {
					prefix = "Table"
				}
			case "v":
				prefix = "View"
			case "i":
				prefix = "Index"
			case "I":
				if relpersistence == "u" {
					prefix = "Unlogged partitioned index"
				} else {
					prefix = "Partitioned index"
				}
			case "t":
				prefix = "TOAST table"
			case "c":
				prefix = "Composite type"
			case "f":
				prefix = "Foreign table"
			case "p":
				if relpersistence == "u" {
					prefix = "Unlogged partitioned table"
				} else {
					prefix = "Partitioned table"
				}
			default:
				prefix = fmt.Sprintf("?%s?", relkind)
			}
			title = fmt.Sprintf(`%s "%s.%s"`, prefix, scName, tName)

			// Display.
			buf.WriteString(`
SELECT attname AS "Column",
typname AS "Type"`)
			if showColDetails {
				buf.WriteString(`,
COALESCE(attcollation, '') AS "Collation",
IF(attnotnull, 'not null', '') AS "Nullable",
COALESCE(CASE attidentity
WHEN 'a' THEN 'generated always as identity'
WHEN 'd' THEN 'generated by default as identity'
ELSE CASE attgenerated
     WHEN 's' THEN 'generated always as ('||defexpr||') stored'
     ELSE defexpr
     END
END, '') AS "Default"`)
			}
			if relkind == "i" || relkind == "I" {
				// Index.
				buf.WriteString(`,
is_key AS "Key?",
indexdef AS "Definition"`)
			}
			if hasDesc {
				buf.WriteString(`,
COALESCE(description,'') AS "Description"`)
			}

			buf.WriteString(`
FROM cols`)
		}

		return title, buf.String(), qargs
	}
}
