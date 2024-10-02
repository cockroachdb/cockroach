// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlshell

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/errors"
)

var fnDescribeCmdRe = regexp.MustCompile(`^df[anptw]*$`)
var tbDescribeCmdRe = regexp.MustCompile(`^d[tivmsE]*$`)

// describeStage corresponds to the production of one output tables
// during the execution of a describe command. Each stage has a
// title, and a SQL statement with a number of positional arguments.
type describeStage struct {
	title string
	sql   string
	qargs []interface{}
}

func pgInspect(
	args []string,
) (title, sql string, qargs []interface{}, foreach func([]string) []describeStage, err error) {
	origCmd := args[0]
	args = args[1:]
	// Strip the leading `\`.
	cmd := origCmd[1:]

	plus := strings.Contains(cmd, "+")
	inclSystem := strings.Contains(cmd, "S")
	// Remove the characters "S" and "+" from the describe command.
	cmd = strings.TrimRight(cmd, "S+")

	var hasPattern bool
	switch len(args) {
	case 0:
		// OK
	case 1:
		hasPattern = true
		qargs = []interface{}{lexbase.EscapeSQLString(args[0])}
	default:
		return "", "", nil, nil, errors.WithHint(
			errors.Newf("unsupported command: %s with %d arguments", origCmd, len(args)),
			"Use the SQL SHOW statement to inspect your schema.")
	}

	if cmd == `d` && hasPattern {
		return "", describeTableDetails(), qargs, describeOneTableDetails(plus), nil
	}

	switch {
	case cmd == "l":
		title, sql = listAllDbs(hasPattern, plus)
	case cmd == "dn":
		title, sql = listSchemas(hasPattern, plus, inclSystem)
	case cmd == "dC":
		title, sql = listCasts(hasPattern, plus)
	case cmd == "dT":
		title, sql = describeTypes(hasPattern, plus, inclSystem)
	case cmd == "dd":
		title, sql = objectDescription(hasPattern, inclSystem)
	case cmd == "dg" || cmd == "du":
		title, sql = describeRoles(hasPattern, plus, inclSystem)
	case fnDescribeCmdRe.MatchString(cmd):
		flags := strings.TrimPrefix(cmd, "df")
		title, sql = describeFunctions(flags, hasPattern, plus, inclSystem)
	case tbDescribeCmdRe.MatchString(cmd):
		flags := strings.TrimPrefix(cmd, "d")
		title, sql = listTables(flags, hasPattern, plus, inclSystem)
	default:
		return "", "", nil, nil, errors.WithHint(
			errors.Newf("unsupported command: %s with %d arguments", origCmd, len(args)),
			"Use the SQL SHOW statement to inspect your schema.")
	}

	return title, sql, qargs, nil, nil
}

// listAllDbs is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func listAllDbs(hasPattern bool, verbose bool) (string, string) {
	var buf strings.Builder

	buf.WriteString(`SELECT d.datname AS "Name",
       pg_catalog.pg_get_userbyid(d.datdba) AS "Owner",
       pg_catalog.pg_encoding_to_char(d.encoding) AS "Encoding",
       d.datcollate AS "Collate",
       d.datctype AS "Ctype",`)
	// TODO(sql-sessions): "ICU Locale" and "Locale Provider"
	// are omitted because we don't have custom locales in CockroachDB yet.
	printACLColumn(&buf, "d.datacl")
	if verbose {
		// TODO(sql-sessions): "Tablespace" is omitted.
		// TODO(sql-sessions): "Size" is omited.
		// (pg_database_size is not yet supported.)
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

	return "List of databases", buf.String()
}

// listSchemas is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func listSchemas(hasPattern bool, verbose, showSystem bool) (string, string) {
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
  FROM pg_catalog.pg_namespace n
 WHERE TRUE`)

	if !showSystem && !hasPattern {
		buf.WriteString(`
   AND n.nspname !~ '^pg_'
   AND n.nspname <> 'crdb_internal'
   AND n.nspname <> 'information_schema'`)
	}
	if hasPattern {
		buf.WriteString(` AND n.nspname LIKE %[1]s`)
	}

	buf.WriteString(`
ORDER BY 1`)

	return "List of schemas", buf.String()
}

// objectDescription is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func objectDescription(hasPattern bool, showSystem bool) (string, string) {
	var buf strings.Builder

	buf.WriteString(`SELECT DISTINCT
       tt.nspname AS "Schema",
       tt.name AS "Name",
       tt.object AS "Object",
       d.description AS "Description"
  FROM (`)

	// Table constraint descriptions.
	buf.WriteString(`
    SELECT pgc.oid as oid, pgc.conrelid AS tableoid,
           n.nspname as nspname,
           CAST(pgc.conname AS pg_catalog.text) as name,
           CAST('table constraint' AS pg_catalog.text) as object
      FROM pg_catalog.pg_constraint pgc
      JOIN pg_catalog.pg_class c ON c.oid = pgc.conrelid
 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
 WHERE TRUE`)
	if !showSystem && !hasPattern {
		buf.WriteString(`
       AND n.nspname !~ '^pg_'
       AND n.nspname <> 'crdb_internal'
       AND n.nspname <> 'information_schema'`)
	}
	if hasPattern {
		buf.WriteString(` AND pgc.conname LIKE %[1]s`)
	} else {
		buf.WriteString(` AND pg_catalog.pg_table_is_visible(c.oid)`)
	}

	// Domain constraint descriptions.
	buf.WriteString(`
UNION ALL
    SELECT pgc.oid as oid, pgc.conrelid AS tableoid,
           n.nspname as nspname,
           CAST(pgc.conname AS pg_catalog.text) AS name,
           CAST('domain constraint' AS pg_catalog.text) AS object
      FROM pg_catalog.pg_constraint pgc
      JOIN pg_catalog.pg_type t ON t.oid = pgc.contypid
 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
 WHERE TRUE`)
	if !showSystem && !hasPattern {
		buf.WriteString(`
       AND n.nspname !~ '^pg_'
       AND n.nspname <> 'crdb_internal'
       AND n.nspname <> 'information_schema'`)
	}
	if hasPattern {
		buf.WriteString(` AND pgc.conname LIKE %[1]s`)
	} else {
		buf.WriteString(` AND pg_catalog.pg_type_is_visible(t.oid)`)
	}

	// TODO(sql-sessions): The operator class descriptions have been
	// omitted here. (pg_opclass is not supported)
	// TODO(sql-sessions): The operator family descriptions have been
	// omitted here. (pg_opfamily is not supported)
	// TODO(sql-sessions): Rewrite rules for view have been omitted
	// here. (pg_rewrite is not supported)

	buf.WriteString(`) AS tt
  JOIN pg_catalog.pg_description d
    ON (tt.oid = d.objoid AND tt.tableoid = d.classoid AND d.objsubid = 0)
ORDER BY 1,2,3`)

	return "Object descriptions", buf.String()
}

// describeFunctions is adapted from the function fo the same name
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func describeFunctions(
	funcTypes string, hasPattern bool, verbose, showSystem bool,
) (string, string) {
	showAggregate := strings.IndexByte(funcTypes, 'a') >= 0
	showNormal := strings.IndexByte(funcTypes, 'n') >= 0
	showProcedure := strings.IndexByte(funcTypes, 'p') >= 0
	showTrigger := strings.IndexByte(funcTypes, 't') >= 0
	showWindow := strings.IndexByte(funcTypes, 'w') >= 0

	if !(showAggregate || showNormal || showProcedure || showTrigger || showWindow) {
		showAggregate = true
		showNormal = true
		showProcedure = true
		showTrigger = true
		showWindow = true
	}

	var buf strings.Builder
	buf.WriteString(`   SELECT n.nspname AS "Schema",
         p.proname AS "Name",
         pg_catalog.pg_get_function_result(p.oid) AS "Result data type",
         pg_catalog.pg_get_function_arguments(p.oid) AS "Argument data types",
         CASE p.prokind
         WHEN 'a' THEN 'agg'
         WHEN 'w' THEN 'window'
         WHEN 'p' THEN 'proc'
         ELSE 'func'
         END AS "Type"`)
	if verbose {
		buf.WriteString(`, CASE p.provolatile
        WHEN 'i' THEN 'immutable'
        WHEN 's' THEN 'stable'
        WHEN 'v' THEN 'volatile'
        ELSE p.provolatile
        END AS "Volatility",`)
		// TODO(sql-sessions): Column "Parallel" omitted.
		// (pg_proc.proparallel is not supported)
		buf.WriteString(`
        pg_catalog.pg_get_userbyid(p.proowner) AS "Owner",
        CASE WHEN p.prosecdef THEN 'definer' ELSE 'invoker' END AS "Security",`)
		printACLColumn(&buf, "p.proacl")
		// TODO(sql-sessions): Column "Language" omitted.
		// (pg_language is not supported)
		//
		// TODO(sql-sessions): pg_get_function_sqlbody is not called here
		// because it is not supported.
		//
		// TODO(sql-sessions): The "Description" column is currently
		// ineffective for UDFs because of
		// https://github.com/cockroachdb/cockroach/issues/44135
		buf.WriteString(`,
       p.prosrc AS "Source code",
       pg_catalog.obj_description(p.oid, 'pg_proc') AS "Description"`)
	}
	buf.WriteString(`
     FROM pg_catalog.pg_proc p
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
    WHERE TRUE `)
	// TODO(sql-sessions): Filtering based on argument types like
	// in PostgreSQL.
	// TODO(sql-sessions): join on pg_language when verbose; pg_language
	// is not supported.
	if showNormal && showAggregate && showProcedure && showTrigger && showWindow {
		// Do noting.
	} else if showNormal {
		if !showAggregate {
			buf.WriteString(` AND p.prokind != 'a'`)
		}
		if !showProcedure {
			buf.WriteString(` AND (p.prokind IS NULL OR p.prokind <> 'p')`)
		}
		if !showTrigger {
			// TODO(sql-session): Use prorettype like in PostgreSQL here.
			_ = 0 // disable lint SA9003
		}
		if !showWindow {
			buf.WriteString(` AND p.prokind != 'w'`)
		}
	} else {
		buf.WriteString(` AND (FALSE`)
		// Note: at least one of these must be true.
		if showAggregate {
			buf.WriteString(` OR p.prokind = 'a'`)
		}
		if showTrigger {
			// TODO(sql-sessions): Use prorettype here.
			_ = 0 // disable lint SA9003
		}
		if showProcedure {
			buf.WriteString(` OR (p.prokind IS NOT NULL AND p.prokind = 'p')`)
		}
		if showWindow {
			buf.WriteString(` OR p.prokind = 'w'`)
		}
		buf.WriteByte(')')
	}

	if !showSystem && !hasPattern {
		buf.WriteString(`
      AND n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname <> 'crdb_internal'`)
	}

	if hasPattern {
		// TODO(knz): translate pattern to filter on schema name.
		buf.WriteString(` AND p.proname LIKE %[1]s`)
		// TODO(sql-sessions): Filter by argument types.
	} else {
		// Only show visible functions.
		buf.WriteString(`
      AND pg_catalog.pg_function_is_visible(p.oid)`)
	}

	buf.WriteString(` ORDER BY 1, 2, 4`)

	return "List of functions", buf.String()
}

// listTables is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func listTables(tabTypes string, hasPattern bool, verbose, showSystem bool) (string, string) {
	showTables := strings.IndexByte(tabTypes, 't') >= 0
	showIndexes := strings.IndexByte(tabTypes, 'i') >= 0
	showViews := strings.IndexByte(tabTypes, 'v') >= 0
	showMatViews := strings.IndexByte(tabTypes, 'm') >= 0
	showSeq := strings.IndexByte(tabTypes, 's') >= 0
	showForeign := strings.IndexByte(tabTypes, 'E') >= 0

	if !(showTables || showIndexes || showViews || showMatViews || showSeq || showForeign) {
		showTables = true
		showViews = true
		showMatViews = true
		showSeq = true
		showForeign = true
	}

	var buf strings.Builder
	buf.WriteString(`   SELECT n.nspname as "Schema",
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

		// TODO(sql-sessions): Column "Size" omitted here.
		// This is because pg_table_size() is not supported yet.
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
		buf.WriteString(`'r','p',`)
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
		buf.WriteString(`
      AND pg_catalog.pg_table_is_visible(c.oid)`)
	}

	buf.WriteString(`
 ORDER BY 1,2`)

	return "List of relations", buf.String()
}

// listCasts is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func listCasts(hasPattern bool, verbose bool) (string, string) {
	var buf strings.Builder

	buf.WriteString(`   SELECT pg_catalog.format_type(castsource, NULL) AS "Source type",
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
     FROM pg_catalog.pg_cast c
LEFT JOIN pg_catalog.pg_proc p       ON c.castfunc = p.oid
LEFT JOIN pg_catalog.pg_type ts      ON c.castsource = ts.oid
LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid = ts.typnamespace
LEFT JOIN pg_catalog.pg_type tt      ON c.casttarget = tt.oid
LEFT JOIN pg_catalog.pg_namespace nt ON nt.oid = tt.typnamespace`)

	if verbose {
		buf.WriteString(`
LEFT JOIN pg_catalog.pg_description d ON d.classoid = c.tableoid AND d.objoid = c.oid AND d.objsubid = 0`)
	}

	buf.WriteString(`
    WHERE ((true`)

	if hasPattern {
		buf.WriteString(`
          AND (ts.typname LIKE %[1]s
           OR pg_catalog.format_type(ts.oid, NULL) LIKE %[1]s)`)
	} else {
		buf.WriteString(`
          AND pg_catalog.pg_type_is_visible(ts.oid)`)
	}

	buf.WriteString(`)
       OR (true`)

	if hasPattern {
		buf.WriteString(`
          AND (tt.typname LIKE %[1]s
           OR pg_catalog.format_type(tt.oid, NULL) LIKE %[1]s)`)
	} else {
		buf.WriteString(`
          AND pg_catalog.pg_type_is_visible(tt.oid)`)
	}

	buf.WriteString(`))
ORDER BY 1, 2`)

	return "List of casts", buf.String()
}

// describeTypes is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func describeTypes(hasPattern bool, verbose, showSystem bool) (string, string) {
	var buf strings.Builder

	buf.WriteString(`   SELECT n.nspname AS "Schema",
          pg_catalog.format_type(t.oid, NULL) AS "Name",`)

	if verbose {
		buf.WriteString(`
          t.typname AS "Internal name",
          CASE
          WHEN t.typrelid != 0 THEN CAST('tuple' AS pg_catalog.text)
          WHEN t.typlen < 0    THEN CAST('var' AS pg_catalog.text)
          ELSE CAST(t.typlen AS pg_catalog.text)
          END AS "Size",
          pg_catalog.array_to_string(
              ARRAY(
                  SELECT e.enumlabel
                    FROM pg_catalog.pg_enum e
                   WHERE e.enumtypid = t.oid
                ORDER BY e.enumsortorder
              ), e'\n') AS "Elements",
           pg_catalog.pg_get_userbyid(t.typowner) AS "Owner",`)
		printACLColumn(&buf, "t.typacl")
		buf.WriteByte(',')
	}

	buf.WriteString(`
          COALESCE(pg_catalog.obj_description(t.oid, 'pg_type'),'') AS "Description"
     FROM pg_catalog.pg_type t
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace`)

	// Do not include complex types (typrelid!=0) unless they are standalone
	// composite types.
	buf.WriteString(`
    WHERE (t.typrelid = 0
          OR (SELECT c.relkind = 'c'
                FROM pg_catalog.pg_class c
               WHERE c.oid = t.typrelid))`)

	// Do not include array types unless the pattern contains [].
	// The original source code is:
	// if (pattern == NULL || strstr(pattern, "[]") == NULL)
	//    ... avoid array types using NOT EXISTS ...
	//
	// Alhough we have an equivalent of "pattern == NULL" here, we
	// cannot evaluate the pattern here for "[]": it will only be
	// provided later during query expansion in the caller. So what we
	// do instead is evaluate it inside SQL.
	//
	// For this, we transform the original C code to predicate logic:
	// P:  pattern != NULL
	// TA: pattern contains "[]"
	// X:  type is an array (EXISTS)
	//
	// The expression from the original source code, expressed in
	// predicate logic, is: IF ((!P) OR (!TA)) THEN (!X)
	// Boolean formula for "IF A THEN B" is ((!A) OR B)
	//
	// So the above is equivalent to:
	//    !((!P) OR (!TA)) OR (!X)
	// which is:
	//    (P AND TA) OR (!X)
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

	return "List of data types", buf.String()
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
func describeRoles(hasPattern bool, verbose, showSystem bool) (string, string) {
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
	} else if hasPattern {
		buf.WriteString(`
 WHERE r.rolname LIKE %[1]s`)
	}

	// Presentation.
	buf.WriteString(`)
SELECT rolname AS "Role name",
       array_to_string(ARRAY(
         SELECT a FROM (VALUES
          (IF(rolsuper,        'Superuser', NULL)),
          (IF(NOT rolinherit,  'No inheritance', NULL)),
          (IF(rolcreaterole,   'Create role', NULL)),
          (IF(rolcreatedb,     'Create DB', NULL)),
          (IF(NOT rolcanlogin, 'Cannot login', NULL)),
          (IF(rolconnlimit = 0,
              'No connections',
              IF(rolconnlimit > 0,
                 rolconnlimit::STRING || ' connection' || IF(rolconnlimit>1, 's',''),
                 NULL))),
          (IF(rolreplication,  'Replication', NULL)),
          (IF(rolbypassrls,    'Bypass RLS', NULL)),
          ('Password valid until ' || rolvaliduntil)
         ) AS v(a) WHERE v.a IS NOT NULL),
         ', ') AS "Attributes",
       memberof AS "Member of"`)

	if verbose {
		buf.WriteString(`,
       COALESCE(description, '') AS "Description"`)
	}

	buf.WriteString(`
  FROM roles`)
	return "List of roles", buf.String()
}

// describeTableDetails is adapted from the function of the same name in the
// PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func describeTableDetails() string {
	var buf strings.Builder

	// Note: we are pre-computing all the attributes from pg_class
	// here that we will need in describeOneTableDetails.
	buf.WriteString(`   SELECT c.oid,
          n.nspname,
          c.relname,
          c.relkind,
          c.relpersistence,
          c.relchecks > 0,
          c.relhasindex,
          EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid = c.oid AND contype = 'f') AS relhasfkey,
          EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE confrelid = c.oid AND contype = 'f') AS relhasifkey,
          EXISTS(SELECT 1 FROM pg_catalog.pg_statistic_ext WHERE stxrelid = c.oid)
     FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname LIKE %[1]s
 ORDER BY 2,3`)

	return buf.String()
}

// describeOneTableDetails is adapted from the function of the same
// name in the PostgreSQL sources, file src/bin/psql/describe.c.
// Please keep them in sync.
func describeOneTableDetails(verbose bool) func([]string) (extraStages []describeStage) {
	return func(selectedTable []string) (extraStages []describeStage) {
		oid := selectedTable[0]
		scName := selectedTable[1]
		tName := selectedTable[2]
		relkind := selectedTable[3]
		relpersistence := selectedTable[4]
		relhaschecks := selectedTable[5]
		relhasindex := selectedTable[6]
		relhasfkey := selectedTable[7]
		relhasifkey := selectedTable[8]
		relhasstats := selectedTable[9]

		var buf strings.Builder

		var title string
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

			// TODO(sql-sessions): The column that owns this sequence
			// is omitted here.

		default:
			showColDetails := false
			switch relkind {
			case "r", "v", "m", "f", "c", "p":
				showColDetails = true
			}

			buf.WriteString(`WITH cols AS (
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
       COALESCE(
         CASE attidentity
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

		// Assemble the display stages. The first stage is the basic
		// information about the table, using the SQL query generated
		// above.
		// What follows is the footers.
		firstStage := describeStage{
			title: title,
			sql:   buf.String(),
			qargs: []interface{}{oid},
		}
		extraStages = append(extraStages, firstStage)

		switch relkind {
		case "i", "I":
			// Footer information about an index.

			buf.Reset()

			buf.WriteString(`WITH idx AS (
SELECT i.indisunique, i.indisprimary, i.indisclustered,
       i.indisvalid,
       (NOT i.indimmediate)
       AND EXISTS (
           SELECT 1
             FROM pg_catalog.pg_constraint
            WHERE conrelid = i.indrelid
              AND conindid = i.indexrelid
              AND contype IN ('p','u','x')
              AND condeferrable
       ) AS condeferrable,
       (NOT i.indimmediate)
       AND EXISTS (
           SELECT 1
             FROM pg_catalog.pg_constraint
            WHERE conrelid = i.indrelid
              AND conindid = i.indexrelid
              AND contype IN ('p','u','x')
              AND condeferred
       ) AS condeferred,
       i.indisreplident,
       i.indnullsnotdistinct,
       a.amname, c2.relname as indtable,
       pg_catalog.pg_get_expr(i.indpred, i.indrelid, true) AS indpred
  FROM pg_catalog.pg_index i,
       pg_catalog.pg_class c,
       pg_catalog.pg_class c2,
       pg_catalog.pg_am a
 WHERE i.indexrelid = c.oid
   AND c.oid = %[1]s
   AND c.relam = a.oid
   AND i.indrelid = c2.oid)
SELECT IF(indisprimary, 'primary key, ',
          IF(indisunique, 'unique'||
             IF(indnullsnotdistinct, ' nulls not distinct', '')||', ', ''))||
       amname||', for table '||
       pg_catalog.quote_ident(%[2]s)||'.'||
       pg_catalog.quote_ident(indtable)||
       IF(length(indpred)>0, ', predicate('||indpred||')', '')||
       IF(indisclustered, ', clustered', '')||
       IF(NOT indisvalid, ', invalid', '')||
       IF(condeferrable, ', deferrable', '')||
       IF(condeferred, ', initially deferred', '')||
       IF(indisreplident, ', replica identity', '')
       AS "Properties"
  FROM idx`)

			idxStage := describeStage{
				title: "",
				sql:   buf.String(),
				qargs: []interface{}{oid, lexbase.EscapeSQLString(scName)},
			}
			extraStages = append(extraStages, idxStage)
		}

		switch relkind {
		case "r", "m", "f", "p", "I", "t":
			// print indexes.
			if relhasindex == "t" {
				buf.Reset()

				buf.WriteString(`WITH idx AS (
   SELECT c2.relname AS idxname,
          i.indisprimary, i.indisunique, i.indisclustered,
          i.indisvalid,
          pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) as indexdef,
          pg_catalog.pg_get_constraintdef(con.oid, true) as condef,
          contype, condeferrable, condeferred,
          i.indisreplident
     FROM pg_catalog.pg_class c,
          pg_catalog.pg_class c2,
          pg_catalog.pg_index i
LEFT JOIN pg_catalog.pg_constraint con
       ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
    WHERE c.oid = %[1]s
      AND c.oid = i.indrelid
      AND i.indexrelid = c2.oid)
SELECT pg_catalog.quote_ident(idxname) ||
       IF(contype = 'x', ' ' || condef,
          IF(indisprimary, ' PRIMARY KEY,',
             IF(indisunique,
                IF(contype = 'u', ' UNIQUE CONSTRAINT,', ' UNIQUE,'), ''))||
          ' ' || substring(indexdef FROM position(' USING ' IN indexdef)+7) ||
          IF(condeferrable, ' DEFERRABLE', '')||
          IF(condeferred, ' INITIALLY DEFERRED', ''))||
       IF(indisclustered, ' CLUSTER', '')||
       IF(NOT indisvalid, ' INVALID', '')||
       IF(indisreplident, ' REPLICA IDENTITY', '')
       AS "Indexes"
  FROM idx
ORDER BY indisprimary DESC, idxname`)

				idxStage := describeStage{
					title: "",
					sql:   buf.String(),
					qargs: []interface{}{oid},
				}
				extraStages = append(extraStages, idxStage)
			}

			// print table (and column) check constraints.

			if relhaschecks == "t" {
				buf.Reset()

				buf.WriteString(`WITH cons AS (
SELECT r.conname,
       pg_catalog.pg_get_constraintdef(r.oid, true) AS condef
  FROM pg_catalog.pg_constraint r
 WHERE r.conrelid = %[1]s AND r.contype = 'c'
)
  SELECT pg_catalog.quote_ident(conname) || ' ' || condef
         AS "Check constraints"
    FROM cons
ORDER BY conname`)

				checkStage := describeStage{
					title: "",
					sql:   buf.String(),
					qargs: []interface{}{oid},
				}
				extraStages = append(extraStages, checkStage)
			}

			// print foreign-key constraints.
			if relhasfkey == "t" {
				buf.Reset()
				buf.WriteString(`WITH cons AS (
SELECT conname,
       pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
       conrelid::pg_catalog.regclass AS ontable
  FROM pg_catalog.pg_constraint r
 WHERE r.conrelid = %[1]s
   AND r.contype = 'f' AND (r.conparentid = 0 OR r.conparentid IS NULL))
  SELECT 'TABLE ' || pg_catalog.quote_ident(ontable::STRING) ||
         ' CONSTRAINT ' || pg_catalog.quote_ident(conname) || ' ' || condef
         AS "Foreign-key constraints"
    FROM cons
ORDER BY conname`)

				fkeyStage := describeStage{
					title: "",
					sql:   buf.String(),
					qargs: []interface{}{oid},
				}
				extraStages = append(extraStages, fkeyStage)
			}

			// print incoming foreign-key references.
			if relhasifkey == "t" {
				buf.Reset()
				buf.WriteString(`WITH cons AS (
SELECT conname,
       pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
       conrelid::pg_catalog.regclass AS ontable
  FROM pg_catalog.pg_constraint r
 WHERE r.confrelid = %[1]s
   AND r.contype = 'f')
  SELECT 'TABLE ' || pg_catalog.quote_ident(ontable::STRING) ||
         ' CONSTRAINT ' || pg_catalog.quote_ident(conname) || ' ' || condef
         AS "Referenced by"
    FROM cons
ORDER BY conname`)

				fkeyStage := describeStage{
					title: "",
					sql:   buf.String(),
					qargs: []interface{}{oid},
				}
				extraStages = append(extraStages, fkeyStage)
			}

			// print any extended statistics
			if relhasstats == "t" && verbose {
				buf.Reset()

				buf.WriteString(`WITH stat AS (
SELECT oid,
       stxrelid::pg_catalog.regclass AS tb,
       stxnamespace::pg_catalog.regnamespace AS nsp,
       stxname,
       (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(attname),', ')
					FROM pg_catalog.unnest(stxkeys) s(attnum)
			    JOIN pg_catalog.pg_attribute a
            ON (stxrelid = a.attrelid
           AND a.attnum = s.attnum
       AND NOT attisdropped)
       ) AS columns,
       'd' = any(stxkind) AS hasndist,
       'f' = any(stxkind) AS hasdeps,
       'm' = any(stxkind) AS hasmcv,
       stxstattarget
  FROM pg_catalog.pg_statistic_ext stat
 WHERE stxrelid = %[1]s)
  SELECT pg_catalog.quote_ident(nsp)||'.'||pg_catalog.quote_ident(stxname)||
         IF((hasndist OR hasdeps OR hasmcv) AND NOT (hasndist AND hasdeps AND hasmcv),
            '('||
            IF(hasndist,
               'ndistinct' || IF(hasdeps OR hasmcv, ', ', ''),
               '')||
            IF(hasdeps, 'dependencies' || IF(hasmcv, ', ', ''), '')||
            IF(hasmcv, 'mcv', '')||
            ')',
           '')||
         ' ON '||columns||' FROM ' || pg_catalog.quote_ident(tb::STRING) ||
         IF(stxstattarget <> -1 AND stxstattarget IS NOT NULL,
            '; STATISTICS ' || stxstattarget::STRING, '')
         AS "Statistics objects"
    FROM stat
ORDER BY stat.oid`)

				statStage := describeStage{
					title: "",
					sql:   buf.String(),
					qargs: []interface{}{oid},
				}
				extraStages = append(extraStages, statStage)
			}
		}

		switch relkind {
		case "v", "m":
			if verbose {
				viewStage := describeStage{
					title: "",
					sql:   `SELECT pg_catalog.pg_get_viewdef(%[1]s::pg_catalog.oid, true) AS "View definition"`,
					qargs: []interface{}{oid},
				}
				extraStages = append(extraStages, viewStage)
			}
		}
		return extraStages
	}
}
