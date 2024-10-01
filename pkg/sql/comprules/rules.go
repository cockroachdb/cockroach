// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package comprules

import (
	"context"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/sql/compengine"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
)

// GetCompMethods exposes the completion heuristics defined in this
// package.
func GetCompMethods() []compengine.Method {
	return []compengine.Method{
		method("keywords", completeKeyword),
		method("functions", completeFunction),
		method("objects", completeObjectInCurrentDatabase),
		method("schemas", completeSchemaInCurrentDatabase),
		method("dbs", completeDatabase),
		method("xobjs", completeObjectInOtherDatabase),
		method("xsch", completeSchemaInOtherDatabase),
	}
}

var compIdentAfterPeriod = regexp.MustCompile(`\.(i'|_)`)

func completeKeyword(ctx context.Context, c compengine.Context) (compengine.Rows, error) {
	// We complete a keyword in the following cases:
	//
	// - cursor not after a period (.), and EITHER:
	//   - cursor in whitespace, i.e. in-between other tokens; OR
	//   - cursor inside a non-quoted identifier-like token.
	//
	// In all other cases, there's no keyword to be found.
	//
	// Note how in particular we do not complete after a period.
	// This is because in SQL only object names can appear after
	// a period, so it's never expected to find a keyword there.
	//
	// We also do not complete inside quoted identifiers: if the user
	// types {"sel} and then tries to autocomplete, we should understand
	// they are interested in an object/name starting with "sel", not a
	// keyword, because keywords can never be quoted.
	curTok := c.RelToken(0)
	sketch := c.Sketch()
	var prefix string
	var start, end int

	switch {
	case compIdentAfterPeriod.MatchString(sketch):
		c.Trace("not completing after period")
		return nil, nil

	case c.CursorInSpace():
		start = c.QueryPos()
		end = start

	case c.AtWordOrInSpaceFollowingWord() && !curTok.Quoted:
		prefix = curTok.Str
		start = int(curTok.Start)
		end = int(curTok.End)

	default:
		c.Trace("not completing")
		return nil, nil
	}

	c.Trace("completing for %q (%d,%d)", prefix, start, end)
	const query = `
SELECT upper(word),
       'keyword' AS category,
       COALESCE(catdesc,'') AS description,
       $2:::INT AS start,
       $3:::INT AS end
  FROM pg_catalog.pg_get_keywords()
 WHERE left(word, length($1:::STRING)) = $1:::STRING`
	iter, err := c.Query(ctx, query, prefix, start, end)
	return iter, err
}

// A surely not qualified possible function name.
// Also, function names cannot appear directly after a semicolon.
var compNotQualProcRe = regexp.MustCompile(`[^.;](i'|_)`)

// A schema-qualified possible builtin name.
// Also, function names cannot appear directly after a semicolon.
var compMaybeQualProcRe = regexp.MustCompile(`[^.;]i\.(['_]|i')`)

func completeFunction(ctx context.Context, c compengine.Context) (compengine.Rows, error) {
	// Complete function names:
	//
	// - at whitespace after keywords.
	// - after a period.
	//
	var prefix string
	var start, end int
	var schemaName string
	atWord := c.AtWordOrInSpaceFollowingWord()
	sketch := c.Sketch()
	switch {
	case compMaybeQualProcRe.MatchString(sketch):
		start = int(c.RelToken(-1).Start)
		schemaName = c.RelToken(-1).Str
		if atWord {
			start = int(c.RelToken(-2).Start)
			schemaName = c.RelToken(-2).Str
			prefix = c.RelToken(0).Str
		}
		end = int(c.RelToken(0).End)

	case compNotQualProcRe.MatchString(sketch):
		switch {
		case c.CursorInToken() && atWord:
			curTok := c.RelToken(0)
			prefix = curTok.Str
			start = int(curTok.Start)
			end = int(curTok.End)

		default:
			start = c.QueryPos()
			end = start
		}

	default:
		c.Trace("not completing")
		return nil, nil
	}

	c.Trace("completing for %q (%d,%d) with schema %q", prefix, start, end, schemaName)
	// Note: we use min(p.oid) ... GROUP BY p.proname to cover the case
	// there are multiple overloads. This ensures we have only one entry
	// in the completion results for that function. Its reported
	// description will also be the description for the first overload.
	// Separately, we GROUP BY n.nspname to ensure that a UDF with
	// the same name as a pg_catalog function gets reported as a
	// separate completion entry.
	const query = `
WITH p AS (
SELECT min(p.oid) AS oid, p.proname, n.nspname
  FROM pg_catalog.pg_proc p
  JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
 WHERE left(p.proname, length($1:::STRING)) = $1:::STRING
 AND ((length($4) > 0 AND $4 = n.nspname)
   OR (length($4) = 0 AND n.nspname = ANY current_schemas(true)))
GROUP BY p.proname, n.nspname
)
SELECT DISTINCT
       IF(length($4) > 0, pg_catalog.quote_ident($4:::STRING) || '.', '') ||
       pg_catalog.quote_ident(proname) || '(' AS completion,
       'functions' AS category,
       IF(length($4) = 0, '(from schema '||nspname||') ', '') ||
       substr(COALESCE(pg_catalog.obj_description(oid, 'pg_proc'),''), e'[^.\n]{0,80}') AS description,
       $2:::INT AS start,
       $3:::INT AS end
  FROM p
ORDER BY 1,3,4,5
`
	iter, err := c.Query(ctx, query, prefix, start, end, schemaName)
	return iter, err
}

// A database name can only occur after a keyword or a comma (,).
var compDbRe = regexp.MustCompile(`[i,](i'|_)`)

func completeDatabase(ctx context.Context, c compengine.Context) (compengine.Rows, error) {
	var prefix string
	var start, end int
	sketch := c.Sketch()
	switch {
	case !compDbRe.MatchString(sketch):
		c.Trace("not completing")
		return nil, nil

	case c.CursorInToken() && c.AtWordOrInSpaceFollowingWord():
		curTok := c.RelToken(0)
		prefix = curTok.Str
		start = int(curTok.Start)
		end = int(curTok.End)

	default:
		// Not inside an identifier (e.g inside a comma, or at space).
		start = c.QueryPos()
		end = start
	}

	c.Trace("completing for %q (%d,%d)", prefix, start, end)
	const query = `
WITH d AS (SELECT oid, datname FROM pg_catalog.pg_database)
SELECT datname AS completion,
       'database' AS category,
       substr(COALESCE(sc.comment, ''), e'[^\n]{0,80}') as description,
       $2:::INT AS start,
       $3:::INT AS end
  FROM d
LEFT OUTER JOIN system.public.comments sc
    ON d.oid = sc.object_id
   AND sc.type = 0
 WHERE left(datname, length($1:::STRING)) = $1::STRING
ORDER BY 1,3,4,5
`
	iter, err := c.Query(ctx, query, prefix, start, end)
	return iter, err
}

// A local (current schema) object can only occur after non-period.
var compLocalTableRe = regexp.MustCompile(`[^.;](i'|_)`)

// A schema-qualified object in the current db, or a db
// qualified object.
var compOneQualPrefixRe = regexp.MustCompile(`[^.;]i\.(['_]|i')`)

func completeObjectInCurrentDatabase(
	ctx context.Context, c compengine.Context,
) (compengine.Rows, error) {
	var schema string
	atWord := c.AtWordOrInSpaceFollowingWord()
	sketch := c.Sketch()
	hasSchemaPrefix := false
	switch {
	case compLocalTableRe.MatchString(sketch):
		schema = "IN (TABLE unnest(current_schemas(true)))"

	case compOneQualPrefixRe.MatchString(sketch):
		schemaTok := c.RelToken(-1)
		if atWord {
			schemaTok = c.RelToken(-2)
		}
		schema = "= " + lexbase.EscapeSQLString(schemaTok.Str)
		hasSchemaPrefix = true

	default:
		c.Trace("not completing")
		return nil, nil
	}

	var prefix string
	var start, end int
	switch {
	case c.CursorInToken() && atWord:
		curTok := c.RelToken(0)
		prefix = curTok.Str
		start = int(curTok.Start)
		end = int(curTok.End)
	default:
		start = c.QueryPos()
		end = start
	}

	c.Trace("completing for %q (%d,%d), schema: %s", prefix, start, end, schema)
	// We only include pg_catalog relations in the following cases:
	//
	//    - when the cursor is position after an explicit schema qualification;
	//      in which case we're going to filter to that schema anyway.
	//    - when the completion prefix already includes the `pg_` prefix,
	//      in which case we can assume the user wants to see these tables.
	const queryT = `
         SELECT c.relname AS completion,
                'relation' AS category,
                substr(COALESCE(d.description, ''), e'[^\n]{0,80}') as description,
                $2:::INT AS start,
                $3:::INT AS end
           FROM pg_catalog.pg_class c
           JOIN pg_catalog.pg_namespace n
                ON c.relnamespace = n.oid AND n.nspname %s
LEFT OUTER JOIN pg_catalog.pg_description d
                ON c.oid = d.objoid AND d.classoid = 'pg_catalog.pg_class'::REGCLASS::OID
          WHERE c.reltype != 0
            AND left(relname, length($1:::STRING)) = $1::STRING
            AND (nspname != 'pg_catalog' OR $4:::BOOL OR left($1:::STRING, 3) = 'pg_')
`
	query := fmt.Sprintf(queryT, schema)
	iter, err := c.Query(ctx, query, prefix, start, end, hasSchemaPrefix)
	return iter, err
}

func completeSchemaInCurrentDatabase(
	ctx context.Context, c compengine.Context,
) (compengine.Rows, error) {
	switch {
	case compLocalTableRe.MatchString(c.Sketch()):
	default:
		c.Trace("not completing")
		return nil, nil
	}

	var prefix string
	var start, end int
	switch {
	case c.CursorInToken() && c.AtWordOrInSpaceFollowingWord():
		curTok := c.RelToken(0)
		prefix = curTok.Str
		start = int(curTok.Start)
		end = int(curTok.End)
	default:
		start = c.QueryPos()
		end = start
	}

	c.Trace("completing for %q (%d,%d)", prefix, start, end)
	const query = `
         SELECT n.nspname AS completion,
                'schema' AS category,
                substr(COALESCE(d.description, ''), e'[^\n]{0,80}') as description,
                $2:::INT AS start,
                $3:::INT AS end
           FROM pg_catalog.pg_namespace n
LEFT OUTER JOIN pg_catalog.pg_description d
                ON n.oid = d.objoid AND d.classoid = 'pg_catalog.pg_namespace'::REGCLASS::OID
 WHERE left(nspname, length($1:::STRING)) = $1::STRING
ORDER BY 1,3,4,5
`
	iter, err := c.Query(ctx, query, prefix, start, end)
	return iter, err
}

func completeSchemaInOtherDatabase(
	ctx context.Context, c compengine.Context,
) (compengine.Rows, error) {
	var dbname string
	atWord := c.AtWordOrInSpaceFollowingWord()
	switch {
	case compOneQualPrefixRe.MatchString(c.Sketch()):
		dbTok := c.RelToken(-1)
		if atWord {
			dbTok = c.RelToken(-2)
		}
		dbname = dbTok.Str

	default:
		c.Trace("not completing")
		return nil, nil
	}

	var prefix string
	var start, end int
	switch {
	case atWord:
		curTok := c.RelToken(0)
		prefix = curTok.Str
		start = int(curTok.Start)
		end = int(curTok.End)

	default:
		start = c.QueryPos()
		end = start
	}

	c.Trace("completing for %q (%d,%d) in db %q",
		prefix, start, end, dbname)

	// TODO(knz): also pull comments.
	const query = `
SELECT schema_name AS completion,
       'schema' AS category,
       '' as description,
       $2:::INT AS start,
       $3:::INT AS end
  FROM "".information_schema.schemata
 WHERE catalog_name = $4:::STRING
   AND left(schema_name, length($1:::STRING)) = $1:::STRING
ORDER BY 1,3,4,5
`
	iter, err := c.Query(ctx, query, prefix, start, end, dbname)
	return iter, err
}

// An object name with two prefix qualifications.
var compTwoQualPrefixRe = regexp.MustCompile(`[^.;]i\.i\.(['_]|i')`)

func completeObjectInOtherDatabase(
	ctx context.Context, c compengine.Context,
) (compengine.Rows, error) {
	var schema string
	atWord := c.AtWordOrInSpaceFollowingWord()
	sketch := c.Sketch()
	var dbTok scanner.InspectToken
	switch {
	case compOneQualPrefixRe.MatchString(sketch):
		schema = "public"
		dbTok = c.RelToken(-1)
		if atWord {
			dbTok = c.RelToken(-2)
		}

	case compTwoQualPrefixRe.MatchString(sketch):
		schemaTok := c.RelToken(-1)
		dbTok = c.RelToken(-3)
		if atWord {
			schemaTok = c.RelToken(-2)
			dbTok = c.RelToken(-4)
		}
		schema = schemaTok.Str

	default:
		c.Trace("not completing")
		return nil, nil
	}
	dbname := dbTok.Str

	var prefix string
	var start, end int
	switch {
	case c.CursorInToken() && atWord:
		curTok := c.RelToken(0)
		prefix = curTok.Str
		start = int(curTok.Start)
		end = int(curTok.End)
	default:
		start = c.QueryPos()
		end = start
	}

	c.Trace("completing for %q (%d,%d), schema: %q, db: %q", prefix, start, end, schema, dbname)
	const query = `
WITH t AS (
SELECT name, table_id
  FROM "".crdb_internal.tables
 WHERE database_name = $4:::STRING
   AND schema_name = $5:::STRING
   AND left(name, length($1:::STRING)) = $1:::STRING
)
SELECT name AS completion,
       'relation' AS category,
       substr(COALESCE(cc.description, ''), e'[^\n]{0,80}') as description,
       $2:::INT AS start,
       $3:::INT AS end
  FROM t
LEFT OUTER JOIN "".pg_catalog.pg_description cc
    ON t.table_id = cc.objoid AND cc.classoid = 'pg_catalog.pg_class'::REGCLASS::OID
`
	iter, err := c.Query(ctx, query, prefix, start, end, dbname, schema)
	return iter, err
}
