// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
		method("builtins", completeBuiltin),
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

	case c.AtWord() && !curTok.Quoted:
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

// A surely not qualified possible builtin name.
var compNotQualProcRe = regexp.MustCompile(`[^.](i'|_)`)

// A qualified possible builtin name.
var compMaybeQualProcRe = regexp.MustCompile(`i\.['_]|i\.i'`)

var compVSchemaRe = regexp.MustCompile(`pg_catalog|crdb_internal|information_schema`)

func completeBuiltin(ctx context.Context, c compengine.Context) (compengine.Rows, error) {
	// Complete builtin names:
	//
	// - at whitespace after keywords.
	// - after a period, if the identifier before the period is a vschema.
	//
	var prefix string
	var start, end int
	var extraPrefix string
	atWord := c.AtWord()
	sketch := c.Sketch()
	switch {
	case compMaybeQualProcRe.MatchString(sketch) &&
		((!atWord && compVSchemaRe.MatchString(c.RelToken(-1).Str)) ||
			(atWord && compVSchemaRe.MatchString(c.RelToken(-2).Str))):
		start = int(c.RelToken(-1).Start)
		prefix = c.RelToken(-1).Str + "."
		if atWord {
			start = int(c.RelToken(-2).Start)
			prefix = c.RelToken(-2).Str + "."
		}
		// crdb has this weird thing where every unqualified built-in "X"
		// also exists as "pg_catalog.X". So when we search for
		// completions after "pg_catalog.", we can strip that prefix from
		// the search. However, we must be careful to add it back in the
		// completion results, so that the prefix does not get stripped
		// when the completion occurs.
		if prefix == "pg_catalog." {
			prefix = ""
			extraPrefix = "pg_catalog."
		}
		if atWord {
			prefix += c.RelToken(0).Str
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

	c.Trace("completing for %q (%d,%d)", prefix, start, end)
	// TODO(knz): use the comment extraction functions from pg_catalog
	// instead of crdb_internal. This requires exposing comments for
	// built-in functions through pg_catalog.
	const query = `
WITH p AS (SELECT DISTINCT proname FROM pg_catalog.pg_proc)
SELECT $4:::STRING || proname || '(' AS completion,
       'functions' AS category,
       substr(COALESCE((SELECT details
          FROM "".crdb_internal.builtin_functions f2
         WHERE f2.function = p.proname
         LIMIT 1), ''), e'[^\n]{0,80}') AS description,
       $2:::INT AS start,
       $3:::INT AS end
  FROM p
 WHERE left(proname, length($1:::STRING)) = $1:::STRING`
	iter, err := c.Query(ctx, query, prefix, start, end, extraPrefix)
	return iter, err
}

// A database name can only occur after a keyword or a comma (,).
var compDbRe = regexp.MustCompile(`i(i'|_)|,(_|i')`)

func completeDatabase(ctx context.Context, c compengine.Context) (compengine.Rows, error) {
	var prefix string
	var start, end int
	sketch := c.Sketch()
	switch {
	case !compDbRe.MatchString(sketch):
		c.Trace("not completing")
		return nil, nil

	case c.CursorInToken() && c.AtWord():
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
	atWord := c.AtWord()
	sketch := c.Sketch()
	switch {
	case compLocalTableRe.MatchString(sketch):
		schema = "IN (TABLE unnest(current_schemas(true)))"

	case compOneQualPrefixRe.MatchString(sketch):
		schemaTok := c.RelToken(-1)
		if atWord {
			schemaTok = c.RelToken(-2)
		}
		schema = "= " + lexbase.EscapeSQLString(schemaTok.Str)

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

	c.Trace("completing for %q (%d,%d), schema: %s", prefix, start, end, schema)
	const queryT = `
WITH n AS (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname %s),
     t AS (SELECT oid, relname FROM pg_catalog.pg_class WHERE reltype != 0 AND relnamespace IN (TABLE n))
SELECT relname AS completion,
       'relation' AS category,
       substr(COALESCE(pc.comment, sc.comment, ''), e'[^\n]{0,80}') as description,
       $2:::INT AS start,
       $3:::INT AS end
  FROM t
LEFT OUTER JOIN system.public.comments sc
    ON t.oid = sc.object_id AND sc.type = 1
LEFT OUTER JOIN "".crdb_internal.predefined_comments pc
    ON t.oid = pc.object_id AND pc.type = 1
 WHERE left(relname, length($1:::STRING)) = $1::STRING
`
	query := fmt.Sprintf(queryT, schema)
	iter, err := c.Query(ctx, query, prefix, start, end)
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
	case c.CursorInSpace():
		start = c.QueryPos()
		end = start
	default:
		curTok := c.RelToken(0)
		prefix = curTok.Str
		start = int(curTok.Start)
		end = int(curTok.End)
	}

	c.Trace("completing for %q (%d,%d)", prefix, start, end)
	const query = `
SELECT nspname AS completion,
       'schema' AS category,
       substr(COALESCE(pc.comment, sc.comment, ''), e'[^\n]{0,80}') as description,
       $2:::INT AS start,
       $3:::INT AS end
  FROM pg_catalog.pg_namespace t
LEFT OUTER JOIN system.public.comments sc
    ON t.oid = sc.object_id AND sc.type = 4
LEFT OUTER JOIN "".crdb_internal.predefined_comments pc
    ON t.oid = pc.object_id AND pc.type = 4
 WHERE left(nspname, length($1:::STRING)) = $1::STRING
`
	iter, err := c.Query(ctx, query, prefix, start, end)
	return iter, err
}

func completeSchemaInOtherDatabase(
	ctx context.Context, c compengine.Context,
) (compengine.Rows, error) {
	var dbname string
	atWord := c.AtWord()
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
	atWord := c.AtWord()
	sketch := c.Sketch()
	switch {
	case compOneQualPrefixRe.MatchString(sketch):
		schema = "public"

	case compTwoQualPrefixRe.MatchString(sketch):
		schemaTok := c.RelToken(-1)
		if atWord {
			schemaTok = c.RelToken(-2)
		}
		schema = schemaTok.Str

	default:
		c.Trace("not completing")
		return nil, nil
	}

	var dbTok scanner.InspectToken
	switch {
	case compOneQualPrefixRe.MatchString(sketch):
		dbTok = c.RelToken(-1)
		if atWord {
			dbTok = c.RelToken(-2)
		}

	case compTwoQualPrefixRe.MatchString(sketch):
		dbTok = c.RelToken(-3)
		if atWord {
			dbTok = c.RelToken(-4)
		}
	}
	dbname := dbTok.Str

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
       substr(COALESCE(pc.comment, sc.comment, ''), e'[^\n]{0,80}') as description,
       $2:::INT AS start,
       $3:::INT AS end
  FROM t
LEFT OUTER JOIN system.public.comments sc
    ON t.table_id = sc.object_id AND sc.type = 1
LEFT OUTER JOIN "".crdb_internal.predefined_comments pc
    ON t.table_id = pc.object_id AND pc.type = 1
`
	iter, err := c.Query(ctx, query, prefix, start, end, dbname, schema)
	return iter, err
}
