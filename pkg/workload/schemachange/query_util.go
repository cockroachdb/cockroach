// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package schemachange

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

const (
	// descJSONQuery returns the JSONified version of all descriptors in the
	// current database joined with system.namespace.
	//
	// NOTE:descJSONQuery injects "virtual" system.namespace entries for function
	// descriptors as they do not have "proper" namespace entries.
	//
	// id::int | schema_id::int | name::text | descriptor::json
	descJSONQuery = `SELECT
		descriptor.id,
		"parentSchemaID" AS schema_id,
		namespace.name AS name,
		crdb_internal.pb_to_json('desc', descriptor) AS descriptor
	FROM system.descriptor
	JOIN (
		SELECT * FROM system.namespace
			UNION
		SELECT
			"parentID",
			"parentSchemaID",
			(json_each).@1 AS name,
			(json_array_elements((json_each).@2->'signatures')->'id')::INT8 AS id
		FROM (
			SELECT
				ns."parentID",
				ns.id AS "parentSchemaID",
				json_each(crdb_internal.pb_to_json('desc', descriptor)->'schema'->'functions')
			FROM system.descriptor
			JOIN system.namespace ns ON ns.id = descriptor.id
			WHERE crdb_internal.pb_to_json('desc', descriptor) ? 'schema'
		)
	) namespace ON namespace.id = descriptor.id
	WHERE "parentID" = (SELECT id FROM system.namespace WHERE name = current_database() AND "parentID" = 0)
	`

	// tableDescQuery returns the JSONified version of all table descriptors in
	// the current database. Views and sequences are NOT included in the result
	// set.
	//
	// [descJSONQuery] must be bound to the name "descriptors".
	//
	// id::int | schema_id::int | name::text | descriptor::json
	tableDescQuery = `SELECT * FROM descriptors WHERE descriptor ? 'table' AND NOT (descriptor->'table' ? 'viewQuery' OR descriptor->'table' ? 'sequenceOpts') `

	// colDescQuery returns the JSONified version of all table columns in the current database.
	//
	// [descJSONQuery] must be bound to the name "descriptors".
	// [tableDescQuery] must be bound to the name "tables".
	//
	// schema_id::int | table_id::int | table_name::text | table_descriptor::json | column::json
	colDescQuery = `SELECT
		schema_id,
		tables.id AS table_id,
		tables.name AS table_name,
		tables.descriptor->'table' AS table_descriptor,
		json_array_elements(descriptor->'table'->'columns') AS col
	FROM tables`

	// enumDescsQuery returns the JSONified version of all enum descriptors in
	// the current database.
	//
	// [descJSONQuery] must be bound to the name "descriptors".
	//
	// id::int | schema_id::int | name::text | descriptor::json
	enumDescsQuery = `SELECT id, schema_id, name, descriptor->'type' AS descriptor FROM descriptors WHERE descriptor ? 'type'`

	// enumDescsQuery returns the JSONified version of all enum members, along
	// with their enum descriptors, in the current database.
	//
	// [enumDescsQuery] must be bound to the name "enums".
	//
	// id::int | schema_id::int | name::text | descriptor::json | member::json
	enumMemberDescsQuery = `SELECT *, jsonb_array_elements(descriptor->'enumMembers') AS member FROM enums`

	// functionDescsQuery returns the JSONified version of all function descriptors in the current database.
	//
	// [descJSONQuery] must be bound to the name "descriptors".
	//
	// id::int | schema_id::int | name::text | descriptor::json
	functionDescsQuery = `SELECT id, schema_id, name, descriptor->'function' AS descriptor FROM descriptors WHERE descriptor ? 'function'`

	regionsFromClusterQuery = `SELECT * FROM [SHOW REGIONS FROM CLUSTER]`

	functionDepsQuery = `SELECT
	objid AS from_oid, refobjid AS to_oid
FROM
	pg_depend AS d
WHERE
	d.classid = 'pg_catalog.pg_proc'::REGCLASS::INT8
	AND d.refclassid = 'pg_catalog.pg_proc'::REGCLASS::INT8`
)

func regionsFromDatabaseQuery(database string) string {
	return fmt.Sprintf(`SELECT * FROM [SHOW REGIONS FROM DATABASE %q]`, database)
}

func superRegionsFromDatabaseQuery(database string) string {
	return fmt.Sprintf(`SELECT * FROM [SHOW SUPER REGIONS FROM DATABASE %q]`, database)
}

type CTE struct {
	As    string
	Query string
}

// With is a helper for building queries utilizing Common Table Expressions
// (CTEs). Managing reusable and composable SQL queries in Golang is quite
// difficult without building out a full expression engine. The denormalized
// nature of the descriptor table, however, necessitates some degree of
// reusability. Our solution is to provide many CTE expression as constants and
// allow them to be composed with With.
// Usage:
//
//	With([]CTE{
//		{"descriptors", descJSONQuery},
//		{"table_descs", `SELECT id, descriptor->'table' FROM descriptors WHERE descriptor ? 'table'`},
//	}, `SELECT * FROM descriptors WHERE EXISTS(table_descs)`)
func With(ctes []CTE, query string) string {
	var b strings.Builder
	_, _ = b.WriteString("WITH ")
	for i, cte := range ctes {
		if i > 0 {
			_, _ = b.WriteString(",\n")
		}
		_, _ = fmt.Fprintf(&b, "%q AS (%s)", cte.As, cte.Query)
	}
	_, _ = b.WriteRune('\n')
	_, _ = b.WriteString(query)
	return b.String()
}

// Scan is a convenience wrapper around [CollectOne] that uses [pgx.RowTo] as
// fn.
func Scan[T any](
	ctx context.Context, og *operationGenerator, tx pgx.Tx, query string, args ...any,
) (result T, err error) {
	return CollectOne[T](ctx, og, tx, pgx.RowTo[T], query, args...)
}

// CollectOne is a convenience wrapper around [pgx.CollectOneRow] that logs the
// query and results via [LogQueryResults]. Usage:
//
//	CollectOne(ctx, og, tx, pgx.RowTo[string], `SELECT $1`, "bar")
func CollectOne[T any](
	ctx context.Context,
	og *operationGenerator,
	tx pgx.Tx,
	fn pgx.RowToFunc[T],
	query string,
	args ...any,
) (T, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		var zero T
		return zero, errors.Wrapf(err, "CollectOne: Query: %s Args: %s ", query, args)
	}

	result, err := pgx.CollectOneRow[T](rows, fn)
	if err != nil {
		var zero T
		return zero, errors.Wrapf(err, "CollectOne: CollectOneRow: Query: %s Args: %s", query, args)
	}

	og.LogQueryResults(query, result, args...)
	return result, nil
}

// Collect is a convenience wrapper around [pgx.CollectRows] that logs the
// query and results via [LogQueryResults]. Usage:
//
//	Collect(ctx, og, tx, pgx.RowTo[int], `SELECT * FROM gen_sequence(0, 100)`)
func Collect[T any](
	ctx context.Context,
	og *operationGenerator,
	tx pgx.Tx,
	fn pgx.RowToFunc[T],
	query string,
	args ...any,
) (result []T, err error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "Collect: Query: %s Args: %s", query, args)
	}

	results, err := pgx.CollectRows(rows, fn)
	if err != nil {
		return nil, errors.Wrapf(err, "Collect: CollectRows: Query: %s Args: %s", query, args)
	}

	og.LogQueryResults(query, results, args...)
	return results, nil
}
