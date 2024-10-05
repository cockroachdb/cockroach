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
	// id::int | schema_id::int | name::text | descriptor::json
	descJSONQuery = `SELECT
		descriptor.id,
		"parentSchemaID" AS schema_id,
		namespace.name AS name,
		crdb_internal.pb_to_json('desc', descriptor) AS descriptor
	FROM system.descriptor
	JOIN system.namespace ON namespace.id = descriptor.id
	WHERE "parentID" = (SELECT id FROM system.namespace WHERE name = current_database() AND "parentID" = 0)
	`

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
)

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
		return zero, errors.Wrapf(err, "CollectOne: Query: %q %q", query, args)
	}

	result, err := pgx.CollectOneRow[T](rows, fn)
	if err != nil {
		var zero T
		return zero, errors.Wrapf(err, "CollectOne: CollectOneRow: %q %q", query, args)
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
		return nil, errors.Wrapf(err, "Collect: Query: %q %q", query, args)
	}

	results, err := pgx.CollectRows(rows, fn)
	if err != nil {
		return nil, errors.Wrapf(err, "Collect: CollectRows: %q %q", query, args)
	}

	og.LogQueryResults(query, results, args...)
	return results, nil
}
