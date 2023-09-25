// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package schemachange

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

// Scan is a convenience wrapper around [CollectOne] that uses [pgx.RowTo] as
// fn.
func Scan[T any](ctx context.Context, og *operationGenerator, tx pgx.Tx, query string, args ...any) (result T, err error) {
	return CollectOne[T](ctx, og, tx, pgx.RowTo[T], query, args...)
}

// CollectOne is a convenience wrapper around [pgx.CollectOneRow] that logs the
// query and results via [LogQueryResults]. Usage:
//
//	CollectOne(ctx, og, tx, pgx.RowTo[string], `SELECT $1`, "bar")
func CollectOne[T any](ctx context.Context, og *operationGenerator, tx pgx.Tx, fn pgx.RowToFunc[T], query string, args ...any) (T, error) {
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
func Collect[T any](ctx context.Context, og *operationGenerator, tx pgx.Tx, fn pgx.RowToFunc[T], query string, args ...any) (result []T, err error) {
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
