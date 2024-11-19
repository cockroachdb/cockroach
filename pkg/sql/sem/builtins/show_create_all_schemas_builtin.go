// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// getSchemaIDs returns the set of schema ids from
// crdb_internal.show_create_all_schemas for a specified database.
func getSchemaIDs(
	ctx context.Context, evalPlanner eval.Planner, txn *kv.Txn, dbName string, acc *mon.BoundAccount,
) (schemaIDs []int64, retErr error) {
	query := fmt.Sprintf(`
		SELECT descriptor_id
		FROM %s.crdb_internal.create_schema_statements
		WHERE database_name = $1
		`, lexbase.EscapeSQLIdent(dbName))
	it, err := evalPlanner.QueryIteratorEx(
		ctx,
		"crdb_internal.show_create_all_schemas",
		sessiondata.NoSessionDataOverride,
		query,
		dbName,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, it.Close())
	}()

	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		tid := tree.MustBeDInt(it.Cur()[0])

		schemaIDs = append(schemaIDs, int64(tid))
		if err = acc.Grow(ctx, int64(unsafe.Sizeof(tid))); err != nil {
			return nil, err
		}
	}
	if err != nil {
		return schemaIDs, err
	}

	return schemaIDs, nil
}

// getSchemaCreateStatement gets the create statement to recreate a schema (ignoring fks)
// for a given schema id in a database.
func getSchemaCreateStatement(
	ctx context.Context, evalPlanner eval.Planner, txn *kv.Txn, id int64, dbName string,
) (tree.Datum, error) {
	query := fmt.Sprintf(`
		SELECT
			create_statement
		FROM %s.crdb_internal.create_schema_statements
		WHERE descriptor_id = $1
	`, lexbase.EscapeSQLIdent(dbName))
	row, err := evalPlanner.QueryRowEx(
		ctx,
		"crdb_internal.show_create_all_schemas",
		sessiondata.NoSessionDataOverride,
		query,
		id,
	)

	if err != nil {
		return nil, err
	}
	return row[0], nil
}
