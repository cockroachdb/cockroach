// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the LICENSE file.

package builtins

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

func getRoutineCreateStatements(
	ctx context.Context, evalPlanner eval.Planner, txn *kv.Txn, dbName string, acc *mon.BoundAccount,
) (statements []string, retErr error) {
	query := fmt.Sprintf(`
		SELECT create_statement
		FROM %s.crdb_internal.create_function_statements
		WHERE database_name = $1
		UNION ALL
		SELECT create_statement
		FROM %s.crdb_internal.create_procedure_statements
		WHERE database_name = $1
	`, lexbase.EscapeSQLIdent(dbName), lexbase.EscapeSQLIdent(dbName))

	it, err := evalPlanner.QueryIteratorEx(
		ctx,
		"crdb_internal.show_create_all_routines",
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
		stmt := tree.MustBeDString(it.Cur()[0])
		statements = append(statements, string(stmt))

		if accErr := acc.Grow(ctx, int64(len(stmt))); accErr != nil {
			return nil, accErr
		}
	}
	if err != nil {
		return statements, err
	}
	return statements, nil
}
