// Copyright 2025 The Cockroach Authors.
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

// We must make two separate queries and get IDs separately for functions and procedures, as they exist on
// separate internal virtual tables and this is a convenient way to get CREATE statements for all routines
// from the same generator and command.
func getRoutineCreateStatementIds(
	ctx context.Context, evalPlanner eval.Planner, txn *kv.Txn, dbName string, acc *mon.BoundAccount,
) (funcIDs []int64, procIDs []int64, retErr error) {
	escapedDB := lexbase.EscapeSQLIdent(dbName)

	funcsQuery := fmt.Sprintf(`
		SELECT function_id
		FROM %s.crdb_internal.create_function_statements
		WHERE database_name = $1`, escapedDB)

	procsQuery := fmt.Sprintf(`
		SELECT procedure_id
		FROM %s.crdb_internal.create_procedure_statements
		WHERE database_name = $1`, escapedDB)

	itFuncs, err := evalPlanner.QueryIteratorEx(
		ctx,
		"crdb_internal.show_create_all_routines",
		sessiondata.NoSessionDataOverride,
		funcsQuery,
		dbName,
	)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, itFuncs.Close())
	}()

	// We need to execute two separate queries because functions and procedures have their information stored
	// in different internal virtual tables. First, we collect their IDs to avoid holding all of the CREATE statements
	// in memory at once, which could result in hih memory usages for databases with many routines.
	var ok bool
	for ok, err = itFuncs.Next(ctx); ok; ok, err = itFuncs.Next(ctx) {
		id := tree.MustBeDInt(itFuncs.Cur()[0])
		funcIDs = append(funcIDs, int64(id))
		if growErr := acc.Grow(ctx, int64(unsafe.Sizeof(id))); growErr != nil {
			return nil, nil, growErr
		}
	}
	if err != nil {
		return nil, nil, err
	}

	itProcs, err := evalPlanner.QueryIteratorEx(
		ctx,
		"crdb_internal.show_create_all_routines",
		sessiondata.NoSessionDataOverride,
		procsQuery,
		dbName,
	)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, itProcs.Close())
	}()

	for ok, err = itProcs.Next(ctx); ok; ok, err = itProcs.Next(ctx) {
		id := tree.MustBeDInt(itProcs.Cur()[0])
		procIDs = append(procIDs, int64(id))
		if growErr := acc.Grow(ctx, int64(unsafe.Sizeof(id))); growErr != nil {
			return nil, nil, growErr
		}
	}
	if err != nil {
		return nil, nil, err
	}

	return funcIDs, procIDs, nil
}

func getFunctionCreateStatement(
	ctx context.Context, evalPlanner eval.Planner, txn *kv.Txn, id int64, dbName string,
) (tree.Datum, error) {
	query := fmt.Sprintf(`
		SELECT create_statement
		FROM %s.crdb_internal.create_function_statements
		WHERE function_id = $1
	`, lexbase.EscapeSQLIdent(dbName))
	row, err := evalPlanner.QueryRowEx(
		ctx,
		"crdb_internal.show_create_all_routines",
		sessiondata.NoSessionDataOverride,
		query,
		id,
	)

	if err != nil {
		return nil, err
	}
	return row[0], nil
}

func getProcedureCreateStatement(
	ctx context.Context, evalPlanner eval.Planner, txn *kv.Txn, id int64, dbName string,
) (tree.Datum, error) {
	query := fmt.Sprintf(`
		SELECT create_statement
		FROM %s.crdb_internal.create_procedure_statements
		WHERE procedure_id = $1
	`, lexbase.EscapeSQLIdent(dbName))
	row, err := evalPlanner.QueryRowEx(
		ctx,
		"crdb_internal.show_create_all_routines",
		sessiondata.NoSessionDataOverride,
		query,
		id,
	)

	if err != nil {
		return nil, err
	}
	return row[0], nil
}
