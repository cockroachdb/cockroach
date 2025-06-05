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

type triggerIdPair struct {
	triggerID int64
	tableID   int64
}

// TODO: Make the crdb_internal.create_trigger_statements table
func getTriggerIds(
	ctx context.Context, evalPlanner eval.Planner, txn *kv.Txn, dbName string, acc *mon.BoundAccount,
) (triggerIds []triggerIdPair, retErr error) { //TODO: Check up on ID field names etc
	//TODO: Want to get (trigger_id, table_id)
	query := fmt.Sprintf(`
SELECT trigger_id, table_id 
FROM %s.crdb_internal.create_trigger_statements 
WHERE database_name=$1`, lexbase.EscapeSQLIdent(dbName))

	it, err := evalPlanner.QueryIteratorEx(ctx,
		"crdb_internal.show_create_all_triggers",
		sessiondata.NoSessionDataOverride,
		query,
		dbName)

	if err != nil {
		return nil, err
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, it.Close())
	}()

	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		triggerID := tree.MustBeDInt(it.Cur()[0])
		tableID := tree.MustBeDInt(it.Cur()[1])
		pair := triggerIdPair{int64(triggerID), int64(tableID)}
		triggerIds = append(triggerIds, pair)
		if err = acc.Grow(ctx, int64(unsafe.Sizeof(pair))); err != nil {
			return nil, err
		}
	}
	if err != nil {
		return triggerIds, err
	}
	return triggerIds, nil
}

func getTriggerCreateStatement(
	ctx context.Context,
	evalPlanner eval.Planner,
	txn *kv.Txn,
	triggerIDPair triggerIdPair,
	dbName string,
) (_ tree.Datum, err error) {
	query := fmt.Sprintf(`
SELECT create_statement, trigger_id
FROM %s.crdb_internal.create_trigger_statements
WHERE table_id = $1`, lexbase.EscapeSQLIdent(dbName))

	iter, err := evalPlanner.QueryIteratorEx(ctx,
		"crdb_internal.show_create_all_triggers",
		sessiondata.NoSessionDataOverride,
		query,
		triggerIDPair.tableID)
	if err != nil {
		return nil, err
	}

	defer func() {
		closeErr := iter.Close()
		err = errors.CombineErrors(err, closeErr)
	}()
	for {
		next, err := iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}
		row := iter.Cur()
		if len(row) != 2 {
			return nil, errors.Newf("expected 2 columns in result, got %d", len(row))
		}
		if tree.MustBeDInt(row[1]) != tree.DInt(triggerIDPair.triggerID) {
			continue
		}
		return row[0], nil
	}

	if err != nil {
		return nil, err
	}
	return nil, nil
}
