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

type tableTriggerPair struct {
	triggerID int64
	tableID   int64
}

func getTriggerIds(
	ctx context.Context, evalPlanner eval.Planner, txn *kv.Txn, dbName string, acc *mon.BoundAccount,
) (triggerIds []tableTriggerPair, retErr error) {
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
		pair := tableTriggerPair{int64(triggerID), int64(tableID)}
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
	tableTriggerIds tableTriggerPair,
	dbName string,
) (_ tree.Datum, err error) {
	// Here, we query by `table_id` and 'trigger_id' but the query is only actually indexed by 'table_id'.
	// This is because the `crdb_internal.create_trigger_statements` table only has a virtual index on `table_id`
	// due to the fact that internal virtual tables do not support multi-column indexes
	// and `trigger_id` values are only unique within a table, not between tables.
	query := fmt.Sprintf(`
SELECT create_statement, trigger_id
FROM %s.crdb_internal.create_trigger_statements
WHERE table_id = $1 AND trigger_id=$2`, lexbase.EscapeSQLIdent(dbName))

	row, err := evalPlanner.QueryRowEx(
		ctx,
		"crdb_internal.show_create_all_triggers",
		sessiondata.NoSessionDataOverride,
		query,
		tableTriggerIds.tableID, tableTriggerIds.triggerID)
	if err != nil {
		return nil, err
	}

	return row[0], nil
}
