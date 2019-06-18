// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

type splitNode struct {
	optColumnsSlot

	force          bool
	tableDesc      *sqlbase.TableDescriptor
	index          *sqlbase.IndexDescriptor
	rows           planNode
	run            splitRun
	expirationTime hlc.Timestamp
}

// Split executes a KV split.
// Privileges: INSERT on table.
func (p *planner) Split(ctx context.Context, n *tree.Split) (planNode, error) {
	tableDesc, index, err := p.getTableAndIndex(ctx, &n.TableOrIndex, privilege.INSERT)
	if err != nil {
		return nil, err
	}
	// Calculate the desired types for the select statement. It is OK if the
	// select statement returns fewer columns (the relevant prefix is used).
	desiredTypes := make([]*types.T, len(index.ColumnIDs))
	for i, colID := range index.ColumnIDs {
		c, err := tableDesc.FindColumnByID(colID)
		if err != nil {
			return nil, err
		}
		desiredTypes[i] = &c.Type
	}

	// Create the plan for the split rows source.
	rows, err := p.newPlan(ctx, n.Rows, desiredTypes)
	if err != nil {
		return nil, err
	}

	cols := planColumns(rows)
	if len(cols) == 0 {
		return nil, errors.Errorf("no columns in SPLIT AT data")
	}
	if len(cols) > len(index.ColumnIDs) {
		return nil, errors.Errorf("too many columns in SPLIT AT data")
	}
	for i := range cols {
		if !cols[i].Typ.Equivalent(desiredTypes[i]) {
			return nil, errors.Errorf(
				"SPLIT AT data column %d (%s) must be of type %s, not type %s",
				i+1, index.ColumnNames[i], desiredTypes[i], cols[i].Typ,
			)
		}
	}

	expirationTime, err := parseExpirationTime(&p.semaCtx, p.EvalContext(), n.ExpireExpr)
	if err != nil {
		return nil, err
	}

	return &splitNode{
		force:          p.SessionData().ForceSplitAt,
		tableDesc:      tableDesc.TableDesc(),
		index:          index,
		rows:           rows,
		expirationTime: expirationTime,
	}, nil
}

var splitNodeColumns = sqlbase.ResultColumns{
	{
		Name: "key",
		Typ:  types.Bytes,
	},
	{
		Name: "pretty",
		Typ:  types.String,
	},
	{
		Name: "split_enforced_until",
		Typ:  types.Timestamp,
	},
}

// splitRun contains the run-time state of splitNode during local execution.
type splitRun struct {
	lastSplitKey       []byte
	lastExpirationTime hlc.Timestamp
}

func (n *splitNode) startExec(params runParams) error {
	stickyBitEnabled := params.EvalContext().Settings.Version.IsActive(cluster.VersionStickyBit)
	// TODO(jeffreyxiao): Remove this error, splitNode.force, and
	// experimental_force_split_at in v20.1.
	// This check is not intended to be foolproof. The setting could be outdated
	// because of gossip inconsistency, or it could change halfway through the
	// SPLIT AT's execution. It is, however, likely to prevent user error and
	// confusion in the common case.
	if !n.force && storagebase.MergeQueueEnabled.Get(&params.p.ExecCfg().Settings.SV) && !stickyBitEnabled {
		return errors.New("splits would be immediately discarded by merge queue; " +
			"disable the merge queue first by running 'SET CLUSTER SETTING kv.range_merge.queue_enabled = false'")
	}
	return nil
}

func (n *splitNode) Next(params runParams) (bool, error) {
	// TODO(radu): instead of performing the splits sequentially, accumulate all
	// the split keys and then perform the splits in parallel (e.g. split at the
	// middle key and recursively to the left and right).

	if ok, err := n.rows.Next(params); err != nil || !ok {
		return ok, err
	}

	rowKey, err := getRowKey(n.tableDesc, n.index, n.rows.Values())
	if err != nil {
		return false, err
	}

	// TODO(jeffreyxiao): Remove this check in v20.1.
	// Don't set the manual flag if the cluster is not up-to-date.
	stickyBitEnabled := params.EvalContext().Settings.Version.IsActive(cluster.VersionStickyBit)
	expirationTime := hlc.Timestamp{}
	if stickyBitEnabled {
		expirationTime = n.expirationTime
	}
	if err := params.extendedEvalCtx.ExecCfg.DB.AdminSplit(params.ctx, rowKey, rowKey, expirationTime); err != nil {
		return false, err
	}

	n.run.lastSplitKey = rowKey
	n.run.lastExpirationTime = expirationTime

	return true, nil
}

func (n *splitNode) Values() tree.Datums {
	splitEnforcedUntil := tree.DNull
	if (n.run.lastExpirationTime != hlc.Timestamp{}) {
		splitEnforcedUntil = tree.TimestampToInexactDTimestamp(n.run.lastExpirationTime)
	}
	return tree.Datums{
		tree.NewDBytes(tree.DBytes(n.run.lastSplitKey)),
		tree.NewDString(keys.PrettyPrint(nil /* valDirs */, n.run.lastSplitKey)),
		splitEnforcedUntil,
	}
}

func (n *splitNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}

// getRowKey generates a key that corresponds to a row (or prefix of a row) in a table or index.
// Both tableDesc and index are required (index can be the primary index).
func getRowKey(
	tableDesc *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor, values []tree.Datum,
) ([]byte, error) {
	colMap := make(map[sqlbase.ColumnID]int)
	for i := range values {
		colMap[index.ColumnIDs[i]] = i
	}
	prefix := sqlbase.MakeIndexKeyPrefix(tableDesc, index.ID)
	key, _, err := sqlbase.EncodePartialIndexKey(
		tableDesc, index, len(values), colMap, values, prefix,
	)
	if err != nil {
		return nil, err
	}
	return key, nil
}

// parseExpriationTime parses an expression into a hlc.Timestamp representing
// the expiration time of the split.
func parseExpirationTime(
	semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, expireExpr tree.Expr,
) (hlc.Timestamp, error) {
	if expireExpr == nil {
		return hlc.MaxTimestamp, nil
	}
	typedExpireExpr, err := expireExpr.TypeCheck(semaCtx, types.String)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	if !tree.IsConst(evalCtx, typedExpireExpr) {
		return hlc.Timestamp{}, errors.Errorf("SPLIT AT: only constant expressions are allowed for expiration")
	}
	d, err := typedExpireExpr.Eval(evalCtx)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	stmtTimestamp := evalCtx.GetStmtTimestamp()
	ts, err := tree.DatumToHLC(evalCtx, stmtTimestamp, d)
	if err != nil {
		return ts, errors.Wrap(err, "SPLIT AT")
	}
	if ts.GoTime().Before(stmtTimestamp) {
		return ts, errors.Errorf("SPLIT AT: expiration time should be greater than or equal to current time")
	}
	return ts, nil
}
