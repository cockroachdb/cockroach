// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
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

	expirationTime, err := parseExpirationTime(n.ExpireExpr, &p.semaCtx, p.EvalContext())
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
}

// splitRun contains the run-time state of splitNode during local execution.
type splitRun struct {
	lastSplitKey []byte
}

func (n *splitNode) startExec(params runParams) error {
	stickyBitEnabled := params.EvalContext().Settings.Version.IsActive(cluster.VersionStickyBit)
	// TODO(jeffreyxiao): Remove this error in v20.1.
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

	return true, nil
}

func (n *splitNode) Values() tree.Datums {
	return tree.Datums{
		tree.NewDBytes(tree.DBytes(n.run.lastSplitKey)),
		tree.NewDString(keys.PrettyPrint(nil /* valDirs */, n.run.lastSplitKey)),
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
	expireExpr tree.Expr, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext,
) (hlc.Timestamp, error) {
	if expireExpr == nil {
		return hlc.MaxTimestamp, nil
	}
	expirationTime := hlc.Timestamp{}
	stmtTimestamp := evalCtx.GetStmtTimestamp()
	typedExpireExpr, err := expireExpr.TypeCheck(semaCtx, types.String)
	if err != nil {
		return expirationTime, err
	}
	d, err := typedExpireExpr.Eval(evalCtx)
	if err != nil {
		return expirationTime, err
	}
	var convErr error
	switch d := d.(type) {
	case *tree.DString:
		s := string(*d)
		// Attempt to parse as timestamp.
		// The expiration time can be seen by the user, so use precision of microseconds.
		if dt, err := tree.ParseDTimestamp(evalCtx, s, time.Microsecond); err == nil {
			expirationTime.WallTime = dt.Time.UnixNano()
			break
		}
		// Attempt to parse as a decimal.
		if dec, _, err := apd.NewFromString(s); err == nil {
			expirationTime, convErr = tree.DecimalToHLC(dec)
			break
		}
		// Attempt to parse as an interval.
		if iv, err := tree.ParseDInterval(s); err == nil {
			expirationTime.WallTime = duration.Add(evalCtx, stmtTimestamp, iv.Duration).UnixNano()
			break
		}
		convErr = errors.Errorf("SPLIT AT: value is neither timestamp, decimal, nor interval")
	case *tree.DTimestamp:
		expirationTime.WallTime = d.UnixNano()
	case *tree.DTimestampTZ:
		expirationTime.WallTime = d.UnixNano()
	case *tree.DInt:
		expirationTime.WallTime = int64(*d)
	case *tree.DDecimal:
		expirationTime, convErr = tree.DecimalToHLC(&d.Decimal)
	case *tree.DInterval:
		expirationTime.WallTime = duration.Add(evalCtx, stmtTimestamp, d.Duration).UnixNano()
	default:
		convErr = errors.Errorf("SPLIT AT: expected timestamp, decimal, or interval, got %s (%T)", d.ResolvedType(), d)
	}
	if convErr != nil {
		return expirationTime, convErr
	}
	if expirationTime.Less(hlc.Timestamp{}) {
		return expirationTime, errors.Errorf("SPLIT AT: timestamp before 1970-01-01T00:00:00Z is invalid")
	}
	return expirationTime, nil
}
