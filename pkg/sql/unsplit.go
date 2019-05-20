// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

type unsplitNode struct {
	optColumnsSlot

	tableDesc *sqlbase.TableDescriptor
	index     *sqlbase.IndexDescriptor
	rows      planNode
	run       unsplitRun
}

// Unsplit executes a KV unsplit.
// Privileges: INSERT on table.
func (p *planner) Unsplit(ctx context.Context, n *tree.Unsplit) (planNode, error) {
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

	// Create the plan for the unsplit rows source.
	rows, err := p.newPlan(ctx, n.Rows, desiredTypes)
	if err != nil {
		return nil, err
	}

	cols := planColumns(rows)
	if len(cols) == 0 {
		return nil, errors.Errorf("no columns in UNSPLIT AT data")
	}
	if len(cols) > len(index.ColumnIDs) {
		return nil, errors.Errorf("too many columns in UNSPLIT AT data")
	}
	for i := range cols {
		if !cols[i].Typ.Equivalent(desiredTypes[i]) {
			return nil, errors.Errorf(
				"UNSPLIT AT data column %d (%s) must be of type %s, not type %s",
				i+1, index.ColumnNames[i], desiredTypes[i], cols[i].Typ,
			)
		}
	}

	return &unsplitNode{
		tableDesc: tableDesc.TableDesc(),
		index:     index,
		rows:      rows,
	}, nil
}

var unsplitNodeColumns = sqlbase.ResultColumns{
	{
		Name: "key",
		Typ:  types.Bytes,
	},
	{
		Name: "pretty",
		Typ:  types.String,
	},
}

// unsplitRun contains the run-time state of unsplitNode during local execution.
type unsplitRun struct {
	lastUnsplitKey []byte
}

func (n *unsplitNode) startExec(params runParams) error {
	stickyBitEnabled := params.EvalContext().Settings.Version.IsActive(cluster.VersionStickyBit)
	// TODO(jeffreyxiao): Remove this error in v20.1.
	if !stickyBitEnabled {
		return pgerror.Newf(pgerror.CodeObjectNotInPrerequisiteStateError,
			`UNSPLIT AT requires all nodes to be upgraded to %s`,
			cluster.VersionByKey(cluster.VersionCreateStats),
		)
	}
	return nil
}

func (n *unsplitNode) Next(params runParams) (bool, error) {
	if ok, err := n.rows.Next(params); err != nil || !ok {
		return ok, err
	}

	row := n.rows.Values()
	rowKey, err := getRowKey(n.tableDesc, n.index, row)
	if err != nil {
		return false, err
	}

	if err := params.extendedEvalCtx.ExecCfg.DB.AdminUnsplit(params.ctx, rowKey); err != nil {
		ctx := tree.NewFmtCtx(tree.FmtSimple)
		row.Format(ctx)
		return false, pgerror.Wrapf(err, pgerror.CodeDataExceptionError, "could not UNSPLIT AT %s", ctx)
	}

	n.run.lastUnsplitKey = rowKey

	return true, nil
}

func (n *unsplitNode) Values() tree.Datums {
	return tree.Datums{
		tree.NewDBytes(tree.DBytes(n.run.lastUnsplitKey)),
		tree.NewDString(keys.PrettyPrint(nil /* valDirs */, n.run.lastUnsplitKey)),
	}
}

func (n *unsplitNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}
