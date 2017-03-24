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
//
// Author: Matt Jibson

package sql

import (
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Split executes a KV split.
// Privileges: INSERT on table.
func (p *planner) Split(ctx context.Context, n *parser.Split) (planNode, error) {
	var tn *parser.TableName
	var err error
	if n.Table.TableNameReference != nil {
		// Variant: ALTER TABLE ... SPLIT AT ...
		tn, err = n.Table.NormalizeWithDatabaseName(p.session.Database)
	} else {
		// Variant: ALTER INDEX ... SPLIT AT ...
		tn, err = p.expandIndexName(ctx, &n.Index)
	}
	if err != nil {
		return nil, err
	}

	tableDesc, err := p.getTableDesc(ctx, tn)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, sqlbase.NewUndefinedTableError(tn.String())
	}
	if err := p.CheckPrivilege(tableDesc, privilege.INSERT); err != nil {
		return nil, err
	}

	// Determine which index to use.
	var index sqlbase.IndexDescriptor
	if n.Table.TableNameReference != nil {
		index = tableDesc.PrimaryIndex
	} else {
		normIdxName := n.Index.Index.Normalize()
		status, i, err := tableDesc.FindIndexByNormalizedName(normIdxName)
		if err != nil {
			return nil, err
		}
		if status != sqlbase.DescriptorActive {
			return nil, errors.Errorf("unknown index %s", normIdxName)
		}
		index = tableDesc.Indexes[i]
	}

	// Calculate the desired types for the select statement. It is OK if the
	// select statement returns fewer columns (the relevant prefix is used).
	desiredTypes := make([]parser.Type, len(index.ColumnIDs))
	for i, colID := range index.ColumnIDs {
		c, err := tableDesc.FindColumnByID(colID)
		if err != nil {
			return nil, err
		}
		desiredTypes[i] = c.Type.ToDatumType()
	}

	// Create the plan for the split rows source.
	rows, err := p.newPlan(ctx, n.Rows, desiredTypes, false /* auto commit */)
	if err != nil {
		return nil, err
	}

	cols := rows.Columns()
	if len(cols) == 0 {
		return nil, errors.Errorf("no columns in SPLIT AT data")
	}
	if len(cols) > len(index.ColumnIDs) {
		return nil, errors.Errorf("too many columns in SPLIT AT data")
	}
	for i := range cols {
		if !cols[i].Typ.Equivalent(desiredTypes[i]) {
			return nil, errors.Errorf(
				"SPLIT AT data column %d must be of type %s, not type %s",
				i+1, desiredTypes[i], cols[i].Typ,
			)
		}
	}

	return &splitNode{
		p:         p,
		tableDesc: tableDesc,
		index:     index,
		rows:      rows,
	}, nil
}

type splitNode struct {
	p            *planner
	tableDesc    *sqlbase.TableDescriptor
	index        sqlbase.IndexDescriptor
	rows         planNode
	lastSplitKey []byte
}

func (n *splitNode) Start(ctx context.Context) error {
	return n.rows.Start(ctx)
}

func (n *splitNode) Next(ctx context.Context) (bool, error) {
	// TODO(radu): instead of performing the splits sequentially, accumulate all
	// the split keys and then perform the splits in parallel (e.g. split at the
	// middle key and recursively to the left and right).

	if ok, err := n.rows.Next(ctx); err != nil || !ok {
		return ok, err
	}

	values := make([]parser.Datum, 0, len(n.index.ColumnIDs))
	values = append(values, n.rows.Values()...)
	for len(values) < len(n.index.ColumnIDs) {
		// It is acceptable if only a prefix of the columns are given values. We
		// could generate a partial key, but it's easier to just encode NULLs for
		// the rest of the columns.
		values = append(values, parser.DNull)
	}

	colMap := make(map[sqlbase.ColumnID]int)
	for i, colID := range n.index.ColumnIDs {
		colMap[colID] = i
	}
	prefix := sqlbase.MakeIndexKeyPrefix(n.tableDesc, n.index.ID)
	key, _, err := sqlbase.EncodeIndexKey(n.tableDesc, &n.index, colMap, values, prefix)
	if err != nil {
		return false, err
	}
	n.lastSplitKey = keys.MakeRowSentinelKey(key)

	if err := n.p.session.execCfg.DB.AdminSplit(ctx, n.lastSplitKey); err != nil {
		return false, err
	}

	return true, nil
}

func (n *splitNode) Values() parser.Datums {
	return parser.Datums{
		parser.NewDBytes(parser.DBytes(n.lastSplitKey)),
		parser.NewDString(keys.PrettyPrint(n.lastSplitKey)),
	}
}

func (n *splitNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}

func (*splitNode) Columns() ResultColumns {
	return ResultColumns{
		{
			Name: "key",
			Typ:  parser.TypeBytes,
		},
		{
			Name: "pretty",
			Typ:  parser.TypeString,
		},
	}
}

func (*splitNode) Ordering() orderingInfo  { return orderingInfo{} }
func (*splitNode) MarkDebug(_ explainMode) {}

func (n *splitNode) DebugValues() debugValues {
	return debugValues{
		rowIdx: 0,
		key:    "",
		value:  parser.DNull.String(),
		output: debugValueRow,
	}
}
