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
	if n.Index == nil {
		// Variant: ALTER TABLE ... SPLIT AT ...
		tn, err = n.Table.NormalizeWithDatabaseName(p.session.Database)
	} else {
		// Variant: ALTER INDEX ... SPLIT AT ...
		tn, err = p.expandIndexName(ctx, n.Index)
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
	if n.Index == nil {
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

	// Determine how to use the remaining argument expressions.
	if len(index.ColumnIDs) != len(n.Exprs) {
		return nil, errors.Errorf("expected %d expressions, got %d", len(index.ColumnIDs), len(n.Exprs))
	}
	typedExprs := make([]parser.TypedExpr, len(n.Exprs))
	for i, expr := range n.Exprs {
		c, err := tableDesc.FindColumnByID(index.ColumnIDs[i])
		if err != nil {
			return nil, err
		}
		desired := c.Type.ToDatumType()
		typedExpr, err := p.analyzeExpr(ctx, expr, nil, parser.IndexedVarHelper{}, desired, true, "SPLIT AT")
		if err != nil {
			return nil, err
		}
		typedExprs[i] = typedExpr
	}

	return &splitNode{
		p:         p,
		tableDesc: tableDesc,
		index:     index,
		exprs:     typedExprs,
	}, nil
}

type splitNode struct {
	p         *planner
	tableDesc *sqlbase.TableDescriptor
	index     sqlbase.IndexDescriptor
	exprs     []parser.TypedExpr
	key       []byte
}

func (n *splitNode) Start(ctx context.Context) error {
	values := make([]parser.Datum, len(n.exprs))
	colMap := make(map[sqlbase.ColumnID]int)
	for i, e := range n.exprs {
		val, err := e.Eval(&n.p.evalCtx)
		if err != nil {
			return err
		}
		values[i] = val
		c, err := n.tableDesc.FindColumnByID(n.index.ColumnIDs[i])
		if err != nil {
			return err
		}
		colMap[c.ID] = i
	}
	prefix := sqlbase.MakeIndexKeyPrefix(n.tableDesc, n.index.ID)
	key, _, err := sqlbase.EncodeIndexKey(n.tableDesc, &n.index, colMap, values, prefix)
	if err != nil {
		return err
	}
	n.key = keys.MakeRowSentinelKey(key)

	return n.p.execCfg.DB.AdminSplit(ctx, n.key)
}

func (n *splitNode) Next(context.Context) (bool, error) {
	return n.key != nil, nil
}

func (n *splitNode) Values() parser.Datums {
	k := n.key
	n.key = nil
	return parser.Datums{
		parser.NewDBytes(parser.DBytes(k)),
		parser.NewDString(keys.PrettyPrint(k)),
	}
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

func (*splitNode) Close(context.Context)   {}
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
