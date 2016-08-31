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
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/pkg/errors"
)

// Split executes a KV split.
// Privileges: INSERT on table.
func (p *planner) Split(n *parser.Split) (planNode, error) {
	var index sqlbase.IndexDescriptor
	var tableDesc *sqlbase.TableDescriptor
	if n.Index == nil {
		tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
		if err != nil {
			return nil, err
		}
		tableDesc, err = p.getTableDesc(tn)
		if err != nil {
			return nil, err
		}
		index = tableDesc.PrimaryIndex
	} else {
		tn, err := n.Index.Table.NormalizeWithDatabaseName(p.session.Database)
		if err != nil {
			return nil, err
		}
		tableDesc, err = p.getTableDesc(tn)
		if err != nil {
			return nil, err
		}
		normIdxName := sqlbase.NormalizeName(n.Index.Index)
		status, i, err := tableDesc.FindIndexByNormalizedName(normIdxName)
		if err != nil {
			return nil, err
		}
		if status != sqlbase.DescriptorActive {
			return nil, errors.Errorf("unknown index %s", normIdxName)
		}
		index = tableDesc.Indexes[i]
	}
	if err := p.checkPrivilege(tableDesc, privilege.INSERT); err != nil {
		return nil, err
	}
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
		typedExpr, err := p.analyzeExpr(expr, nil, nil, desired, true, "SPLIT AT")
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

func (n *splitNode) Start() error {
	values := make([]parser.Datum, len(n.exprs))
	colMap := make(map[sqlbase.ColumnID]int)
	for i, e := range n.exprs {
		if err := n.p.startSubqueryPlans(e); err != nil {
			return err
		}
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
	return n.p.execCfg.DB.AdminSplit(n.key)
}

func (n *splitNode) expandPlan() error {
	for _, e := range n.exprs {
		if err := n.p.expandSubqueryPlans(e); err != nil {
			return err
		}
	}
	return nil
}

func (n *splitNode) Next() (bool, error) {
	return n.key != nil, nil
}

func (n *splitNode) Values() parser.DTuple {
	k := n.key
	n.key = nil
	return parser.DTuple{
		parser.NewDBytes(parser.DBytes(k)),
		parser.NewDString(keys.PrettyPrint(k)),
	}
}

func (*splitNode) Columns() []ResultColumn {
	return []ResultColumn{
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

func (*splitNode) Ordering() orderingInfo              { return orderingInfo{} }
func (*splitNode) ExplainTypes(_ func(string, string)) {}
func (*splitNode) SetLimitHint(_ int64, _ bool)        {}
func (*splitNode) MarkDebug(_ explainMode)             {}

func (*splitNode) ExplainPlan(_ bool) (name, description string, children []planNode) {
	return "empty", "-", nil
}

func (n *splitNode) DebugValues() debugValues {
	return debugValues{
		rowIdx: 0,
		key:    "",
		value:  parser.DNull.String(),
		output: debugValueRow,
	}
}
