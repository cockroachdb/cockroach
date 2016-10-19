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
	"bytes"
	"fmt"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/pkg/errors"
)

const maxSplitRetries = 4

// Split executes a KV split.
// Privileges: INSERT on table.
func (p *planner) Split(n *parser.Split) (planNode, error) {
	var tableName parser.NormalizableTableName
	if n.Index == nil {
		tableName = n.Table
	} else {
		tableName = n.Index.Table
	}

	// Check that the table exists and that the user has permission.
	tn, err := tableName.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	if n.Index != nil && n.Index.SearchTable {
		realTableName, err := p.findTableContainingIndex(tn.DatabaseName, tn.TableName)
		if err != nil {
			return nil, err
		}
		n.Index.Index = tn.TableName
		tn = realTableName
	}

	tableDesc, err := p.getTableDesc(tn)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, sqlbase.NewUndefinedTableError(tn.String())
	}
	if err := p.checkPrivilege(tableDesc, privilege.INSERT); err != nil {
		return nil, err
	}

	// Determine which index to use.
	var index sqlbase.IndexDescriptor
	if n.Index == nil {
		index = tableDesc.PrimaryIndex
	} else {
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
		typedExpr, err := p.analyzeExpr(expr, nil, parser.IndexedVarHelper{}, desired, true, "SPLIT AT")
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

	// We retry a few times if we hit a "conflict updating range descriptors"
	// error; this can happen if we try to split right after table creation.
	// TODO(radu): we should find a way to prevent this error, like waiting for
	// whatever condition we need to wait.
	for r := retry.Start(retry.Options{MaxRetries: maxSplitRetries}); ; {
		err := n.p.execCfg.DB.AdminSplit(context.TODO(), n.key)
		if err != nil &&
			strings.Contains(err.Error(), storage.ErrMsgConflictUpdatingRangeDesc) &&
			r.Next() {
			continue
		}
		return err
	}
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

func (*splitNode) Close()                       {}
func (*splitNode) Ordering() orderingInfo       { return orderingInfo{} }
func (*splitNode) SetLimitHint(_ int64, _ bool) {}
func (*splitNode) MarkDebug(_ explainMode)      {}

func (n *splitNode) ExplainPlan(_ bool) (name, description string, children []planNode) {
	var buf bytes.Buffer
	for i, e := range n.exprs {
		if i > 0 {
			buf.WriteString(", ")
		}
		e.Format(&buf, parser.FmtSimple)
		children = n.p.collectSubqueryPlans(e, children)
	}
	return "split", buf.String(), children
}

func (n *splitNode) ExplainTypes(regTypes func(string, string)) {
	for i, expr := range n.exprs {
		regTypes(fmt.Sprintf("expr %d", i), parser.AsStringWithFlags(expr, parser.FmtShowTypes))
	}
}

func (n *splitNode) DebugValues() debugValues {
	return debugValues{
		rowIdx: 0,
		key:    "",
		value:  parser.DNull.String(),
		output: debugValueRow,
	}
}
