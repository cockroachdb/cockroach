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
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/pkg/errors"
)

// Split executes a KV split.
func (p *planner) Split(n *parser.Split) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}
	tableDesc, err := p.getTableDesc(tn)
	if err != nil {
		return nil, err
	}
	pi := tableDesc.PrimaryIndex
	if len(pi.ColumnIDs) != len(n.Exprs) {
		return nil, errors.Errorf("expected %d expressions, got %d", len(pi.ColumnIDs), len(n.Exprs))
	}
	values := make([]parser.Datum, len(n.Exprs))
	colMap := make(map[sqlbase.ColumnID]int)
	for i, e := range n.Exprs {
		c, err := tableDesc.FindColumnByID(pi.ColumnIDs[i])
		if err != nil {
			return nil, err
		}
		typ := c.Type.ToDatumType()
		te, err := e.TypeCheck(&p.semaCtx, typ)
		if err != nil {
			return nil, err
		}
		d, err := te.Eval(&p.evalCtx)
		if err != nil {
			return nil, err
		}
		if !typ.TypeEqual(d) {
			return nil, errors.Errorf("expected type %s for column %d, got %s", typ.Type(), i+1, d.Type())
		}
		values[i] = d
		colMap[c.ID] = i
	}
	prefix := sqlbase.MakeIndexKeyPrefix(tableDesc, pi.ID)
	key, _, err := sqlbase.EncodeIndexKey(tableDesc, &pi, colMap, values, prefix)
	if err != nil {
		return nil, err
	}
	key = keys.MakeRowSentinelKey(key)
	err = p.execCfg.DB.AdminSplit(key)
	return &emptyNode{}, err
}
