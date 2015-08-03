// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
)

// planner is the centerpiece of SQL statement execution combining session
// state and database state with the logic for SQL execution.
type planner struct {
	db      *client.DB
	session Session
}

// makePlan creates the query plan for a single SQL statement. The returned
// plan needs to be iterated over using planNode.Next() and planNode.Values()
// in order to retrieve matching rows.
func (p *planner) makePlan(stmt parser.Statement) (planNode, error) {
	switch n := stmt.(type) {
	case *parser.CreateDatabase:
		return p.CreateDatabase(n)
	case *parser.CreateTable:
		return p.CreateTable(n)
	case *parser.Delete:
		return p.Delete(n)
	case *parser.Grant:
		return p.Grant(n)
	case *parser.Insert:
		return p.Insert(n)
	case *parser.ParenSelect:
		return p.makePlan(n.Select)
	case *parser.Revoke:
		return p.Revoke(n)
	case *parser.Select:
		return p.Select(n)
	case *parser.Set:
		return p.Set(n)
	case *parser.ShowColumns:
		return p.ShowColumns(n)
	case *parser.ShowDatabases:
		return p.ShowDatabases(n)
	case *parser.ShowGrants:
		return p.ShowGrants(n)
	case *parser.ShowIndex:
		return p.ShowIndex(n)
	case *parser.ShowTables:
		return p.ShowTables(n)
	case *parser.Truncate:
		return p.Truncate(n)
	case *parser.Update:
		return p.Update(n)
	case parser.Values:
		return p.Values(n)
	default:
		return nil, fmt.Errorf("unknown statement type: %T", stmt)
	}
}

func (p *planner) getAliasedTableDesc(n parser.TableExpr) (*structured.TableDescriptor, error) {
	ate, ok := n.(*parser.AliasedTableExpr)
	if !ok {
		return nil, util.Errorf("TODO(pmattis): unsupported FROM: %s", n)
	}
	table, ok := ate.Expr.(*parser.QualifiedName)
	if !ok {
		return nil, util.Errorf("TODO(pmattis): unsupported FROM: %s", n)
	}
	return p.getTableDesc(table)
}

func (p *planner) normalizeTableName(qname *parser.QualifiedName) error {
	if qname == nil || qname.Base == "" {
		return fmt.Errorf("empty table name: %s", qname)
	}
	if len(qname.Indirect) == 0 {
		if p.session.Database == "" {
			return fmt.Errorf("no database specified")
		}
		qname.Indirect = append(qname.Indirect, parser.NameIndirection(qname.Base))
		qname.Base = parser.Name(p.session.Database)
	}
	return nil
}

// planNode defines the interface for executing a query or portion of a query.
type planNode interface {
	// Columns returns the column names. The length of the returned slice is
	// guaranteed to be equal to the length of the tuple returned by Values().
	Columns() []string
	// Values returns the values at the current row.
	Values() parser.DTuple
	// Next advances to the next row, returning false if an error is encountered
	// or if there is no next row.
	Next() bool
	// Err returns the error, if any, encountered during iteration.
	Err() error
}

var _ planNode = &scanNode{}
var _ planNode = &valuesNode{}

// TODO(pmattis): orderByNode, groupByNode, joinNode.
