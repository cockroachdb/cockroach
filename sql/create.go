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
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/log"
)

// CreateDatabase creates a database.
// Privileges: "root" user.
//   Notes: postgres requires superuser or "CREATEDB".
//          mysql uses the mysqladmin command.
func (p *planner) CreateDatabase(n *parser.CreateDatabase) (planNode, error) {
	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	if p.user != security.RootUser {
		return nil, fmt.Errorf("only %s is allowed to create databases", security.RootUser)
	}

	desc := makeDatabaseDesc(n)

	if err := p.createDescriptor(databaseKey{string(n.Name)}, &desc, n.IfNotExists); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}

// CreateIndex creates an index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) CreateIndex(n *parser.CreateIndex) (planNode, error) {
	tableDesc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}

	if _, err := tableDesc.FindIndexByName(string(n.Name)); err == nil {
		if n.IfNotExists {
			// Noop.
			return &valuesNode{}, nil
		}
		return nil, fmt.Errorf("index %q already exists", string(n.Name))
	}

	if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	indexDesc := IndexDescriptor{
		Name:             string(n.Name),
		Unique:           n.Unique,
		ColumnNames:      n.Columns,
		StoreColumnNames: n.Storing,
	}
	tableDesc.Indexes = append(tableDesc.Indexes, indexDesc)

	if err := tableDesc.AllocateIDs(); err != nil {
		return nil, err
	}

	// `indexDesc` changed on us when we called `tableDesc.AllocateIDs()`.
	indexDesc = tableDesc.Indexes[len(tableDesc.Indexes)-1]

	// Get all the rows affected.
	// TODO(vivek): Avoid going through Select.
	// TODO(tamird): Support partial indexes?
	row, err := p.Select(&parser.Select{
		Exprs: parser.SelectExprs{parser.StarSelectExpr()},
		From:  parser.TableExprs{&parser.AliasedTableExpr{Expr: n.Table}},
	})
	if err != nil {
		return nil, err
	}

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex := map[ColumnID]int{}
	for i, name := range row.Columns() {
		c, err := tableDesc.FindColumnByName(name)
		if err != nil {
			return nil, err
		}
		colIDtoRowIndex[c.ID] = i
	}

	// TODO(tamird): This will fall down in production use. We need to do
	// something better (see #2036). In particular, this implementation
	// has the following problems:
	// - Very large tables will generate an enormous batch here. This
	// isn't really a problem in itself except that it will exacerbate
	// the other issue:
	// - Any non-quiescent table that this runs against will end up with
	// an inconsistent index. This is because as inserts/updates continue
	// to roll in behind this operation's read front, the written index
	// will become incomplete/stale before it's written.
	var b client.Batch
	b.Put(MakeDescMetadataKey(tableDesc.GetID()), tableDesc)

	for row.Next() {
		rowVals := row.Values()

		secondaryIndexEntries, err := encodeSecondaryIndexes(
			tableDesc.ID, []IndexDescriptor{indexDesc}, colIDtoRowIndex, rowVals)
		if err != nil {
			return nil, err
		}

		for _, secondaryIndexEntry := range secondaryIndexEntries {
			if log.V(2) {
				log.Infof("CPut %q -> %v", secondaryIndexEntry.key, secondaryIndexEntry.value)
			}
			b.CPut(secondaryIndexEntry.key, secondaryIndexEntry.value, nil)
		}
	}

	if err := row.Err(); err != nil {
		return nil, err
	}

	// Mark transaction as operating on the system DB.
	p.txn.SetSystemDBTrigger()

	if err := p.txn.Run(&b); err != nil {
		return nil, convertBatchError(tableDesc, b, err)
	}

	return &valuesNode{}, nil
}

// CreateTable creates a table.
// Privileges: CREATE on database.
//   Notes: postgres/mysql require CREATE on database.
func (p *planner) CreateTable(n *parser.CreateTable) (planNode, error) {
	if err := n.Table.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}

	dbDesc, err := p.getDatabaseDesc(n.Table.Database())
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	desc, err := makeTableDesc(n)
	if err != nil {
		return nil, err
	}
	// Inherit permissions from the database descriptor.
	desc.Privileges = dbDesc.GetPrivileges()

	if err := desc.AllocateIDs(); err != nil {
		return nil, err
	}

	if err := p.createDescriptor(tableKey{dbDesc.ID, n.Table.Table()}, &desc, n.IfNotExists); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}
