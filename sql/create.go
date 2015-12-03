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

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
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
//          mysql requires INDEX on the table.
func (p *planner) CreateIndex(n *parser.CreateIndex) (planNode, error) {
	tableDesc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}

	status, i, err := tableDesc.FindIndexByName(string(n.Name))
	if err == nil {
		if status == DescriptorIncomplete {
			switch tableDesc.Mutations[i].Direction {
			case DescriptorMutation_DROP:
				return nil, fmt.Errorf("index %q being dropped, try again later", string(n.Name))

			case DescriptorMutation_ADD:
				// Noop, will fail in AllocateIDs below.
			}
		}
		if n.IfNotExists {
			// Noop.
			return &valuesNode{}, nil
		}
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

	tableDesc.addIndexMutation(indexDesc, DescriptorMutation_ADD)

	if err := tableDesc.AllocateIDs(); err != nil {
		return nil, err
	}

	if err := p.txn.Put(MakeDescMetadataKey(tableDesc.GetID()), wrapDescriptor(tableDesc)); err != nil {
		return nil, err
	}

	// Process mutation synchronously.
	if err := p.applyMutations(tableDesc); err != nil {
		return nil, err
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

	desc, err := makeTableDesc(n, dbDesc.ID)
	if err != nil {
		return nil, err
	}
	// Inherit permissions from the database descriptor.
	desc.Privileges = dbDesc.GetPrivileges()

	if len(desc.PrimaryIndex.ColumnNames) == 0 {
		// Ensure a Primary Key exists.
		s := "experimental_unique_int()"
		col := ColumnDescriptor{
			Name: "rowid",
			Type: ColumnType{
				Kind: ColumnType_INT,
			},
			DefaultExpr: &s,
		}
		desc.AddColumn(col)
		idx := IndexDescriptor{
			Unique:      true,
			ColumnNames: []string{col.Name},
		}
		if err := desc.AddIndex(idx, true); err != nil {
			return nil, err
		}
	}

	if err := desc.AllocateIDs(); err != nil {
		return nil, err
	}

	if err := p.createDescriptor(tableKey{dbDesc.ID, n.Table.Table()}, &desc, n.IfNotExists); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}
