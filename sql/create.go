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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
)

// CreateDatabase creates a database.
// Privileges: "root" user.
//   Notes: postgres requires superuser or "CREATEDB".
//          mysql uses the mysqladmin command.
func (p *planner) CreateDatabase(n *parser.CreateDatabase) (planNode, *roachpb.Error) {
	if n.Name == "" {
		return nil, roachpb.NewError(errEmptyDatabaseName)
	}

	if p.user != security.RootUser {
		return nil, roachpb.NewUErrorf("only %s is allowed to create databases", security.RootUser)
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
func (p *planner) CreateIndex(n *parser.CreateIndex) (planNode, *roachpb.Error) {
	tableDesc, pErr := p.getTableDesc(n.Table)
	if pErr != nil {
		return nil, pErr
	}

	status, i, err := tableDesc.FindIndexByName(string(n.Name))
	if err == nil {
		if status == DescriptorIncomplete {
			switch tableDesc.Mutations[i].Direction {
			case DescriptorMutation_DROP:
				return nil, roachpb.NewUErrorf("index %q being dropped, try again later", string(n.Name))

			case DescriptorMutation_ADD:
				// Noop, will fail in AllocateIDs below.
			}
		}
		if n.IfNotExists {
			// Noop.
			return &valuesNode{}, nil
		}
	}

	if pErr := p.checkPrivilege(tableDesc, privilege.CREATE); pErr != nil {
		return nil, pErr
	}

	indexDesc := IndexDescriptor{
		Name:             string(n.Name),
		Unique:           n.Unique,
		StoreColumnNames: n.Storing,
	}
	if pErr := indexDesc.fillColumns(n.Columns); pErr != nil {
		return nil, pErr
	}

	tableDesc.addIndexMutation(indexDesc, DescriptorMutation_ADD)
	tableDesc.UpVersion = true
	mutationID := tableDesc.NextMutationID
	tableDesc.NextMutationID++
	if pErr := tableDesc.AllocateIDs(); pErr != nil {
		return nil, pErr
	}

	if pErr := p.txn.Put(MakeDescMetadataKey(tableDesc.GetID()), wrapDescriptor(tableDesc)); pErr != nil {
		return nil, pErr
	}
	p.notifySchemaChange(tableDesc.ID, mutationID)

	return &valuesNode{}, nil
}

// CreateTable creates a table.
// Privileges: CREATE on database.
//   Notes: postgres/mysql require CREATE on database.
func (p *planner) CreateTable(n *parser.CreateTable) (planNode, *roachpb.Error) {
	if err := n.Table.NormalizeTableName(p.session.Database); err != nil {
		return nil, roachpb.NewError(err)
	}

	dbDesc, pErr := p.getDatabaseDesc(n.Table.Database())
	if pErr != nil {
		return nil, pErr
	}

	if pErr := p.checkPrivilege(dbDesc, privilege.CREATE); pErr != nil {
		return nil, pErr
	}

	desc, pErr := makeTableDesc(n, dbDesc.ID)
	if pErr != nil {
		return nil, pErr
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
			Hidden:      true,
		}
		desc.AddColumn(col)
		idx := IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{col.Name},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		}
		if pErr := desc.AddIndex(idx, true); pErr != nil {
			return nil, pErr
		}
	}

	if pErr := desc.AllocateIDs(); pErr != nil {
		return nil, pErr
	}

	if pErr := p.createDescriptor(tableKey{dbDesc.ID, n.Table.Table()}, &desc, n.IfNotExists); pErr != nil {
		return nil, pErr
	}
	return &valuesNode{}, nil
}
