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
// Author: XisiHuang (cockhuangxh@163.com)

package sql

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
)

var (
	errEmptyColumnName = errors.New("empty column name")
	errEmptyIndexName  = errors.New("empty index name")
	errEmptyTableName  = errors.New("empty table name")
)

// RenameDatabase renames the database.
// Privileges: "root" user.
//   Notes: postgres requires superuser, db owner, or "CREATEDB".
//          mysql >= 5.1.23 does not allow database renames.
func (p *planner) RenameDatabase(n *parser.RenameDatabase) (planNode, error) {
	if n.Name == "" || n.NewName == "" {
		return nil, errEmptyDatabaseName
	}

	if p.user != security.RootUser {
		return nil, fmt.Errorf("only %s is allowed to rename databases", security.RootUser)
	}

	dbDesc, err := p.getDatabaseDesc(string(n.Name))
	if err != nil {
		return nil, err
	}

	if n.Name == n.NewName {
		// Noop.
		return &valuesNode{}, nil
	}

	// Now update the nameMetadataKey and the descriptor.
	descKey := MakeDescMetadataKey(dbDesc.GetID())
	dbDesc.SetName(string(n.NewName))

	if err := dbDesc.Validate(); err != nil {
		return nil, err
	}

	b := client.Batch{}
	b.CPut(databaseKey{string(n.NewName)}.Key(), dbDesc.GetID(), nil)
	b.Put(descKey, dbDesc)
	b.Del(databaseKey{string(n.Name)}.Key())

	// Mark transaction as operating on the system DB.
	p.txn.SetSystemDBTrigger()
	if err := p.txn.Run(&b); err != nil {
		if _, ok := err.(*proto.ConditionFailedError); ok {
			return nil, fmt.Errorf("the new database name %q already exists", string(n.NewName))
		}
		return nil, err
	}

	return &valuesNode{}, nil
}

// RenameTable renames the table.
// Privileges: DROP on source table, CREATE on destination database.
//   Notes: postgres requires the table owner.
//          mysql requires ALTER, DROP on the original table, and CREATE, INSERT
//          on the new table (and does not copy privileges over).
func (p *planner) RenameTable(n *parser.RenameTable) (planNode, error) {
	if err := n.NewName.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}

	if n.NewName.Table() == "" {
		return nil, errEmptyTableName
	}

	if err := n.Name.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}

	dbDesc, err := p.getDatabaseDesc(n.Name.Database())
	if err != nil {
		return nil, err
	}

	tbKey := tableKey{dbDesc.ID, string(n.Name.Table())}.Key()

	// Check if table exists.
	gr, err := p.txn.Get(tbKey)
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		if n.IfExists {
			// Noop.
			return &valuesNode{}, nil
		}
		// Key does not exist, but we want it to: error out.
		return nil, fmt.Errorf("table %q does not exist", n.Name.Table())
	}

	targetDbDesc, err := p.getDatabaseDesc(n.NewName.Database())
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(targetDbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	if n.Name.Database() == n.NewName.Database() && n.Name.Table() == n.NewName.Table() {
		// Noop.
		return &valuesNode{}, nil
	}

	tableDesc, err := p.getTableDesc(n.Name)
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(tableDesc, privilege.DROP); err != nil {
		return nil, err
	}

	tableDesc.SetName(n.NewName.Table())

	newTbKey := tableKey{targetDbDesc.ID, n.NewName.Table()}.Key()
	descKey := MakeDescMetadataKey(tableDesc.GetID())

	if err := tableDesc.Validate(); err != nil {
		return nil, err
	}

	b := client.Batch{}
	b.Put(descKey, tableDesc)
	b.CPut(newTbKey, tableDesc.GetID(), nil)
	b.Del(tbKey)
	// Mark transaction as operating on the system DB.
	p.txn.SetSystemDBTrigger()
	if err := p.txn.Run(&b); err != nil {
		if _, ok := err.(*proto.ConditionFailedError); ok {
			return nil, fmt.Errorf("table name %q already exists", n.NewName.Table())
		}
		return nil, err
	}

	return &valuesNode{}, nil
}

// RenameIndex renames the index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameIndex(n *parser.RenameIndex) (planNode, error) {
	newIdxName := string(n.NewName)
	if newIdxName == "" {
		return nil, errEmptyIndexName
	}

	if err := n.Name.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}

	tableDesc, err := p.getTableDesc(n.Name)
	if err != nil {
		return nil, err
	}

	idxName := n.Name.Index()
	idx, err := tableDesc.FindIndexByName(idxName)
	if err != nil {
		if n.IfExists {
			// Noop.
			return &valuesNode{}, nil
		}
		// index does not exist, but we want it to: error out.
		return nil, fmt.Errorf("index %q does not exist", idxName)
	}

	if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	if equalName(idxName, newIdxName) {
		// Noop.
		return &valuesNode{}, nil
	}

	if _, err := tableDesc.FindIndexByName(newIdxName); err == nil {
		return nil, fmt.Errorf("index name %q already exists", n.NewName)
	}

	idx.Name = newIdxName
	descKey := MakeDescMetadataKey(tableDesc.GetID())
	if err := tableDesc.Validate(); err != nil {
		return nil, err
	}
	if err := p.txn.Put(descKey, tableDesc); err != nil {
		return nil, err
	}
	// Mark transaction as operating on the system DB.
	p.txn.SetSystemDBTrigger()

	return &valuesNode{}, nil
}

// RenameColumn renames the column.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameColumn(n *parser.RenameColumn) (planNode, error) {
	newColName := string(n.NewName)
	if newColName == "" {
		return nil, errEmptyColumnName
	}

	if err := n.Table.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}

	dbDesc, err := p.getDatabaseDesc(n.Table.Database())
	if err != nil {
		return nil, err
	}

	// Check if table exists.
	tbKey := tableKey{dbDesc.ID, n.Table.Table()}.Key()
	gr, err := p.txn.Get(tbKey)
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		if n.IfExists {
			// Noop.
			return &valuesNode{}, nil
		}
		// Key does not exist, but we want it to: error out.
		return nil, fmt.Errorf("table %q does not exist", n.Table.Table())
	}

	tableDesc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}

	colName := string(n.Name)
	column, err := tableDesc.FindColumnByName(colName)
	// n.IfExists only applies to table, no need to check here.
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	if equalName(colName, newColName) {
		// Noop.
		return &valuesNode{}, nil
	}

	if _, err := tableDesc.FindColumnByName(newColName); err == nil {
		return nil, fmt.Errorf("column name %q already exists", newColName)
	}

	for _, idx := range tableDesc.Indexes {
		for i, id := range idx.ColumnIDs {
			if id == column.ID {
				idx.ColumnNames[i] = newColName
			}
		}
	}
	column.Name = newColName

	descKey := MakeDescMetadataKey(tableDesc.GetID())
	if err := tableDesc.Validate(); err != nil {
		return nil, err
	}
	if err := p.txn.Put(descKey, tableDesc); err != nil {
		return nil, err
	}
	// Mark transaction as operating on the system DB.
	p.txn.SetSystemDBTrigger()

	return &valuesNode{}, nil
}
