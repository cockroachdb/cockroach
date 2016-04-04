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
// Author: XisiHuang (cockhuangxh@163.com)

package sql

import (
	"errors"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
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
// Privileges: security.RootUser user.
//   Notes: postgres requires superuser, db owner, or "CREATEDB".
//          mysql >= 5.1.23 does not allow database renames.
func (p *planner) RenameDatabase(n *parser.RenameDatabase) (planNode, *roachpb.Error) {
	if n.Name == "" || n.NewName == "" {
		return nil, roachpb.NewError(errEmptyDatabaseName)
	}

	if p.session.User != security.RootUser {
		return nil, roachpb.NewUErrorf("only %s is allowed to rename databases", security.RootUser)
	}

	dbDesc, pErr := p.getDatabaseDesc(string(n.Name))
	if pErr != nil {
		return nil, pErr
	}

	if n.Name == n.NewName {
		// Noop.
		return &emptyNode{}, nil
	}

	// Now update the nameMetadataKey and the descriptor.
	descKey := MakeDescMetadataKey(dbDesc.GetID())
	dbDesc.SetName(string(n.NewName))

	if err := dbDesc.Validate(); err != nil {
		return nil, roachpb.NewError(err)
	}

	newKey := databaseKey{string(n.NewName)}.Key()
	oldKey := databaseKey{string(n.Name)}.Key()
	descID := dbDesc.GetID()
	descDesc := wrapDescriptor(dbDesc)

	b := client.Batch{}
	b.CPut(newKey, descID, nil)
	b.Put(descKey, descDesc)
	b.Del(oldKey)

	if pErr := p.txn.Run(&b); pErr != nil {
		if _, ok := pErr.GetDetail().(*roachpb.ConditionFailedError); ok {
			return nil, roachpb.NewUErrorf("the new database name %q already exists", string(n.NewName))
		}
		return nil, pErr
	}

	p.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		if err := expectDescriptorID(systemConfig, newKey, descID); err != nil {
			return err
		}
		if err := expectDescriptor(systemConfig, descKey, descDesc); err != nil {
			return err
		}
		return expectDeleted(systemConfig, oldKey)
	})

	return &emptyNode{}, nil
}

// RenameTable renames the table.
// Privileges: DROP on source table, CREATE on destination database.
//   Notes: postgres requires the table owner.
//          mysql requires ALTER, DROP on the original table, and CREATE, INSERT
//          on the new table (and does not copy privileges over).
func (p *planner) RenameTable(n *parser.RenameTable) (planNode, *roachpb.Error) {
	if err := n.NewName.NormalizeTableName(p.session.Database); err != nil {
		return nil, roachpb.NewError(err)
	}

	if n.NewName.Table() == "" {
		return nil, roachpb.NewError(errEmptyTableName)
	}

	if err := n.Name.NormalizeTableName(p.session.Database); err != nil {
		return nil, roachpb.NewError(err)
	}

	dbDesc, pErr := p.getDatabaseDesc(n.Name.Database())
	if pErr != nil {
		return nil, pErr
	}

	tbKey := tableKey{dbDesc.ID, n.Name.Table()}.Key()

	// Check if table exists.
	gr, pErr := p.txn.Get(tbKey)
	if pErr != nil {
		return nil, pErr
	}
	if !gr.Exists() {
		if n.IfExists {
			// Noop.
			return &emptyNode{}, nil
		}
		// Key does not exist, but we want it to: error out.
		return nil, roachpb.NewUErrorf("table %q does not exist", n.Name.Table())
	}

	targetDbDesc, pErr := p.getDatabaseDesc(n.NewName.Database())
	if pErr != nil {
		return nil, pErr
	}

	if err := p.checkPrivilege(targetDbDesc, privilege.CREATE); err != nil {
		return nil, roachpb.NewError(err)
	}

	if n.Name.Database() == n.NewName.Database() && n.Name.Table() == n.NewName.Table() {
		// Noop.
		return &emptyNode{}, nil
	}

	tableDesc, pErr := p.getTableDesc(n.Name)
	if pErr != nil {
		return nil, pErr
	}

	if err := p.checkPrivilege(&tableDesc, privilege.DROP); err != nil {
		return nil, roachpb.NewError(err)
	}

	tableDesc.SetName(n.NewName.Table())
	tableDesc.ParentID = targetDbDesc.ID

	descKey := MakeDescMetadataKey(tableDesc.GetID())
	newTbKey := tableKey{targetDbDesc.ID, n.NewName.Table()}.Key()

	if err := tableDesc.Validate(); err != nil {
		return nil, roachpb.NewError(err)
	}

	descID := tableDesc.GetID()
	descDesc := wrapDescriptor(&tableDesc)

	b := client.Batch{}
	b.Put(descKey, descDesc)
	b.CPut(newTbKey, descID, nil)
	b.Del(tbKey)

	if pErr := p.txn.Run(&b); pErr != nil {
		if _, ok := pErr.GetDetail().(*roachpb.ConditionFailedError); ok {
			return nil, roachpb.NewUErrorf("table name %q already exists", n.NewName.Table())
		}
		return nil, pErr
	}

	p.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		if err := expectDescriptorID(systemConfig, newTbKey, descID); err != nil {
			return err
		}
		if err := expectDescriptor(systemConfig, descKey, descDesc); err != nil {
			return err
		}
		return expectDeleted(systemConfig, tbKey)
	})

	return &emptyNode{}, nil
}

// RenameIndex renames the index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameIndex(n *parser.RenameIndex) (planNode, *roachpb.Error) {
	newIdxName := string(n.NewName)
	if newIdxName == "" {
		return nil, roachpb.NewError(errEmptyIndexName)
	}

	if err := n.Index.Table.NormalizeTableName(p.session.Database); err != nil {
		return nil, roachpb.NewError(err)
	}

	tableDesc, pErr := p.getTableDesc(n.Index.Table)
	if pErr != nil {
		return nil, pErr
	}

	idxName := string(n.Index.Index)
	status, i, err := tableDesc.FindIndexByName(idxName)
	if err != nil {
		if n.IfExists {
			// Noop.
			return &emptyNode{}, nil
		}
		// Index does not exist, but we want it to: error out.
		return nil, roachpb.NewError(err)
	}

	if err := p.checkPrivilege(&tableDesc, privilege.CREATE); err != nil {
		return nil, roachpb.NewError(err)
	}

	if equalName(idxName, newIdxName) {
		// Noop.
		return &emptyNode{}, nil
	}

	if _, _, err := tableDesc.FindIndexByName(newIdxName); err == nil {
		return nil, roachpb.NewUErrorf("index name %q already exists", n.NewName)
	}

	if status == DescriptorActive {
		tableDesc.Indexes[i].Name = newIdxName
	} else {
		tableDesc.Mutations[i].GetIndex().Name = newIdxName
	}

	tableDesc.UpVersion = true
	descKey := MakeDescMetadataKey(tableDesc.GetID())
	if err := tableDesc.Validate(); err != nil {
		return nil, roachpb.NewError(err)
	}
	if pErr := p.txn.Put(descKey, wrapDescriptor(&tableDesc)); pErr != nil {
		return nil, pErr
	}
	p.notifySchemaChange(tableDesc.ID, invalidMutationID)
	return &emptyNode{}, nil
}

// RenameColumn renames the column.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameColumn(n *parser.RenameColumn) (planNode, *roachpb.Error) {
	newColName := string(n.NewName)
	if newColName == "" {
		return nil, roachpb.NewError(errEmptyColumnName)
	}

	if err := n.Table.NormalizeTableName(p.session.Database); err != nil {
		return nil, roachpb.NewError(err)
	}

	dbDesc, pErr := p.getDatabaseDesc(n.Table.Database())
	if pErr != nil {
		return nil, pErr
	}

	// Check if table exists.
	tbKey := tableKey{dbDesc.ID, n.Table.Table()}.Key()
	gr, pErr := p.txn.Get(tbKey)
	if pErr != nil {
		return nil, pErr
	}
	if !gr.Exists() {
		if n.IfExists {
			// Noop.
			return &emptyNode{}, nil
		}
		// Key does not exist, but we want it to: error out.
		return nil, roachpb.NewUErrorf("table %q does not exist", n.Table.Table())
	}

	tableDesc, pErr := p.getTableDesc(n.Table)
	if pErr != nil {
		return nil, pErr
	}

	colName := string(n.Name)
	status, i, err := tableDesc.FindColumnByName(colName)
	// n.IfExists only applies to table, no need to check here.
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	var column *ColumnDescriptor
	if status == DescriptorActive {
		column = &tableDesc.Columns[i]
	} else {
		column = tableDesc.Mutations[i].GetColumn()
	}

	if err := p.checkPrivilege(&tableDesc, privilege.CREATE); err != nil {
		return nil, roachpb.NewError(err)
	}

	if equalName(colName, newColName) {
		// Noop.
		return &emptyNode{}, nil
	}

	if _, _, err := tableDesc.FindColumnByName(newColName); err == nil {
		return nil, roachpb.NewUErrorf("column name %q already exists", newColName)
	}

	// Rename the column in the indexes.
	renameColumnInIndex := func(idx *IndexDescriptor) {
		for i, id := range idx.ColumnIDs {
			if id == column.ID {
				idx.ColumnNames[i] = newColName
			}
		}
	}
	for i := range tableDesc.Indexes {
		renameColumnInIndex(&tableDesc.Indexes[i])
	}
	for _, m := range tableDesc.Mutations {
		if idx := m.GetIndex(); idx != nil {
			renameColumnInIndex(idx)
		}
	}
	column.Name = newColName
	tableDesc.UpVersion = true

	descKey := MakeDescMetadataKey(tableDesc.GetID())
	if err := tableDesc.Validate(); err != nil {
		return nil, roachpb.NewError(err)
	}
	if pErr := p.txn.Put(descKey, wrapDescriptor(&tableDesc)); pErr != nil {
		return nil, pErr
	}
	p.notifySchemaChange(tableDesc.ID, invalidMutationID)
	return &emptyNode{}, nil
}
