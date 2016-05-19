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
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
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
func (p *planner) RenameDatabase(n *parser.RenameDatabase) (planNode, error) {
	if n.Name == "" || n.NewName == "" {
		return nil, errEmptyDatabaseName
	}

	if p.session.User != security.RootUser {
		return nil, fmt.Errorf("only %s is allowed to rename databases", security.RootUser)
	}

	dbDesc, err := p.getDatabaseDesc(string(n.Name))
	if err != nil {
		return nil, err
	}
	if dbDesc == nil {
		return nil, newUndefinedDatabaseError(string(n.Name))
	}

	if n.Name == n.NewName {
		// Noop.
		return &emptyNode{}, nil
	}

	// Now update the nameMetadataKey and the descriptor.
	descKey := sqlbase.MakeDescMetadataKey(dbDesc.GetID())
	dbDesc.SetName(string(n.NewName))

	if err := dbDesc.Validate(); err != nil {
		return nil, err
	}

	newKey := databaseKey{string(n.NewName)}.Key()
	oldKey := databaseKey{string(n.Name)}.Key()
	descID := dbDesc.GetID()
	descDesc := sqlbase.WrapDescriptor(dbDesc)

	b := client.Batch{}
	b.CPut(newKey, descID, nil)
	b.Put(descKey, descDesc)
	b.Del(oldKey)

	if err := p.txn.Run(&b); err != nil {
		if _, ok := err.(*roachpb.ConditionFailedError); ok {
			return nil, fmt.Errorf("the new database name %q already exists", string(n.NewName))
		}
		return nil, err
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
	if dbDesc == nil {
		return nil, newUndefinedDatabaseError(n.Name.Database())
	}

	tbKey := tableKey{dbDesc.ID, n.Name.Table()}.Key()

	// Check if table exists.
	gr, err := p.txn.Get(tbKey)
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		if n.IfExists {
			// Noop.
			return &emptyNode{}, nil
		}
		// Key does not exist, but we want it to: error out.
		return nil, fmt.Errorf("table %q does not exist", n.Name.Table())
	}

	targetDbDesc, err := p.getDatabaseDesc(n.NewName.Database())
	if err != nil {
		return nil, err
	}
	if targetDbDesc == nil {
		return nil, newUndefinedDatabaseError(n.NewName.Database())
	}

	if err := p.checkPrivilege(targetDbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	if n.Name.Database() == n.NewName.Database() && n.Name.Table() == n.NewName.Table() {
		// Noop.
		return &emptyNode{}, nil
	}

	tableDesc, err := p.getTableDesc(n.Name)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil || tableDesc.State != sqlbase.TableDescriptor_PUBLIC {
		return nil, newUndefinedTableError(n.Name.String())
	}

	if err := p.checkPrivilege(tableDesc, privilege.DROP); err != nil {
		return nil, err
	}

	tableDesc.SetName(n.NewName.Table())
	tableDesc.ParentID = targetDbDesc.ID

	descKey := sqlbase.MakeDescMetadataKey(tableDesc.GetID())
	newTbKey := tableKey{targetDbDesc.ID, n.NewName.Table()}.Key()

	if err := tableDesc.Validate(); err != nil {
		return nil, err
	}

	descID := tableDesc.GetID()
	descDesc := sqlbase.WrapDescriptor(tableDesc)

	if err := tableDesc.SetUpVersion(); err != nil {
		return nil, err
	}
	renameDetails := sqlbase.TableDescriptor_RenameInfo{
		OldParentID: uint32(dbDesc.ID),
		OldName:     n.Name.Table()}
	tableDesc.Renames = append(tableDesc.Renames, renameDetails)
	if err := p.writeTableDesc(tableDesc); err != nil {
		return nil, err
	}

	// We update the descriptor to the new name, but also leave the mapping of the
	// old name to the id, so that the name is not reused until the schema changer
	// has made sure it's not in use any more.
	b := client.Batch{}
	b.Put(descKey, descDesc)
	b.CPut(newTbKey, descID, nil)

	if err := p.txn.Run(&b); err != nil {
		if _, ok := err.(*roachpb.ConditionFailedError); ok {
			return nil, fmt.Errorf("table name %q already exists", n.NewName.Table())
		}
		return nil, err
	}
	p.notifySchemaChange(tableDesc.ID, sqlbase.InvalidMutationID)

	p.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		if err := expectDescriptorID(systemConfig, newTbKey, descID); err != nil {
			return err
		}
		if err := expectDescriptor(systemConfig, descKey, descDesc); err != nil {
			return err
		}
		return nil
	})

	return &emptyNode{}, nil
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

	if err := n.Index.Table.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}

	tableDesc, err := p.getTableDesc(n.Index.Table)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, newUndefinedTableError(n.Index.Table.String())
	}

	idxName := string(n.Index.Index)
	status, i, err := tableDesc.FindIndexByName(idxName)
	if err != nil {
		if n.IfExists {
			// Noop.
			return &emptyNode{}, nil
		}
		// Index does not exist, but we want it to: error out.
		return nil, err
	}

	if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	if sqlbase.EqualName(idxName, newIdxName) {
		// Noop.
		return &emptyNode{}, nil
	}

	if _, _, err := tableDesc.FindIndexByName(newIdxName); err == nil {
		return nil, fmt.Errorf("index name %q already exists", n.NewName)
	}

	if status == sqlbase.DescriptorActive {
		tableDesc.Indexes[i].Name = newIdxName
	} else {
		tableDesc.Mutations[i].GetIndex().Name = newIdxName
	}

	if err := tableDesc.SetUpVersion(); err != nil {
		return nil, err
	}
	descKey := sqlbase.MakeDescMetadataKey(tableDesc.GetID())
	if err := tableDesc.Validate(); err != nil {
		return nil, err
	}
	if err := p.txn.Put(descKey, sqlbase.WrapDescriptor(tableDesc)); err != nil {
		return nil, err
	}
	p.notifySchemaChange(tableDesc.ID, sqlbase.InvalidMutationID)
	return &emptyNode{}, nil
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
	if dbDesc == nil {
		return nil, newUndefinedDatabaseError(n.Table.Database())
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
			return &emptyNode{}, nil
		}
		// Key does not exist, but we want it to: error out.
		return nil, fmt.Errorf("table %q does not exist", n.Table.Table())
	}

	tableDesc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, newUndefinedTableError(n.Table.String())
	}

	colName := string(n.Name)
	status, i, err := tableDesc.FindColumnByName(colName)
	// n.IfExists only applies to table, no need to check here.
	if err != nil {
		return nil, err
	}
	var column *sqlbase.ColumnDescriptor
	if status == sqlbase.DescriptorActive {
		column = &tableDesc.Columns[i]
	} else {
		column = tableDesc.Mutations[i].GetColumn()
	}

	if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	if sqlbase.EqualName(colName, newColName) {
		// Noop.
		return &emptyNode{}, nil
	}

	if _, _, err := tableDesc.FindColumnByName(newColName); err == nil {
		return nil, fmt.Errorf("column name %q already exists", newColName)
	}

	// Rename the column in the indexes.
	renameColumnInIndex := func(idx *sqlbase.IndexDescriptor) {
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
	if err := tableDesc.SetUpVersion(); err != nil {
		return nil, err
	}

	descKey := sqlbase.MakeDescMetadataKey(tableDesc.GetID())
	if err := tableDesc.Validate(); err != nil {
		return nil, err
	}
	if err := p.txn.Put(descKey, sqlbase.WrapDescriptor(tableDesc)); err != nil {
		return nil, err
	}
	p.notifySchemaChange(tableDesc.ID, sqlbase.InvalidMutationID)
	return &emptyNode{}, nil
}
