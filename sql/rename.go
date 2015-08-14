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
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
)

// RenameDatabase renames the database.
// Privileges: "root" user.
//   Notes: postgres requires superuser, db owner, or "CREATEDB".
//          mysql >= 5.1.23 does not allow database renames.
func (p *planner) RenameDatabase(n *parser.RenameDatabase) (planNode, error) {
	if n.Name == "" || n.NewName == "" {
		return nil, errEmptyDatabaseName
	}

	if n.Name == n.NewName {
		//noop
		return &valuesNode{}, nil
	}

	if p.user != security.RootUser {
		return nil, fmt.Errorf("only %s is allowed to rename databases", security.RootUser)
	}

	dbDesc, err := p.getDatabaseDesc(string(n.Name))
	if err != nil {
		return nil, err
	}

	// Now update the nameMetadataKey and the descriptor.
	descKey := MakeDescMetadataKey(dbDesc.GetID())
	dbDesc.SetName(string(n.NewName))

	b := client.Batch{}
	b.CPut(databaseKey{string(n.NewName)}.Key(), descKey, nil)
	b.Put(descKey, dbDesc)
	b.Del(databaseKey{string(n.Name)}.Key())

	if err := p.txn.Run(&b); err != nil {
		if _, ok := err.(*proto.ConditionFailedError); ok {
			return nil, fmt.Errorf("the new database name %s already exists", string(n.NewName))
		}
		return nil, err
	}

	return &valuesNode{}, nil
}

// RenameTable renames the table.
// Privileges: WRITE on database.
//   Notes: postgres requires the table owner.
//          mysql requires ALTER, DROP on the original table, and CREATE, INSERT
//          on the new table (and does not copy privileges over).
func (p *planner) RenameTable(n *parser.RenameTable) (planNode, error) {
	if n.NewName == "" {
		return nil, errEmptyTableName
	}

	if err := n.Name.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}

	if n.Name.Table() == string(n.NewName) {
		// Noop.
		return &valuesNode{}, nil
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

	if err := p.checkPrivilege(dbDesc, privilege.WRITE); err != nil {
		return nil, err
	}

	tableDesc, err := p.getTableDesc(n.Name)
	if err != nil {
		return nil, err
	}

	tableDesc.SetName(string(n.NewName))

	newTbKey := tableKey{dbDesc.ID, string(n.NewName)}.Key()
	descKey := MakeDescMetadataKey(tableDesc.GetID())

	b := client.Batch{}
	b.Put(descKey, tableDesc)
	b.CPut(newTbKey, descKey, nil)
	b.Del(tbKey)
	if err := p.txn.Run(&b); err != nil {
		if _, ok := err.(*proto.ConditionFailedError); ok {
			return nil, fmt.Errorf("table name %q already exists", n.NewName)
		}
		return nil, err
	}

	return &valuesNode{}, nil
}
