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
	"github.com/cockroachdb/cockroach/structured"
)

// RenameDatabase alters a databsase name
// Privileges: "root" user.
//   Notes: postgres requires superuser, db owner, or "CREATEDB".
//          mysql >= 5.1.23 does not allow database renames
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
	descKey := structured.MakeDescMetadataKey(dbDesc.GetID())
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
