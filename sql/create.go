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

	if err := p.writeDescriptor(databaseKey{string(n.Name)}, &desc, n.IfNotExists); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}

// CreateTable creates a table.
// Privileges: WRITE on database.
//   Notes: postgres/mysql require CREATE on database.
func (p *planner) CreateTable(n *parser.CreateTable) (planNode, error) {
	if err := n.Table.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}

	dbDesc, err := p.getDatabaseDesc(n.Table.Database())
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(dbDesc, privilege.WRITE); err != nil {
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

	if err := p.writeDescriptor(tableKey{dbDesc.ID, n.Table.Table()}, &desc, n.IfNotExists); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}
