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
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
)

// CreateDatabase creates a database.
func (p *planner) CreateDatabase(n *parser.CreateDatabase) (planNode, error) {
	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	nameKey := keys.MakeNameMetadataKey(structured.RootNamespaceID, string(n.Name))
	desc := makeDatabaseDesc(n)

	if err := p.writeDescriptor(nameKey, &desc, n.IfNotExists); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}

// CreateTable creates a table.
func (p *planner) CreateTable(n *parser.CreateTable) (planNode, error) {
	if err := p.normalizeTableName(n.Table); err != nil {
		return nil, err
	}

	dbDesc, err := p.getDatabaseDesc(n.Table.Database())
	if err != nil {
		return nil, err
	}

	desc, err := makeTableDesc(n)
	if err != nil {
		return nil, err
	}
	if err := desc.AllocateIDs(); err != nil {
		return nil, err
	}

	nameKey := keys.MakeNameMetadataKey(dbDesc.ID, n.Table.Table())
	if err := p.writeDescriptor(nameKey, &desc, n.IfNotExists); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}
