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
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
)

// CreateDatabase creates a database.
func (p *planner) CreateDatabase(n *parser.CreateDatabase) (planNode, error) {
	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	nameKey := keys.MakeNameMetadataKey(structured.RootNamespaceID, strings.ToLower(n.Name))
	if gr, err := p.db.Get(nameKey); err != nil {
		return nil, err
	} else if gr.Exists() {
		if n.IfNotExists {
			return &valuesNode{}, nil
		}
		return nil, fmt.Errorf("database \"%s\" already exists", n.Name)
	}
	ir, err := p.db.Inc(keys.DescIDGenerator, 1)
	if err != nil {
		return nil, err
	}
	nsID := uint32(ir.ValueInt() - 1)
	// TODO(pmattis): Need to handle if-not-exists here as well.
	if err := p.db.CPut(nameKey, nsID, nil); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}

// CreateTable creates a table.
func (p *planner) CreateTable(n *parser.CreateTable) (planNode, error) {
	var err error
	n.Table, err = p.normalizeTableName(n.Table)
	if err != nil {
		return nil, err
	}

	dbID, err := p.lookupDatabase(n.Table.Database())
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

	nameKey := keys.MakeNameMetadataKey(dbID, n.Table.Table())

	// This isn't strictly necessary as the conditional put below will fail if
	// the key already exists, but it seems good to avoid the table ID allocation
	// in most cases when the table already exists.
	if gr, err := p.db.Get(nameKey); err != nil {
		return nil, err
	} else if gr.Exists() {
		if n.IfNotExists {
			return &valuesNode{}, nil
		}
		return nil, fmt.Errorf("table \"%s\" already exists", n.Table)
	}

	ir, err := p.db.Inc(keys.DescIDGenerator, 1)
	if err != nil {
		return nil, err
	}
	desc.ID = uint32(ir.ValueInt() - 1)

	// TODO(pmattis): Be cognizant of error messages when this is ported to the
	// server. The error currently returned below is likely going to be difficult
	// to interpret.
	// TODO(pmattis): Need to handle if-not-exists here as well.
	err = p.db.Txn(func(txn *client.Txn) error {
		descKey := keys.MakeDescMetadataKey(desc.ID)
		b := &client.Batch{}
		b.CPut(nameKey, descKey, nil)
		b.Put(descKey, &desc)
		return txn.Commit(b)
	})
	if err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}
