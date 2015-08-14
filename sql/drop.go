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
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/structured"
)

// DropTable drops a table.
// Privileges: WRITE on table.
//   Notes: postgres allows only the table owner to DROP a table.
//          mysql requires the DROP privilege on the table.
func (p *planner) DropTable(n *parser.DropTable) (planNode, error) {
	// TODO(XisiHuang): should do truncate and delete descriptor in
	// the same txn
	for i, tableQualifiedName := range n.Names {
		if err := tableQualifiedName.NormalizeTableName(p.session.Database); err != nil {
			return nil, err
		}

		dbDesc, err := p.getDatabaseDesc(tableQualifiedName.Database())
		if err != nil {
			return nil, err
		}

		tbKey := tableKey{dbDesc.ID, tableQualifiedName.Table()}
		nameKey := tbKey.Key()
		gr, err := p.txn.Get(nameKey)
		if err != nil {
			return nil, err
		}

		if !gr.Exists() {
			if n.IfExists {
				// Noop.
				continue
			}
			// Key does not exist, but we want it to: error out.
			return nil, fmt.Errorf("table %q does not exist", tbKey.Name())
		}

		tableDesc := structured.TableDescriptor{}
		if err := p.txn.GetProto(gr.ValueBytes(), &tableDesc); err != nil {
			return nil, err
		}
		if err := tableDesc.Validate(); err != nil {
			return nil, err
		}

		if err := p.checkPrivilege(&tableDesc, privilege.WRITE); err != nil {
			return nil, err
		}

		if _, err = p.Truncate(&parser.Truncate{Tables: n.Names[i : i+1]}); err != nil {
			return nil, err
		}

		// Delete table descriptor
		descKey := gr.ValueBytes()
		b := &client.Batch{}
		b.Del(descKey)
		b.Del(nameKey)
		err = p.txn.Run(b)
		if err != nil {
			return nil, err
		}
	}
	return &valuesNode{}, nil
}

// DropDatabase drops a database.
// Privileges: WRITE on database.
//   Notes: postgres allows only the database owner to DROP a database.
//          mysql requires the DROP privileges on the database.
// TODO(XisiHuang): our DROP DATABASE is like the postgres DROP SCHEMA
// (cockroach database == postgres schema). the postgres default of not
// dropping the schema if there are dependent objects is more sensible
// (see the RESTRICT and CASCADE options).
func (p *planner) DropDatabase(n *parser.DropDatabase) (planNode, error) {
	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	nameKey := structured.MakeNameMetadataKey(structured.RootNamespaceID, string(n.Name))
	gr, err := p.txn.Get(nameKey)
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		if n.IfExists {
			// Noop.
			return &valuesNode{}, nil
		}
		return nil, fmt.Errorf("database %q does not exist", n.Name)
	}

	descKey := gr.ValueBytes()
	desc := structured.DatabaseDescriptor{}
	if err := p.txn.GetProto(descKey, &desc); err != nil {
		return nil, err
	}
	if err := desc.Validate(); err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(&desc, privilege.WRITE); err != nil {
		return nil, err
	}

	tbNames, err := p.getTableNames(&desc)
	if err != nil {
		return nil, err
	}

	if _, err := p.DropTable(&parser.DropTable{Names: tbNames}); err != nil {
		return nil, err
	}

	b := &client.Batch{}
	b.Del(descKey)
	b.Del(nameKey)
	if err := p.txn.Run(b); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}
