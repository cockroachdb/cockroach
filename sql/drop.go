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
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util"
)

// DropDatabase drops a database.
// Privileges: DROP on database.
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

	nameKey := MakeNameMetadataKey(keys.RootNamespaceID, string(n.Name))
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

	descKey := MakeDescMetadataKey(ID(gr.ValueInt()))
	desc := &Descriptor{}
	if err := p.txn.GetProto(descKey, desc); err != nil {
		return nil, err
	}
	dbDesc := desc.GetDatabase()
	if dbDesc == nil {
		return nil, util.Errorf("%q is not a database", n.Name)
	}
	if err := dbDesc.Validate(); err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(dbDesc, privilege.DROP); err != nil {
		return nil, err
	}

	tbNames, err := p.getTableNames(dbDesc)
	if err != nil {
		return nil, err
	}

	if _, err := p.DropTable(&parser.DropTable{Names: tbNames}); err != nil {
		return nil, err
	}

	zoneKey := MakeZoneKey(dbDesc.ID)

	b := &client.Batch{}
	b.Del(descKey)
	b.Del(nameKey)
	// Delete the zone config entry for this database.
	b.Del(zoneKey)

	p.testingVerifyMetadata = func(systemConfig config.SystemConfig) error {
		for _, key := range [...]roachpb.Key{descKey, nameKey, zoneKey} {
			if err := expectDeleted(systemConfig, key); err != nil {
				return err
			}
		}
		return nil
	}

	if err := p.txn.Run(b); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}

// DropIndex drops an index.
// Privileges: CREATE on table.
//   Notes: postgres allows only the index owner to DROP an index.
//          mysql requires the INDEX privilege on the table.
func (p *planner) DropIndex(n *parser.DropIndex) (planNode, error) {
	for _, indexQualifiedName := range n.Names {
		if err := indexQualifiedName.NormalizeTableName(p.session.Database); err != nil {
			return nil, err
		}

		tableDesc, err := p.getTableDesc(indexQualifiedName)
		if err != nil {
			return nil, err
		}

		if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
			return nil, err
		}

		idxName := indexQualifiedName.Index()
		status, i, err := tableDesc.FindIndexByName(idxName)
		if err != nil {
			if n.IfExists {
				// Noop.
				return &valuesNode{}, nil
			}
			// Index does not exist, but we want it to: error out.
			return nil, err
		}
		switch status {
		case DescriptorActive:
			tableDesc.addIndexMutation(tableDesc.Indexes[i], DescriptorMutation_DROP)
			tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)

		case DescriptorIncomplete:
			switch tableDesc.Mutations[i].Direction {
			case DescriptorMutation_ADD:
				return nil, fmt.Errorf("index %q in the middle of being added, try again later", idxName)

			case DescriptorMutation_DROP:
				return &valuesNode{}, nil
			}
		}
		tableDesc.UpVersion = true
		if err := tableDesc.Validate(); err != nil {
			return nil, err
		}

		if err := p.txn.Put(MakeDescMetadataKey(tableDesc.GetID()), wrapDescriptor(tableDesc)); err != nil {
			return nil, err
		}
		// Process mutation synchronously.
		if err := p.applyMutations(tableDesc); err != nil {
			return nil, err
		}
	}
	return &valuesNode{}, nil
}

// DropTable drops a table.
// Privileges: DROP on table.
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

		desc := &Descriptor{}
		descKey := MakeDescMetadataKey(ID(gr.ValueInt()))
		if err := p.txn.GetProto(descKey, desc); err != nil {
			return nil, err
		}
		tableDesc := desc.GetTable()
		if tableDesc == nil {
			return nil, util.Errorf("%q is not a table", tbKey.Name())
		}
		if err := tableDesc.Validate(); err != nil {
			return nil, err
		}

		if err := p.checkPrivilege(tableDesc, privilege.DROP); err != nil {
			return nil, err
		}

		if _, err := p.Truncate(&parser.Truncate{Tables: n.Names[i : i+1]}); err != nil {
			return nil, err
		}

		zoneKey := MakeZoneKey(tableDesc.ID)

		// Delete table descriptor
		b := &client.Batch{}
		b.Del(descKey)
		b.Del(nameKey)
		// Delete the zone config entry for this table.
		b.Del(zoneKey)

		p.testingVerifyMetadata = func(systemConfig config.SystemConfig) error {
			for _, key := range [...]roachpb.Key{descKey, nameKey, zoneKey} {
				if err := expectDeleted(systemConfig, key); err != nil {
					return err
				}
			}
			return nil
		}

		if err := p.txn.Run(b); err != nil {
			return nil, err
		}
	}
	return &valuesNode{}, nil
}
