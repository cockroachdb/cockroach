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
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
)

// DropDatabase drops a database.
// Privileges: DROP on database.
//   Notes: postgres allows only the database owner to DROP a database.
//          mysql requires the DROP privileges on the database.
// TODO(XisiHuang): our DROP DATABASE is like the postgres DROP SCHEMA
// (cockroach database == postgres schema). the postgres default of not
// dropping the schema if there are dependent objects is more sensible
// (see the RESTRICT and CASCADE options).
func (p *planner) DropDatabase(n *parser.DropDatabase) (planNode, *roachpb.Error) {
	if n.Name == "" {
		return nil, roachpb.NewError(errEmptyDatabaseName)
	}

	nameKey := MakeNameMetadataKey(keys.RootNamespaceID, string(n.Name))
	gr, pErr := p.txn.Get(nameKey)
	if pErr != nil {
		return nil, pErr
	}
	if !gr.Exists() {
		if n.IfExists {
			// Noop.
			return &valuesNode{}, nil
		}
		return nil, roachpb.NewUErrorf("database %q does not exist", n.Name)
	}

	descKey := MakeDescMetadataKey(ID(gr.ValueInt()))
	desc := &Descriptor{}
	if pErr := p.txn.GetProto(descKey, desc); pErr != nil {
		return nil, pErr
	}
	dbDesc := desc.GetDatabase()
	if dbDesc == nil {
		return nil, roachpb.NewErrorf("%q is not a database", n.Name)
	}
	if err := dbDesc.Validate(); err != nil {
		return nil, roachpb.NewError(err)
	}

	if pErr := p.checkPrivilege(dbDesc, privilege.DROP); pErr != nil {
		return nil, pErr
	}

	tbNames, pErr := p.getTableNames(dbDesc)
	if pErr != nil {
		return nil, pErr
	}

	if _, pErr := p.DropTable(&parser.DropTable{Names: tbNames}); pErr != nil {
		return nil, pErr
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

	if pErr := p.txn.Run(b); pErr != nil {
		return nil, pErr
	}
	return &valuesNode{}, nil
}

// DropIndex drops an index.
// Privileges: CREATE on table.
//   Notes: postgres allows only the index owner to DROP an index.
//          mysql requires the INDEX privilege on the table.
func (p *planner) DropIndex(n *parser.DropIndex) (planNode, *roachpb.Error) {
	for _, indexQualifiedName := range n.Names {
		if err := indexQualifiedName.NormalizeTableName(p.session.Database); err != nil {
			return nil, roachpb.NewError(err)
		}

		tableDesc, pErr := p.getTableDesc(indexQualifiedName)
		if pErr != nil {
			return nil, pErr
		}

		if pErr := p.checkPrivilege(tableDesc, privilege.CREATE); pErr != nil {
			return nil, pErr
		}

		idxName := indexQualifiedName.Index()
		status, i, pErr := tableDesc.FindIndexByName(idxName)
		if pErr != nil {
			if n.IfExists {
				// Noop.
				return &valuesNode{}, nil
			}
			// Index does not exist, but we want it to: error out.
			return nil, pErr
		}
		switch status {
		case DescriptorActive:
			tableDesc.addIndexMutation(tableDesc.Indexes[i], DescriptorMutation_DROP)
			tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)

		case DescriptorIncomplete:
			switch tableDesc.Mutations[i].Direction {
			case DescriptorMutation_ADD:
				return nil, roachpb.NewUErrorf("index %q in the middle of being added, try again later", idxName)

			case DescriptorMutation_DROP:
				return &valuesNode{}, nil
			}
		}
		tableDesc.UpVersion = true
		if err := tableDesc.Validate(); err != nil {
			return nil, roachpb.NewError(err)
		}

		if pErr := p.txn.Put(MakeDescMetadataKey(tableDesc.GetID()), wrapDescriptor(tableDesc)); pErr != nil {
			return nil, pErr
		}
		// Process mutation synchronously.
		if pErr := p.applyMutations(tableDesc); pErr != nil {
			return nil, pErr
		}
	}
	return &valuesNode{}, nil
}

// DropTable drops a table.
// Privileges: DROP on table.
//   Notes: postgres allows only the table owner to DROP a table.
//          mysql requires the DROP privilege on the table.
func (p *planner) DropTable(n *parser.DropTable) (planNode, *roachpb.Error) {
	// TODO(XisiHuang): should do truncate and delete descriptor in
	// the same txn
	for i, tableQualifiedName := range n.Names {
		if err := tableQualifiedName.NormalizeTableName(p.session.Database); err != nil {
			return nil, roachpb.NewError(err)
		}

		dbDesc, pErr := p.getDatabaseDesc(tableQualifiedName.Database())
		if pErr != nil {
			return nil, pErr
		}

		tbKey := tableKey{dbDesc.ID, tableQualifiedName.Table()}
		nameKey := tbKey.Key()
		gr, pErr := p.txn.Get(nameKey)
		if pErr != nil {
			return nil, pErr
		}

		if !gr.Exists() {
			if n.IfExists {
				// Noop.
				continue
			}
			// Key does not exist, but we want it to: error out.
			return nil, roachpb.NewUErrorf("table %q does not exist", tbKey.Name())
		}

		desc := &Descriptor{}
		descKey := MakeDescMetadataKey(ID(gr.ValueInt()))
		if pErr := p.txn.GetProto(descKey, desc); pErr != nil {
			return nil, pErr
		}
		tableDesc := desc.GetTable()
		if tableDesc == nil {
			return nil, roachpb.NewErrorf("%q is not a table", tbKey.Name())
		}
		if err := tableDesc.Validate(); err != nil {
			return nil, roachpb.NewError(err)
		}

		if pErr := p.checkPrivilege(tableDesc, privilege.DROP); pErr != nil {
			return nil, pErr
		}

		if _, pErr := p.Truncate(&parser.Truncate{Tables: n.Names[i : i+1]}); pErr != nil {
			return nil, pErr
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

		if pErr := p.txn.Run(b); pErr != nil {
			return nil, pErr
		}
	}
	return &valuesNode{}, nil
}
