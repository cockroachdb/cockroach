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
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
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

	// Check that the database exists.
	dbDesc, err := p.getDatabaseDesc(string(n.Name))
	if err != nil {
		return nil, err
	}
	if dbDesc == nil {
		if n.IfExists {
			// Noop.
			return &emptyNode{}, nil
		}
		return nil, databaseDoesNotExistError(string(n.Name))
	}

	if err := p.checkPrivilege(dbDesc, privilege.DROP); err != nil {
		return nil, err
	}

	tbNames, err := p.getTableNames(dbDesc)
	if err != nil {
		return nil, err
	}

	tbNameStrings := make([]string, len(tbNames))
	for i, tbName := range tbNames {
		tbDesc, err := p.dropTableImpl(tbName)
		if err != nil {
			return nil, err
		}
		if tbDesc == nil {
			// Database claims to have this table, but it does not exist.
			return nil, util.Errorf("table %q was described by database %q, but does not exist",
				tbName.String(), n.Name)
		}
		tbNameStrings[i] = tbDesc.Name
	}

	zoneKey, nameKey, descKey := getKeysForDatabaseDescriptor(dbDesc)

	b := &client.Batch{}
	b.Del(descKey)
	b.Del(nameKey)
	// Delete the zone config entry for this database.
	b.Del(zoneKey)

	p.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		for _, key := range [...]roachpb.Key{descKey, nameKey, zoneKey} {
			if err := expectDeleted(systemConfig, key); err != nil {
				return err
			}
		}
		return nil
	})

	if err := p.txn.Run(b); err != nil {
		return nil, err
	}

	// Log Drop Database event.
	if err := MakeEventLogger(p.leaseMgr).InsertEventRecord(p.txn,
		EventLogDropDatabase,
		int32(dbDesc.ID),
		int32(p.evalCtx.NodeID),
		struct {
			DatabaseName  string
			Statement     string
			User          string
			DroppedTables []string
		}{n.Name.String(), n.String(), p.session.User, tbNameStrings},
	); err != nil {
		return nil, err
	}
	return &emptyNode{}, nil
}

// DropIndex drops an index.
// Privileges: CREATE on table.
//   Notes: postgres allows only the index owner to DROP an index.
//          mysql requires the INDEX privilege on the table.
func (p *planner) DropIndex(n *parser.DropIndex) (planNode, error) {
	for _, index := range n.IndexList {
		if err := index.Table.NormalizeTableName(p.session.Database); err != nil {
			return nil, err
		}

		tableDesc, err := p.getTableDesc(index.Table)
		if err != nil {
			return nil, err
		}
		if tableDesc == nil {
			return nil, tableDoesNotExistError(index.Table.String())
		}

		if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
			return nil, err
		}
		idxName := string(index.Index)
		status, i, err := tableDesc.FindIndexByName(idxName)
		if err != nil {
			if n.IfExists {
				// Noop.
				return &emptyNode{}, nil
			}
			// Index does not exist, but we want it to: error out.
			return nil, err
		}
		// Queue the mutation.
		switch status {
		case DescriptorActive:
			tableDesc.addIndexMutation(tableDesc.Indexes[i], DescriptorMutation_DROP)
			tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)

		case DescriptorIncomplete:
			switch tableDesc.Mutations[i].Direction {
			case DescriptorMutation_ADD:
				return nil, fmt.Errorf("index %q in the middle of being added, try again later", idxName)

			case DescriptorMutation_DROP:
				return &emptyNode{}, nil
			}
		}
		mutationID, err := tableDesc.finalizeMutation()
		if err != nil {
			return nil, err
		}
		if err := tableDesc.Validate(); err != nil {
			return nil, err
		}
		if err := p.writeTableDesc(tableDesc); err != nil {
			return nil, err
		}
		p.notifySchemaChange(tableDesc.ID, mutationID)
	}
	return &emptyNode{}, nil
}

// DropTable drops a table.
// Privileges: DROP on table.
//   Notes: postgres allows only the table owner to DROP a table.
//          mysql requires the DROP privilege on the table.
func (p *planner) DropTable(n *parser.DropTable) (planNode, error) {
	for i := range n.Names {
		droppedDesc, err := p.dropTableImpl(n.Names[i])
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			if n.IfExists {
				continue
			}
			// Table does not exist, but we want it to: error out.
			return nil, tableDoesNotExistError(n.Names[i].String())
		}
		// Log a Drop Table event for this table.
		if err := MakeEventLogger(p.leaseMgr).InsertEventRecord(p.txn,
			EventLogDropTable,
			int32(droppedDesc.ID),
			int32(p.evalCtx.NodeID),
			struct {
				TableName string
				Statement string
				User      string
			}{droppedDesc.Name, n.String(), p.session.User},
		); err != nil {
			return nil, err
		}
	}
	return &emptyNode{}, nil
}

// dropTableImpl is used to drop a single table by name, which can result from
// either a DROP TABLE or DROP DATABASE statement. This method returns the
// dropped table descriptor, to be used for the purpose of logging the event.
// The table is not actually truncated or deleted synchronously. Instead, it is
// marked as deleted (meaning up_version is set and deleted is set) and the
// actual deletion happens async in a schema changer. Note that, courtesy of
// up_version, the actual truncation and dropping will only happen once every
// node ACKs the version of the descriptor with the deleted bit set, meaning the
// lease manager will not hand out new leases for it and existing leases are
// released).
// If the table does not exist, this function returns a nil descriptor.
func (p *planner) dropTableImpl(name *parser.QualifiedName,
) (*TableDescriptor, error) {
	tableDesc, err := p.getTableDesc(name)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, err
	}

	if err := p.checkPrivilege(tableDesc, privilege.DROP); err != nil {
		return nil, err
	}

	if err := tableDesc.setUpVersion(); err != nil {
		return nil, err
	}
	tableDesc.Deleted = true
	if err = p.writeTableDesc(tableDesc); err != nil {
		return nil, err
	}
	p.notifySchemaChange(tableDesc.ID, invalidMutationID)

	verifyMetadataCallback := func(systemConfig config.SystemConfig, tableID ID) error {
		desc, err := GetTableDesc(systemConfig, tableID)
		if err != nil {
			return err
		}
		if desc == nil {
			return util.Errorf("table %d missing", tableID)
		}
		if desc.Deleted {
			return nil
		}
		return util.Errorf("expected table %d to be marked as deleted", tableID)
	}
	p.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		return verifyMetadataCallback(systemConfig, tableDesc.ID)
	})

	return tableDesc, nil
}

// truncateAndDropTable batches all the commands required for truncating and deleting the
// table descriptor.
// It is called from a mutation, async wrt the DROP statement.
func truncateAndDropTable(tableDesc *TableDescriptor, db *client.DB) error {
	return db.Txn(func(txn *client.Txn) error {
		if err := truncateTable(tableDesc, txn); err != nil {
			return err
		}
		zoneKey, nameKey, descKey := getKeysForTableDescriptor(tableDesc)
		// Delete table descriptor
		b := client.Batch{}
		b.Del(descKey)
		b.Del(nameKey)
		// Delete the zone config entry for this table.
		b.Del(zoneKey)
		txn.SetSystemConfigTrigger()
		return txn.Run(&b)
	})
}
