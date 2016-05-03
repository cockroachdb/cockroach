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

type dropDatabaseNode struct {
	p      *planner
	n      *parser.DropDatabase
	dbDesc *DatabaseDescriptor
	td     []*TableDescriptor
}

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

	// Check that the database exists.
	dbDesc, pErr := p.getDatabaseDesc(string(n.Name))
	if pErr != nil {
		return nil, pErr
	}
	if dbDesc == nil {
		if n.IfExists {
			// Noop.
			return &emptyNode{}, nil
		}
		return nil, roachpb.NewError(databaseDoesNotExistError(string(n.Name)))
	}

	if err := p.checkPrivilege(dbDesc, privilege.DROP); err != nil {
		return nil, roachpb.NewError(err)
	}

	tbNames, pErr := p.getTableNames(dbDesc)
	if pErr != nil {
		return nil, pErr
	}

	td := make([]*TableDescriptor, len(tbNames))
	for i, tbName := range tbNames {
		tbDesc, pErr := p.dropTablePrepare(tbName)
		if pErr != nil {
			return nil, pErr
		}
		if tbDesc == nil {
			// Database claims to have this table, but it does not exist.
			return nil, roachpb.NewErrorf("table %q was described by database %q, but does not exist",
				tbName.String())
		}
		td[i] = tbDesc
	}

	return &dropDatabaseNode{n: n, p: p, dbDesc: dbDesc, td: td}, nil
}

func (n *dropDatabaseNode) Start() *roachpb.Error {
	tbNameStrings := make([]string, len(n.td))
	for i, tbDesc := range n.td {
		if err := n.p.dropTableImpl(tbDesc); err != nil {
			return err
		}
		tbNameStrings[i] = tbDesc.Name
	}

	zoneKey, nameKey, descKey := getKeysForDatabaseDescriptor(n.dbDesc)

	b := &client.Batch{}
	b.Del(descKey)
	b.Del(nameKey)
	// Delete the zone config entry for this database.
	b.Del(zoneKey)

	n.p.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		for _, key := range [...]roachpb.Key{descKey, nameKey, zoneKey} {
			if err := expectDeleted(systemConfig, key); err != nil {
				return err
			}
		}
		return nil
	})

	if pErr := n.p.txn.Run(b); pErr != nil {
		return pErr
	}

	// Log Drop Database event.
	return MakeEventLogger(n.p.leaseMgr).InsertEventRecord(n.p.txn,
		EventLogDropDatabase,
		int32(n.dbDesc.ID),
		int32(n.p.evalCtx.NodeID),
		struct {
			DatabaseName  string
			Statement     string
			User          string
			DroppedTables []string
		}{n.n.Name.String(), n.n.String(), n.p.session.User, tbNameStrings},
	)
}

func (n *dropDatabaseNode) Next() bool                           { return false }
func (n *dropDatabaseNode) Columns() []ResultColumn              { return make([]ResultColumn, 0) }
func (n *dropDatabaseNode) ExplainTypes(fn func(string, string)) {}
func (n *dropDatabaseNode) Ordering() orderingInfo               { return orderingInfo{} }
func (n *dropDatabaseNode) Values() parser.DTuple                { return parser.DTuple{} }
func (n *dropDatabaseNode) DebugValues() debugValues             { return debugValues{} }
func (n *dropDatabaseNode) PErr() *roachpb.Error                 { return nil }
func (n *dropDatabaseNode) SetLimitHint(_ int64, _ bool)         {}
func (n *dropDatabaseNode) MarkDebug(mode explainMode)           {}
func (n *dropDatabaseNode) ExplainPlan(v bool) (string, string, []planNode) {
	return "drop database", "", nil
}

type dropIndexNode struct {
	p *planner
	n *parser.DropIndex
}

// DropIndex drops an index.
// Privileges: CREATE on table.
//   Notes: postgres allows only the index owner to DROP an index.
//          mysql requires the INDEX privilege on the table.
func (p *planner) DropIndex(n *parser.DropIndex) (planNode, *roachpb.Error) {
	for _, index := range n.IndexList {
		if err := index.Table.NormalizeTableName(p.session.Database); err != nil {
			return nil, roachpb.NewError(err)
		}

		tableDesc, pErr := p.getTableDesc(index.Table)
		if pErr != nil {
			return nil, pErr
		}
		if tableDesc == nil {
			return nil, roachpb.NewError(tableDoesNotExistError(index.Table.String()))
		}

		if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
			return nil, roachpb.NewError(err)
		}
	}
	return &dropIndexNode{n: n, p: p}, nil
}

func (n *dropIndexNode) Start() *roachpb.Error {
	for _, index := range n.n.IndexList {
		// Need to retrieve the descriptor again for each index name in
		// the list: when two or more index names refer to the same table,
		// the mutation list and new version number created by the first
		// drop need to be visible to the second drop.
		tableDesc, pErr := n.p.getTableDesc(index.Table)
		if pErr != nil || tableDesc == nil {
			// makePlan() and Start() ultimately run within the same
			// transaction. If we got a descriptor during makePlan(), we
			// must have it here too.
			panic(fmt.Sprintf("table descriptor for %s became unavailable within same txn", index.Table))
		}
		idxName := string(index.Index)
		status, i, err := tableDesc.FindIndexByName(idxName)
		if err != nil {
			if n.n.IfExists {
				// Noop.
				continue
			}
			// Index does not exist, but we want it to: error out.
			return roachpb.NewError(err)
		}
		// Queue the mutation.
		switch status {
		case DescriptorActive:
			tableDesc.addIndexMutation(tableDesc.Indexes[i], DescriptorMutation_DROP)
			tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)

		case DescriptorIncomplete:
			switch tableDesc.Mutations[i].Direction {
			case DescriptorMutation_ADD:
				return roachpb.NewUErrorf("index %q in the middle of being added, try again later", idxName)

			case DescriptorMutation_DROP:
				continue
			}
		}
		mutationID, err := tableDesc.finalizeMutation()
		if err != nil {
			return roachpb.NewError(err)
		}
		if err := tableDesc.Validate(); err != nil {
			return roachpb.NewError(err)
		}
		if pErr := n.p.writeTableDesc(tableDesc); pErr != nil {
			return pErr
		}
		n.p.notifySchemaChange(tableDesc.ID, mutationID)
	}
	return nil
}

func (n *dropIndexNode) Next() bool                           { return false }
func (n *dropIndexNode) Columns() []ResultColumn              { return make([]ResultColumn, 0) }
func (n *dropIndexNode) ExplainTypes(fn func(string, string)) {}
func (n *dropIndexNode) Ordering() orderingInfo               { return orderingInfo{} }
func (n *dropIndexNode) Values() parser.DTuple                { return parser.DTuple{} }
func (n *dropIndexNode) DebugValues() debugValues             { return debugValues{} }
func (n *dropIndexNode) PErr() *roachpb.Error                 { return nil }
func (n *dropIndexNode) SetLimitHint(_ int64, _ bool)         {}
func (n *dropIndexNode) MarkDebug(mode explainMode)           {}
func (n *dropIndexNode) ExplainPlan(v bool) (string, string, []planNode) {
	return "drop index", "", nil
}

type dropTableNode struct {
	p  *planner
	n  *parser.DropTable
	td []*TableDescriptor
}

// DropTable drops a table.
// Privileges: DROP on table.
//   Notes: postgres allows only the table owner to DROP a table.
//          mysql requires the DROP privilege on the table.
func (p *planner) DropTable(n *parser.DropTable) (planNode, *roachpb.Error) {
	td := make([]*TableDescriptor, 0, len(n.Names))
	for _, name := range n.Names {
		droppedDesc, err := p.dropTablePrepare(name)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			if n.IfExists {
				continue
			}
			// Table does not exist, but we want it to: error out.
			return nil, roachpb.NewError(tableDoesNotExistError(name.String()))
		}
		td = append(td, droppedDesc)
	}
	if len(td) == 0 {
		return &emptyNode{}, nil
	}
	return &dropTableNode{p: p, n: n, td: td}, nil
}

func (n *dropTableNode) Start() *roachpb.Error {
	for _, droppedDesc := range n.td {
		if droppedDesc == nil {
			continue
		}
		if err := n.p.dropTableImpl(droppedDesc); err != nil {
			return err
		}
		// Log a Drop Table event for this table.
		if pErr := MakeEventLogger(n.p.leaseMgr).InsertEventRecord(n.p.txn,
			EventLogDropTable,
			int32(droppedDesc.ID),
			int32(n.p.evalCtx.NodeID),
			struct {
				TableName string
				Statement string
				User      string
			}{droppedDesc.Name, n.n.String(), n.p.session.User},
		); pErr != nil {
			return pErr
		}
	}
	return nil
}

func (n *dropTableNode) Next() bool                           { return false }
func (n *dropTableNode) Columns() []ResultColumn              { return make([]ResultColumn, 0) }
func (n *dropTableNode) ExplainTypes(fn func(string, string)) {}
func (n *dropTableNode) Ordering() orderingInfo               { return orderingInfo{} }
func (n *dropTableNode) Values() parser.DTuple                { return parser.DTuple{} }
func (n *dropTableNode) DebugValues() debugValues             { return debugValues{} }
func (n *dropTableNode) PErr() *roachpb.Error                 { return nil }
func (n *dropTableNode) SetLimitHint(_ int64, _ bool)         {}
func (n *dropTableNode) MarkDebug(mode explainMode)           {}
func (n *dropTableNode) ExplainPlan(v bool) (string, string, []planNode) {
	return "drop table", "", nil
}

// dropTablePrepare/dropTableImpl is used to drop a single table by
// name, which can result from either a DROP TABLE or DROP DATABASE
// statement. This method returns the dropped table descriptor, to be
// used for the purpose of logging the event.  The table is not
// actually truncated or deleted synchronously. Instead, it is marked
// as deleted (meaning up_version is set and deleted is set) and the
// actual deletion happens async in a schema changer. Note that,
// courtesy of up_version, the actual truncation and dropping will
// only happen once every node ACKs the version of the descriptor with
// the deleted bit set, meaning the lease manager will not hand out
// new leases for it and existing leases are released).
// If the table does not exist, this function returns a nil descriptor.
func (p *planner) dropTablePrepare(name *parser.QualifiedName,
) (*TableDescriptor, *roachpb.Error) {
	tableDesc, pErr := p.getTableDesc(name)
	if pErr != nil {
		return nil, pErr
	}
	if tableDesc == nil {
		return nil, pErr
	}

	if pErr := p.checkPrivilege(tableDesc, privilege.DROP); pErr != nil {
		return nil, roachpb.NewError(pErr)
	}
	return tableDesc, nil
}

func (p *planner) dropTableImpl(tableDesc *TableDescriptor) *roachpb.Error {
	if err := tableDesc.setUpVersion(); err != nil {
		return roachpb.NewError(err)
	}
	tableDesc.Deleted = true
	if pErr := p.writeTableDesc(tableDesc); pErr != nil {
		return pErr
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

	return nil
}

// truncateAndDropTable batches all the commands required for truncating and deleting the
// table descriptor.
// It is called from a mutation, async wrt the DROP statement.
func truncateAndDropTable(tableDesc *TableDescriptor, db *client.DB) *roachpb.Error {
	return db.Txn(func(txn *client.Txn) *roachpb.Error {
		if pErr := truncateTable(tableDesc, txn); pErr != nil {
			return pErr
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
