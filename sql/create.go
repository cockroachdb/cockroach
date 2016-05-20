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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
)

type createDatabaseNode struct {
	p *planner
	n *parser.CreateDatabase
}

// CreateDatabase creates a database.
// Privileges: security.RootUser user.
//   Notes: postgres requires superuser or "CREATEDB".
//          mysql uses the mysqladmin command.
func (p *planner) CreateDatabase(n *parser.CreateDatabase) (planNode, error) {
	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	if n.Encoding != nil {
		encoding, err := n.Encoding.ResolveAsType(parser.TypeString)
		if err != nil {
			return nil, err
		}
		encodingStr := string(*encoding.(*parser.DString))
		// We only support UTF8 (and aliases for UTF8).
		if !(strings.EqualFold(encodingStr, "UTF8") ||
			strings.EqualFold(encodingStr, "UTF-8") ||
			strings.EqualFold(encodingStr, "UNICODE")) {
			return nil, fmt.Errorf("%s is not a supported encoding", encoding)
		}
	}

	if p.session.User != security.RootUser {
		return nil, util.Errorf("only %s is allowed to create databases", security.RootUser)
	}

	return &createDatabaseNode{p: p, n: n}, nil
}

func (n *createDatabaseNode) expandPlan() error {
	return nil
}

func (n *createDatabaseNode) Start() error {
	desc := makeDatabaseDesc(n.n)

	created, err := n.p.createDescriptor(databaseKey{string(n.n.Name)}, &desc, n.n.IfNotExists)
	if err != nil {
		return err
	}
	if created {
		// Log Create Database event.
		if err := MakeEventLogger(n.p.leaseMgr).InsertEventRecord(n.p.txn,
			EventLogCreateDatabase,
			int32(desc.ID),
			int32(n.p.evalCtx.NodeID),
			struct {
				DatabaseName string
				Statement    string
				User         string
			}{n.n.Name.String(), n.n.String(), n.p.session.User},
		); err != nil {
			return err
		}
	}
	return nil
}

func (n *createDatabaseNode) Next() bool                          { return false }
func (n *createDatabaseNode) Columns() []ResultColumn             { return make([]ResultColumn, 0) }
func (n *createDatabaseNode) Ordering() orderingInfo              { return orderingInfo{} }
func (n *createDatabaseNode) Values() parser.DTuple               { return parser.DTuple{} }
func (n *createDatabaseNode) DebugValues() debugValues            { return debugValues{} }
func (n *createDatabaseNode) ExplainTypes(_ func(string, string)) {}
func (n *createDatabaseNode) Err() error                          { return nil }
func (n *createDatabaseNode) SetLimitHint(_ int64, _ bool)        {}
func (n *createDatabaseNode) MarkDebug(mode explainMode)          {}
func (n *createDatabaseNode) ExplainPlan(v bool) (string, string, []planNode) {
	return "create database", "", nil
}

type createIndexNode struct {
	p         *planner
	n         *parser.CreateIndex
	tableDesc *sqlbase.TableDescriptor
}

// CreateIndex creates an index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires INDEX on the table.
func (p *planner) CreateIndex(n *parser.CreateIndex) (planNode, error) {
	tableDesc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, newUndefinedTableError(n.Table.String())
	}

	if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &createIndexNode{p: p, tableDesc: tableDesc, n: n}, nil
}

func (n *createIndexNode) expandPlan() error {
	return nil
}

func (n *createIndexNode) Start() error {
	status, i, err := n.tableDesc.FindIndexByName(string(n.n.Name))
	if err == nil {
		if status == sqlbase.DescriptorIncomplete {
			switch n.tableDesc.Mutations[i].Direction {
			case sqlbase.DescriptorMutation_DROP:
				return fmt.Errorf("index %q being dropped, try again later", string(n.n.Name))

			case sqlbase.DescriptorMutation_ADD:
				// Noop, will fail in AllocateIDs below.
			}
		}
		if n.n.IfNotExists {
			return nil
		}
	}

	indexDesc := sqlbase.IndexDescriptor{
		Name:             string(n.n.Name),
		Unique:           n.n.Unique,
		StoreColumnNames: n.n.Storing,
	}
	if err := indexDesc.FillColumns(n.n.Columns); err != nil {
		return err
	}

	n.tableDesc.AddIndexMutation(indexDesc, sqlbase.DescriptorMutation_ADD)
	mutationID, err := n.tableDesc.FinalizeMutation()
	if err != nil {
		return err
	}
	if err := n.tableDesc.AllocateIDs(); err != nil {
		return err
	}

	if err := n.p.txn.Put(
		sqlbase.MakeDescMetadataKey(n.tableDesc.GetID()),
		sqlbase.WrapDescriptor(n.tableDesc)); err != nil {
		return err
	}
	n.p.notifySchemaChange(n.tableDesc.ID, mutationID)

	return nil
}

func (n *createIndexNode) Next() bool                          { return false }
func (n *createIndexNode) Columns() []ResultColumn             { return make([]ResultColumn, 0) }
func (n *createIndexNode) Ordering() orderingInfo              { return orderingInfo{} }
func (n *createIndexNode) Values() parser.DTuple               { return parser.DTuple{} }
func (n *createIndexNode) DebugValues() debugValues            { return debugValues{} }
func (n *createIndexNode) ExplainTypes(_ func(string, string)) {}
func (n *createIndexNode) Err() error                          { return nil }
func (n *createIndexNode) SetLimitHint(_ int64, _ bool)        {}
func (n *createIndexNode) MarkDebug(mode explainMode)          {}
func (n *createIndexNode) ExplainPlan(v bool) (string, string, []planNode) {
	return "create index", "", nil
}

type createTableNode struct {
	p      *planner
	n      *parser.CreateTable
	dbDesc *sqlbase.DatabaseDescriptor
}

// CreateTable creates a table.
// Privileges: CREATE on database.
//   Notes: postgres/mysql require CREATE on database.
func (p *planner) CreateTable(n *parser.CreateTable) (planNode, error) {
	if err := n.Table.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}

	dbDesc, err := p.getDatabaseDesc(n.Table.Database())
	if err != nil {
		return nil, err
	}
	if dbDesc == nil {
		return nil, newUndefinedDatabaseError(n.Table.Database())
	}

	if err := p.checkPrivilege(dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &createTableNode{p: p, n: n, dbDesc: dbDesc}, nil
}

func hoistConstraints(n *parser.CreateTable) {
	for _, d := range n.Defs {
		if col, ok := d.(*parser.ColumnTableDef); ok {
			if col.CheckExpr != nil {
				n.Defs = append(n.Defs, &parser.CheckConstraintTableDef{Expr: col.CheckExpr})
				col.CheckExpr = nil
			}
		}
	}
}

func (n *createTableNode) expandPlan() error {
	return nil
}

func (n *createTableNode) Start() error {
	hoistConstraints(n.n)
	desc, err := sqlbase.MakeTableDesc(n.n, n.dbDesc.ID)
	if err != nil {
		return err
	}
	// Inherit permissions from the database descriptor.
	desc.Privileges = n.dbDesc.GetPrivileges()

	if len(desc.PrimaryIndex.ColumnNames) == 0 {
		// Ensure a Primary Key exists.
		s := "unique_rowid()"
		col := sqlbase.ColumnDescriptor{
			Name: "rowid",
			Type: sqlbase.ColumnType{
				Kind: sqlbase.ColumnType_INT,
			},
			DefaultExpr: &s,
			Hidden:      true,
			Nullable:    false,
		}
		desc.AddColumn(col)
		idx := sqlbase.IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{col.Name},
			ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
		}
		if err := desc.AddIndex(idx, true); err != nil {
			return err
		}
	}

	if err := desc.AllocateIDs(); err != nil {
		return err
	}

	var fkTargets []fkTargetUpdate
	for _, def := range n.n.Defs {
		if col, ok := def.(*parser.ColumnTableDef); ok {
			if col.References.Table != nil {
				modified, err := n.resolveColFK(&desc, string(col.Name), col.References.Table, string(col.References.Col))
				if err != nil {
					return err
				}
				fkTargets = append(fkTargets, modified)
			}
		}
	}

	created, err := n.p.createDescriptor(tableKey{n.dbDesc.ID, n.n.Table.Table()}, &desc, n.n.IfNotExists)
	if err != nil {
		return err
	}

	if err := n.finalizeFKs(&desc, fkTargets); err != nil {
		return err
	}

	if created {
		// Log Create Table event.
		if err := MakeEventLogger(n.p.leaseMgr).InsertEventRecord(n.p.txn,
			EventLogCreateTable,
			int32(desc.ID),
			int32(n.p.evalCtx.NodeID),
			struct {
				TableName string
				Statement string
				User      string
			}{n.n.Table.String(), n.n.String(), n.p.session.User},
		); err != nil {
			return err
		}
	}

	return nil
}

func (n *createTableNode) Next() bool                          { return false }
func (n *createTableNode) Columns() []ResultColumn             { return make([]ResultColumn, 0) }
func (n *createTableNode) Ordering() orderingInfo              { return orderingInfo{} }
func (n *createTableNode) Values() parser.DTuple               { return parser.DTuple{} }
func (n *createTableNode) DebugValues() debugValues            { return debugValues{} }
func (n *createTableNode) Err() error                          { return nil }
func (n *createTableNode) ExplainTypes(_ func(string, string)) {}
func (n *createTableNode) SetLimitHint(_ int64, _ bool)        {}
func (n *createTableNode) MarkDebug(mode explainMode)          {}
func (n *createTableNode) ExplainPlan(v bool) (string, string, []planNode) {
	return "create table", "", nil
}

// FK resolution runs before the referencing (child) table is created, meaning
// its ID, which needs to be noted on the referenced tables, is not yet
// determined. This struct accumulates the information needed to edit a
// referenced table after the referencing table is created and has an ID.
type fkTargetUpdate struct {
	target *sqlbase.TableDescriptor // table to update
	idx    int                      // index into target.Indexes
	srcIdx sqlbase.IndexID          // ID of source index
}

func (n *createTableNode) resolveColFK(
	tbl *sqlbase.TableDescriptor, fromCol string, targetTable *parser.QualifiedName, targetCol string,
) (fkTargetUpdate, error) {
	var ret fkTargetUpdate
	src, err := tbl.FindActiveColumnByName(fromCol)
	if err != nil {
		return ret, err
	}

	target, err := n.p.getTableDesc(targetTable)
	if err != nil {
		return ret, err
	}
	if target == nil {
		return ret, fmt.Errorf("referenced table %q not found", targetTable.String())
	}
	ret.target = target
	// If a column isn't specified, attempt to default to PK.
	if targetCol == "" {
		if len(target.PrimaryIndex.ColumnNames) != 1 {
			return ret, util.Errorf("must specify a single unique column to reference %q", targetTable.String())
		}
		targetCol = target.PrimaryIndex.ColumnNames[0]
	}

	fk, err := target.FindActiveColumnByName(targetCol)
	if err != nil {
		return ret, err
	}

	if src.Type.Kind != fk.Type.Kind {
		return ret, fmt.Errorf("type of %q (%s) does not match foreign key %q.%q (%s)", fromCol, src.Type.Kind, target.Name, fk.Name, fk.Type.Kind)
	}

	found := false
	// Find the index corresponding to the referenced column.
	for i, idx := range target.AllNonDropIndexes() {
		if idx.Unique && idx.ColumnIDs[0] == fk.ID {
			ret.idx = i
			found = true
			break
		}
	}
	if !found {
		return ret, fmt.Errorf("foreign key requires a unique index on %s.%s", targetTable.String(), targetCol)
	}

	found = false
	for i, idx := range tbl.Indexes {
		if tbl.Indexes[i].ColumnNames[0] == src.Name {
			tbl.Indexes[i].ForeignKey = &sqlbase.TableAndIndexID{
				Table: target.ID,
				Index: target.Indexes[ret.idx].ID,
			}
			ret.srcIdx = idx.ID
			found = true
			break
		}
	}
	if !found {
		return ret, fmt.Errorf("foreign key column %q must be the prefix of an index", src.Name)
	}

	tbl.State = sqlbase.TableDescriptor_ADD
	return ret, nil
}

func (n *createTableNode) finalizeFKs(desc *sqlbase.TableDescriptor, fkTargets []fkTargetUpdate) error {
	for _, t := range fkTargets {
		t.target.Indexes[t.idx].ReferencedBy = append(
			t.target.Indexes[t.idx].ReferencedBy,
			&sqlbase.TableAndIndexID{Table: desc.ID, Index: t.srcIdx},
		)

		// TODO(dt): Only save each referenced table once.
		if err := t.target.SetUpVersion(); err != nil {
			return err
		}
		if err := t.target.Validate(); err != nil {
			return err
		}
		if err := n.p.writeTableDesc(t.target); err != nil {
			return err
		}
		n.p.notifySchemaChange(t.target.ID, sqlbase.InvalidMutationID)
	}

	if desc.State == sqlbase.TableDescriptor_ADD {
		desc.State = sqlbase.TableDescriptor_PUBLIC

		if err := desc.SetUpVersion(); err != nil {
			return err
		}
		if err := desc.Validate(); err != nil {
			return err
		}
		if err := n.p.writeTableDesc(desc); err != nil {
			return err
		}
		n.p.notifySchemaChange(desc.ID, sqlbase.InvalidMutationID)
	}
	return nil
}
