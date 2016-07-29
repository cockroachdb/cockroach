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
	"bytes"
	"fmt"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
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
		encoding, err := n.Encoding.ResolveAsType(&p.semaCtx, parser.TypeString)
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
		return nil, errors.Errorf("only %s is allowed to create databases", security.RootUser)
	}

	return &createDatabaseNode{p: p, n: n}, nil
}

func (n *createDatabaseNode) expandPlan() error {
	return nil
}

func (n *createDatabaseNode) Start() error {
	desc := makeDatabaseDesc(n.n)

	created, err := n.p.createDatabase(&desc, n.n.IfNotExists)
	if err != nil {
		return err
	}
	if created {
		// Log Create Database event. This is an auditable log event and is
		// recorded in the same transaction as the table descriptor update.
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

func (n *createDatabaseNode) Next() (bool, error)                 { return false, nil }
func (n *createDatabaseNode) Columns() []ResultColumn             { return make([]ResultColumn, 0) }
func (n *createDatabaseNode) Ordering() orderingInfo              { return orderingInfo{} }
func (n *createDatabaseNode) Values() parser.DTuple               { return parser.DTuple{} }
func (n *createDatabaseNode) DebugValues() debugValues            { return debugValues{} }
func (n *createDatabaseNode) ExplainTypes(_ func(string, string)) {}
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
	tableDesc, err := p.mustGetTableDesc(n.Table)
	if err != nil {
		return nil, err
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

	mutationIdx := len(n.tableDesc.Mutations)
	n.tableDesc.AddIndexMutation(indexDesc, sqlbase.DescriptorMutation_ADD)
	mutationID, err := n.tableDesc.FinalizeMutation()
	if err != nil {
		return err
	}
	if err := n.tableDesc.AllocateIDs(); err != nil {
		return err
	}

	if n.n.Interleave != nil {
		index := n.tableDesc.Mutations[mutationIdx].GetIndex()
		if err := n.p.addInterleave(n.tableDesc, index, n.n.Interleave); err != nil {
			return err
		}
		if err := n.p.finalizeInterleave(n.tableDesc, *index); err != nil {
			return err
		}
	}

	if err := n.p.txn.Put(
		sqlbase.MakeDescMetadataKey(n.tableDesc.GetID()),
		sqlbase.WrapDescriptor(n.tableDesc)); err != nil {
		return err
	}

	// Record index creation in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	if err := MakeEventLogger(n.p.leaseMgr).InsertEventRecord(n.p.txn,
		EventLogCreateIndex,
		int32(n.tableDesc.ID),
		int32(n.p.evalCtx.NodeID),
		struct {
			TableName  string
			IndexName  string
			Statement  string
			User       string
			MutationID uint32
		}{n.tableDesc.Name, n.n.Name.String(), n.n.String(), n.p.session.User, uint32(mutationID)},
	); err != nil {
		return err
	}
	n.p.notifySchemaChange(n.tableDesc.ID, mutationID)

	return nil
}

func (n *createIndexNode) Next() (bool, error)                 { return false, nil }
func (n *createIndexNode) Columns() []ResultColumn             { return make([]ResultColumn, 0) }
func (n *createIndexNode) Ordering() orderingInfo              { return orderingInfo{} }
func (n *createIndexNode) Values() parser.DTuple               { return parser.DTuple{} }
func (n *createIndexNode) DebugValues() debugValues            { return debugValues{} }
func (n *createIndexNode) ExplainTypes(_ func(string, string)) {}
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

	dbDesc, err := p.mustGetDatabaseDesc(n.Table.Database())
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &createTableNode{p: p, n: n, dbDesc: dbDesc}, nil
}

func hoistConstraints(n *parser.CreateTable) {
	for _, d := range n.Defs {
		if col, ok := d.(*parser.ColumnTableDef); ok {
			if col.CheckExpr.Expr != nil {
				def := &parser.CheckConstraintTableDef{Expr: col.CheckExpr.Expr}
				if col.CheckExpr.ConstraintName != "" {
					def.Name = col.CheckExpr.ConstraintName
				}
				n.Defs = append(n.Defs, def)
				col.CheckExpr.Expr = nil
			}
		}
	}
}

func (n *createTableNode) expandPlan() error {
	return nil
}

func (n *createTableNode) Start() error {
	hoistConstraints(n.n)
	desc, err := MakeTableDesc(n.n, n.dbDesc.ID)
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

	if n.n.Interleave != nil {
		if err := n.p.addInterleave(&desc, &desc.PrimaryIndex, n.n.Interleave); err != nil {
			return err
		}
	}

	// FKs are resolved after the descriptor is otherwise complete and IDs have
	// been allocated since the FKs will reference those IDs.
	var fkTargets []fkTargetUpdate
	for _, def := range n.n.Defs {
		switch d := def.(type) {
		case *parser.ColumnTableDef:
			if d.References.Table != nil {
				var targetCol parser.NameList
				if d.References.Col != "" {
					targetCol = append(targetCol, string(d.References.Col))
				}
				modified, err := n.resolveFK(&desc, parser.NameList{string(d.Name)}, d.References.Table, targetCol, d.References.ConstraintName)
				if err != nil {
					return err
				}
				fkTargets = append(fkTargets, modified)
			}
		case *parser.ForeignKeyConstraintTableDef:
			modified, err := n.resolveFK(&desc, d.FromCols, d.Table, d.ToCols, d.Name)
			if err != nil {
				return err
			}
			fkTargets = append(fkTargets, modified)
		}
	}

	// Multiple FKs from the same column would potentially result in ambiguous or
	// unexpected behavior with conflicting CASCADE/RESTRICT/etc behaviors.
	colsInFKs := make(map[sqlbase.ColumnID]struct{})
	for _, t := range fkTargets {
		i, err := desc.FindIndexByID(t.srcIdx)
		if err != nil {
			return errors.Wrap(err, "could not resolve FK index to check for columns overlaps")
		}
		for x := range i.ColumnIDs {
			if _, ok := colsInFKs[i.ColumnIDs[x]]; ok {
				return errors.Errorf("column %q cannot be used by multiple foreign key constraints", i.ColumnNames[x])
			}
			colsInFKs[i.ColumnIDs[x]] = struct{}{}
		}
	}

	// We need to validate again after adding the FKs, but the desc still doesn't
	// have a valid ID, so we briefly set it to something to get past validation.
	savedID := desc.ID
	if desc.ID == 0 {
		desc.ID = keys.MaxReservedDescID + 1
	}
	// Only validate the table because backreferences aren't created yet.
	// Everything is validated below.
	err = desc.ValidateTable()
	desc.ID = savedID
	if err != nil {
		return err
	}

	created, err := n.p.createDescriptor(
		tableKey{n.dbDesc.ID, n.n.Table.Table()}, &desc, n.n.IfNotExists)
	if err != nil {
		return err
	}

	if created {
		if err := n.finalizeFKs(&desc, fkTargets); err != nil {
			return err
		}

		for _, index := range desc.AllNonDropIndexes() {
			if len(index.Interleave.Ancestors) > 0 {
				if err := n.p.finalizeInterleave(&desc, index); err != nil {
					return err
				}
			}
		}

		if err := desc.Validate(n.p.txn); err != nil {
			return err
		}

		// Log Create Table event. This is an auditable log event and is
		// recorded in the same transaction as the table descriptor update.
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

func (n *createTableNode) Next() (bool, error)                 { return false, nil }
func (n *createTableNode) Columns() []ResultColumn             { return make([]ResultColumn, 0) }
func (n *createTableNode) Ordering() orderingInfo              { return orderingInfo{} }
func (n *createTableNode) Values() parser.DTuple               { return parser.DTuple{} }
func (n *createTableNode) DebugValues() debugValues            { return debugValues{} }
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
	srcIdx    sqlbase.IndexID          // ID of source (referencing) index
	target    *sqlbase.TableDescriptor // Table to update
	targetIdx sqlbase.IndexID          // ID of target (referenced) index
}

func (n *createTableNode) resolveFK(
	tbl *sqlbase.TableDescriptor,
	fromCols parser.NameList,
	targetTable *parser.QualifiedName,
	targetColNames parser.NameList,
	constraintName parser.Name,
) (fkTargetUpdate, error) {
	var ret fkTargetUpdate
	srcCols, err := tbl.FindActiveColumnsByNames(fromCols)
	if err != nil {
		return ret, err
	}

	target, err := n.p.getTableDesc(targetTable)
	if err != nil {
		return ret, err
	}
	if target == nil {
		if targetTable.String() == tbl.Name {
			target = tbl
		} else {
			return ret, fmt.Errorf("referenced table %q not found", targetTable.String())
		}
	}
	ret.target = target

	// If no columns are specified, attempt to default to PK.
	if len(targetColNames) == 0 {
		targetColNames = target.PrimaryIndex.ColumnNames
	}

	targetCols, err := target.FindActiveColumnsByNames(targetColNames)
	if err != nil {
		return ret, err
	}

	if len(targetCols) != len(srcCols) {
		return ret, errors.Errorf("%d columns must reference exactly %d columns in referenced table (found %d)",
			len(srcCols), len(srcCols), len(targetCols))
	}

	for i := range srcCols {
		if s, t := srcCols[i], targetCols[i]; s.Type.Kind != t.Type.Kind {
			return ret, fmt.Errorf("type of %q (%s) does not match foreign key %q.%q (%s)",
				s.Name, s.Type.Kind, target.Name, t.Name, t.Type.Kind)
		}
	}

	type indexMatch bool
	const (
		matchExact  indexMatch = true
		matchPrefix indexMatch = false
	)

	// Referenced cols must be unique, thus referenced indexes must match exactly.
	// Referencing cols have no uniqueness requirement and thus may match a
	// strict prefix of an index.
	matchesIndex := func(
		cols []sqlbase.ColumnDescriptor, idx sqlbase.IndexDescriptor, exact indexMatch,
	) bool {
		if len(cols) > len(idx.ColumnIDs) || (exact && len(cols) != len(idx.ColumnIDs)) {
			return false
		}

		for i := range cols {
			if cols[i].ID != idx.ColumnIDs[i] {
				return false
			}
		}
		return true
	}

	if matchesIndex(targetCols, target.PrimaryIndex, matchExact) {
		ret.targetIdx = target.PrimaryIndex.ID
	} else {
		found := false
		// Find the index corresponding to the referenced column.
		for _, idx := range target.Indexes {
			if idx.Unique && matchesIndex(targetCols, idx, matchExact) {
				ret.targetIdx = idx.ID
				found = true
				break
			}
		}
		if !found {
			return ret, fmt.Errorf("foreign key requires table %q have a unique index on %s", targetTable.String(), colNames(targetCols))
		}
	}

	if constraintName == "" {
		constraintName = parser.Name(fmt.Sprintf("fk_%s_ref_%s", fromCols[0], target.Name))
	}

	ref := sqlbase.ForeignKeyReference{Table: target.ID, Index: ret.targetIdx, Name: string(constraintName)}

	if matchesIndex(srcCols, tbl.PrimaryIndex, matchPrefix) {
		tbl.PrimaryIndex.ForeignKey = ref
		ret.srcIdx = tbl.PrimaryIndex.ID
	} else {
		found := false
		for i := range tbl.Indexes {
			if matchesIndex(srcCols, tbl.Indexes[i], matchPrefix) {
				tbl.Indexes[i].ForeignKey = ref
				ret.srcIdx = tbl.Indexes[i].ID
				found = true
				break
			}
		}
		if !found {
			return ret, fmt.Errorf("foreign key columns %s must be the prefix of an index", colNames(srcCols))
		}
	}

	// Create the table non-public, since we'll need to ensure the FK back-refs
	// are in place before we can allow writes.
	tbl.State = sqlbase.TableDescriptor_ADD
	return ret, nil
}

// colNames converts a []colDesc to a human-readable string for use in error messages.
func colNames(cols []sqlbase.ColumnDescriptor) string {
	var s bytes.Buffer
	s.WriteString(`("`)
	for i, c := range cols {
		if i != 0 {
			s.WriteString(`", "`)
		}
		s.WriteString(c.Name)
	}
	s.WriteString(`")`)
	return s.String()
}

func (p *planner) saveNonmutationAndNotify(td *sqlbase.TableDescriptor) error {
	if err := td.SetUpVersion(); err != nil {
		return err
	}
	if err := td.ValidateTable(); err != nil {
		return err
	}
	if err := p.writeTableDesc(td); err != nil {
		return err
	}
	p.notifySchemaChange(td.ID, sqlbase.InvalidMutationID)
	return nil
}

// addInterleave marks an index as one that is interleaved in some parent data
// according to the given definition.
func (p *planner) addInterleave(
	desc *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor, interleave *parser.InterleaveDef,
) error {
	if interleave.DropBehavior != parser.DropDefault {
		return util.UnimplementedWithIssueErrorf(
			7854, "unsupported shorthand %s", interleave.DropBehavior)
	}

	parentTable, err := p.mustGetTableDesc(interleave.Parent)
	if err != nil {
		return err
	}
	parentIndex := parentTable.PrimaryIndex

	if len(interleave.Fields) != len(parentIndex.ColumnIDs) {
		return fmt.Errorf("interleaved columns must match parent")
	}
	if len(interleave.Fields) > len(index.ColumnIDs) {
		return fmt.Errorf("declared columns must match index being interleaved")
	}
	for i, targetColID := range parentIndex.ColumnIDs {
		targetCol, err := parentTable.FindColumnByID(targetColID)
		if err != nil {
			return err
		}
		col, err := desc.FindColumnByID(index.ColumnIDs[i])
		if err != nil {
			return err
		}
		if sqlbase.NormalizeName(interleave.Fields[i]) != sqlbase.NormalizeName(col.Name) {
			return fmt.Errorf("declared columns must match index being interleaved")
		}
		if col.Type != targetCol.Type ||
			index.ColumnDirections[i] != parentIndex.ColumnDirections[i] {

			return fmt.Errorf("interleaved columns must match parent")
		}
	}

	ancestorPrefix := append(
		[]sqlbase.InterleaveDescriptor_Ancestor(nil), parentIndex.Interleave.Ancestors...)
	intl := sqlbase.InterleaveDescriptor_Ancestor{
		TableID:         parentTable.ID,
		IndexID:         parentIndex.ID,
		SharedPrefixLen: uint32(len(parentIndex.ColumnIDs)),
	}
	for _, ancestor := range ancestorPrefix {
		intl.SharedPrefixLen -= ancestor.SharedPrefixLen
	}
	index.Interleave = sqlbase.InterleaveDescriptor{Ancestors: append(ancestorPrefix, intl)}

	desc.State = sqlbase.TableDescriptor_ADD
	return nil
}

func (n *createTableNode) finalizeFKs(desc *sqlbase.TableDescriptor, fkTargets []fkTargetUpdate) error {
	for _, t := range fkTargets {
		targetIdx, err := t.target.FindIndexByID(t.targetIdx)
		if err != nil {
			return err
		}
		backref := sqlbase.ForeignKeyReference{Table: desc.ID, Index: t.srcIdx}
		targetIdx.ReferencedBy = append(targetIdx.ReferencedBy, backref)

		// For self-referencing FKs, the ref was added before the table had an ID
		// assigned so we need to update it now to the assigned value.
		if t.target == desc {
			srcIdx, err := desc.FindIndexByID(t.srcIdx)
			if err != nil {
				return err
			}
			srcIdx.ForeignKey.Table = desc.ID
			continue
		}

		// TODO(dt): Only save each referenced table once.
		if err := n.p.saveNonmutationAndNotify(t.target); err != nil {
			return err
		}
	}

	if desc.State == sqlbase.TableDescriptor_ADD {
		desc.State = sqlbase.TableDescriptor_PUBLIC

		if err := n.p.saveNonmutationAndNotify(desc); err != nil {
			return err
		}
	}
	return nil
}

// finalizeInterleave creats backreferences from an interleaving parent to the
// child data being interleaved.
func (p *planner) finalizeInterleave(
	desc *sqlbase.TableDescriptor, index sqlbase.IndexDescriptor,
) error {
	// TODO(dan): This is similar to finalizeFKs. Consolidate them
	if len(index.Interleave.Ancestors) == 0 {
		return nil
	}
	// Only the last ancestor needs the backreference.
	ancestor := index.Interleave.Ancestors[len(index.Interleave.Ancestors)-1]
	var ancestorTable *sqlbase.TableDescriptor
	if ancestor.TableID == desc.ID {
		ancestorTable = desc
	} else {
		var err error
		ancestorTable, err = sqlbase.GetTableDescFromID(p.txn, ancestor.TableID)
		if err != nil {
			return err
		}
	}
	ancestorIndex, err := ancestorTable.FindIndexByID(ancestor.IndexID)
	if err != nil {
		return err
	}
	ancestorIndex.InterleavedBy = append(ancestorIndex.InterleavedBy,
		sqlbase.ForeignKeyReference{Table: desc.ID, Index: index.ID})

	if err := p.saveNonmutationAndNotify(ancestorTable); err != nil {
		return err
	}

	if desc.State == sqlbase.TableDescriptor_ADD {
		desc.State = sqlbase.TableDescriptor_PUBLIC

		if err := p.saveNonmutationAndNotify(desc); err != nil {
			return err
		}
	}

	return nil
}

// CreateTableDescriptor turns a schema string into a TableDescriptor.
func CreateTableDescriptor(
	id, parentID sqlbase.ID, schema string, privileges *sqlbase.PrivilegeDescriptor,
) sqlbase.TableDescriptor {
	stmt, err := parser.ParseOneTraditional(schema)
	if err != nil {
		log.Fatal(context.TODO(), err)
	}

	desc, err := MakeTableDesc(stmt.(*parser.CreateTable), parentID)
	if err != nil {
		log.Fatal(context.TODO(), err)
	}

	desc.Privileges = privileges

	desc.ID = id
	if err := desc.AllocateIDs(); err != nil {
		log.Fatalf(context.TODO(), "%s: %v", desc.Name, err)
	}

	return desc
}

// MakeTableDesc creates a table descriptor from a CreateTable statement.
func MakeTableDesc(p *parser.CreateTable, parentID sqlbase.ID) (sqlbase.TableDescriptor, error) {
	desc := sqlbase.TableDescriptor{}
	if err := p.Table.NormalizeTableName(""); err != nil {
		return desc, err
	}
	desc.Name = p.Table.Table()
	desc.ParentID = parentID
	desc.FormatVersion = sqlbase.FamilyFormatVersion
	// We don't use version 0.
	desc.Version = 1

	var primaryIndexColumnSet map[parser.Name]struct{}
	for _, def := range p.Defs {
		switch d := def.(type) {
		case *parser.ColumnTableDef:
			col, idx, err := sqlbase.MakeColumnDefDescs(d)
			if err != nil {
				return desc, err
			}
			desc.AddColumn(*col)
			if idx != nil {
				if err := desc.AddIndex(*idx, d.PrimaryKey); err != nil {
					return desc, err
				}
			}
			if d.Family.Create || len(d.Family.Name) > 0 {
				// Pass true for `create` and `ifNotExists` because when we're creating
				// a table, we always want to create the specified family if it doesn't
				// exist.
				err := desc.AddColumnToFamilyMaybeCreate(col.Name, string(d.Family.Name), true, true)
				if err != nil {
					return desc, err
				}
			}

		case *parser.IndexTableDef:
			idx := sqlbase.IndexDescriptor{
				Name:             string(d.Name),
				StoreColumnNames: d.Storing,
			}
			if err := idx.FillColumns(d.Columns); err != nil {
				return desc, err
			}
			if err := desc.AddIndex(idx, false); err != nil {
				return desc, err
			}
			if d.Interleave != nil {
				return desc, util.UnimplementedWithIssueErrorf(2972, "interleaving is not yet supported")
			}
		case *parser.UniqueConstraintTableDef:
			idx := sqlbase.IndexDescriptor{
				Name:             string(d.Name),
				Unique:           true,
				StoreColumnNames: d.Storing,
			}
			if err := idx.FillColumns(d.Columns); err != nil {
				return desc, err
			}
			if err := desc.AddIndex(idx, d.PrimaryKey); err != nil {
				return desc, err
			}
			if d.PrimaryKey {
				primaryIndexColumnSet = make(map[parser.Name]struct{})
				for _, c := range d.Columns {
					primaryIndexColumnSet[c.Column] = struct{}{}
				}
			}
			if d.Interleave != nil {
				return desc, util.UnimplementedWithIssueErrorf(2972, "interleaving is not yet supported")
			}
		case *parser.CheckConstraintTableDef:
			// CHECK expressions seem to vary across databases. Wikipedia's entry on
			// Check_constraint (https://en.wikipedia.org/wiki/Check_constraint) says
			// that if the constraint refers to a single column only, it is possible to
			// specify the constraint as part of the column definition. Postgres allows
			// specifying them anywhere about any columns, but it moves all constraints to
			// the table level (i.e., columns never have a check constraint themselves). We
			// will adhere to the stricter definition.

			preFn := func(expr parser.Expr) (err error, recurse bool, newExpr parser.Expr) {
				qname, ok := expr.(*parser.QualifiedName)
				if !ok {
					// Not a qname, don't do anything to this node.
					return nil, true, expr
				}

				if err := qname.NormalizeColumnName(); err != nil {
					return err, false, nil
				}

				if qname.IsStar() {
					return fmt.Errorf("* not allowed in constraint %q", d.Expr.String()), false, nil
				}
				col, err := desc.FindActiveColumnByName(qname.Column())
				if err != nil {
					return fmt.Errorf("column %q not found for constraint %q", qname.String(), d.Expr.String()), false, nil
				}
				// Convert to a dummy datum of the correct type.
				return nil, false, col.Type.ToDatumType()
			}

			expr, err := parser.SimpleVisit(d.Expr, preFn)
			if err != nil {
				return desc, err
			}

			if err := sqlbase.SanitizeVarFreeExpr(expr, parser.TypeBool, "CHECK"); err != nil {
				return desc, err
			}

			var p parser.Parser
			if p.AggregateInExpr(expr) {
				return desc, fmt.Errorf("Aggregate functions are not allowed in CHECK expressions")
			}

			check := &sqlbase.TableDescriptor_CheckConstraint{Expr: d.Expr.String()}
			if len(d.Name) > 0 {
				check.Name = string(d.Name)
			}
			desc.Checks = append(desc.Checks, check)

		case *parser.FamilyTableDef:
			names := make([]string, len(d.Columns))
			for i, col := range d.Columns {
				names[i] = string(col.Column)
			}
			fam := sqlbase.ColumnFamilyDescriptor{
				Name:        string(d.Name),
				ColumnNames: names,
			}
			desc.AddFamily(fam)

		case *parser.ForeignKeyConstraintTableDef:
			// Pass for now since FKs can reference other elements and thus are
			// resolved only after the rest of the desc is constructed.

		default:
			return desc, errors.Errorf("unsupported table def: %T", def)
		}
	}

	if primaryIndexColumnSet != nil {
		// Primary index columns are not nullable.
		for i := range desc.Columns {
			if _, ok := primaryIndexColumnSet[parser.Name(desc.Columns[i].Name)]; ok {
				desc.Columns[i].Nullable = false
			}
		}
	}

	return desc, nil
}
