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
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	tableDesc, err := p.mustGetTableDesc(tn)
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
	status, i, err := n.tableDesc.FindIndexByName(n.n.Name)
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
		StoreColumnNames: n.n.Storing.ToStrings(),
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
	p          *planner
	n          *parser.CreateTable
	dbDesc     *sqlbase.DatabaseDescriptor
	insertPlan planNode
	selectPlan planNode
}

// CreateTable creates a table.
// Privileges: CREATE on database.
//   Notes: postgres/mysql require CREATE on database.
func (p *planner) CreateTable(n *parser.CreateTable) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	dbDesc, err := p.mustGetDatabaseDesc(tn.Database())
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	hoistConstraints(n)
	for _, def := range n.Defs {
		switch t := def.(type) {
		case *parser.ForeignKeyConstraintTableDef:
			if _, err := t.Table.NormalizeWithDatabaseName(p.session.Database); err != nil {
				return nil, err
			}
		}
	}

	var selectPlan planNode
	if n.As() {
		selectPlan, err = p.getSelectPlan(n)
		if err != nil {
			return nil, err
		}
	}
	return &createTableNode{p: p, n: n, dbDesc: dbDesc, selectPlan: selectPlan}, nil
}

func removeParens(sel parser.SelectStatement) (parser.SelectStatement, error) {
	switch ps := sel.(type) {
	case *parser.SelectClause:
		return ps, nil
	case *parser.ParenSelect:
		return removeParens(ps.Select.Select)
	default:
		return nil, errors.Errorf("Invalid Select type.")
	}
}

func (p *planner) getSelectPlan(n *parser.CreateTable) (planNode, error) {
	selNoParens, err := removeParens(n.AsSource.Select)
	if err != nil {
		return nil, errors.Errorf("Invalid Select type.")
	}
	s, err := p.SelectClause(selNoParens.(*parser.SelectClause), n.AsSource.OrderBy, n.AsSource.Limit, []parser.Datum{}, 0)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func hoistConstraints(n *parser.CreateTable) {
	for _, d := range n.Defs {
		if col, ok := d.(*parser.ColumnTableDef); ok {
			if col.CheckExpr.Expr != nil {
				n.Defs = append(n.Defs,
					&parser.CheckConstraintTableDef{
						Expr: col.CheckExpr.Expr,
						Name: col.CheckExpr.ConstraintName,
					},
				)
				col.CheckExpr.Expr = nil
			}
			if col.References.Table.TableNameReference != nil {
				var targetCol parser.NameList
				if col.References.Col != "" {
					targetCol = append(targetCol, col.References.Col)
				}
				n.Defs = append(n.Defs, &parser.ForeignKeyConstraintTableDef{
					Table:    col.References.Table,
					FromCols: parser.NameList{col.Name},
					ToCols:   targetCol,
					Name:     col.References.ConstraintName,
				})
				col.References.Table = parser.NormalizableTableName{}
			}
		}
	}
}

func (n *createTableNode) expandPlan() error {
	if n.n.As() {
		return n.selectPlan.expandPlan()
	}
	return nil
}

func (n *createTableNode) Start() error {
	var desc sqlbase.TableDescriptor
	var err error
	if n.n.As() {
		desc, err = makeTableDescIfAs(n.n, n.dbDesc.ID, n.selectPlan.Columns())
	} else {
		desc, err = MakeTableDesc(n.n, n.dbDesc.ID)
	}
	if err != nil {
		return err
	}

	tKey := tableKey{parentID: n.dbDesc.ID, name: n.n.Table.TableName().Table()}
	key := tKey.Key()
	if exists, err := n.p.descExists(key); err == nil && exists {
		if n.n.IfNotExists {
			return nil
		}
		return descriptorAlreadyExistsErr{&desc, tKey.Name()}
	} else if err != nil {
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

	id, err := generateUniqueDescID(n.p.txn)
	if err != nil {
		return err
	}
	desc.SetID(id)

	if err := desc.AllocateIDs(); err != nil {
		return err
	}

	if n.n.Interleave != nil {
		if err := n.p.addInterleave(&desc, &desc.PrimaryIndex, n.n.Interleave); err != nil {
			return err
		}
	}

	// FKs are resolved after the descriptor is otherwise complete and IDs have
	// been allocated since the FKs will reference those IDs. Resolution also
	// accumulated updates to other tables (adding backreferences) in the passed
	// map -- anything in that map should be saved when the table is created.
	affected := make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	for _, def := range n.n.Defs {
		if d, ok := def.(*parser.ForeignKeyConstraintTableDef); ok {
			err := n.p.resolveFK(&desc, d, affected, sqlbase.ConstraintValidity_Validated)
			if err != nil {
				return err
			}
		}
	}

	// Multiple FKs from the same column would potentially result in ambiguous or
	// unexpected behavior with conflicting CASCADE/RESTRICT/etc behaviors.
	colsInFKs := make(map[sqlbase.ColumnID]struct{})
	for _, idx := range desc.Indexes {
		if idx.ForeignKey.IsSet() {
			for i := range idx.ColumnIDs {
				if _, ok := colsInFKs[idx.ColumnIDs[i]]; ok {
					return errors.Errorf("column %q cannot be used by multiple foreign key constraints", idx.ColumnNames[i])
				}
				colsInFKs[idx.ColumnIDs[i]] = struct{}{}
			}
		}
	}

	// We need to validate again after adding the FKs.
	// Only validate the table because backreferences aren't created yet.
	// Everything is validated below.
	err = desc.ValidateTable()
	if err != nil {
		return err
	}

	created, err := n.p.createDescriptorWithID(key, id, &desc)
	if err != nil {
		return err
	}

	if created {
		for _, updated := range affected {
			if err := n.p.saveNonmutationAndNotify(updated); err != nil {
				return err
			}
		}
		if desc.Adding() {
			n.p.notifySchemaChange(desc.ID, sqlbase.InvalidMutationID)
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

	if n.n.As() {
		resultColumns := n.selectPlan.Columns()
		if err != nil {
			return err
		}
		desiredTypesFromSelect := make([]parser.Datum, len(resultColumns))
		for i, col := range resultColumns {
			desiredTypesFromSelect[i] = col.Typ
		}
		insert := &parser.Insert{Table: &n.n.Table, Rows: n.n.AsSource}
		insertPlan, err := n.p.Insert(insert, desiredTypesFromSelect, false)
		if err != nil {
			return err
		}
		n.insertPlan = insertPlan
		err = insertPlan.expandPlan()
		if err != nil {
			return err
		}
		err = insertPlan.Start()
		if err != nil {
			return err
		}
		// This loop is done here instead of in the Next method
		// since CREATE TABLE is a DDL statement and Executor only
		// runs Next() for statements with type "Rows".
		for done := true; done; done, err = insertPlan.Next() {
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *createTableNode) Next() (bool, error) {
	return false, nil
}

func (n *createTableNode) Columns() []ResultColumn             { return make([]ResultColumn, 0) }
func (n *createTableNode) Ordering() orderingInfo              { return orderingInfo{} }
func (n *createTableNode) Values() parser.DTuple               { return parser.DTuple{} }
func (n *createTableNode) DebugValues() debugValues            { return debugValues{} }
func (n *createTableNode) ExplainTypes(_ func(string, string)) {}
func (n *createTableNode) SetLimitHint(_ int64, _ bool)        {}
func (n *createTableNode) MarkDebug(mode explainMode)          {}
func (n *createTableNode) ExplainPlan(v bool) (string, string, []planNode) {
	if n.n.As() {
		return "create table", "create table as", []planNode{n.selectPlan}
	}
	return "create table", "", nil
}

// resolveFK looks up the tables and columns mentioned in a `REFERENCES`
// constraint and adds metadata representing that constraint to the descriptor.
// It may, in doing so, add to or alter descriptors in the passed in `backrefs`
// map of other tables that need to be updated when this table is created.
// Constraints that are not known to hold for existing data are created
// "unvalidated", but when table is empty (e.g. during creation), no existing
// data imples no existing violations, and thus the constraint can be created
// without the unvalidated flag.
func (p *planner) resolveFK(
	tbl *sqlbase.TableDescriptor,
	d *parser.ForeignKeyConstraintTableDef,
	backrefs map[sqlbase.ID]*sqlbase.TableDescriptor,
	mode sqlbase.ConstraintValidity,
) error {
	targetTable := d.Table.TableName()
	target, err := p.getTableDesc(targetTable)
	if err != nil {
		return err
	}
	// Special-case: self-referencing FKs (i.e. referencing another col in the
	// same table) will reference a table name that doesn't exist yet (since we
	// are creating it).
	if target == nil {
		if targetTable.Table() == tbl.Name {
			target = tbl
		} else {
			return fmt.Errorf("referenced table %q not found", targetTable.String())
		}
	} else {
		// Since this FK is referencing another table, this table must be created in
		// a non-public "ADD" state and made public only after all leases on the
		// other table are updated to include the backref.
		if mode == sqlbase.ConstraintValidity_Validated {
			tbl.State = sqlbase.TableDescriptor_ADD
			if err := tbl.SetUpVersion(); err != nil {
				return err
			}
		}

		// If we resolve the same table more than once, we only want to edit a
		// single instance of it, so replace target with previously resolved table.
		if prev, ok := backrefs[target.ID]; ok {
			target = prev
		} else {
			backrefs[target.ID] = target
		}
	}

	srcCols, err := tbl.FindActiveColumnsByNames(d.FromCols)
	if err != nil {
		return err
	}

	targetColNames := d.ToCols
	// If no columns are specified, attempt to default to PK.
	if len(targetColNames) == 0 {
		targetColNames = make(parser.NameList, len(target.PrimaryIndex.ColumnNames))
		for i, n := range target.PrimaryIndex.ColumnNames {
			targetColNames[i] = parser.Name(n)
		}
	}

	targetCols, err := target.FindActiveColumnsByNames(targetColNames)
	if err != nil {
		return err
	}

	if len(targetCols) != len(srcCols) {
		return errors.Errorf("%d columns must reference exactly %d columns in referenced table (found %d)",
			len(srcCols), len(srcCols), len(targetCols))
	}

	for i := range srcCols {
		if s, t := srcCols[i], targetCols[i]; s.Type.Kind != t.Type.Kind {
			return fmt.Errorf("type of %q (%s) does not match foreign key %q.%q (%s)",
				s.Name, s.Type.Kind, target.Name, t.Name, t.Type.Kind)
		}
	}

	type indexMatch bool
	const (
		matchExact  indexMatch = true
		matchPrefix indexMatch = false
	)

	// Referenced cols must be unique, thus referenced indexes must match exactly.
	// Referencing cols have no uniqueness requirement and thus may match a strict
	// prefix of an index.
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

	constraintName := string(d.Name)
	if constraintName == "" {
		constraintName = fmt.Sprintf("fk_%s_ref_%s", d.FromCols[0], target.Name)
	}

	var targetIdx *sqlbase.IndexDescriptor
	if matchesIndex(targetCols, target.PrimaryIndex, matchExact) {
		targetIdx = &target.PrimaryIndex
	} else {
		found := false
		// Find the index corresponding to the referenced column.
		for i, idx := range target.Indexes {
			if idx.Unique && matchesIndex(targetCols, idx, matchExact) {
				targetIdx = &target.Indexes[i]
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("foreign key requires table %q have a unique index on %s", targetTable.String(), colNames(targetCols))
		}
	}

	ref := sqlbase.ForeignKeyReference{Table: target.ID, Index: targetIdx.ID, Name: constraintName}
	if mode == sqlbase.ConstraintValidity_Unvalidated {
		ref.Validity = sqlbase.ConstraintValidity_Unvalidated
	}
	backref := sqlbase.ForeignKeyReference{Table: tbl.ID}

	if matchesIndex(srcCols, tbl.PrimaryIndex, matchPrefix) {
		if tbl.PrimaryIndex.ForeignKey.IsSet() {
			return fmt.Errorf("columns cannot be used by multiple foreign key constraints")
		}
		tbl.PrimaryIndex.ForeignKey = ref
		backref.Index = tbl.PrimaryIndex.ID
		targetIdx.ReferencedBy = append(targetIdx.ReferencedBy, backref)
		return nil
	}
	for i := range tbl.Indexes {
		if matchesIndex(srcCols, tbl.Indexes[i], matchPrefix) {
			if tbl.Indexes[i].ForeignKey.IsSet() {
				return fmt.Errorf("columns cannot be used by multiple foreign key constraints")
			}
			tbl.Indexes[i].ForeignKey = ref
			backref.Index = tbl.Indexes[i].ID
			targetIdx.ReferencedBy = append(targetIdx.ReferencedBy, backref)
			return nil
		}
	}
	return fmt.Errorf("foreign key columns %s must be the prefix of an index", colNames(srcCols))
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

	tn, err := interleave.Parent.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return err
	}

	parentTable, err := p.mustGetTableDesc(tn)
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
		if sqlbase.NormalizeName(interleave.Fields[i]) != sqlbase.ReNormalizeName(col.Name) {
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

// makeTableDescIfAs is the MakeTableDesc method for when we have a table
// that is created with the CREATE AS format.
func makeTableDescIfAs(
	p *parser.CreateTable, parentID sqlbase.ID, resultColumns []ResultColumn,
) (sqlbase.TableDescriptor, error) {
	desc := sqlbase.TableDescriptor{
		ParentID:      parentID,
		FormatVersion: sqlbase.InterleavedFormatVersion,
		Version:       1,
	}
	tableName, err := p.Table.Normalize()
	if err != nil {
		return desc, err
	}
	desc.Name = tableName.String()
	for _, colRes := range resultColumns {
		colType, _ := parser.DatumTypeToColumnType(colRes.Typ)
		columnTableDef := parser.ColumnTableDef{Name: parser.Name(colRes.Name), Type: colType}
		col, _, err := sqlbase.MakeColumnDefDescs(&columnTableDef)
		if err != nil {
			return desc, err
		}
		desc.AddColumn(*col)
	}
	return desc, nil
}

// MakeTableDesc creates a table descriptor from a CreateTable statement.
func MakeTableDesc(p *parser.CreateTable, parentID sqlbase.ID) (sqlbase.TableDescriptor, error) {
	desc := sqlbase.TableDescriptor{
		ParentID:      parentID,
		FormatVersion: sqlbase.InterleavedFormatVersion,
		Version:       1,
	}
	t, err := p.Table.Normalize()
	if err != nil {
		return desc, err
	}
	desc.Name = string(t.TableName)

	generatedNames := map[string]struct{}{}

	var primaryIndexColumnSet map[string]struct{}
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
				StoreColumnNames: d.Storing.ToStrings(),
			}
			if err := idx.FillColumns(d.Columns); err != nil {
				return desc, err
			}
			if err := desc.AddIndex(idx, false); err != nil {
				return desc, err
			}
			if d.Interleave != nil {
				return desc, util.UnimplementedWithIssueErrorf(9148, "use CREATE INDEX to make interleaved indexes")
			}
		case *parser.UniqueConstraintTableDef:
			idx := sqlbase.IndexDescriptor{
				Name:             string(d.Name),
				Unique:           true,
				StoreColumnNames: d.Storing.ToStrings(),
			}
			if err := idx.FillColumns(d.Columns); err != nil {
				return desc, err
			}
			if err := desc.AddIndex(idx, d.PrimaryKey); err != nil {
				return desc, err
			}
			if d.PrimaryKey {
				primaryIndexColumnSet = make(map[string]struct{})
				for _, c := range d.Columns {
					primaryIndexColumnSet[sqlbase.NormalizeName(c.Column)] = struct{}{}
				}
			}
			if d.Interleave != nil {
				return desc, util.UnimplementedWithIssueErrorf(9148, "use CREATE INDEX to make interleaved indexes")
			}
		case *parser.CheckConstraintTableDef:
			ck, err := makeCheckConstraint(desc, d, generatedNames)
			if err != nil {
				return desc, err
			}
			desc.Checks = append(desc.Checks, ck)

		case *parser.FamilyTableDef:
			fam := sqlbase.ColumnFamilyDescriptor{
				Name:        string(d.Name),
				ColumnNames: d.Columns.ToStrings(),
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
			if _, ok := primaryIndexColumnSet[sqlbase.ReNormalizeName(desc.Columns[i].Name)]; ok {
				desc.Columns[i].Nullable = false
			}
		}
	}

	return desc, nil
}

func makeCheckConstraint(
	desc sqlbase.TableDescriptor, d *parser.CheckConstraintTableDef, inuseNames map[string]struct{},
) (*sqlbase.TableDescriptor_CheckConstraint, error) {
	// CHECK expressions seem to vary across databases. Wikipedia's entry on
	// Check_constraint (https://en.wikipedia.org/wiki/Check_constraint) says
	// that if the constraint refers to a single column only, it is possible to
	// specify the constraint as part of the column definition. Postgres allows
	// specifying them anywhere about any columns, but it moves all constraints to
	// the table level (i.e., columns never have a check constraint themselves). We
	// will adhere to the stricter definition.

	var nameBuf bytes.Buffer
	name := string(d.Name)

	generateName := name == ""
	if generateName {
		nameBuf.WriteString("check")
	}

	preFn := func(expr parser.Expr) (err error, recurse bool, newExpr parser.Expr) {
		vBase, ok := expr.(parser.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return nil, true, expr
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return err, false, nil
		}

		c, ok := v.(*parser.ColumnItem)
		if !ok {
			return nil, true, expr
		}

		col, err := desc.FindActiveColumnByName(c.ColumnName)
		if err != nil {
			return fmt.Errorf("column %q not found for constraint %q",
				c.ColumnName, d.Expr.String()), false, nil
		}
		if generateName {
			nameBuf.WriteByte('_')
			nameBuf.WriteString(col.Name)
		}
		// Convert to a dummy datum of the correct type.
		return nil, false, col.Type.ToDatumType()
	}

	expr, err := parser.SimpleVisit(d.Expr, preFn)
	if err != nil {
		return nil, err
	}

	var p parser.Parser
	if p.AggregateInExpr(expr) {
		return nil, fmt.Errorf("aggregate functions are not allowed in CHECK expressions")
	}

	if err := sqlbase.SanitizeVarFreeExpr(expr, parser.TypeBool, "CHECK"); err != nil {
		return nil, err
	}
	if generateName {
		name = nameBuf.String()

		// If generated name isn't unique, attempt to add a number to the end to
		// get a unique name.
		if _, ok := inuseNames[name]; ok {
			i := 1
			for {
				appended := fmt.Sprintf("%s%d", name, i)
				if _, ok := inuseNames[appended]; !ok {
					name = appended
					break
				}
				i++
			}
		}
		if inuseNames != nil {
			inuseNames[name] = struct{}{}
		}
	}
	return &sqlbase.TableDescriptor_CheckConstraint{Expr: d.Expr.String(), Name: name}, nil
}
