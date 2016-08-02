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

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
)

// Show a session-local variable name.
func (p *planner) Show(n *parser.Show) (planNode, error) {
	name := strings.ToUpper(n.Name)

	v := &valuesNode{columns: []ResultColumn{{Name: name, Typ: parser.TypeString}}}

	switch name {
	case `DATABASE`:
		v.rows = append(v.rows, []parser.Datum{parser.NewDString(p.session.Database)})
	case `TIME ZONE`:
		v.rows = append(v.rows, []parser.Datum{parser.NewDString(p.session.Location.String())})
	case `SYNTAX`:
		v.rows = append(v.rows, []parser.Datum{parser.NewDString(parser.Syntax(p.session.Syntax).String())})
	case `DEFAULT_TRANSACTION_ISOLATION`:
		level := p.session.DefaultIsolationLevel.String()
		v.rows = append(v.rows, []parser.Datum{parser.NewDString(level)})
	case `TRANSACTION ISOLATION LEVEL`:
		v.rows = append(v.rows, []parser.Datum{parser.NewDString(p.txn.Proto.Isolation.String())})
	case `TRANSACTION PRIORITY`:
		v.rows = append(v.rows, []parser.Datum{parser.NewDString(p.txn.UserPriority.String())})
	default:
		return nil, fmt.Errorf("unknown variable: %q", name)
	}

	return v, nil
}

// ShowColumns of a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW COLUMNS statement.
//          mysql only returns columns you have privileges on.
func (p *planner) ShowColumns(n *parser.ShowColumns) (planNode, error) {
	desc, err := p.mustGetTableDesc(n.Table)
	if err != nil {
		return nil, err
	}
	if err := p.anyPrivilege(desc); err != nil {
		return nil, err
	}

	v := &valuesNode{
		columns: []ResultColumn{
			{Name: "Field", Typ: parser.TypeString},
			{Name: "Type", Typ: parser.TypeString},
			{Name: "Null", Typ: parser.TypeBool},
			{Name: "Default", Typ: parser.TypeString},
		},
	}

	for i, col := range desc.Columns {
		defaultExpr := parser.DNull
		if e := desc.Columns[i].DefaultExpr; e != nil {
			defaultExpr = parser.NewDString(*e)
		}
		v.rows = append(v.rows, []parser.Datum{
			parser.NewDString(desc.Columns[i].Name),
			parser.NewDString(col.Type.SQLString()),
			parser.MakeDBool(parser.DBool(desc.Columns[i].Nullable)),
			defaultExpr,
		})
	}
	return v, nil
}

// showCreateInterleave returns an INTERLEAVE IN PARENT clause for the specified
// index, if applicable.
func (p *planner) showCreateInterleave(idx *sqlbase.IndexDescriptor) (string, error) {
	if len(idx.Interleave.Ancestors) == 0 {
		return "", nil
	}
	intl := idx.Interleave
	parentTable, err := sqlbase.GetTableDescFromID(p.txn, intl.Ancestors[len(intl.Ancestors)-1].TableID)
	if err != nil {
		return "", err
	}
	var sharedPrefixLen int
	for _, ancestor := range intl.Ancestors {
		sharedPrefixLen += int(ancestor.SharedPrefixLen)
	}
	interleavedColumnNames := quoteNames(idx.ColumnNames[:sharedPrefixLen]...)
	s := fmt.Sprintf(" INTERLEAVE IN PARENT %s (%s)", parentTable.Name, interleavedColumnNames)
	return s, nil
}

// ShowCreateTable returns a CREATE TABLE statement for the specified table in
// Traditional syntax.
// Privileges: Any privilege on table.
func (p *planner) ShowCreateTable(n *parser.ShowCreateTable) (planNode, error) {
	desc, err := p.mustGetTableDesc(n.Table)
	if err != nil {
		return nil, err
	}
	if err := p.anyPrivilege(desc); err != nil {
		return nil, err
	}

	v := &valuesNode{
		columns: []ResultColumn{
			{Name: "Table", Typ: parser.TypeString},
			{Name: "CreateTable", Typ: parser.TypeString},
		},
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE %s (", quoteNames(n.Table.String()))
	var primary string
	for i, col := range desc.VisibleColumns() {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString("\n\t")
		fmt.Fprintf(&buf, "%s %s", quoteNames(col.Name), col.Type.SQLString())
		if col.NullableConstraintName != "" {
			fmt.Fprintf(&buf, " CONSTRAINT %s", col.NullableConstraintName)
		}
		if col.Nullable {
			buf.WriteString(" NULL")
		} else {
			buf.WriteString(" NOT NULL")
		}
		if col.DefaultExpr != nil {
			if col.DefaultExprConstraintName != "" {
				fmt.Fprintf(&buf, " CONSTRAINT %s", col.DefaultExprConstraintName)
			}
			fmt.Fprintf(&buf, " DEFAULT %s", *col.DefaultExpr)
		}
		if len(desc.PrimaryIndex.ColumnIDs) > 0 && desc.PrimaryIndex.ColumnIDs[0] == col.ID {
			// Only set primary if the primary key is on a visible column (not rowid).
			primary = fmt.Sprintf(",\n\tCONSTRAINT %s PRIMARY KEY (%s)",
				quoteNames(desc.PrimaryIndex.Name),
				quoteNames(desc.PrimaryIndex.ColumnNames...),
			)
		}
	}
	buf.WriteString(primary)
	for _, idx := range desc.Indexes {
		var storing string
		if len(idx.StoreColumnNames) > 0 {
			storing = fmt.Sprintf(" STORING (%s)", quoteNames(idx.StoreColumnNames...))
		}
		interleave, err := p.showCreateInterleave(&idx)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(&buf, ",\n\t%sINDEX %s (%s)%s%s",
			isUnique[idx.Unique],
			quoteNames(idx.Name),
			quoteNames(idx.ColumnNames...),
			storing,
			interleave,
		)
	}
	for _, fam := range desc.Families {
		activeColumnNames := make([]string, 0, len(fam.ColumnNames))
		for i, colID := range fam.ColumnIDs {
			if _, err := desc.FindActiveColumnByID(colID); err == nil {
				activeColumnNames = append(activeColumnNames, fam.ColumnNames[i])
			}
		}
		fmt.Fprintf(&buf, ",\n\tFAMILY %s (%s)",
			quoteNames(fam.Name),
			quoteNames(activeColumnNames...),
		)
	}

	for _, e := range desc.Checks {
		fmt.Fprintf(&buf, ",\n\t")
		if len(e.Name) > 0 {
			fmt.Fprintf(&buf, "CONSTRAINT %s ", quoteNames(e.Name))
		}
		fmt.Fprintf(&buf, "CHECK (%s)", e.Expr)
	}

	buf.WriteString("\n)")
	interleave, err := p.showCreateInterleave(&desc.PrimaryIndex)
	if err != nil {
		return nil, err
	}
	buf.WriteString(interleave)

	v.rows = append(v.rows, []parser.Datum{
		parser.NewDString(n.Table.String()),
		parser.NewDString(buf.String()),
	})
	return v, nil
}

var isUnique = map[bool]string{true: "UNIQUE "}

// quoteName quotes based on Traditional syntax and adds commas between names.
func quoteNames(names ...string) string {
	return parser.NameList(names).String()
}

// ShowDatabases returns all the databases.
// Privileges: None.
//   Notes: postgres does not have a "show databases"
//          mysql has a "SHOW DATABASES" permission, but we have no system-level permissions.
func (p *planner) ShowDatabases(n *parser.ShowDatabases) (planNode, error) {
	// TODO(pmattis): This could be implemented as:
	//
	//   SELECT id FROM system.namespace WHERE parentID = 0

	prefix := sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, "")
	sr, err := p.txn.Scan(prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}
	v := &valuesNode{columns: []ResultColumn{{Name: "Database", Typ: parser.TypeString}}}
	for db := range virtualSchemaMap {
		v.rows = append(v.rows, []parser.Datum{parser.NewDString(db)})
	}
	for _, row := range sr {
		_, name, err := encoding.DecodeUnsafeStringAscending(
			bytes.TrimPrefix(row.Key, prefix), nil)
		if err != nil {
			return nil, err
		}
		v.rows = append(v.rows, []parser.Datum{parser.NewDString(name)})
	}
	return v, nil
}

// ShowGrants returns grant details for the specified objects and users.
// TODO(marc): implement no targets (meaning full scan).
// Privileges: None.
//   Notes: postgres does not have a SHOW GRANTS statement.
//          mysql only returns the user's privileges.
func (p *planner) ShowGrants(n *parser.ShowGrants) (planNode, error) {
	if n.Targets == nil {
		return nil, errors.Errorf("TODO(marc): implement SHOW GRANT with no targets")
	}
	descriptors, err := p.getDescriptorsFromTargetList(*n.Targets)
	if err != nil {
		return nil, err
	}

	objectType := "Database"
	if n.Targets.Tables != nil {
		objectType = "Table"
	}

	v := &valuesNode{
		columns: []ResultColumn{
			{Name: objectType, Typ: parser.TypeString},
			{Name: "User", Typ: parser.TypeString},
			{Name: "Privileges", Typ: parser.TypeString},
		},
	}
	var wantedUsers map[string]struct{}
	if len(n.Grantees) != 0 {
		wantedUsers = make(map[string]struct{})
	}
	for _, u := range n.Grantees {
		wantedUsers[u] = struct{}{}
	}

	for _, descriptor := range descriptors {
		userPrivileges := descriptor.GetPrivileges().Show()
		for _, userPriv := range userPrivileges {
			if wantedUsers != nil {
				if _, ok := wantedUsers[userPriv.User]; !ok {
					continue
				}
			}
			v.rows = append(v.rows, []parser.Datum{
				parser.NewDString(descriptor.GetName()),
				parser.NewDString(userPriv.User),
				parser.NewDString(userPriv.Privileges),
			})
		}
	}
	return v, nil
}

// ShowIndex returns all the indexes for a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW INDEXES statement.
//          mysql requires some privilege for any column.
func (p *planner) ShowIndex(n *parser.ShowIndex) (planNode, error) {
	desc, err := p.mustGetTableDesc(n.Table)
	if err != nil {
		return nil, err
	}
	if err := p.anyPrivilege(desc); err != nil {
		return nil, err
	}

	v := &valuesNode{
		columns: []ResultColumn{
			{Name: "Table", Typ: parser.TypeString},
			{Name: "Name", Typ: parser.TypeString},
			{Name: "Unique", Typ: parser.TypeBool},
			{Name: "Seq", Typ: parser.TypeInt},
			{Name: "Column", Typ: parser.TypeString},
			{Name: "Direction", Typ: parser.TypeString},
			{Name: "Storing", Typ: parser.TypeBool},
		},
	}

	appendRow := func(index sqlbase.IndexDescriptor, colName string, sequence int,
		direction string, isStored bool) {
		v.rows = append(v.rows, []parser.Datum{
			parser.NewDString(n.Table.Table()),
			parser.NewDString(index.Name),
			parser.MakeDBool(parser.DBool(index.Unique)),
			parser.NewDInt(parser.DInt(sequence)),
			parser.NewDString(colName),
			parser.NewDString(direction),
			parser.MakeDBool(parser.DBool(isStored)),
		})
	}

	for _, index := range append([]sqlbase.IndexDescriptor{desc.PrimaryIndex}, desc.Indexes...) {
		sequence := 1
		for i, col := range index.ColumnNames {
			appendRow(index, col, sequence, index.ColumnDirections[i].String(), false)
			sequence++
		}
		for _, col := range index.StoreColumnNames {
			appendRow(index, col, sequence, "N/A", true)
			sequence++
		}
	}
	return v, nil
}

// ShowConstraints returns all the constraints for a table.
// Privileges: None.
//   Notes: postgres does not have a SHOW CONSTRAINTS statement.
//          mysql requires some privilege for any column.
func (p *planner) ShowConstraints(n *parser.ShowConstraints) (planNode, error) {
	desc, err := p.mustGetTableDesc(n.Table)
	if err != nil {
		return nil, err
	}

	v := &valuesNode{
		columns: []ResultColumn{
			{Name: "Table", Typ: parser.TypeString},
			{Name: "Name", Typ: parser.TypeString},
			{Name: "Type", Typ: parser.TypeString},
			{Name: "Column(s)", Typ: parser.TypeString},
			{Name: "Details", Typ: parser.TypeString},
		},
	}

	appendRow := func(name, typ, columns, details string) {
		detailsDatum := parser.DNull
		if details != "" {
			detailsDatum = parser.NewDString(details)
		}
		columnsDatum := parser.DNull
		if columns != "" {
			columnsDatum = parser.NewDString(columns)
		}
		v.rows = append(v.rows, []parser.Datum{
			parser.NewDString(n.Table.Table()),
			parser.NewDString(name),
			parser.NewDString(typ),
			columnsDatum,
			detailsDatum,
		})
	}

	for _, index := range append([]sqlbase.IndexDescriptor{desc.PrimaryIndex}, desc.Indexes...) {
		if index.ID == desc.PrimaryIndex.ID {
			appendRow(index.Name, "PRIMARY KEY", fmt.Sprintf("%+v", index.ColumnNames), "")
		} else if index.Unique {
			appendRow(index.Name, "UNIQUE", fmt.Sprintf("%+v", index.ColumnNames), "")
		}
		if index.ForeignKey.IsSet() {
			other, err := p.getTableLeaseByID(index.ForeignKey.Table)
			if err != nil {
				return nil, errors.Errorf("error resolving table %d referenced in foreign key",
					index.ForeignKey.Table)
			}
			otherIdx, err := other.FindIndexByID(index.ForeignKey.Index)
			if err != nil {
				return nil, errors.Errorf("error resolving index %d in table %s referenced in foreign key",
					index.ForeignKey.Index, other.Name)
			}
			appendRow(index.ForeignKey.Name, "FOREIGN KEY", fmt.Sprintf("%v", index.ColumnNames),
				fmt.Sprintf("%s.%v", other.Name, otherIdx.ColumnNames))
		}
	}
	for _, c := range desc.Checks {
		appendRow(c.Name, "CHECK", "", c.Expr)
	}

	for _, c := range desc.Columns {
		if c.DefaultExprConstraintName != "" {
			appendRow(c.DefaultExprConstraintName, "DEFAULT", c.Name, *c.DefaultExpr)
		}
		if c.NullableConstraintName != "" {
			if c.Nullable {
				appendRow(c.NullableConstraintName, "NULL", c.Name, "")
			} else {
				appendRow(c.NullableConstraintName, "NOT NULL", c.Name, "")
			}
		}
	}

	// Sort the results by constraint name.
	sort := &sortNode{
		ctx: p.ctx(),
		ordering: sqlbase.ColumnOrdering{
			{ColIdx: 0, Direction: encoding.Ascending},
			{ColIdx: 1, Direction: encoding.Ascending},
		},
		columns: v.columns,
	}
	return &selectTopNode{source: v, sort: sort}, nil
}

// ShowTables returns all the tables.
// Privileges: None.
//   Notes: postgres does not have a SHOW TABLES statement.
//          mysql only returns tables you have privileges on.
func (p *planner) ShowTables(n *parser.ShowTables) (planNode, error) {
	// TODO(pmattis): This could be implemented as:
	//
	//   SELECT name FROM system.namespace
	//     WHERE parentID = (SELECT id FROM system.namespace
	//                       WHERE parentID = 0 AND name = <database>)

	name := n.Name
	if name == nil {
		if p.session.Database == "" {
			return nil, errNoDatabase
		}
		name = &parser.QualifiedName{Base: parser.Name(p.session.Database)}
	}
	dbDesc, err := p.mustGetDatabaseDesc(string(name.Base))
	if err != nil {
		return nil, err
	}

	tableNames, err := p.getTableNames(dbDesc)
	if err != nil {
		return nil, err
	}
	v := &valuesNode{columns: []ResultColumn{{Name: "Table", Typ: parser.TypeString}}}
	for _, name := range tableNames {
		v.rows = append(v.rows, []parser.Datum{parser.NewDString(name.Table())})
	}

	return v, nil
}
