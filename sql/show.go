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

	v := p.newContainerValuesNode(ResultColumns{{Name: name, Typ: parser.TypeString}}, 0)

	var newRow parser.DTuple
	switch name {
	case `DATABASE`:
		newRow = parser.DTuple{parser.NewDString(p.session.Database)}
	case `TIME ZONE`:
		newRow = parser.DTuple{parser.NewDString(p.session.Location.String())}
	case `SYNTAX`:
		newRow = parser.DTuple{parser.NewDString(parser.Syntax(p.session.Syntax).String())}
	case `DEFAULT_TRANSACTION_ISOLATION`:
		level := p.session.DefaultIsolationLevel.String()
		newRow = parser.DTuple{parser.NewDString(level)}
	case `TRANSACTION ISOLATION LEVEL`:
		newRow = parser.DTuple{parser.NewDString(p.txn.Proto.Isolation.String())}
	case `TRANSACTION PRIORITY`:
		newRow = parser.DTuple{parser.NewDString(p.txn.UserPriority.String())}
	default:
		return nil, fmt.Errorf("unknown variable: %q", name)
	}
	if newRow != nil {
		if err := v.rows.AddRow(newRow); err != nil {
			v.rows.Close()
			return nil, err
		}
	}

	return v, nil
}

// ShowColumns of a table.
// Privileges: None.
//   Notes: postgres does not have a SHOW COLUMNS statement.
//          mysql only returns columns you have privileges on.
func (p *planner) ShowColumns(n *parser.ShowColumns) (planNode, error) {
	desc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, sqlbase.NewUndefinedTableError(n.Table.String())
	}
	columns := ResultColumns{
		{Name: "Field", Typ: parser.TypeString},
		{Name: "Type", Typ: parser.TypeString},
		{Name: "Null", Typ: parser.TypeBool},
		{Name: "Default", Typ: parser.TypeString},
	}
	v := p.newContainerValuesNode(columns, 0)
	for i, col := range desc.Columns {
		defaultExpr := parser.Datum(parser.DNull)
		if e := desc.Columns[i].DefaultExpr; e != nil {
			defaultExpr = parser.NewDString(*e)
		}
		newRow := parser.DTuple{
			parser.NewDString(desc.Columns[i].Name),
			parser.NewDString(col.Type.SQLString()),
			parser.MakeDBool(parser.DBool(desc.Columns[i].Nullable)),
			defaultExpr,
		}
		if err := v.rows.AddRow(newRow); err != nil {
			v.rows.Close()
			return nil, err
		}
	}
	return v, nil
}

// ShowCreateTable returns a CREATE TABLE statement for the specified table in
// Traditional syntax.
// Privileges: None.
func (p *planner) ShowCreateTable(n *parser.ShowCreateTable) (planNode, error) {
	desc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, sqlbase.NewUndefinedTableError(n.Table.String())
	}
	columns := ResultColumns{
		{Name: "Table", Typ: parser.TypeString},
		{Name: "CreateTable", Typ: parser.TypeString},
	}
	v := p.newContainerValuesNode(columns, 0)

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
		if desc.PrimaryIndex.ColumnIDs[0] == col.ID {
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
		fmt.Fprintf(&buf, ",\n\t%sINDEX %s (%s)%s",
			isUnique[idx.Unique],
			quoteNames(idx.Name),
			quoteNames(idx.ColumnNames...),
			storing,
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

	if err := v.rows.AddRow(parser.DTuple{
		parser.NewDString(n.Table.String()),
		parser.NewDString(buf.String()),
	}); err != nil {
		v.rows.Close()
		return nil, err
	}
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
	v := p.newContainerValuesNode(ResultColumns{{Name: "Database", Typ: parser.TypeString}}, 0)
	for _, row := range sr {
		_, name, err := encoding.DecodeUnsafeStringAscending(
			bytes.TrimPrefix(row.Key, prefix), nil)
		if err != nil {
			return nil, err
		}
		if err := v.rows.AddRow(parser.DTuple{parser.NewDString(name)}); err != nil {
			v.rows.Close()
			return nil, err
		}
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

	columns := ResultColumns{
		{Name: objectType, Typ: parser.TypeString},
		{Name: "User", Typ: parser.TypeString},
		{Name: "Privileges", Typ: parser.TypeString},
	}
	v := p.newContainerValuesNode(columns, 0)
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
			newRow := parser.DTuple{
				parser.NewDString(descriptor.GetName()),
				parser.NewDString(userPriv.User),
				parser.NewDString(userPriv.Privileges),
			}
			if err := v.rows.AddRow(newRow); err != nil {
				return nil, err
			}
		}
	}
	return v, nil
}

// ShowIndex returns all the indexes for a table.
// Privileges: None.
//   Notes: postgres does not have a SHOW INDEXES statement.
//          mysql requires some privilege for any column.
func (p *planner) ShowIndex(n *parser.ShowIndex) (planNode, error) {
	desc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, sqlbase.NewUndefinedTableError(n.Table.String())
	}

	columns := ResultColumns{
		{Name: "Table", Typ: parser.TypeString},
		{Name: "Name", Typ: parser.TypeString},
		{Name: "Unique", Typ: parser.TypeBool},
		{Name: "Seq", Typ: parser.TypeInt},
		{Name: "Column", Typ: parser.TypeString},
		{Name: "Direction", Typ: parser.TypeString},
		{Name: "Storing", Typ: parser.TypeBool},
	}
	v := p.newContainerValuesNode(columns, 0)

	appendRow := func(index sqlbase.IndexDescriptor, colName string, sequence int,
		direction string, isStored bool) error {
		newRow := parser.DTuple{
			parser.NewDString(n.Table.Table()),
			parser.NewDString(index.Name),
			parser.MakeDBool(parser.DBool(index.Unique)),
			parser.NewDInt(parser.DInt(sequence)),
			parser.NewDString(colName),
			parser.NewDString(direction),
			parser.MakeDBool(parser.DBool(isStored)),
		}
		if err := v.rows.AddRow(newRow); err != nil {
			return err
		}
		return nil
	}
	for _, index := range append([]sqlbase.IndexDescriptor{desc.PrimaryIndex}, desc.Indexes...) {
		sequence := 1
		for i, col := range index.ColumnNames {
			if err := appendRow(index, col, sequence, index.ColumnDirections[i].String(), false); err != nil {
				return nil, err
			}
			sequence++
		}
		for _, col := range index.StoreColumnNames {
			if err := appendRow(index, col, sequence, "N/A", true); err != nil {
				return nil, err
			}
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
	desc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, sqlbase.NewUndefinedTableError(n.Table.String())
	}

	columns := ResultColumns{
		{Name: "Table", Typ: parser.TypeString},
		{Name: "Name", Typ: parser.TypeString},
		{Name: "Type", Typ: parser.TypeString},
		{Name: "Column(s)", Typ: parser.TypeString},
		{Name: "Details", Typ: parser.TypeString},
	}
	v := p.newContainerValuesNode(columns, 0)

	appendRow := func(name, typ, columns, details string) error {
		detailsDatum := parser.DNull
		if details != "" {
			detailsDatum = parser.NewDString(details)
		}
		columnsDatum := parser.DNull
		if columns != "" {
			columnsDatum = parser.NewDString(columns)
		}
		newRow := []parser.Datum{
			parser.NewDString(n.Table.Table()),
			parser.NewDString(name),
			parser.NewDString(typ),
			columnsDatum,
			detailsDatum,
		}
		return v.rows.AddRow(newRow)
	}

	for _, index := range append([]sqlbase.IndexDescriptor{desc.PrimaryIndex}, desc.Indexes...) {
		if index.ID == desc.PrimaryIndex.ID {
			if err := appendRow(index.Name, "PRIMARY KEY", fmt.Sprintf("%+v", index.ColumnNames), ""); err != nil {
				return nil, err
			}
		} else if index.Unique {
			if err := appendRow(index.Name, "UNIQUE", fmt.Sprintf("%+v", index.ColumnNames), ""); err != nil {
				return nil, err
			}
		}
		if index.ForeignKey != nil {
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
			if err := appendRow("", "FOREIGN KEY", fmt.Sprintf("%v", index.ColumnNames),
				fmt.Sprintf("%s.%v", other.Name, otherIdx.ColumnNames)); err != nil {
				return nil, err
			}
		}
	}
	for _, c := range desc.Checks {
		if err := appendRow(c.Name, "CHECK", "", c.Expr); err != nil {
			return nil, err
		}
	}

	for _, c := range desc.Columns {
		if c.DefaultExprConstraintName != "" {
			if err := appendRow(c.DefaultExprConstraintName, "DEFAULT", c.Name, *c.DefaultExpr); err != nil {
				return nil, err
			}
		}
		if c.NullableConstraintName != "" {
			if c.Nullable {
				err = appendRow(c.NullableConstraintName, "NULL", c.Name, "")
			} else {
				err = appendRow(c.NullableConstraintName, "NOT NULL", c.Name, "")
			}
			if err != nil {
				return nil, err
			}
		}
	}

	// Sort the results by constraint name.
	sort := &sortNode{
		ordering: sqlbase.ColumnOrdering{{0, encoding.Ascending}, {1, encoding.Ascending}},
		columns:  v.columns,
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
	dbDesc, err := p.getDatabaseDesc(string(name.Base))
	if err != nil {
		return nil, err
	}
	if dbDesc == nil {
		return nil, sqlbase.NewUndefinedDatabaseError(string(name.Base))
	}

	tableNames, err := p.getTableNames(dbDesc)
	if err != nil {
		return nil, err
	}
	v := p.newContainerValuesNode(ResultColumns{{Name: "Table", Typ: parser.TypeString}}, len(tableNames))
	for _, name := range tableNames {
		if err := v.rows.AddRow(parser.DTuple{parser.NewDString(name.Table())}); err != nil {
			return nil, err
		}
	}

	return v, nil
}
