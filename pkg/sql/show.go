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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

var varGen = map[string]func(p *planner) string{
	`DATABASE`:                      func(p *planner) string { return p.session.Database },
	`DEFAULT_TRANSACTION_ISOLATION`: func(p *planner) string { return p.session.DefaultIsolationLevel.String() },
	`SYNTAX`:                        func(p *planner) string { return parser.Syntax(p.session.Syntax).String() },
	`TIME ZONE`:                     func(p *planner) string { return p.session.Location.String() },
	`TRANSACTION ISOLATION LEVEL`:   func(p *planner) string { return p.txn.Proto.Isolation.String() },
	`TRANSACTION PRIORITY`:          func(p *planner) string { return p.txn.UserPriority.String() },
}
var varNames = func() []string {
	res := make([]string, 0, len(varGen))
	for vName := range varGen {
		res = append(res, vName)
	}
	sort.Strings(res)
	return res
}()

// Show a session-local variable name.
func (p *planner) Show(n *parser.Show) (planNode, error) {
	name := strings.ToUpper(n.Name)

	var columns ResultColumns

	switch name {
	case `ALL`:
		columns = ResultColumns{
			{Name: "Variable", Typ: parser.TypeString},
			{Name: "Value", Typ: parser.TypeString},
		}
	default:
		if _, ok := varGen[name]; !ok {
			return nil, fmt.Errorf("unknown variable: %q", name)
		}
		columns = ResultColumns{{Name: name, Typ: parser.TypeString}}
	}

	return &delayedNode{
		p:       p,
		name:    "SHOW " + name,
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			switch name {
			case `ALL`:
				for _, vName := range varNames {
					gen := varGen[vName]
					value := gen(p)
					if err := v.rows.AddRow(p.ctx(), parser.DTuple{parser.NewDString(vName),
						parser.NewDString(value)}); err != nil {
						v.rows.Close(p.ctx())
						return nil, err
					}
				}
			default:
				// The key in varGen is guaranteed to exist thanks to the
				// check above.
				gen := varGen[name]
				value := gen(p)
				if err := v.rows.AddRow(p.ctx(), parser.DTuple{parser.NewDString(value)}); err != nil {
					v.rows.Close(p.ctx())
					return nil, err
				}
			}

			return v, nil
		},
	}, nil
}

// ShowColumns of a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW COLUMNS statement.
//          mysql only returns columns you have privileges on.
func (p *planner) ShowColumns(n *parser.ShowColumns) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	desc, err := p.mustGetTableDesc(tn)
	if err != nil {
		return nil, err
	}
	if err := p.anyPrivilege(desc); err != nil {
		return nil, err
	}

	columns := ResultColumns{
		{Name: "Field", Typ: parser.TypeString},
		{Name: "Type", Typ: parser.TypeString},
		{Name: "Null", Typ: parser.TypeBool},
		{Name: "Default", Typ: parser.TypeString},
	}
	return &delayedNode{
		p:       p,
		name:    "SHOW COLUMNS FROM " + tn.String(),
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			for i, col := range desc.Columns {
				defaultExpr := parser.DNull
				if e := desc.Columns[i].DefaultExpr; e != nil {
					defaultExpr = parser.NewDString(*e)
				}
				newRow := parser.DTuple{
					parser.NewDString(desc.Columns[i].Name),
					parser.NewDString(col.Type.SQLString()),
					parser.MakeDBool(parser.DBool(desc.Columns[i].Nullable)),
					defaultExpr,
				}
				if err := v.rows.AddRow(p.ctx(), newRow); err != nil {
					v.rows.Close(p.ctx())
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
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
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	desc, err := p.mustGetTableDesc(tn)
	if err != nil {
		return nil, err
	}
	if err := p.anyPrivilege(desc); err != nil {
		return nil, err
	}

	columns := ResultColumns{
		{Name: "Table", Typ: parser.TypeString},
		{Name: "CreateTable", Typ: parser.TypeString},
	}

	return &delayedNode{
		p:       p,
		name:    "SHOW CREATE TABLE " + tn.String(),
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
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
				if col.Nullable {
					buf.WriteString(" NULL")
				} else {
					buf.WriteString(" NOT NULL")
				}
				if col.DefaultExpr != nil {
					fmt.Fprintf(&buf, " DEFAULT %s", *col.DefaultExpr)
				}
				if desc.IsPhysicalTable() && desc.PrimaryIndex.ColumnIDs[0] == col.ID {
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
					v.rows.Close(p.ctx())
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
				v.rows.Close(p.ctx())
				return nil, err
			}
			buf.WriteString(interleave)

			if err := v.rows.AddRow(p.ctx(), parser.DTuple{
				parser.NewDString(n.Table.String()),
				parser.NewDString(buf.String()),
			}); err != nil {
				v.rows.Close(p.ctx())
				return nil, err
			}
			return v, nil
		},
	}, nil
}

var isUnique = map[bool]string{true: "UNIQUE "}

// quoteName quotes based on Traditional syntax and adds commas between names.
func quoteNames(names ...string) string {
	nameList := make(parser.NameList, len(names))
	for i, n := range names {
		nameList[i] = parser.Name(n)
	}
	return parser.AsString(nameList)
}

// ShowCreateView returns a CREATE VIEW statement for the specified view in
// Traditional syntax.
// Privileges: Any privilege on view.
func (p *planner) ShowCreateView(n *parser.ShowCreateView) (planNode, error) {
	tn, err := n.View.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	desc, err := p.mustGetViewDesc(tn)
	if err != nil {
		return nil, err
	}
	if err := p.anyPrivilege(desc); err != nil {
		return nil, err
	}

	columns := ResultColumns{
		{Name: "View", Typ: parser.TypeString},
		{Name: "CreateView", Typ: parser.TypeString},
	}
	return &delayedNode{
		p:       p,
		name:    "SHOW CREATE VIEW " + tn.String(),
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			var buf bytes.Buffer
			fmt.Fprintf(&buf, "CREATE VIEW %s ", quoteNames(n.View.String()))

			// Determine whether custom column names were specified when the view
			// was created, and include them if so.
			customColNames := false
			stmt, err := parser.ParseOneTraditional(desc.ViewQuery)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse underlying query from view %q", tn)
			}
			sel, ok := stmt.(*parser.Select)
			if !ok {
				return nil, errors.Errorf("failed to parse underlying query from view %q as a select", tn)
			}

			// When constructing the Select plan, make sure we don't require any
			// privileges on the underlying tables.
			p.skipSelectPrivilegeChecks = true
			defer func() { p.skipSelectPrivilegeChecks = false }()

			sourcePlan, err := p.Select(sel, []parser.Type{}, false)
			if err != nil {
				return nil, err
			}
			for i, col := range sourcePlan.Columns() {
				if col.Name != desc.Columns[i].Name {
					customColNames = true
					break
				}
			}
			if customColNames {
				colNames := make([]string, 0, len(desc.Columns))
				for _, col := range desc.Columns {
					colNames = append(colNames, col.Name)
				}
				fmt.Fprintf(&buf, "(%s) ", strings.Join(colNames, ", "))
			}

			fmt.Fprintf(&buf, "AS %s", desc.ViewQuery)
			if err := v.rows.AddRow(p.ctx(), parser.DTuple{
				parser.NewDString(n.View.String()),
				parser.NewDString(buf.String()),
			}); err != nil {
				v.rows.Close(p.ctx())
				return nil, err
			}
			return v, nil
		},
	}, nil
}

// ShowDatabases returns all the databases.
// Privileges: None.
//   Notes: postgres does not have a "show databases"
//          mysql has a "SHOW DATABASES" permission, but we have no system-level permissions.
func (p *planner) ShowDatabases(n *parser.ShowDatabases) (planNode, error) {
	// TODO(pmattis): This could be implemented as:
	//
	//   SELECT id FROM system.namespace WHERE parentID = 0

	columns := ResultColumns{{Name: "Database", Typ: parser.TypeString}}

	return &delayedNode{
		p:       p,
		name:    "SHOW DATABASES",
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			prefix := sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, "")
			sr, err := p.txn.Scan(prefix, prefix.PrefixEnd(), 0)
			if err != nil {
				return nil, err
			}
			v := p.newContainerValuesNode(columns, 0)
			for _, db := range p.session.virtualSchemas.orderedNames {
				if err := v.rows.AddRow(p.ctx(), parser.DTuple{parser.NewDString(db)}); err != nil {
					v.rows.Close(p.ctx())
					return nil, err
				}
			}
			for _, row := range sr {
				_, name, err := encoding.DecodeUnsafeStringAscending(
					bytes.TrimPrefix(row.Key, prefix), nil)
				if err != nil {
					v.rows.Close(p.ctx())
					return nil, err
				}
				if err := v.rows.AddRow(p.ctx(), parser.DTuple{parser.NewDString(name)}); err != nil {
					v.rows.Close(p.ctx())
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
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

	return &delayedNode{
		p:       p,
		name:    "SHOW GRANTS",
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)
			var wantedUsers map[string]struct{}
			if len(n.Grantees) != 0 {
				wantedUsers = make(map[string]struct{})
			}
			for _, u := range n.Grantees {
				wantedUsers[string(u)] = struct{}{}
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
						parser.NewDString(userPriv.PrivilegeString()),
					}
					if err := v.rows.AddRow(p.ctx(), newRow); err != nil {
						v.rows.Close(p.ctx())
						return nil, err
					}
				}
			}
			return v, nil
		},
	}, nil
}

// ShowIndex returns all the indexes for a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW INDEXES statement.
//          mysql requires some privilege for any column.
func (p *planner) ShowIndex(n *parser.ShowIndex) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	desc, err := p.mustGetTableDesc(tn)
	if err != nil {
		return nil, err
	}
	if err := p.anyPrivilege(desc); err != nil {
		return nil, err
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

	return &delayedNode{
		p:       p,
		name:    "SHOW INDEX FROM " + tn.String(),
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			appendRow := func(index sqlbase.IndexDescriptor, colName string, sequence int,
				direction string, isStored bool) error {
				newRow := parser.DTuple{
					parser.NewDString(tn.Table()),
					parser.NewDString(index.Name),
					parser.MakeDBool(parser.DBool(index.Unique)),
					parser.NewDInt(parser.DInt(sequence)),
					parser.NewDString(colName),
					parser.NewDString(direction),
					parser.MakeDBool(parser.DBool(isStored)),
				}
				return v.rows.AddRow(p.ctx(), newRow)
			}

			for _, index := range append([]sqlbase.IndexDescriptor{desc.PrimaryIndex}, desc.Indexes...) {
				sequence := 1
				for i, col := range index.ColumnNames {
					if err := appendRow(index, col, sequence, index.ColumnDirections[i].String(), false); err != nil {
						v.rows.Close(p.ctx())
						return nil, err
					}
					sequence++
				}
				for _, col := range index.StoreColumnNames {
					if err := appendRow(index, col, sequence, "N/A", true); err != nil {
						v.rows.Close(p.ctx())
						return nil, err
					}
					sequence++
				}
			}
			return v, nil
		},
	}, nil
}

// ShowConstraints returns all the constraints for a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW CONSTRAINTS statement.
//          mysql requires some privilege for any column.
func (p *planner) ShowConstraints(n *parser.ShowConstraints) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	desc, err := p.mustGetTableDesc(tn)
	if err != nil {
		return nil, err
	}
	if err := p.anyPrivilege(desc); err != nil {
		return nil, err
	}

	columns := ResultColumns{
		{Name: "Table", Typ: parser.TypeString},
		{Name: "Name", Typ: parser.TypeString},
		{Name: "Type", Typ: parser.TypeString},
		{Name: "Column(s)", Typ: parser.TypeString},
		{Name: "Details", Typ: parser.TypeString},
	}

	return &delayedNode{
		p:       p,
		name:    "SHOW CONSTRAINTS FROM " + tn.String(),
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			info, err := desc.GetConstraintInfo(p.txn)
			if err != nil {
				return nil, err
			}
			for name, c := range info {
				detailsDatum := parser.DNull
				if c.Details != "" {
					detailsDatum = parser.NewDString(c.Details)
				}
				columnsDatum := parser.DNull
				if c.Columns != nil {
					columnsDatum = parser.NewDString(strings.Join(c.Columns, ", "))
				}
				kind := string(c.Kind)
				if c.Unvalidated {
					kind += " (UNVALIDATED)"
				}
				newRow := []parser.Datum{
					parser.NewDString(tn.Table()),
					parser.NewDString(name),
					parser.NewDString(kind),
					columnsDatum,
					detailsDatum,
				}
				if err := v.rows.AddRow(p.ctx(), newRow); err != nil {
					v.rows.Close(p.ctx())
					return nil, err
				}
			}

			// Sort the results by constraint name.
			sort := &sortNode{
				ctx: p.ctx(),
				p:   p,
				ordering: sqlbase.ColumnOrdering{
					{ColIdx: 0, Direction: encoding.Ascending},
					{ColIdx: 1, Direction: encoding.Ascending},
				},
				columns: v.columns,
			}
			return &selectTopNode{source: v, sort: sort}, nil
		},
	}, nil
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
	name := p.session.Database
	if n.Database != "" {
		name = string(n.Database)
	}
	if name == "" {
		return nil, errNoDatabase
	}

	columns := ResultColumns{{Name: "Table", Typ: parser.TypeString}}
	return &delayedNode{
		p:       p,
		name:    "SHOW TABLES FROM " + name,
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			dbDesc, err := p.mustGetDatabaseDesc(name)
			if err != nil {
				return nil, err
			}

			tableNames, err := p.getTableNames(dbDesc)
			if err != nil {
				return nil, err
			}

			v := p.newContainerValuesNode(columns, len(tableNames))
			for _, name := range tableNames {
				tableName := name.Table()
				// Check to see if the table has been dropped.
				if _, err := p.mustGetTableOrViewDesc(&name); err != nil {
					if err == errTableDropped {
						tableName += " (dropped)"
					} else {
						return nil, err
					}
				}

				if err := v.rows.AddRow(p.ctx(), parser.DTuple{parser.NewDString(tableName)}); err != nil {
					v.rows.Close(p.ctx())
					return nil, err
				}
			}

			return v, nil
		},
	}, nil
}

// ShowUsers returns all the users.
// Privileges: SELECT on system.users.
func (p *planner) ShowUsers(n *parser.ShowUsers) (planNode, error) {
	stmt, err := parser.ParseOneTraditional(`SELECT username FROM system.users`)
	if err != nil {
		return nil, err
	}
	return p.newPlan(stmt, nil, true)
}

// Help returns usage information for the builtin functions
// Privileges: None
func (p *planner) Help(n *parser.Help) (planNode, error) {
	name := strings.ToLower(n.Name.String())
	columns := ResultColumns{
		{Name: "Function", Typ: parser.TypeString},
		{Name: "Signature", Typ: parser.TypeString},
		{Name: "Category", Typ: parser.TypeString},
		{Name: "Details", Typ: parser.TypeString},
	}
	return &delayedNode{
		p:       p,
		name:    "HELP " + name,
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			matches, ok := parser.Builtins[name]
			// TODO(dt): support fuzzy matching.
			if !ok {
				return v, nil
			}

			for _, f := range matches {
				row := parser.DTuple{
					parser.NewDString(name),
					parser.NewDString(f.Signature()),
					parser.NewDString(f.Category()),
					parser.NewDString(f.Info),
				}
				if err := v.rows.AddRow(p.ctx(), row); err != nil {
					v.rows.Close(p.ctx())
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
}
