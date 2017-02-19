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

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

const (
	// PgServerVersion is the latest version of postgres that we claim to support.
	PgServerVersion = "9.5.0"
)

var varGen = map[string]func(p *planner) string{
	`database`:                      func(p *planner) string { return p.session.Database },
	`default_transaction_isolation`: func(p *planner) string { return p.session.DefaultIsolationLevel.String() },
	`syntax`:                        func(p *planner) string { return parser.Syntax(p.session.Syntax).String() },
	`time zone`:                     func(p *planner) string { return p.session.Location.String() },
	`transaction isolation level`:   func(p *planner) string { return p.txn.Proto.Isolation.String() },
	`transaction priority`:          func(p *planner) string { return p.txn.UserPriority.String() },
	`max_index_keys`:                func(_ *planner) string { return "32" },
	`search_path`:                   func(p *planner) string { return strings.Join(p.session.SearchPath, ", ") },
	`server_version`:                func(_ *planner) string { return PgServerVersion },
	`session_user`:                  func(p *planner) string { return p.session.User },
}
var varNames = func() []string {
	res := make([]string, 0, len(varGen))
	for vName := range varGen {
		res = append(res, vName)
	}
	sort.Strings(res)
	return res
}()

const (
	checkSchemaQuery = `
		SELECT SCHEMA_NAME
		FROM information_schema.schemata
		WHERE SCHEMA_NAME=$1
		LIMIT 1`
	checkTableQuery = `
		SELECT TABLE_SCHEMA
		FROM information_schema.tables
		WHERE
			TABLE_SCHEMA=$1 AND
			TABLE_NAME=$2
		LIMIT 1`
	checkTablePrivilegesQuery = `
		SELECT TABLE_NAME
		FROM information_schema.table_privileges
		WHERE
			TABLE_SCHEMA=$1 AND
			TABLE_NAME=$2 AND
			GRANTEE=$3
		LIMIT 1`
)

// checkDBExists checks if the database exists by using the security.RootUser.
func checkDBExists(p *planner, db string) error {
	values, err := p.queryRowsAsRoot(checkSchemaQuery, db)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return sqlbase.NewUndefinedDatabaseError(db)
	}
	return nil
}

// checkTableExists checks if the table exists by using the security.RootUser.
func checkTableExists(p *planner, tn *parser.TableName) error {
	values, err := p.queryRowsAsRoot(checkTableQuery, tn.Database(), tn.Table())
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return sqlbase.NewUndefinedTableError(tn.String())
	}
	return nil
}

// checkTablePrivileges checks if the user has been granted privileges to
// see the specified table.
func checkTablePrivileges(p *planner, tn *parser.TableName) error {
	// Skip the checking if the table is a virtual table.
	if virDesc, err := p.session.virtualSchemas.getVirtualTableDesc(tn); err != nil {
		return err
	} else if virDesc != nil {
		return nil
	}

	values, err := p.queryRowsAsRoot(checkTablePrivilegesQuery, tn.Database(), tn.Table(), p.session.User)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return fmt.Errorf("user %s has no privileges on table %s", p.session.User, tn.String())
	}
	return nil
}

// runInDB runs the closure with the provided database set to the session's current
// database, and resets the session's database afterwards. This is necessary to get
// visibility into information_schema if the current user isn't root.
func runInDB(p *planner, tempDB string, f func() error) error {
	origDatabase := p.evalCtx.Database
	p.evalCtx.Database = tempDB
	err := f()
	p.evalCtx.Database = origDatabase
	return err
}

// queryInfoSchema queries the information_schema with the provided SQL query and
// uses the results to populate a valuesNode.
func queryInfoSchema(
	p *planner, columns ResultColumns, db string, sql string, args ...interface{},
) (*valuesNode, error) {
	v := p.newContainerValuesNode(columns, 0)
	if err := runInDB(p, db, func() error {
		rows, err := p.queryRows(sql, args...)
		if err != nil {
			return err
		}
		for _, r := range rows {
			if _, err := v.rows.AddRow(r); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		v.rows.Close()
		return nil, err
	}
	return v, nil
}

// Show a session-local variable name.
func (p *planner) Show(n *parser.Show) (planNode, error) {
	origName := n.Name
	name := strings.ToLower(n.Name)

	var columns ResultColumns

	switch name {
	case `all`:
		columns = ResultColumns{
			{Name: "Variable", Typ: parser.TypeString},
			{Name: "Value", Typ: parser.TypeString},
		}
	default:
		if _, ok := varGen[name]; !ok {
			return nil, fmt.Errorf("unknown variable: %q", origName)
		}
		columns = ResultColumns{{Name: name, Typ: parser.TypeString}}
	}

	return &delayedNode{
		name:    "SHOW " + origName,
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			switch name {
			case `all`:
				for _, vName := range varNames {
					gen := varGen[vName]
					value := gen(p)
					if _, err := v.rows.AddRow(
						parser.Datums{parser.NewDString(vName), parser.NewDString(value)},
					); err != nil {
						v.rows.Close()
						return nil, err
					}
				}
			default:
				// The key in varGen is guaranteed to exist thanks to the
				// check above.
				gen := varGen[name]
				value := gen(p)
				if _, err := v.rows.AddRow(parser.Datums{parser.NewDString(value)}); err != nil {
					v.rows.Close()
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

	columns := ResultColumns{
		{Name: "Field", Typ: parser.TypeString},
		{Name: "Type", Typ: parser.TypeString},
		{Name: "Null", Typ: parser.TypeBool},
		{Name: "Default", Typ: parser.TypeString},
		{Name: "Indices", Typ: parser.TypeString},
	}
	return &delayedNode{
		name:    "SHOW COLUMNS FROM " + tn.String(),
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			const getColumnsQuery = `
				SELECT
					COLUMN_NAME AS "Field",
					DATA_TYPE AS "Type",
					(IS_NULLABLE != 'NO') AS "Null",
					COLUMN_DEFAULT AS "Default",
					IF(inames[1] IS NULL, ARRAY[]:::STRING[], inames) AS "Indices"
				FROM
					(SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION,
									ARRAY_AGG(INDEX_NAME) AS inames
						 FROM
								 (SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION
										FROM information_schema.columns
									 WHERE TABLE_SCHEMA=$1 AND TABLE_NAME=$2)
								 LEFT OUTER JOIN
								 (SELECT COLUMN_NAME, INDEX_NAME
										FROM information_schema.statistics
									 WHERE TABLE_SCHEMA=$1 AND TABLE_NAME=$2)
								 USING(COLUMN_NAME)
						GROUP BY COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION
					 )
				ORDER BY ORDINAL_POSITION`

			db := tn.Database()
			if err := checkDBExists(p, db); err != nil {
				return nil, err
			}

			if err := checkTableExists(p, tn); err != nil {
				return nil, err
			}

			if err := checkTablePrivileges(p, tn); err != nil {
				return nil, err
			}

			return queryInfoSchema(p, columns, db, getColumnsQuery, tn.Database(), tn.Table())
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
					v.rows.Close()
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
				v.rows.Close()
				return nil, err
			}
			buf.WriteString(interleave)

			if _, err := v.rows.AddRow(parser.Datums{
				parser.NewDString(n.Table.String()),
				parser.NewDString(buf.String()),
			}); err != nil {
				v.rows.Close()
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
			if _, err := v.rows.AddRow(parser.Datums{
				parser.NewDString(n.View.String()),
				parser.NewDString(buf.String()),
			}); err != nil {
				v.rows.Close()
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
	const getDatabases = `SELECT SCHEMA_NAME AS "Database" FROM information_schema.schemata
							ORDER BY "Database"`
	stmt, err := parser.ParseOneTraditional(getDatabases)
	if err != nil {
		return nil, err
	}
	return p.newPlan(stmt, nil, true)
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
		name:    "SHOW GRANTS",
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			queryFn := func(sql string, args ...interface{}) error {
				rows, err := p.queryRows(sql, args...)
				if err != nil {
					return err
				}
				for _, r := range rows {
					if _, err := v.rows.AddRow(r); err != nil {
						return err
					}
				}
				return nil
			}

			// Get grants of database from information_schema.schema_privileges
			// if the type of target is database.
			if n.Targets.Databases != nil {
				// TODO(nvanbenschoten): Clean up parameter assignment throughout.
				var params []interface{}
				var paramHolders []string
				paramSeq := 1
				for _, db := range n.Targets.Databases.ToStrings() {
					if err := checkDBExists(p, db); err != nil {
						v.rows.Close()
						return nil, err
					}

					paramHolders = append(paramHolders, fmt.Sprintf("$%d", paramSeq))
					paramSeq++
					params = append(params, db)
				}
				schemaGrants := fmt.Sprintf(`SELECT TABLE_SCHEMA AS "Database", GRANTEE AS "User",
									PRIVILEGE_TYPE AS "Privileges" FROM information_schema.schema_privileges
									WHERE TABLE_SCHEMA IN (%s)`, strings.Join(paramHolders, ","))
				if n.Grantees != nil {
					paramHolders = paramHolders[:0]
					for _, grantee := range n.Grantees.ToStrings() {
						paramHolders = append(paramHolders, fmt.Sprintf("$%d", paramSeq))
						params = append(params, grantee)
						paramSeq++
					}
					schemaGrants = fmt.Sprintf(`%s AND GRANTEE IN(%s)`, schemaGrants, strings.Join(paramHolders, ","))
				}
				if err := queryFn(schemaGrants, params...); err != nil {
					v.rows.Close()
					return nil, err
				}
			}

			// Get grants of table from information_schema.table_privileges
			// if the type of target is table.
			if n.Targets.Tables != nil {
				// TODO(nvanbenschoten): Clean up parameter assignment throughout.
				var params []interface{}
				var paramHolders []string
				paramSeq := 1
				for _, tableTarget := range n.Targets.Tables {
					tableGlob, err := tableTarget.NormalizeTablePattern()
					if err != nil {
						v.rows.Close()
						return nil, err
					}
					tables, err := p.expandTableGlob(tableGlob)
					if err != nil {
						v.rows.Close()
						return nil, err
					}
					for i := range tables {
						if err := checkTableExists(p, &tables[i]); err != nil {
							v.rows.Close()
							return nil, err
						}

						paramHolders = append(paramHolders, fmt.Sprintf("($%d,$%d)",
							paramSeq, paramSeq+1))
						params = append(params, tables[i].Database(), tables[i].Table())
						paramSeq += 2

					}
				}
				tableGrants := fmt.Sprintf(`SELECT TABLE_NAME, GRANTEE, PRIVILEGE_TYPE FROM information_schema.table_privileges
									WHERE (TABLE_SCHEMA, TABLE_NAME) IN (%s)`, strings.Join(paramHolders, ","))
				if n.Grantees != nil {
					paramHolders = paramHolders[:0]
					for _, grantee := range n.Grantees.ToStrings() {
						paramHolders = append(paramHolders, fmt.Sprintf("$%d", paramSeq))
						params = append(params, grantee)
						paramSeq++
					}
					tableGrants = fmt.Sprintf(`%s AND GRANTEE IN(%s)`, tableGrants, strings.Join(paramHolders, ","))
				}
				if err := queryFn(tableGrants, params...); err != nil {
					v.rows.Close()
					return nil, err
				}
			}

			// Sort the result by target name, user name and privileges.
			return &sortNode{
				p:    p,
				plan: v,
				ordering: sqlbase.ColumnOrdering{
					{ColIdx: 0, Direction: encoding.Ascending},
					{ColIdx: 1, Direction: encoding.Ascending},
					{ColIdx: 2, Direction: encoding.Ascending},
				},
				columns: v.columns,
			}, nil
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

	columns := ResultColumns{
		{Name: "Table", Typ: parser.TypeString},
		{Name: "Name", Typ: parser.TypeString},
		{Name: "Unique", Typ: parser.TypeBool},
		{Name: "Seq", Typ: parser.TypeInt},
		{Name: "Column", Typ: parser.TypeString},
		{Name: "Direction", Typ: parser.TypeString},
		{Name: "Storing", Typ: parser.TypeBool},
		{Name: "Implicit", Typ: parser.TypeBool},
	}
	return &delayedNode{
		name:    "SHOW INDEXES FROM " + tn.String(),
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			const getIndexes = `
				SELECT
					TABLE_NAME AS "Table",
					INDEX_NAME AS "Name",
					NOT NON_UNIQUE AS "Unique",
					SEQ_IN_INDEX AS "Seq",
					COLUMN_NAME AS "Column",
					DIRECTION AS "Direction",
					STORING AS "Storing",
					IMPLICIT AS "Implicit"
				FROM information_schema.statistics
				WHERE
					TABLE_SCHEMA=$1 AND
					TABLE_NAME=$2`

			db := tn.Database()
			if err := checkDBExists(p, db); err != nil {
				return nil, err
			}

			if err := checkTableExists(p, tn); err != nil {
				return nil, err
			}

			if err := checkTablePrivileges(p, tn); err != nil {
				return nil, err
			}

			return queryInfoSchema(p, columns, db, getIndexes, tn.Database(), tn.Table())
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
				if _, err := v.rows.AddRow(newRow); err != nil {
					v.Close()
					return nil, err
				}
			}

			// Sort the results by constraint name.
			return &sortNode{
				p:    p,
				plan: v,
				ordering: sqlbase.ColumnOrdering{
					{ColIdx: 0, Direction: encoding.Ascending},
					{ColIdx: 1, Direction: encoding.Ascending},
				},
				columns: v.columns,
			}, nil
		},
	}, nil
}

// ShowTables returns all the tables.
// Privileges: None.
//   Notes: postgres does not have a SHOW TABLES statement.
//          mysql only returns tables you have privileges on.
func (p *planner) ShowTables(n *parser.ShowTables) (planNode, error) {
	name := p.session.Database
	if n.Database != "" {
		name = string(n.Database)
	}
	if name == "" {
		return nil, errNoDatabase
	}

	columns := ResultColumns{{Name: "Table", Typ: parser.TypeString}}
	return &delayedNode{
		name:    "SHOW TABLES FROM " + name,
		columns: columns,
		constructor: func(p *planner) (planNode, error) {
			const getTablesQuery = `
				SELECT TABLE_NAME
				FROM information_schema.tables
				WHERE tables.TABLE_SCHEMA=$1
				ORDER BY tables.TABLE_NAME`

			if err := checkDBExists(p, name); err != nil {
				return nil, err
			}

			return queryInfoSchema(p, columns, name, getTablesQuery, name)
		},
	}, nil
}

// ShowUsers returns all the users.
// Privileges: SELECT on system.users.
func (p *planner) ShowUsers(n *parser.ShowUsers) (planNode, error) {
	stmt, err := parser.ParseOneTraditional(`SELECT username FROM system.users ORDER BY 1`)
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
				row := parser.Datums{
					parser.NewDString(name),
					parser.NewDString(f.Signature()),
					parser.NewDString(f.Category()),
					parser.NewDString(f.Info),
				}
				if _, err := v.rows.AddRow(row); err != nil {
					v.Close()
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
}
