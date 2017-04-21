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

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

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
func checkDBExists(ctx context.Context, p *planner, db string) error {
	values, err := p.queryRowsAsRoot(ctx, checkSchemaQuery, db)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return sqlbase.NewUndefinedDatabaseError(db)
	}
	return nil
}

// checkTableExists checks if the table exists by using the security.RootUser.
func checkTableExists(ctx context.Context, p *planner, tn *parser.TableName) error {
	values, err := p.queryRowsAsRoot(ctx, checkTableQuery, tn.Database(), tn.Table())
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
func checkTablePrivileges(ctx context.Context, p *planner, tn *parser.TableName) error {
	// Skip the checking if the table is a virtual table.
	if virDesc, err := p.session.virtualSchemas.getVirtualTableDesc(tn); err != nil {
		return err
	} else if virDesc != nil {
		return nil
	}

	values, err := p.queryRowsAsRoot(
		ctx, checkTablePrivilegesQuery, tn.Database(), tn.Table(), p.session.User)
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
	ctx context.Context,
	p *planner,
	columns ResultColumns,
	db string,
	sql string,
	args ...interface{},
) (*valuesNode, error) {
	v := p.newContainerValuesNode(columns, 0)
	if err := runInDB(p, db, func() error {
		rows, err := p.queryRows(ctx, sql, args...)
		if err != nil {
			return err
		}
		for _, r := range rows {
			if _, err := v.rows.AddRow(ctx, r); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		v.rows.Close(ctx)
		return nil, err
	}
	return v, nil
}

func (p *planner) showClusterSetting(name string) (planNode, error) {
	var columns ResultColumns
	var populate func(ctx context.Context, v *valuesNode) error

	switch name {
	case "all":
		columns = ResultColumns{
			{Name: "name", Typ: parser.TypeString},
			{Name: "current_value", Typ: parser.TypeString},
			{Name: "type", Typ: parser.TypeString},
			{Name: "description", Typ: parser.TypeString},
		}
		populate = func(ctx context.Context, v *valuesNode) error {
			for _, k := range settings.Keys() {
				setting, desc, _ := settings.Lookup(k)
				if _, err := v.rows.AddRow(ctx, parser.Datums{
					parser.NewDString(k),
					parser.NewDString(setting.String()),
					parser.NewDString(setting.Typ()),
					parser.NewDString(desc),
				}); err != nil {
					return err
				}
			}
			return nil
		}

	default:
		val, _, ok := settings.Lookup(name)
		if !ok {
			return nil, errors.Errorf("unknown setting: %q", name)
		}
		var d parser.Datum
		switch s := val.(type) {
		case *settings.IntSetting:
			d = parser.NewDInt(parser.DInt(s.Get()))
		case *settings.StringSetting:
			d = parser.NewDString(s.String())
		case *settings.BoolSetting:
			d = parser.MakeDBool(parser.DBool(s.Get()))
		case *settings.FloatSetting:
			d = parser.NewDFloat(parser.DFloat(s.Get()))
		case *settings.DurationSetting:
			d = &parser.DInterval{Duration: duration.Duration{Nanos: s.Get().Nanoseconds()}}
		case *settings.ByteSizeSetting:
			d = parser.NewDString(s.String())
		default:
			return nil, errors.Errorf("unknown setting type for %s: %s", name, val.Typ())
		}
		columns = ResultColumns{{Name: name, Typ: d.ResolvedType()}}
		populate = func(ctx context.Context, v *valuesNode) error {
			_, err := v.rows.AddRow(ctx, parser.Datums{d})
			return err
		}
	}

	return &delayedNode{
		name:    "SHOW CLUSTER SETTING " + name,
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 1)

			if err := populate(ctx, v); err != nil {
				v.rows.Close(ctx)
				return nil, err
			}
			return v, nil
		},
	}, nil
}

// Show a session-local variable name.
func (p *planner) Show(n *parser.Show) (planNode, error) {
	origName := n.Name
	name := strings.ToLower(n.Name)

	if n.ClusterSetting {
		return p.showClusterSetting(name)
	}

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
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			switch name {
			case `all`:
				for _, vName := range varNames {
					gen := varGen[vName]
					value := gen.Get(p)
					if _, err := v.rows.AddRow(
						ctx, parser.Datums{parser.NewDString(vName), parser.NewDString(value)},
					); err != nil {
						v.rows.Close(ctx)
						return nil, err
					}
				}
			default:
				// The key in varGen is guaranteed to exist thanks to the
				// check above.
				gen := varGen[name]
				value := gen.Get(p)
				if _, err := v.rows.AddRow(ctx, parser.Datums{parser.NewDString(value)}); err != nil {
					v.rows.Close(ctx)
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
func (p *planner) ShowColumns(ctx context.Context, n *parser.ShowColumns) (planNode, error) {
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
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
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
			if err := checkDBExists(ctx, p, db); err != nil {
				return nil, err
			}

			if err := checkTableExists(ctx, p, tn); err != nil {
				return nil, err
			}

			if err := checkTablePrivileges(ctx, p, tn); err != nil {
				return nil, err
			}

			return queryInfoSchema(ctx, p, columns, db, getColumnsQuery, tn.Database(), tn.Table())
		},
	}, nil
}

// showCreateInterleave returns an INTERLEAVE IN PARENT clause for the specified
// index, if applicable.
func (p *planner) showCreateInterleave(
	ctx context.Context, idx *sqlbase.IndexDescriptor,
) (string, error) {
	if len(idx.Interleave.Ancestors) == 0 {
		return "", nil
	}
	intl := idx.Interleave
	parentTable, err := sqlbase.GetTableDescFromID(ctx, p.txn, intl.Ancestors[len(intl.Ancestors)-1].TableID)
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

// ShowCreateTable returns a CREATE TABLE statement for the specified table.
// Privileges: Any privilege on table.
func (p *planner) ShowCreateTable(
	ctx context.Context, n *parser.ShowCreateTable,
) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	desc, err := mustGetTableDesc(ctx, p.txn, p.getVirtualTabler(), tn)
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
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			s, err := p.showCreateTable(ctx, tn.TableName, desc)
			if err != nil {
				v.rows.Close(ctx)
				return nil, err
			}

			if _, err := v.rows.AddRow(ctx, parser.Datums{
				parser.NewDString(tn.String()),
				parser.NewDString(s),
			}); err != nil {
				v.rows.Close(ctx)
				return nil, err
			}
			return v, nil
		},
	}, nil
}

func (p *planner) showCreateTable(
	ctx context.Context, tn parser.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE %s (", tn)
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
				makeIndexColNames(desc.PrimaryIndex),
			)
		}
	}
	buf.WriteString(primary)
	for _, idx := range desc.Indexes {
		var storing string
		if len(idx.StoreColumnNames) > 0 {
			storing = fmt.Sprintf(" STORING (%s)", quoteNames(idx.StoreColumnNames...))
		}
		interleave, err := p.showCreateInterleave(ctx, &idx)
		if err != nil {
			return "", err
		}
		if fk := idx.ForeignKey; fk.IsSet() {
			fkTable, err := p.session.leases.getTableLeaseByID(ctx, p.txn, fk.Table)
			if err != nil {
				return "", err
			}
			fkIdx, err := fkTable.FindIndexByID(fk.Index)
			if err != nil {
				return "", err
			}
			fmt.Fprintf(&buf, ",\n\tCONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				parser.Name(fk.Name),
				quoteNames(idx.ColumnNames...),
				parser.Name(fkTable.Name),
				quoteNames(fkIdx.ColumnNames...),
			)
		} else {
			fmt.Fprintf(&buf, ",\n\t%sINDEX %s (%s)%s%s",
				isUnique[idx.Unique],
				quoteNames(idx.Name),
				makeIndexColNames(idx),
				storing,
				interleave,
			)
		}
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

	interleave, err := p.showCreateInterleave(ctx, &desc.PrimaryIndex)
	if err != nil {
		return "", err
	}
	buf.WriteString(interleave)

	return buf.String(), nil
}

func makeIndexColNames(d sqlbase.IndexDescriptor) string {
	var buf bytes.Buffer
	for i, name := range d.ColumnNames {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%s %s", parser.Name(name), d.ColumnDirections[i])
	}
	return buf.String()
}

var isUnique = map[bool]string{true: "UNIQUE "}

// quoteName quotes and adds commas between names.
func quoteNames(names ...string) string {
	nameList := make(parser.NameList, len(names))
	for i, n := range names {
		nameList[i] = parser.Name(n)
	}
	return parser.AsString(nameList)
}

// ShowCreateView returns a CREATE VIEW statement for the specified view.
// Privileges: Any privilege on view.
func (p *planner) ShowCreateView(ctx context.Context, n *parser.ShowCreateView) (planNode, error) {
	tn, err := n.View.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	desc, err := mustGetViewDesc(ctx, p.txn, p.getVirtualTabler(), tn)
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
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			var buf bytes.Buffer
			fmt.Fprintf(&buf, "CREATE VIEW %s ", tn.TableName)

			// Determine whether custom column names were specified when the view
			// was created, and include them if so.
			customColNames := false
			stmt, err := parser.ParseOne(desc.ViewQuery)
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

			sourcePlan, err := p.Select(ctx, sel, []parser.Type{}, false)
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
			if _, err := v.rows.AddRow(ctx, parser.Datums{
				parser.NewDString(n.View.String()),
				parser.NewDString(buf.String()),
			}); err != nil {
				v.rows.Close(ctx)
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
func (p *planner) ShowDatabases(ctx context.Context, n *parser.ShowDatabases) (planNode, error) {
	const getDatabases = `SELECT SCHEMA_NAME AS "Database" FROM information_schema.schemata
							ORDER BY "Database"`
	stmt, err := parser.ParseOne(getDatabases)
	if err != nil {
		return nil, err
	}
	return p.newPlan(ctx, stmt, nil, true)
}

// ShowGrants returns grant details for the specified objects and users.
// TODO(marc): implement no targets (meaning full scan).
// Privileges: None.
//   Notes: postgres does not have a SHOW GRANTS statement.
//          mysql only returns the user's privileges.
func (p *planner) ShowGrants(ctx context.Context, n *parser.ShowGrants) (planNode, error) {
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
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			queryFn := func(sql string, args ...interface{}) error {
				rows, err := p.queryRows(ctx, sql, args...)
				if err != nil {
					return err
				}
				for _, r := range rows {
					if _, err := v.rows.AddRow(ctx, r); err != nil {
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
					if err := checkDBExists(ctx, p, db); err != nil {
						v.rows.Close(ctx)
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
					v.rows.Close(ctx)
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
						v.rows.Close(ctx)
						return nil, err
					}
					tables, err := expandTableGlob(ctx, p.txn, p.getVirtualTabler(), p.session.Database, tableGlob)
					if err != nil {
						v.rows.Close(ctx)
						return nil, err
					}
					for i := range tables {
						if err := checkTableExists(ctx, p, &tables[i]); err != nil {
							v.rows.Close(ctx)
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
					v.rows.Close(ctx)
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
func (p *planner) ShowIndex(ctx context.Context, n *parser.ShowIndex) (planNode, error) {
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
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
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
			if err := checkDBExists(ctx, p, db); err != nil {
				return nil, err
			}

			if err := checkTableExists(ctx, p, tn); err != nil {
				return nil, err
			}

			if err := checkTablePrivileges(ctx, p, tn); err != nil {
				return nil, err
			}

			return queryInfoSchema(ctx, p, columns, db, getIndexes, tn.Database(), tn.Table())
		},
	}, nil
}

// ShowConstraints returns all the constraints for a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW CONSTRAINTS statement.
//          mysql requires some privilege for any column.
func (p *planner) ShowConstraints(
	ctx context.Context, n *parser.ShowConstraints,
) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	desc, err := mustGetTableDesc(ctx, p.txn, p.getVirtualTabler(), tn)
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
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			info, err := desc.GetConstraintInfo(ctx, p.txn)
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
				if _, err := v.rows.AddRow(ctx, newRow); err != nil {
					v.Close(ctx)
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
func (p *planner) ShowTables(ctx context.Context, n *parser.ShowTables) (planNode, error) {
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
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			const getTablesQuery = `
				SELECT TABLE_NAME
				FROM information_schema.tables
				WHERE tables.TABLE_SCHEMA=$1
				ORDER BY tables.TABLE_NAME`

			if err := checkDBExists(ctx, p, name); err != nil {
				return nil, err
			}

			return queryInfoSchema(ctx, p, columns, name, getTablesQuery, name)
		},
	}, nil
}

// ShowUsers returns all the users.
// Privileges: SELECT on system.users.
func (p *planner) ShowUsers(ctx context.Context, n *parser.ShowUsers) (planNode, error) {
	stmt, err := parser.ParseOne(`SELECT username FROM system.users ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	return p.newPlan(ctx, stmt, nil, true)
}

// Help returns usage information for the builtin functions
// Privileges: None
func (p *planner) Help(ctx context.Context, n *parser.Help) (planNode, error) {
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
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
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
				if _, err := v.rows.AddRow(ctx, row); err != nil {
					v.Close(ctx)
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
}
