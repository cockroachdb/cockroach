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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

const (
	checkSchemaQuery = `
		SELECT SCHEMA_NAME
		FROM "".information_schema.schemata
		WHERE SCHEMA_NAME=$1
		LIMIT 1`
	checkTableQuery = `
		SELECT TABLE_SCHEMA
		FROM "".information_schema.tables
		WHERE
			TABLE_SCHEMA=$1 AND
			TABLE_NAME=$2
		LIMIT 1`
	checkTablePrivilegesQuery = `
		SELECT TABLE_NAME
		FROM "".information_schema.table_privileges
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

func (p *planner) showClusterSetting(ctx context.Context, name string) (planNode, error) {
	if name == "all" {
		return p.delegateQuery(ctx, "SHOW CLUSTER SETTINGS",
			"TABLE crdb_internal.cluster_settings", nil, nil)
	}

	val, ok := settings.Lookup(name)
	if !ok {
		return nil, errors.Errorf("unknown setting: %q", name)
	}
	var dType parser.Type
	switch val.(type) {
	case *settings.IntSetting, *settings.EnumSetting:
		dType = parser.TypeInt
	case *settings.StringSetting, *settings.ByteSizeSetting:
		dType = parser.TypeString
	case *settings.BoolSetting:
		dType = parser.TypeBool
	case *settings.FloatSetting:
		dType = parser.TypeFloat
	case *settings.DurationSetting:
		dType = parser.TypeInterval
	default:
		return nil, errors.Errorf("unknown setting type for %s: %s", name, val.Typ())
	}

	columns := sqlbase.ResultColumns{{Name: name, Typ: dType}}
	return &delayedNode{
		name:    "SHOW CLUSTER SETTING " + name,
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			d := parser.DNull
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
			case *settings.EnumSetting:
				d = parser.NewDInt(parser.DInt(s.Get()))
			case *settings.ByteSizeSetting:
				d = parser.NewDString(s.String())
			}

			v := p.newContainerValuesNode(columns, 0)
			if _, err := v.rows.AddRow(ctx, parser.Datums{d}); err != nil {
				v.rows.Close(ctx)
				return nil, err
			}
			return v, nil
		},
	}, nil
}

// Show a session-local variable name.
func (p *planner) Show(ctx context.Context, n *parser.Show) (planNode, error) {
	origName := n.Name
	name := strings.ToLower(n.Name)

	if n.ClusterSetting {
		return p.showClusterSetting(ctx, name)
	}

	if name == "all" {
		return p.delegateQuery(ctx, "SHOW SESSION ALL", "TABLE crdb_internal.session_variables",
			nil, nil)
	}

	if _, ok := varGen[name]; !ok {
		return nil, fmt.Errorf("unknown variable: %q", origName)
	}

	varName := parser.EscapeSQLString(name)
	return p.delegateQuery(ctx, "SHOW "+varName,
		fmt.Sprintf(
			`SELECT value AS %[1]s FROM crdb_internal.session_variables `+
				`WHERE variable = %[2]s`,
			parser.Name(name).String(), varName),
		nil, nil)
}

// ShowColumns of a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW COLUMNS statement.
//          mysql only returns columns you have privileges on.
func (p *planner) ShowColumns(ctx context.Context, n *parser.ShowColumns) (planNode, error) {
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
										FROM "".information_schema.columns
									 WHERE TABLE_SCHEMA=%[1]s AND TABLE_NAME=%[2]s)
								 LEFT OUTER JOIN
								 (SELECT COLUMN_NAME, INDEX_NAME
										FROM "".information_schema.statistics
									 WHERE TABLE_SCHEMA=%[1]s AND TABLE_NAME=%[2]s)
								 USING(COLUMN_NAME)
						GROUP BY COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION
					 )
				ORDER BY ORDINAL_POSITION`
	return p.showTableDetails(ctx, "SHOW COLUMNS", n.Table, getColumnsQuery)
}

// showTableDetails extracts information about the given table using
// the given query patterns in SQL. The query pattern must accept two
// formatting parameters: the database and the table name, in that
// order. They will be pre-formatted as SQL string literals.
func (p *planner) showTableDetails(
	ctx context.Context, showType string, t parser.NormalizableTableName, query string,
) (planNode, error) {
	tn, err := t.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}
	db := tn.Database()

	initialCheck := func(ctx context.Context) error {
		if err := checkDBExists(ctx, p, db); err != nil {
			return err
		}
		if err := checkTableExists(ctx, p, tn); err != nil {
			return err
		}
		return checkTablePrivileges(ctx, p, tn)
	}

	return p.delegateQuery(ctx, showType,
		fmt.Sprintf(query,
			parser.EscapeSQLString(tn.Database()),
			parser.EscapeSQLString(tn.Table())),
		initialCheck, nil)
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
	s := fmt.Sprintf(" INTERLEAVE IN PARENT %s (%s)", parser.Name(parentTable.Name), interleavedColumnNames)
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

	desc, err := mustGetTableDesc(ctx, p.txn, p.getVirtualTabler(), tn, true /*allowAdding*/)
	if err != nil {
		return nil, err
	}
	if err := p.anyPrivilege(desc); err != nil {
		return nil, err
	}

	columns := sqlbase.ResultColumns{
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
		buf.WriteString(col.SQLString())
		if desc.IsPhysicalTable() && desc.PrimaryIndex.ColumnIDs[0] == col.ID {
			// Only set primary if the primary key is on a visible column (not rowid).
			primary = fmt.Sprintf(",\n\tCONSTRAINT %s PRIMARY KEY (%s)",
				quoteNames(desc.PrimaryIndex.Name),
				desc.PrimaryIndex.ColNamesString(),
			)
		}
	}
	buf.WriteString(primary)
	for _, idx := range desc.Indexes {
		if fk := idx.ForeignKey; fk.IsSet() {
			fkTable, err := p.session.tables.getTableVersionByID(ctx, p.txn, fk.Table)
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
		}
		interleave, err := p.showCreateInterleave(ctx, &idx)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&buf, ",\n\t%s%s",
			idx.SQLString(""),
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

	interleave, err := p.showCreateInterleave(ctx, &desc.PrimaryIndex)
	if err != nil {
		return "", err
	}
	buf.WriteString(interleave)

	return buf.String(), nil
}

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

	columns := sqlbase.ResultColumns{
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

			sourcePlan, err := p.Select(ctx, sel, []parser.Type{})
			if err != nil {
				return nil, err
			}
			for i, col := range planColumns(sourcePlan) {
				if col.Name != desc.Columns[i].Name {
					customColNames = true
					break
				}
			}
			if customColNames {
				buf.WriteByte('(')
				for i, col := range desc.Columns {
					if i > 0 {
						buf.WriteString(", ")
					}
					parser.Name(col.Name).Format(&buf, parser.FmtSimple)
				}
				buf.WriteString(") ")
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

// ShowTrace shows the current stored session trace.
// Privileges: None.
func (p *planner) ShowTrace(ctx context.Context, n *parser.ShowTrace) (planNode, error) {
	const traceClause = `
SELECT timestamp,
       timestamp-first_value(timestamp) OVER (ORDER BY timestamp) AS age,
       message,
       context,
       operation,
       span
  FROM (SELECT timestamp,
               regexp_replace(message, e'^.*\\[[^]]*\\] ', '') AS message,
               regexp_extract(message, e'^.*\\[[^]]*\\]') AS context,
               first_value(operation) OVER (PARTITION BY txn_idx, span_idx ORDER BY message_idx) as operation,
               (txn_idx, span_idx) AS span
          FROM crdb_internal.session_trace)
 %s
 ORDER BY timestamp
`
	whereClause := ""
	if n.OnlyKVTrace {
		whereClause = `
WHERE message LIKE 'fetched: %'
   OR message LIKE 'CPut %'
   OR message LIKE 'Put %'
   OR message LIKE 'DelRange %'
   OR message LIKE 'Del %'
   OR message LIKE 'Get %'
   OR message = 'consuming rows'
   OR message = 'starting plan'
   OR message LIKE 'fast path - %'
   OR message LIKE 'querying next range at %'
   OR message LIKE 'output row: %'
   OR message LIKE 'execution failed: %'
   OR message LIKE 'r%: sending batch %'
`
	}

	plan, err := p.delegateQuery(ctx, "SHOW TRACE",
		fmt.Sprintf(traceClause, whereClause), nil, nil)
	if err != nil {
		return nil, err
	}

	if n.Statement == nil {
		// SHOW SESSION TRACE ...
		return plan, nil
	}

	// SHOW TRACE FOR SELECT ...
	stmtPlan, err := p.newPlan(ctx, n.Statement, nil)
	if err != nil {
		plan.Close(ctx)
		return nil, err
	}
	tracePlan, err := p.makeTraceNode(stmtPlan)
	if err != nil {
		plan.Close(ctx)
		stmtPlan.Close(ctx)
		return nil, err
	}

	// inject the tracePlan inside the SHOW query plan.

	// Suggestion from Radu:
	// This is not very elegant and would be cleaner if the traceNode
	// could process the statement source node first, let it emit its
	// trace messages, and only then query the session_trace vtable.
	// Alternatively, the outer SHOW could use a UNION between the
	// traceNode and the query on session_trace.

	// Unfortunately, neither is currently possible because the plan for
	// the query on session_trace cannot be constructed until the trace
	// is complete. If we want to enable EXPLAIN [SHOW TRACE FOR ...],
	// this limitation of the vtable session_trace must be lifted first.
	// TODO(andrei): make this code more elegant.
	if s, ok := plan.(*sortNode); ok {
		if w, ok := s.plan.(*windowNode); ok {
			if r, ok := w.plan.(*renderNode); ok {
				subPlan := r.source.plan
				if f, ok := subPlan.(*filterNode); ok {
					// The filter layer is only present when the KV modifier is
					// specified.
					subPlan = f.source.plan
				}
				if w, ok := subPlan.(*windowNode); ok {
					if r, ok := w.plan.(*renderNode); ok {
						if _, ok := r.source.plan.(*delayedNode); ok {
							r.source.plan.Close(ctx)
							r.source.plan = tracePlan
							return plan, nil
						}
					}
				}
			}
		}
	}

	// We failed to substitute; this is an internal error.
	err = pgerror.NewErrorf(pgerror.CodeInternalError,
		"invalid logical plan structure:\n%s\nwhile inserting:\n%s",
		planToString(ctx, plan),
		planToString(ctx, tracePlan))
	plan.Close(ctx)
	stmtPlan.Close(ctx)
	tracePlan.Close(ctx)
	return nil, err
}

// ShowDatabases returns all the databases.
// Privileges: None.
//   Notes: postgres does not have a "show databases"
//          mysql has a "SHOW DATABASES" permission, but we have no system-level permissions.
func (p *planner) ShowDatabases(ctx context.Context, n *parser.ShowDatabases) (planNode, error) {
	return p.delegateQuery(ctx, "SHOW DATABASES",
		`SELECT SCHEMA_NAME AS "Database" FROM information_schema.schemata ORDER BY "Database"`,
		nil, nil)
}

// ShowGrants returns grant details for the specified objects and users.
// TODO(marc): implement no targets (meaning full scan).
// Privileges: None.
//   Notes: postgres does not have a SHOW GRANTS statement.
//          mysql only returns the user's privileges.
func (p *planner) ShowGrants(ctx context.Context, n *parser.ShowGrants) (planNode, error) {
	if n.Targets == nil {
		return nil, pgerror.Unimplemented("SHOW GRANTS <no target>",
			"cannot use SHOW GRANTS without a target")
	}

	var grantQuery bytes.Buffer
	var params []string
	var initCheck func(context.Context) error

	if n.Targets.Databases != nil {
		// Get grants of database from information_schema.schema_privileges
		// if the type of target is database.
		dbNames := n.Targets.Databases.ToStrings()

		initCheck = func(ctx context.Context) error {
			for _, db := range dbNames {
				if err := checkDBExists(ctx, p, db); err != nil {
					return err
				}
			}
			return nil
		}

		for _, db := range dbNames {
			params = append(params, parser.EscapeSQLString(db))
		}

		fmt.Fprint(&grantQuery,
			`SELECT TABLE_SCHEMA AS "Database", GRANTEE AS "User", PRIVILEGE_TYPE AS "Privileges" `+
				`FROM "".information_schema.schema_privileges`)
		if len(params) == 0 {
			// There are no rows, but we can't simply return emptyNode{} because
			// the result columns must still be defined.
			grantQuery.WriteString(` WHERE false`)
		} else {
			fmt.Fprintf(&grantQuery, ` WHERE TABLE_SCHEMA IN (%s)`, strings.Join(params, ","))
		}
	} else {
		// Get grants of table from information_schema.table_privileges
		// if the type of target is table.
		var allTables parser.TableNames

		for _, tableTarget := range n.Targets.Tables {
			tableGlob, err := tableTarget.NormalizeTablePattern()
			if err != nil {
				return nil, err
			}
			tables, err := expandTableGlob(ctx, p.txn, p.getVirtualTabler(),
				p.session.Database, tableGlob)
			if err != nil {
				return nil, err
			}
			allTables = append(allTables, tables...)
		}

		initCheck = func(ctx context.Context) error {
			for i := range allTables {
				if err := checkTableExists(ctx, p, &allTables[i]); err != nil {
					return err
				}
			}
			return nil
		}

		for i := range allTables {
			params = append(params, fmt.Sprintf("(%s,%s)",
				parser.EscapeSQLString(allTables[i].Database()),
				parser.EscapeSQLString(allTables[i].Table())))
		}

		fmt.Fprint(&grantQuery,
			`SELECT TABLE_NAME AS "Table", GRANTEE AS "User", PRIVILEGE_TYPE AS "Privileges" `+
				`FROM "".information_schema.table_privileges`)
		if len(params) == 0 {
			// There are no rows, but we can't simply return emptyNode{} because
			// the result columns must still be defined.
			grantQuery.WriteString(` WHERE false`)
		} else {
			fmt.Fprintf(&grantQuery, ` WHERE (TABLE_SCHEMA, TABLE_NAME) IN (%s)`, strings.Join(params, ","))
		}
	}

	if n.Grantees != nil {
		params = params[:0]
		for _, grantee := range n.Grantees.ToStrings() {
			params = append(params, parser.EscapeSQLString(grantee))
		}
		fmt.Fprintf(&grantQuery, ` AND GRANTEE IN (%s)`, strings.Join(params, ","))
	}
	fmt.Fprint(&grantQuery, " ORDER BY 1,2,3")
	return p.delegateQuery(ctx, "SHOW GRANTS", grantQuery.String(), initCheck, nil)
}

// ShowIndex returns all the indexes for a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW INDEXES statement.
//          mysql requires some privilege for any column.
func (p *planner) ShowIndex(ctx context.Context, n *parser.ShowIndex) (planNode, error) {
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
				FROM "".information_schema.statistics
				WHERE TABLE_SCHEMA=%[1]s AND TABLE_NAME=%[2]s`
	return p.showTableDetails(ctx, "SHOW INDEX", n.Table, getIndexes)
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

	desc, err := mustGetTableDesc(ctx, p.txn, p.getVirtualTabler(), tn, true /*allowAdding*/)
	if err != nil {
		return nil, err
	}
	if err := p.anyPrivilege(desc); err != nil {
		return nil, err
	}

	columns := sqlbase.ResultColumns{
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

func (p *planner) ShowQueries(ctx context.Context, n *parser.ShowQueries) (planNode, error) {
	query := `TABLE crdb_internal.node_queries`
	if n.Cluster {
		query = `TABLE crdb_internal.cluster_queries`
	}
	return p.delegateQuery(ctx, "SHOW QUERIES", query, nil, nil)
}

// ShowJobs returns all the jobs.
// Privileges: None.
func (p *planner) ShowJobs(ctx context.Context, n *parser.ShowJobs) (planNode, error) {
	return p.delegateQuery(ctx, "SHOW JOBS", "TABLE crdb_internal.jobs", nil, nil)
}

func (p *planner) ShowSessions(ctx context.Context, n *parser.ShowSessions) (planNode, error) {
	query := `TABLE crdb_internal.node_sessions`
	if n.Cluster {
		query = `TABLE crdb_internal.cluster_sessions`
	}
	return p.delegateQuery(ctx, "SHOW SESSIONS", query, nil, nil)
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
	initialCheck := func(ctx context.Context) error {
		return checkDBExists(ctx, p, name)
	}

	const getTablesQuery = `
				SELECT TABLE_NAME AS "Table"
				FROM "".information_schema.tables
				WHERE tables.TABLE_SCHEMA=%[1]s
				ORDER BY tables.TABLE_NAME`

	return p.delegateQuery(ctx, "SHOW TABLES",
		fmt.Sprintf(getTablesQuery, parser.EscapeSQLString(name)),
		initialCheck, nil)
}

// ShowTransactionStatus implements the plan for SHOW TRANSACTION STATUS.
// This statement is usually handled as a special case in Executor,
// but for FROM [SHOW TRANSACTION STATUS] we will arrive here too.
func (p *planner) ShowTransactionStatus(ctx context.Context) (planNode, error) {
	return p.Show(ctx, &parser.Show{Name: "transaction status"})
}

// ShowUsers returns all the users.
// Privileges: SELECT on system.users.
func (p *planner) ShowUsers(ctx context.Context, n *parser.ShowUsers) (planNode, error) {
	return p.delegateQuery(ctx, "SHOW USERS",
		`SELECT username FROM system.users ORDER BY 1`, nil, nil)
}

// Help returns usage information for the given builtin function.
// Privileges: None
func (p *planner) Help(ctx context.Context, n *parser.Help) (planNode, error) {
	return p.delegateQuery(ctx, "HELP", fmt.Sprintf(
		`SELECT * FROM crdb_internal.builtin_functions WHERE function ILIKE %s`,
		parser.EscapeSQLString(string(n.Name))), nil, nil)
}
