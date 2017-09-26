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

// checkDBExists checks if the database exists by using the security.RootUser.
func checkDBExists(ctx context.Context, p *planner, db string) error {
	if _, err := MustGetDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), db); err != nil {
		return sqlbase.NewUndefinedDatabaseError(db)
	}
	return nil
}

// checkTableExists checks if the table exists by using the security.RootUser.
func checkTableExists(ctx context.Context, p *planner, tn *parser.TableName) error {
	if _, err := MustGetTableOrViewDesc(ctx, p.txn, p.getVirtualTabler(), tn, true /*allowAdding*/); err != nil {
		return sqlbase.NewUndefinedRelationError(tn)
	}
	return nil
}

func (p *planner) ShowClusterSetting(
	ctx context.Context, n *parser.ShowClusterSetting,
) (planNode, error) {
	name := strings.ToLower(n.Name)

	if name == "all" {
		return p.delegateQuery(ctx, "SHOW CLUSTER SETTINGS",
			"TABLE crdb_internal.cluster_settings", nil, nil)
	}

	st := p.session.execCfg.Settings
	val, ok := settings.Lookup(name)
	if !ok {
		return nil, errors.Errorf("unknown setting: %q", name)
	}
	var dType parser.Type
	switch val.(type) {
	case *settings.IntSetting, *settings.EnumSetting:
		dType = parser.TypeInt
	case *settings.StringSetting, *settings.ByteSizeSetting, *settings.StateMachineSetting:
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
				d = parser.NewDInt(parser.DInt(s.Get(&st.SV)))
			case *settings.StringSetting:
				d = parser.NewDString(s.String(&st.SV))
			case *settings.StateMachineSetting:
				// Show consistent values for statemachine settings. This isn't necessary
				// for correctness, but helpful for testability.
				ie := InternalExecutor{LeaseManager: p.LeaseMgr()}
				datums, err := ie.QueryRowInTransaction(
					ctx, "retrieve-prev-setting", p.txn,
					"SELECT value FROM system.settings WHERE name = $1",
					name,
				)
				if err != nil {
					return nil, err
				}
				var prevRawVal []byte
				if len(datums) != 0 {
					dStr, ok := datums[0].(*parser.DString)
					if !ok {
						return nil, errors.New("the existing value is not a string")
					}
					prevRawVal = []byte(string(*dStr))
				}
				// Note that if no entry is found, we pretend that an entry
				// exists which is the version used for the running binary. This
				// may not be 100.00% correct, but it will do. The input is
				// checked more thoroughly when a user tries to change the
				// value, and the corresponding sql migration that makes sure
				// the above select finds something usually runs pretty quickly
				// when the cluster is bootstrapped.
				_, obj, err := s.Validate(&st.SV, prevRawVal, nil)
				if err != nil {
					return nil, errors.Errorf("unable to read existing value: %s", err)
				}
				d = parser.NewDString(obj.(fmt.Stringer).String())
			case *settings.BoolSetting:
				d = parser.MakeDBool(parser.DBool(s.Get(&st.SV)))
			case *settings.FloatSetting:
				d = parser.NewDFloat(parser.DFloat(s.Get(&st.SV)))
			case *settings.DurationSetting:
				d = &parser.DInterval{Duration: duration.Duration{Nanos: s.Get(&st.SV).Nanoseconds()}}
			case *settings.EnumSetting:
				d = parser.NewDInt(parser.DInt(s.Get(&st.SV)))
			case *settings.ByteSizeSetting:
				d = parser.NewDString(s.String(&st.SV))
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
func (p *planner) ShowVar(ctx context.Context, n *parser.ShowVar) (planNode, error) {
	origName := n.Name
	name := strings.ToLower(n.Name)

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
// the given query patterns in SQL. The query pattern must accept
// the following formatting parameters:
// %[1]s the database name as SQL string literal.
// %[2]s the unqualified table name as SQL string literal.
// %[3]s the given table name as SQL string literal.
// %[4]s the database name as SQL identifier.
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
		desc, err := MustGetTableOrViewDesc(ctx, p.txn, p.getVirtualTabler(), tn, true /* allowAdding */)
		if err != nil {
			return err
		}
		return p.anyPrivilege(desc)
	}

	return p.delegateQuery(ctx, showType,
		fmt.Sprintf(query,
			parser.EscapeSQLString(db),
			parser.EscapeSQLString(tn.Table()),
			parser.EscapeSQLString(tn.String()),
			tn.DatabaseName.String()),
		initialCheck, nil)
}

// ShowCreateTable returns a CREATE TABLE statement for the specified table.
// Privileges: Any privilege on table.
func (p *planner) ShowCreateTable(
	ctx context.Context, n *parser.ShowCreateTable,
) (planNode, error) {
	// We make the check whether the name points to a table or not in
	// SQL, so as to avoid a double lookup (a first one to check if the
	// descriptor is of the right type, another to populate the
	// create_statements vtable).
	const showCreateTableQuery = `
     SELECT %[3]s AS "Table",
            IFNULL(create_statement,
                   crdb_internal.force_error('` + pgerror.CodeUndefinedTableError + `',
                                             %[1]s || '.' || %[2]s || ' is not a table')::string
            ) AS "CreateTable"
       FROM (SELECT create_statement FROM %[4]s.crdb_internal.create_statements
              WHERE database_name = %[1]s AND descriptor_name = %[2]s AND descriptor_type = 'table'
              UNION ALL VALUES (NULL) ORDER BY 1 DESC) LIMIT 1
  `
	return p.showTableDetails(ctx, "SHOW CREATE TABLE", n.Table, showCreateTableQuery)
}

// ShowCreateView returns a CREATE VIEW statement for the specified view.
// Privileges: Any privilege on view.
func (p *planner) ShowCreateView(ctx context.Context, n *parser.ShowCreateView) (planNode, error) {
	// We make the check whether the name points to a view or not in
	// SQL, so as to avoid a double lookup (a first one to check if the
	// descriptor is of the right type, another to populate the
	// create_statements vtable).
	const showCreateViewQuery = `
     SELECT %[3]s AS "View",
            IFNULL(create_statement,
                   crdb_internal.force_error('` + pgerror.CodeUndefinedTableError + `',
                                             %[1]s || '.' || %[2]s || ' is not a view')::string
            ) AS "CreateView"
       FROM (SELECT create_statement FROM %[4]s.crdb_internal.create_statements
              WHERE database_name = %[1]s AND descriptor_name = %[2]s AND descriptor_type = 'view'
              UNION ALL VALUES (NULL) ORDER BY 1 DESC) LIMIT 1
  `
	return p.showTableDetails(ctx, "SHOW CREATE VIEW", n.View, showCreateViewQuery)
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
               regexp_replace(message, e'^\\[(?:[^][]|\\[[^]]*\\])*\\] ', '') AS message,
               regexp_extract(message, e'^\\[(?:[^][]|\\[[^]]*\\])*\\]') AS context,
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
   OR message LIKE 'Scan %'
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
		// SHOW TRACE FOR SESSION ...
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
		"invalid logical plan structure:\n%s", planToString(ctx, plan),
	).SetDetailf(
		"while inserting:\n%s", planToString(ctx, tracePlan))
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

	desc, err := MustGetTableDesc(ctx, p.txn, p.getVirtualTabler(), tn, true /*allowAdding*/)
	if err != nil {
		return nil, sqlbase.NewUndefinedRelationError(tn)
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
	return p.delegateQuery(ctx, "SHOW JOBS",
		`SELECT id, type, description, username, status, created, started, finished, modified,
            fraction_completed, error, coordinator_id
       FROM crdb_internal.jobs`,
		nil, nil)
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
	return p.ShowVar(ctx, &parser.ShowVar{Name: "transaction status"})
}

// ShowUsers returns all the users.
// Privileges: SELECT on system.users.
func (p *planner) ShowUsers(ctx context.Context, n *parser.ShowUsers) (planNode, error) {
	return p.delegateQuery(ctx, "SHOW USERS",
		`SELECT username FROM system.users ORDER BY 1`, nil, nil)
}
