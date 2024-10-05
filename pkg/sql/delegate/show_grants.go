// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// delegateShowGrants implements SHOW GRANTS which returns grant details for the
// specified objects and users.
// Privileges: None.
//
//	Notes: postgres does not have a SHOW GRANTS statement.
//	       mysql only returns the user's privileges.
func (d *delegator) delegateShowGrants(n *tree.ShowGrants) (tree.Statement, error) {
	var params []string

	const dbPrivQuery = `
SELECT database_name,
       'database' AS object_type,
       grantee,
       privilege_type,
			 is_grantable::boolean
  FROM "".crdb_internal.cluster_database_privileges`
	const schemaPrivQuery = `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       'schema' AS object_type,
       grantee,
       privilege_type,
       is_grantable::boolean
  FROM "".information_schema.schema_privileges`
	const tablePrivQuery = `
SELECT tp.table_catalog AS database_name,
       tp.table_schema AS schema_name,
       tp.table_name,
       tp.grantee,
       tp.privilege_type,
       tp.is_grantable::boolean,
       CASE
           WHEN s.sequence_name IS NOT NULL THEN 'sequence'
           ELSE 'table'
       END AS object_type
  FROM "".information_schema.table_privileges tp
	LEFT JOIN "".information_schema.sequences s ON (
  	tp.table_catalog = s.sequence_catalog AND
  	tp.table_schema = s.sequence_schema AND
  	tp.table_name = s.sequence_name
	)`
	const typePrivQuery = `
SELECT type_catalog AS database_name,
       type_schema AS schema_name,
       'type' AS object_type,
       type_name,
       grantee,
       privilege_type,
       is_grantable::boolean
FROM "".information_schema.type_privileges`
	const systemPrivilegeQuery = `
SELECT a.username AS grantee,
       privilege AS privilege_type,
       a.privilege
       IN (
          SELECT unnest(grant_options)
            FROM crdb_internal.kv_system_privileges
           WHERE username = a.username
        ) AS is_grantable
  FROM (
        SELECT username, unnest(privileges) AS privilege
          FROM crdb_internal.kv_system_privileges
          WHERE path LIKE '/global%'
       ) AS a`
	const externalConnectionPrivilegeQuery = `
SELECT *
  FROM (
        SELECT name AS connection_name,
               'external_connection' AS object_type,
               a.username AS grantee,
               crdb_internal.privilege_name(privilege_key) AS privilege_type,
               a.privilege_key
               IN (
                  SELECT unnest(grant_options)
                    FROM crdb_internal.kv_system_privileges
                   WHERE username = a.username
                ) AS is_grantable
          FROM (
                SELECT regexp_extract(
                        path,
                        e'/externalconn/(\\S+)'
                       ) AS name,
                       username,
                       unnest(privileges) AS privilege_key
                  FROM crdb_internal.kv_system_privileges
                 WHERE path ~* '^/externalconn/'
               ) AS a
       )`

	// Query grants data for user-defined functions and procedures. Builtin
	// functions are not included.
	routineQuery := fmt.Sprintf(`
WITH fn_grants AS (
  SELECT routine_catalog as database_name,
         routine_schema as schema_name,
         reverse(split_part(reverse(specific_name), '_', 1))::OID as routine_id,
         routine_name,
         grantee,
         privilege_type,
         is_grantable::boolean
  FROM "".information_schema.role_routine_grants
  WHERE reverse(split_part(reverse(specific_name), '_', 1))::INT > %d
)
SELECT database_name,
       schema_name,
       'routine' AS object_type,
       routine_id,
       concat(
				 routine_name,
         '(',
				 pg_get_function_identity_arguments(routine_id),
         ')'
			 ) as routine_signature,
       grantee,
       privilege_type,
       is_grantable
  FROM fn_grants
`, oidext.CockroachPredefinedOIDMax)

	var source bytes.Buffer
	var cond bytes.Buffer
	var nameCols string

	if n.Targets != nil && len(n.Targets.Databases) > 0 {
		// Get grants of database from information_schema.schema_privileges
		// if the type of target is database.
		dbNames := n.Targets.Databases.ToStrings()

		for _, db := range dbNames {
			name := cat.SchemaName{
				CatalogName:     tree.Name(db),
				SchemaName:      tree.Name(catconstants.PublicSchemaName),
				ExplicitCatalog: true,
				ExplicitSchema:  true,
			}
			_, _, err := d.catalog.ResolveSchema(d.ctx, cat.Flags{AvoidDescriptorCaches: true}, &name)
			if err != nil {
				return nil, err
			}
			params = append(params, lexbase.EscapeSQLString(db))
		}

		fmt.Fprint(&source, dbPrivQuery)
		nameCols = "database_name,"
		if len(params) == 0 {
			// There are no rows, but we can't simply return emptyNode{} because
			// the result columns must still be defined.
			cond.WriteString(`WHERE false`)
		} else {
			fmt.Fprintf(&cond, `WHERE database_name IN (%s)`, strings.Join(params, ","))
		}
	} else if n.Targets != nil && len(n.Targets.Schemas) > 0 {
		currDB := d.evalCtx.SessionData().Database

		for _, schema := range n.Targets.Schemas {
			if schema.SchemaName == tree.Name('*') {
				allSchemas, err := d.catalog.GetAllSchemaNamesForDB(d.ctx, schema.CatalogName.String())
				if err != nil {
					return nil, err
				}

				for _, sc := range allSchemas {
					_, _, err := d.catalog.ResolveSchema(d.ctx, cat.Flags{AvoidDescriptorCaches: true}, &sc)
					if err != nil {
						return nil, err
					}
					params = append(params, fmt.Sprintf("(%s,%s)", lexbase.EscapeSQLString(sc.Catalog()), lexbase.EscapeSQLString(sc.Schema())))
				}
			} else {
				_, _, err := d.catalog.ResolveSchema(d.ctx, cat.Flags{AvoidDescriptorCaches: true}, &schema)
				if err != nil {
					return nil, err
				}
				dbName := currDB
				if schema.ExplicitCatalog {
					dbName = schema.Catalog()
				}
				params = append(params, fmt.Sprintf("(%s,%s)", lexbase.EscapeSQLString(dbName), lexbase.EscapeSQLString(schema.Schema())))
			}
		}

		fmt.Fprint(&source, schemaPrivQuery)
		nameCols = "database_name, schema_name,"

		if len(params) != 0 {
			fmt.Fprintf(
				&cond,
				`WHERE (database_name, schema_name) IN (%s)`,
				strings.Join(params, ","),
			)
		}
	} else if n.Targets != nil && len(n.Targets.Types) > 0 {
		for _, typName := range n.Targets.Types {
			t, err := d.catalog.ResolveType(d.ctx, typName)
			if err != nil {
				return nil, err
			}
			if t.UserDefined() {
				params = append(
					params,
					fmt.Sprintf(
						"(%s, %s, %s)",
						lexbase.EscapeSQLString(t.TypeMeta.Name.Catalog),
						lexbase.EscapeSQLString(t.TypeMeta.Name.Schema),
						lexbase.EscapeSQLString(t.TypeMeta.Name.Name),
					),
				)
			} else {
				params = append(
					params,
					fmt.Sprintf(
						"(%s, 'pg_catalog', %s)",
						lexbase.EscapeSQLString(t.TypeMeta.Name.Catalog),
						lexbase.EscapeSQLString(t.TypeMeta.Name.Name),
					),
				)
			}
		}

		fmt.Fprint(&source, typePrivQuery)
		nameCols = "database_name, schema_name, type_name,"
		if len(params) == 0 {
			dbNameClause := "true"
			// If the current database is set, restrict the command to it.
			if currDB := d.evalCtx.SessionData().Database; currDB != "" {
				dbNameClause = fmt.Sprintf("database_name = %s", lexbase.EscapeSQLString(currDB))
			}
			cond.WriteString(fmt.Sprintf(`WHERE %s`, dbNameClause))
		} else {
			fmt.Fprintf(
				&cond,
				`WHERE (database_name, schema_name, type_name) IN (%s)`,
				strings.Join(params, ","),
			)
		}
	} else if n.Targets != nil && (len(n.Targets.Functions) > 0 || len(n.Targets.Procedures) > 0) {
		fmt.Fprint(&source, routineQuery)
		nameCols = "database_name, schema_name, routine_id, routine_signature,"
		fnResolved := intsets.MakeFast()
		routines := n.Targets.Functions
		routineType := tree.UDFRoutine
		if len(n.Targets.Procedures) > 0 {
			routines = n.Targets.Procedures
			routineType = tree.ProcedureRoutine
		}
		for _, fn := range routines {
			un := fn.FuncName.ToUnresolvedObjectName().ToUnresolvedName()
			fd, err := d.catalog.ResolveFunction(
				d.ctx, tree.MakeUnresolvedFunctionName(un), &d.evalCtx.SessionData().SearchPath,
			)
			if err != nil {
				return nil, err
			}
			ol, err := fd.MatchOverload(
				d.ctx, d.catalog, &fn, &d.evalCtx.SessionData().SearchPath, routineType,
				false /* inDropContext */, false, /* tryDefaultExprs */
			)
			if err != nil {
				return nil, err
			}
			fnResolved.Add(int(ol.Oid))
		}
		params = make([]string, fnResolved.Len())
		for i, fnID := range fnResolved.Ordered() {
			params[i] = strconv.Itoa(fnID)
		}
		fmt.Fprintf(&cond, `WHERE routine_id IN (%s)`, strings.Join(params, ","))
	} else if n.Targets != nil && n.Targets.System {
		fmt.Fprint(&source, systemPrivilegeQuery)
		cond.WriteString(`WHERE true`)
	} else if n.Targets != nil && len(n.Targets.ExternalConnections) > 0 {
		nameCols = "connection_name,"

		fmt.Fprint(&source, externalConnectionPrivilegeQuery)
		cond.WriteString(`WHERE true`)
	} else if n.Targets != nil {
		nameCols = "database_name, schema_name, table_name,"
		fmt.Fprint(&source, tablePrivQuery)
		// Get grants of table from information_schema.table_privileges
		// if the type of target is table.
		var allTables tree.TableNames

		for _, tableTarget := range n.Targets.Tables.TablePatterns {
			tableGlob, err := tableTarget.NormalizeTablePattern()
			if err != nil {
				return nil, err
			}
			// We avoid the cache so that we can observe the grants taking
			// a lease, like other SHOW commands.
			tables, _, err := cat.ExpandDataSourceGlob(
				d.ctx, d.catalog, cat.Flags{AvoidDescriptorCaches: true}, tableGlob,
			)
			if err != nil {
				return nil, err
			}
			allTables = append(allTables, tables...)
		}

		for i := range allTables {
			params = append(params, fmt.Sprintf("(%s,%s,%s)",
				lexbase.EscapeSQLString(allTables[i].Catalog()),
				lexbase.EscapeSQLString(allTables[i].Schema()),
				lexbase.EscapeSQLString(allTables[i].Table())))
		}

		if len(params) == 0 {
			// The glob pattern has expanded to zero matching tables.
			// There are no rows, but we can't simply return emptyNode{} because
			// the result columns must still be defined.
			cond.WriteString(`WHERE false`)
		} else {
			fmt.Fprintf(&cond, `WHERE (database_name, schema_name, table_name) IN (%s)`, strings.Join(params, ","))
		}
	} else {
		nameCols = "database_name, schema_name, object_name, object_type,"
		// No target: only look at types, tables and schemas in the current database.
		source.WriteString(
			`SELECT database_name, schema_name, table_name AS object_name, object_type, grantee, privilege_type, is_grantable FROM (`,
		)
		source.WriteString(tablePrivQuery)
		source.WriteByte(')')
		source.WriteString(` UNION ALL ` +
			`SELECT database_name, schema_name, NULL::STRING AS object_name, object_type, grantee, privilege_type, is_grantable FROM (`)
		source.WriteString(schemaPrivQuery)
		source.WriteByte(')')
		source.WriteString(` UNION ALL ` +
			`SELECT database_name, NULL::STRING AS schema_name, NULL::STRING AS object_name, object_type, grantee, privilege_type, is_grantable FROM (`)
		source.WriteString(dbPrivQuery)
		source.WriteByte(')')
		source.WriteString(` UNION ALL ` +
			`SELECT database_name, schema_name, type_name AS object_name, object_type, grantee, privilege_type, is_grantable FROM (`)
		source.WriteString(typePrivQuery)
		source.WriteByte(')')
		source.WriteString(` UNION ALL ` +
			`SELECT database_name, schema_name, routine_signature AS object_name, object_type , grantee, privilege_type, is_grantable FROM (`)
		source.WriteString(routineQuery)
		source.WriteByte(')')
		source.WriteString(` UNION ALL ` +
			`SELECT NULL::STRING AS database_name, NULL::STRING AS schema_name, connection_name AS object_name, object_type , grantee, privilege_type, is_grantable FROM (`)
		source.WriteString(externalConnectionPrivilegeQuery)
		source.WriteByte(')')
		// If the current database is set, restrict the command to it.
		if currDB := d.evalCtx.SessionData().Database; currDB != "" {
			fmt.Fprintf(&cond, ` WHERE database_name = %s OR object_type = 'external_connection'`, lexbase.EscapeSQLString(currDB))
		} else {
			cond.WriteString(`WHERE true`)
		}
	}

	implicitGranteeIn := "true"
	if n.Grantees != nil {
		params = params[:0]
		grantees, err := decodeusername.FromRoleSpecList(
			d.evalCtx.SessionData(), username.PurposeValidation, n.Grantees,
		)
		if err != nil {
			return nil, err
		}
		for _, grantee := range grantees {
			params = append(params, lexbase.EscapeSQLString(grantee.Normalized()))
		}
		implicitGranteeIn = fmt.Sprintf("implicit_grantee IN (%s)", strings.Join(params, ","))
	}
	query := fmt.Sprintf(`
WITH
	r AS (SELECT * FROM (%s) %s),
	j
		AS (
			SELECT
				r.*, i.inheriting_member AS implicit_grantee
			FROM
				r INNER JOIN "".crdb_internal.kv_inherited_role_members AS i ON r.grantee = i.role
			UNION ALL
				SELECT *, r.grantee AS implicit_grantee FROM r
		)
SELECT DISTINCT
	%s grantee, privilege_type, is_grantable
FROM
	j
WHERE
	%s
ORDER BY
	%s grantee, privilege_type, is_grantable
	`, source.String(), cond.String(), nameCols, implicitGranteeIn, nameCols)
	// Terminate on invalid users.
	for _, p := range n.Grantees {

		user, err := decodeusername.FromRoleSpec(
			d.evalCtx.SessionData(), username.PurposeValidation, p,
		)
		if err != nil {
			return nil, err
		}
		// The `public` role is a pseudo-role, so we check it separately. RoleExists
		// should not return true for `public` since other operations like GRANT and
		// REVOKE should fail with a "role `public` does not exist" error if they
		// are used with `public`.
		userExists := user.IsPublicRole()
		if !userExists {
			if err := d.catalog.CheckRoleExists(d.ctx, user); err != nil {
				return nil, err
			}
		}
	}
	return d.parse(query)
}
