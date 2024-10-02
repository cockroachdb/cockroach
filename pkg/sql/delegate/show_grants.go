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

const (
	dbPrivQuery = `
SELECT database_name,
       'database' AS object_type,
       grantee,
       privilege_type,
			 is_grantable::boolean
  FROM "".crdb_internal.cluster_database_privileges`

	schemaPrivQuery = `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       'schema' AS object_type,
       grantee,
       privilege_type,
       is_grantable::boolean
  FROM "".information_schema.schema_privileges`

	tablePrivQuery = `
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

	typePrivQuery = `
SELECT type_catalog AS database_name,
       type_schema AS schema_name,
       'type' AS object_type,
       type_name,
       grantee,
       privilege_type,
       is_grantable::boolean
FROM "".information_schema.type_privileges`

	systemPrivilegeQuery = `
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

	externalConnectionPrivilegeQuery = `
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
)

// Query grants data for user-defined functions and procedures. Builtin
// functions are not included.
var routineQuery = fmt.Sprintf(`
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

// Contains values used to construct the specific query that we need.
type showGrantsSpecifics struct {
	params   []string
	source   string
	cond     string
	nameCols string
}

// delegateShowGrants implements SHOW GRANTS which returns grant details for the
// specified objects and users.
// Privileges: None.
//
//	Notes: postgres does not have a SHOW GRANTS statement.
//	       mysql only returns the user's privileges.
func (d *delegator) delegateShowGrants(n *tree.ShowGrants) (tree.Statement, error) {
	var specifics showGrantsSpecifics
	var err error

	if n.Targets == nil {
		specifics = d.delegateShowGrantsAll()
	} else if len(n.Targets.Databases) > 0 {
		specifics, err = d.delegateShowGrantsDatabase(n)
	} else if len(n.Targets.Schemas) > 0 {
		specifics, err = d.delegateShowGrantsSchema(n)
	} else if len(n.Targets.Types) > 0 {
		specifics, err = d.delegateShowGrantsType(n)
	} else if len(n.Targets.Functions) > 0 || len(n.Targets.Procedures) > 0 {
		specifics, err = d.delegateShowGrantsRoutine(n)
	} else if n.Targets.System {
		specifics = d.delegateShowGrantsSystem()
	} else if len(n.Targets.ExternalConnections) > 0 {
		specifics = d.delegateShowGrantsExternalConnections()
	} else {
		// This includes sequences also
		specifics, err = d.delegateShowGrantsTable(n)
	}

	if err != nil {
		return nil, err
	}

	return d.addWithClause(n, specifics)
}

func (d *delegator) delegateShowGrantsDatabase(n *tree.ShowGrants) (showGrantsSpecifics, error) {
	var source bytes.Buffer
	var cond bytes.Buffer
	var params []string

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
			return showGrantsSpecifics{}, err
		}
		params = append(params, lexbase.EscapeSQLString(db))
	}

	fmt.Fprint(&source, dbPrivQuery)
	if len(params) == 0 {
		// There are no rows, but we can't simply return emptyNode{} because
		// the result columns must still be defined.
		cond.WriteString(`WHERE false`)
	} else {
		fmt.Fprintf(&cond, `WHERE database_name IN (%s)`, strings.Join(params, ","))
	}

	return showGrantsSpecifics{
		source:   source.String(),
		params:   params,
		cond:     cond.String(),
		nameCols: "database_name,",
	}, nil
}

func (d *delegator) delegateShowGrantsSchema(n *tree.ShowGrants) (showGrantsSpecifics, error) {
	var source bytes.Buffer
	var cond bytes.Buffer
	var params []string

	currDB := d.evalCtx.SessionData().Database

	for _, schema := range n.Targets.Schemas {
		if schema.SchemaName == tree.Name('*') {
			allSchemas, err := d.catalog.GetAllSchemaNamesForDB(d.ctx, schema.CatalogName.String())
			if err != nil {
				return showGrantsSpecifics{}, err
			}

			for _, sc := range allSchemas {
				_, _, err := d.catalog.ResolveSchema(d.ctx, cat.Flags{AvoidDescriptorCaches: true}, &sc)
				if err != nil {
					return showGrantsSpecifics{}, err
				}
				params = append(params, fmt.Sprintf("(%s,%s)", lexbase.EscapeSQLString(sc.Catalog()), lexbase.EscapeSQLString(sc.Schema())))
			}
		} else {
			_, _, err := d.catalog.ResolveSchema(d.ctx, cat.Flags{AvoidDescriptorCaches: true}, &schema)
			if err != nil {
				return showGrantsSpecifics{}, err
			}
			dbName := currDB
			if schema.ExplicitCatalog {
				dbName = schema.Catalog()
			}
			params = append(params, fmt.Sprintf("(%s,%s)", lexbase.EscapeSQLString(dbName), lexbase.EscapeSQLString(schema.Schema())))
		}
	}

	fmt.Fprint(&source, schemaPrivQuery)

	if len(params) != 0 {
		fmt.Fprintf(
			&cond,
			`WHERE (database_name, schema_name) IN (%s)`,
			strings.Join(params, ","),
		)
	}

	return showGrantsSpecifics{
		source:   source.String(),
		params:   params,
		cond:     cond.String(),
		nameCols: "database_name, schema_name,",
	}, nil
}

func (d *delegator) delegateShowGrantsType(n *tree.ShowGrants) (showGrantsSpecifics, error) {
	var source bytes.Buffer
	var cond bytes.Buffer
	var params []string

	for _, typName := range n.Targets.Types {
		t, err := d.catalog.ResolveType(d.ctx, typName)
		if err != nil {
			return showGrantsSpecifics{}, err
		}

		var schemaName string

		if t.UserDefined() {
			schemaName = lexbase.EscapeSQLString(t.TypeMeta.Name.Schema)
		} else {
			schemaName = "'pg_catalog'"
		}

		params = append(
			params,
			fmt.Sprintf(
				"(%s, %s, %s)",
				lexbase.EscapeSQLString(t.TypeMeta.Name.Catalog),
				schemaName,
				lexbase.EscapeSQLString(t.TypeMeta.Name.Name),
			),
		)

	}

	fmt.Fprint(&source, typePrivQuery)
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

	return showGrantsSpecifics{
		source:   source.String(),
		params:   params,
		cond:     cond.String(),
		nameCols: "database_name, schema_name, type_name,",
	}, nil
}

func (d *delegator) delegateShowGrantsRoutine(n *tree.ShowGrants) (showGrantsSpecifics, error) {
	var source bytes.Buffer
	var cond bytes.Buffer

	fmt.Fprint(&source, routineQuery)
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
			return showGrantsSpecifics{}, err
		}
		ol, err := fd.MatchOverload(
			d.ctx, d.catalog, &fn, &d.evalCtx.SessionData().SearchPath, routineType,
			false /* inDropContext */, false, /* tryDefaultExprs */
		)
		if err != nil {
			return showGrantsSpecifics{}, err
		}
		fnResolved.Add(int(ol.Oid))
	}
	params := make([]string, fnResolved.Len())
	for i, fnID := range fnResolved.Ordered() {
		params[i] = strconv.Itoa(fnID)
	}
	fmt.Fprintf(&cond, `WHERE routine_id IN (%s)`, strings.Join(params, ","))

	return showGrantsSpecifics{
		source:   source.String(),
		params:   params,
		cond:     cond.String(),
		nameCols: "database_name, schema_name, routine_id, routine_signature,",
	}, nil
}

func (d *delegator) delegateShowGrantsSystem() showGrantsSpecifics {
	var source bytes.Buffer
	fmt.Fprint(&source, systemPrivilegeQuery)

	return showGrantsSpecifics{
		source: source.String(),
	}
}

func (d *delegator) delegateShowGrantsTable(n *tree.ShowGrants) (showGrantsSpecifics, error) {
	var source bytes.Buffer
	var cond bytes.Buffer
	var params []string

	fmt.Fprint(&source, tablePrivQuery)
	// Get grants of table from information_schema.table_privileges
	// if the type of target is table.
	var allTables tree.TableNames

	for _, tableTarget := range n.Targets.Tables.TablePatterns {
		tableGlob, err := tableTarget.NormalizeTablePattern()
		if err != nil {
			return showGrantsSpecifics{}, err
		}
		// We avoid the cache so that we can observe the grants taking
		// a lease, like other SHOW commands.
		tables, _, err := cat.ExpandDataSourceGlob(
			d.ctx, d.catalog, cat.Flags{AvoidDescriptorCaches: true}, tableGlob,
		)
		if err != nil {
			return showGrantsSpecifics{}, err
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

	return showGrantsSpecifics{
		source:   source.String(),
		params:   params,
		cond:     cond.String(),
		nameCols: "database_name, schema_name, table_name,",
	}, nil
}

func (d *delegator) delegateShowGrantsExternalConnections() showGrantsSpecifics {
	var source bytes.Buffer
	fmt.Fprint(&source, externalConnectionPrivilegeQuery)

	return showGrantsSpecifics{
		source:   source.String(),
		nameCols: "connection_name,",
	}
}

func (d *delegator) delegateShowGrantsAll() showGrantsSpecifics {
	var source bytes.Buffer
	var cond bytes.Buffer

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
	}

	return showGrantsSpecifics{
		source:   source.String(),
		cond:     cond.String(),
		nameCols: "database_name, schema_name, object_name, object_type,",
	}
}

func (d *delegator) addWithClause(
	n *tree.ShowGrants, specifics showGrantsSpecifics,
) (tree.Statement, error) {
	params := specifics.params
	source := specifics.source
	cond := specifics.cond
	nameCols := specifics.nameCols
	implicitGranteeIn := "true"
	publicRoleQuery := ""

	if n.Grantees != nil {
		params = params[:0]
		// By default, every user/role inherits privileges from the implicit `public` role.
		// To surface these inherited privileges, we query for all privileges inherited through the `public` role
		// for all show grants statements except for ones which explicitly contain the `public`
		// role, for e.g. SHOW GRANTS FOR PUBLIC.
		addPublicAsImplicitGrantee := true
		grantees, err := decodeusername.FromRoleSpecList(
			d.evalCtx.SessionData(), username.PurposeValidation, n.Grantees,
		)
		if err != nil {
			return nil, err
		}
		for _, grantee := range grantees {
			if grantee.IsPublicRole() {
				// If the public role is explicitly specified as a target within the SHOW GRANTS statement,
				// no need to implicitly query for permissions on the public role.
				addPublicAsImplicitGrantee = false
			}
			params = append(params, lexbase.EscapeSQLString(grantee.Normalized()))
		}
		if addPublicAsImplicitGrantee {
			schemaNameFilter := ""
			if strings.Contains(nameCols, "schema_name") {
				// As the `public` role also contains permissions on virtual schemas like `crdb_internal`,
				// we filter out the virtual schemas when we add privileges inherited through public to avoid noise.
				// In all other cases, we don't filter out virtual schemas.
				schemaNameFilter = "AND schema_name NOT IN ('crdb_internal', 'information_schema', 'pg_catalog', 'pg_extension')"
			}
			// The `publicRoleQuery` adds a lookup to retrieve privileges inherited implicitly through the public role.
			publicRoleQuery = fmt.Sprintf(`
				UNION ALL 
					SELECT 
						%s grantee, privilege_type, is_grantable
					FROM 
						r 
					WHERE 
						grantee = 'public' %s`,
				nameCols, schemaNameFilter)
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
%s
ORDER BY
	%s grantee, privilege_type, is_grantable
	`, source, cond, nameCols, implicitGranteeIn, publicRoleQuery, nameCols)
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
