// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vtable

// InformationSchemaAttributes describes the schema of the
// information_schema.attributes view.
// Postgres: https://www.postgresql.org/docs/16/infoschema-attributes.html
const InformationSchemaAttributes = `
CREATE VIEW information_schema.attributes AS
    SELECT CAST(current_database() AS TEXT) AS udt_catalog,
           CAST(nc.nspname AS TEXT) AS udt_schema,
           CAST(c.relname AS TEXT) AS udt_name,
           CAST(a.attname AS TEXT) AS attribute_name,
           CAST(a.attnum AS INT) AS ordinal_position,
           CAST(pg_get_expr(ad.adbin, ad.adrelid) AS TEXT) AS attribute_default,
           CAST(CASE WHEN a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) THEN 'NO' ELSE 'YES' END
             AS TEXT)
             AS is_nullable, 

           CAST(
             CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                  WHEN nt.nspname = 'pg_catalog' THEN format_type(a.atttypid, null)
                  ELSE 'USER-DEFINED' END
             AS TEXT)
             AS data_type,

           CAST(
             information_schema._pg_char_max_length(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
             AS INT)
             AS character_maximum_length,

           CAST(
             information_schema._pg_char_octet_length(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
             AS INT)
             AS character_octet_length,

           CAST(null AS TEXT) AS character_set_catalog,
           CAST(null AS TEXT) AS character_set_schema,
           CAST(null AS TEXT) AS character_set_name,

           CAST(CASE WHEN nco.nspname IS NOT NULL THEN current_database() END AS TEXT) AS collation_catalog,
           CAST(nco.nspname AS TEXT) AS collation_schema,
           CAST(co.collname AS TEXT) AS collation_name,

           CAST(
             information_schema._pg_numeric_precision(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
             AS INT)
             AS numeric_precision,

           CAST(
             information_schema._pg_numeric_precision_radix(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
             AS INT)
             AS numeric_precision_radix,

           CAST(
             information_schema._pg_numeric_scale(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
             AS INT)
             AS numeric_scale,

           CAST(
             information_schema._pg_datetime_precision(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
             AS INT)
             AS datetime_precision,

           CAST(
             information_schema._pg_interval_type(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
             AS TEXT)
             AS interval_type,
           CAST(null AS INT) AS interval_precision,

           CAST(current_database() AS TEXT) AS attribute_udt_catalog,
           CAST(nt.nspname AS TEXT) AS attribute_udt_schema,
           CAST(t.typname AS TEXT) AS attribute_udt_name,

           CAST(null AS TEXT) AS scope_catalog,
           CAST(null AS TEXT) AS scope_schema,
           CAST(null AS TEXT) AS scope_name,

           CAST(null AS INT) AS maximum_cardinality,
           CAST(a.attnum AS TEXT) AS dtd_identifier,
           CAST('NO' AS TEXT) AS is_derived_reference_attribute

    FROM (pg_attribute a LEFT JOIN pg_attrdef ad ON attrelid = adrelid AND attnum = adnum)
         JOIN (pg_class c JOIN pg_namespace nc ON (c.relnamespace = nc.oid)) ON a.attrelid = c.oid
         JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON a.atttypid = t.oid
         LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
           ON a.attcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')

    WHERE a.attnum > 0 AND NOT a.attisdropped
          AND c.relkind IN ('c')
          AND (pg_has_role(c.relowner, 'USAGE')
               OR has_type_privilege(c.reltype, 'USAGE'));`

// InformationSchemaColumnUDTUsage describes the schema of the
// information_schema.column_udt_usage table.
// Postgres: https://www.postgresql.org/docs/current/infoschema-column-udt-usage.html
const InformationSchemaColumnUDTUsage = `
CREATE TABLE information_schema.column_udt_usage (
  UDT_CATALOG   STRING NOT NULL,
  UDT_SCHEMA    STRING NOT NULL,
  UDT_NAME      STRING NOT NULL,
  TABLE_CATALOG STRING NOT NULL,
  TABLE_SCHEMA  STRING NOT NULL,
  TABLE_NAME    STRING NOT NULL,
  COLUMN_NAME   STRING NOT NULL
)
`

// InformationSchemaColumns describes the schema of the
// information_schema.columns table.
// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-columns.html
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/columns-table.html
const InformationSchemaColumns = `
CREATE TABLE information_schema.columns (
	TABLE_CATALOG            STRING NOT NULL,
	TABLE_SCHEMA             STRING NOT NULL,
	TABLE_NAME               STRING NOT NULL,
	COLUMN_NAME              STRING NOT NULL,
  COLUMN_COMMENT           STRING,
	ORDINAL_POSITION         INT NOT NULL,
	COLUMN_DEFAULT           STRING,
	IS_NULLABLE              STRING NOT NULL,
	DATA_TYPE                STRING NOT NULL,
	CHARACTER_MAXIMUM_LENGTH INT,
	CHARACTER_OCTET_LENGTH   INT,
	NUMERIC_PRECISION        INT,
	NUMERIC_PRECISION_RADIX  INT,
	NUMERIC_SCALE            INT,
	DATETIME_PRECISION       INT,
	INTERVAL_TYPE            STRING,
	INTERVAL_PRECISION       INT,
	CHARACTER_SET_CATALOG    STRING,
	CHARACTER_SET_SCHEMA     STRING,
	CHARACTER_SET_NAME       STRING,
	COLLATION_CATALOG        STRING,
	COLLATION_SCHEMA         STRING,
	COLLATION_NAME           STRING,
	DOMAIN_CATALOG           STRING,
	DOMAIN_SCHEMA            STRING,
	DOMAIN_NAME              STRING,
	UDT_CATALOG              STRING,
	UDT_SCHEMA               STRING,
	UDT_NAME                 STRING,
	SCOPE_CATALOG            STRING,
	SCOPE_SCHEMA             STRING,
	SCOPE_NAME               STRING,
	MAXIMUM_CARDINALITY      INT,
	DTD_IDENTIFIER           STRING,
	IS_SELF_REFERENCING      STRING,
	IS_IDENTITY              STRING,
	IDENTITY_GENERATION      STRING,
	IDENTITY_START           STRING,
	IDENTITY_INCREMENT       STRING,
	IDENTITY_MAXIMUM         STRING,
	IDENTITY_MINIMUM         STRING,
	IDENTITY_CYCLE           STRING,
	IS_GENERATED             STRING,
	GENERATION_EXPRESSION    STRING,          -- MySQL/CockroachDB extension.
	IS_UPDATABLE             STRING,
	IS_HIDDEN                STRING NOT NULL, -- CockroachDB extension for SHOW COLUMNS / dump.
	CRDB_SQL_TYPE            STRING NOT NULL  -- CockroachDB extension for SHOW COLUMNS / dump.
)`

// InformationSchemaAdministrableRoleAuthorizations describes the schema of the
// information_schema.administrable_role_authorizations table.
// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-administrable-role-authorizations.html
// MySQL:    missing
const InformationSchemaAdministrableRoleAuthorizations = `
CREATE TABLE information_schema.administrable_role_authorizations (
	GRANTEE      STRING NOT NULL,
	ROLE_NAME    STRING NOT NULL,
	IS_GRANTABLE STRING NOT NULL
)`

// InformationSchemaApplicableRoles describes the schema of the
// information_schema.applicable_roles table.
// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-applicable-roles.html
// MySQL:    missing
const InformationSchemaApplicableRoles = `
CREATE TABLE information_schema.applicable_roles (
	GRANTEE      STRING NOT NULL,
	ROLE_NAME    STRING NOT NULL,
	IS_GRANTABLE STRING NOT NULL
)`

// InformationSchemaCharacterSets describes the schema of the
// information_schema.character_sets table.
// Postgres: https://www.postgresql.org/docs/9.5/infoschema-character-sets.html
// MySQL:	 https://dev.mysql.com/doc/refman/5.7/en/information-schema-character-sets-table.html
const InformationSchemaCharacterSets = `
CREATE TABLE information_schema.character_sets (
    CHARACTER_SET_CATALOG   STRING,
    CHARACTER_SET_SCHEMA    STRING,
    CHARACTER_SET_NAME      STRING NOT NULL,
    CHARACTER_REPERTOIRE    STRING NOT NULL,
    FORM_OF_USE             STRING NOT NULL,
    DEFAULT_COLLATE_CATALOG STRING,
    DEFAULT_COLLATE_SCHEMA  STRING,
    DEFAULT_COLLATE_NAME    STRING
)`

// InformationSchemaCheckConstraints describes the schema of the
// information_schema.check_constraints table.
// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-check-constraints.html
// MySQL:    missing
const InformationSchemaCheckConstraints = `
CREATE TABLE information_schema.check_constraints (
	CONSTRAINT_CATALOG STRING NOT NULL,
	CONSTRAINT_SCHEMA  STRING NOT NULL,
	CONSTRAINT_NAME    STRING NOT NULL,
	CHECK_CLAUSE       STRING NOT NULL
)`

// InformationSchemaColumnPrivileges describes the schema of the
// information_schema.column_privileges table.
// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-column-privileges.html
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/column-privileges-table.html
const InformationSchemaColumnPrivileges = `
CREATE TABLE information_schema.column_privileges (
	GRANTOR        STRING,
	GRANTEE        STRING NOT NULL,
	TABLE_CATALOG  STRING NOT NULL,
	TABLE_SCHEMA   STRING NOT NULL,
	TABLE_NAME     STRING NOT NULL,
	COLUMN_NAME    STRING NOT NULL,
	PRIVILEGE_TYPE STRING NOT NULL,
	IS_GRANTABLE   STRING
)`

// InformationSchemaSchemata describes the schema of the
// information_schema.schemata table.
const InformationSchemaSchemata = `
CREATE TABLE information_schema.schemata (
	CATALOG_NAME               STRING NOT NULL,
	SCHEMA_NAME                STRING NOT NULL,
	DEFAULT_CHARACTER_SET_NAME STRING,
	SQL_PATH                   STRING,
	CRDB_IS_USER_DEFINED       STRING
)`

// InformationSchemaTables describes the schema of the
// information_schema.tables table.
// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-tables.html
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/tables-table.html
const InformationSchemaTables = `
CREATE TABLE information_schema.tables (
	TABLE_CATALOG      STRING NOT NULL,
	TABLE_SCHEMA       STRING NOT NULL,
	TABLE_NAME         STRING NOT NULL,
	TABLE_TYPE         STRING NOT NULL,
	IS_INSERTABLE_INTO STRING NOT NULL,
	VERSION            INT
)`

// InformationSchemaCollationCharacterSetApplicability describes the schema of
// the information_schema.collation_character_set_applicability table.
// Postgres: https://www.postgresql.org/docs/current/infoschema-collation-character-set-applicab.html
// MySQL:    https://dev.mysql.com/doc/refman/8.0/en/information-schema-collation-character-set-applicability-table.html
const InformationSchemaCollationCharacterSetApplicability = `
CREATE TABLE information_schema.collation_character_set_applicability (
	COLLATION_CATALOG     STRING NOT NULL,
	COLLATION_SCHEMA      STRING NOT NULL,
	COLLATION_NAME        STRING NOT NULL,
	CHARACTER_SET_CATALOG STRING,
	CHARACTER_SET_SCHEMA  STRING,
	CHARACTER_SET_NAME    STRING NOT NULL
)`

// InformationSchemaCollations describes the schema of the
// information_schema.collations table.
// Postgres: https://www.postgresql.org/docs/current/infoschema-collations.html
// MySQL:    https://dev.mysql.com/doc/refman/8.0/en/information-schema-collations-table.html
const InformationSchemaCollations = `
CREATE TABLE information_schema.collations (
	COLLATION_CATALOG STRING NOT NULL,
	COLLATION_SCHEMA  STRING NOT NULL,
	COLLATION_NAME    STRING NOT NULL,
	PAD_ATTRIBUTE     STRING NOT NULL
)`

// InformationSchemaSessionVariables describes the schema of the
// information_schema.session_variables table.
const InformationSchemaSessionVariables = `
CREATE TABLE information_schema.session_variables (
	VARIABLE STRING NOT NULL,
	VALUE STRING NOT NULL
)`

// InformationSchemaConstraintColumnUsage describes the schema of the
// information_schema.constraint_column_usage table.
const InformationSchemaConstraintColumnUsage = `
CREATE TABLE information_schema.constraint_column_usage (
	TABLE_CATALOG      STRING NOT NULL,
	TABLE_SCHEMA       STRING NOT NULL,
	TABLE_NAME         STRING NOT NULL,
	COLUMN_NAME        STRING NOT NULL,
	CONSTRAINT_CATALOG STRING NOT NULL,
	CONSTRAINT_SCHEMA  STRING NOT NULL,
	CONSTRAINT_NAME    STRING NOT NULL
)`

// InformationSchemaKeyColumnUsage describes the schema of the
// information_schema.key_column_usage.
const InformationSchemaKeyColumnUsage = `
CREATE TABLE information_schema.key_column_usage (
	CONSTRAINT_CATALOG STRING NOT NULL,
	CONSTRAINT_SCHEMA  STRING NOT NULL,
	CONSTRAINT_NAME    STRING NOT NULL,
	TABLE_CATALOG      STRING NOT NULL,
	TABLE_SCHEMA       STRING NOT NULL,
	TABLE_NAME         STRING NOT NULL,
	COLUMN_NAME        STRING NOT NULL,
	ORDINAL_POSITION   INT NOT NULL,
	POSITION_IN_UNIQUE_CONSTRAINT INT
)`

// InformationSchemaParameters describes the schema of the
// information_schema.parameters table.
const InformationSchemaParameters = `
CREATE VIEW information_schema.parameters AS
    SELECT CAST(current_database() AS TEXT) AS specific_catalog,
           CAST(n_nspname AS TEXT) AS specific_schema,
           CAST(nameconcatoid(proname, p_oid) AS TEXT) AS specific_name,
           CAST((ss.x).n AS INT) AS ordinal_position,
           CAST(
             CASE WHEN proargmodes IS NULL THEN 'IN'
                WHEN proargmodes[(ss.x).n] = 'i' THEN 'IN'
                WHEN proargmodes[(ss.x).n] = 'o' THEN 'OUT'
                WHEN proargmodes[(ss.x).n] = 'b' THEN 'INOUT'
                WHEN proargmodes[(ss.x).n] = 'v' THEN 'IN'
                WHEN proargmodes[(ss.x).n] = 't' THEN 'OUT'
             END AS TEXT) AS parameter_mode,
           CAST('NO' AS TEXT) AS is_result,
           CAST('NO' AS TEXT) AS as_locator,
           CAST(NULLIF(proargnames[(ss.x).n], '') AS TEXT) AS parameter_name,
           CAST(
             CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                  WHEN nt.nspname = 'pg_catalog' THEN format_type(t.oid, null)
                  ELSE 'USER-DEFINED' END AS TEXT)
             AS data_type,
           CAST(null AS INT) AS character_maximum_length,
           CAST(null AS INT) AS character_octet_length,
           CAST(null AS TEXT) AS character_set_catalog,
           CAST(null AS TEXT) AS character_set_schema,
           CAST(null AS TEXT) AS character_set_name,
           CAST(null AS TEXT) AS collation_catalog,
           CAST(null AS TEXT) AS collation_schema,
           CAST(null AS TEXT) AS collation_name,
           CAST(null AS INT) AS numeric_precision,
           CAST(null AS INT) AS numeric_precision_radix,
           CAST(null AS INT) AS numeric_scale,
           CAST(null AS INT) AS datetime_precision,
           CAST(null AS TEXT) AS interval_type,
           CAST(null AS INT) AS interval_precision,
           CAST(current_database() AS TEXT) AS udt_catalog,
           CAST(nt.nspname AS TEXT) AS udt_schema,
           CAST(t.typname AS TEXT) AS udt_name,
           CAST(null AS TEXT) AS scope_catalog,
           CAST(null AS TEXT) AS scope_schema,
           CAST(null AS TEXT) AS scope_name,
           CAST(null AS INT) AS maximum_cardinality,
           CAST((ss.x).n AS TEXT) AS dtd_identifier,
           CAST(
             CASE WHEN pg_has_role(proowner, 'USAGE')
                  THEN pg_get_function_arg_default(p_oid, (ss.x).n)
                  ELSE NULL END
             AS TEXT) AS parameter_default

    FROM pg_type t, pg_namespace nt,
         (SELECT n.nspname AS n_nspname, p.proname, p.oid AS p_oid, p.proowner,
                 p.proargnames, p.proargmodes,
                 information_schema._pg_expandarray(coalesce(p.proallargtypes, p.proargtypes::oid[])) AS x
          FROM pg_namespace n, pg_proc p
          WHERE n.oid = p.pronamespace
                AND (pg_has_role(p.proowner, 'USAGE') OR
                     has_function_privilege(p.oid, 'EXECUTE'))) AS ss
    WHERE t.oid = (ss.x).x AND t.typnamespace = nt.oid;`

// InformationSchemaReferentialConstraints describes the schema of the
// information_schema.referential_constraints table.
const InformationSchemaReferentialConstraints = `
CREATE TABLE information_schema.referential_constraints (
	CONSTRAINT_CATALOG        STRING NOT NULL,
	CONSTRAINT_SCHEMA         STRING NOT NULL,
	CONSTRAINT_NAME           STRING NOT NULL,
	UNIQUE_CONSTRAINT_CATALOG STRING NOT NULL,
	UNIQUE_CONSTRAINT_SCHEMA  STRING NOT NULL,
	UNIQUE_CONSTRAINT_NAME    STRING,
	MATCH_OPTION              STRING NOT NULL,
	UPDATE_RULE               STRING NOT NULL,
	DELETE_RULE               STRING NOT NULL,
	TABLE_NAME                STRING NOT NULL,
	REFERENCED_TABLE_NAME     STRING NOT NULL
)`

// InformationSchemaRoleTableGrants describes the schema of the
// information_schema.role_table_grants table.
const InformationSchemaRoleTableGrants = `
CREATE TABLE information_schema.role_table_grants (
	GRANTOR        STRING,
	GRANTEE        STRING NOT NULL,
	TABLE_CATALOG  STRING NOT NULL,
	TABLE_SCHEMA   STRING NOT NULL,
	TABLE_NAME     STRING NOT NULL,
	PRIVILEGE_TYPE STRING NOT NULL,
	IS_GRANTABLE   STRING,
	WITH_HIERARCHY STRING
)`

// InformationSchemaRoutines describes the schema of the
// information_schema.routines view.
const InformationSchemaRoutines = `
CREATE VIEW information_schema.routines AS
    SELECT CAST(current_database() AS TEXT) AS specific_catalog,
           CAST(n.nspname AS TEXT) AS specific_schema,
           CAST(nameconcatoid(p.proname, p.oid) AS TEXT) AS specific_name,
           CAST(current_database() AS TEXT) AS routine_catalog,
           CAST(n.nspname AS TEXT) AS routine_schema,
           CAST(p.proname AS TEXT) AS routine_name,
           CAST(CASE p.prokind WHEN 'f' THEN 'FUNCTION' WHEN 'p' THEN 'PROCEDURE' END
             AS TEXT) AS routine_type,
           CAST(null AS TEXT) AS module_catalog,
           CAST(null AS TEXT) AS module_schema,
           CAST(null AS TEXT) AS module_name,
           CAST(null AS TEXT) AS udt_catalog,
           CAST(null AS TEXT) AS udt_schema,
           CAST(null AS TEXT) AS udt_name,

           CAST(
             CASE WHEN p.prokind = 'p' THEN NULL
                  WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                  WHEN nt.nspname = 'pg_catalog' THEN format_type(t.oid, null)
                  ELSE 'USER-DEFINED' END AS TEXT)
             AS data_type,
           CAST(null AS INT) AS character_maximum_length,
           CAST(null AS INT) AS character_octet_length,
           CAST(null AS TEXT) AS character_set_catalog,
           CAST(null AS TEXT) AS character_set_schema,
           CAST(null AS TEXT) AS character_set_name,
           CAST(null AS TEXT) AS collation_catalog,
           CAST(null AS TEXT) AS collation_schema,
           CAST(null AS TEXT) AS collation_name,
           CAST(null AS INT) AS numeric_precision,
           CAST(null AS INT) AS numeric_precision_radix,
           CAST(null AS INT) AS numeric_scale,
           CAST(null AS INT) AS datetime_precision,
           CAST(null AS TEXT) AS interval_type,
           CAST(null AS INT) AS interval_precision,
           CAST(CASE WHEN nt.nspname IS NOT NULL THEN current_database() END AS TEXT) AS type_udt_catalog,
           CAST(nt.nspname AS TEXT) AS type_udt_schema,
           CAST(t.typname AS TEXT) AS type_udt_name,
           CAST(null AS TEXT) AS scope_catalog,
           CAST(null AS TEXT) AS scope_schema,
           CAST(null AS TEXT) AS scope_name,
           CAST(null AS INT) AS maximum_cardinality,
           CAST(CASE WHEN p.prokind <> 'p' THEN 0 END AS TEXT) AS dtd_identifier,

           CAST(CASE WHEN l.lanname = 'sql' THEN 'SQL' ELSE 'EXTERNAL' END AS TEXT)
             AS routine_body,
           CAST(
             CASE WHEN pg_has_role(p.proowner, 'USAGE') THEN p.prosrc ELSE null END
             AS TEXT) AS routine_definition,
           CAST(
             CASE WHEN l.lanname = 'c' THEN p.prosrc ELSE null END
             AS TEXT) AS external_name,
           CAST(upper(l.lanname) AS TEXT) AS external_language,

           CAST('GENERAL' AS TEXT) AS parameter_style,
           CAST(CASE WHEN p.provolatile = 'i' THEN 'YES' ELSE 'NO' END AS TEXT) AS is_deterministic,
           CAST('MODIFIES' AS TEXT) AS sql_data_access,
           CAST(CASE WHEN p.prokind <> 'p' THEN
             CASE WHEN p.proisstrict THEN 'YES' ELSE 'NO' END END AS TEXT) AS is_null_call,
           CAST(null AS TEXT) AS sql_path,
           CAST('YES' AS TEXT) AS schema_level_routine,
           CAST(0 AS INT) AS max_dynamic_result_sets,
           CAST(null AS TEXT) AS is_user_defined_cast,
           CAST(null AS TEXT) AS is_implicitly_invocable,
           CAST(CASE WHEN p.prosecdef THEN 'DEFINER' ELSE 'INVOKER' END AS TEXT) AS security_type,
           CAST(null AS TEXT) AS to_sql_specific_catalog,
           CAST(null AS TEXT) AS to_sql_specific_schema,
           CAST(null AS TEXT) AS to_sql_specific_name,
           CAST('NO' AS TEXT) AS as_locator,
           CAST(null AS TIMESTAMPTZ) AS created,
           CAST(null AS TIMESTAMPTZ) AS last_altered,
           CAST(null AS TEXT) AS new_savepoint_level,
           CAST('NO' AS TEXT) AS is_udt_dependent,

           CAST(null AS TEXT) AS result_cast_from_data_type,
           CAST(null AS TEXT) AS result_cast_as_locator,
           CAST(null AS INT) AS result_cast_char_max_length,
           CAST(null AS INT) AS result_cast_char_octet_length,
           CAST(null AS TEXT) AS result_cast_char_set_catalog,
           CAST(null AS TEXT) AS result_cast_char_set_schema,
           CAST(null AS TEXT) AS result_cast_char_set_name,
           CAST(null AS TEXT) AS result_cast_collation_catalog,
           CAST(null AS TEXT) AS result_cast_collation_schema,
           CAST(null AS TEXT) AS result_cast_collation_name,
           CAST(null AS INT) AS result_cast_numeric_precision,
           CAST(null AS INT) AS result_cast_numeric_precision_radix,
           CAST(null AS INT) AS result_cast_numeric_scale,
           CAST(null AS INT) AS result_cast_datetime_precision,
           CAST(null AS TEXT) AS result_cast_interval_type,
           CAST(null AS INT) AS result_cast_interval_precision,
           CAST(null AS TEXT) AS result_cast_type_udt_catalog,
           CAST(null AS TEXT) AS result_cast_type_udt_schema,
           CAST(null AS TEXT) AS result_cast_type_udt_name,
           CAST(null AS TEXT) AS result_cast_scope_catalog,
           CAST(null AS TEXT) AS result_cast_scope_schema,
           CAST(null AS TEXT) AS result_cast_scope_name,
           CAST(null AS INT) AS result_cast_maximum_cardinality,
           CAST(null AS TEXT) AS result_cast_dtd_identifier

    FROM (pg_catalog.pg_namespace n
          JOIN pg_catalog.pg_proc p ON n.oid = p.pronamespace
          JOIN pg_catalog.pg_language l ON p.prolang = l.oid)
         LEFT JOIN
         (pg_catalog.pg_type t JOIN pg_catalog.pg_namespace nt ON t.typnamespace = nt.oid)
         ON p.prorettype = t.oid AND p.prokind <> 'p'

    WHERE (pg_has_role(p.proowner, 'USAGE')
           OR has_function_privilege(p.oid, 'EXECUTE'));`

// InformationSchemaTypePrivileges describes the schema of the
// information_schema.type_privileges table.
const InformationSchemaTypePrivileges = `
CREATE TABLE information_schema.type_privileges (
	GRANTEE         STRING NOT NULL,
	TYPE_CATALOG    STRING NOT NULL,
	TYPE_SCHEMA     STRING NOT NULL,
	TYPE_NAME       STRING NOT NULL,
	PRIVILEGE_TYPE  STRING NOT NULL,
	IS_GRANTABLE    STRING
)`

// InformationSchemaSchemaPrivileges describes the schema of the
// information_schema.schema_privileges table.
const InformationSchemaSchemaPrivileges = `
CREATE TABLE information_schema.schema_privileges (
	GRANTEE         STRING NOT NULL,
	TABLE_CATALOG   STRING NOT NULL,
	TABLE_SCHEMA    STRING NOT NULL,
	PRIVILEGE_TYPE  STRING NOT NULL,
	IS_GRANTABLE    STRING
)`

// InformationSchemaSequences describes the schema of the
// information_schema.sequences table.
const InformationSchemaSequences = `
CREATE TABLE information_schema.sequences (
    SEQUENCE_CATALOG         STRING NOT NULL,
    SEQUENCE_SCHEMA          STRING NOT NULL,
    SEQUENCE_NAME            STRING NOT NULL,
    DATA_TYPE                STRING NOT NULL,
    NUMERIC_PRECISION        INT NOT NULL,
    NUMERIC_PRECISION_RADIX  INT NOT NULL,
    NUMERIC_SCALE            INT NOT NULL,
    START_VALUE              STRING NOT NULL,
    MINIMUM_VALUE            STRING NOT NULL,
    MAXIMUM_VALUE            STRING NOT NULL,
    INCREMENT                STRING NOT NULL,
    CYCLE_OPTION             STRING NOT NULL
)`

// InformationSchemaStatistics describes the schema of the
// information_schema.statistics table.
const InformationSchemaStatistics = `
CREATE TABLE information_schema.statistics (
	TABLE_CATALOG STRING NOT NULL,
	TABLE_SCHEMA  STRING NOT NULL,
	TABLE_NAME    STRING NOT NULL,
	NON_UNIQUE    STRING NOT NULL,
	INDEX_SCHEMA  STRING NOT NULL,
	INDEX_NAME    STRING NOT NULL,
	SEQ_IN_INDEX  INT NOT NULL,
	COLUMN_NAME   STRING NOT NULL,
	"COLLATION"   STRING,
	CARDINALITY   INT,
	DIRECTION     STRING NOT NULL,
	STORING       STRING NOT NULL,
	IMPLICIT      STRING NOT NULL,
	IS_VISIBLE    STRING NOT NULL,
	VISIBILITY    FLOAT
)`

// InformationSchemaTableConstraint describes the schema of the
// information_schema.table_constraints table.
const InformationSchemaTableConstraint = `
CREATE TABLE information_schema.table_constraints (
	CONSTRAINT_CATALOG STRING NOT NULL,
	CONSTRAINT_SCHEMA  STRING NOT NULL,
	CONSTRAINT_NAME    STRING NOT NULL,
	TABLE_CATALOG      STRING NOT NULL,
	TABLE_SCHEMA       STRING NOT NULL,
	TABLE_NAME         STRING NOT NULL,
	CONSTRAINT_TYPE    STRING NOT NULL,
	IS_DEFERRABLE      STRING NOT NULL,
	INITIALLY_DEFERRED STRING NOT NULL
)`

// InformationSchemaUserDefinedTypes describes the schema of the
// information_schema.user_defined_types view.
const InformationSchemaUserDefinedTypes = `
CREATE VIEW information_schema.user_defined_types AS
    SELECT CAST(current_database() AS TEXT) AS user_defined_type_catalog,
           CAST(n.nspname AS TEXT) AS user_defined_type_schema,
           CAST(c.relname AS TEXT) AS user_defined_type_name,
           CAST('STRUCTURED' AS TEXT) AS user_defined_type_category,
           CAST('YES' AS TEXT) AS is_instantiable,
           CAST(null AS TEXT) AS is_final,
           CAST(null AS TEXT) AS ordering_form,
           CAST(null AS TEXT) AS ordering_category,
           CAST(null AS TEXT) AS ordering_routine_catalog,
           CAST(null AS TEXT) AS ordering_routine_schema,
           CAST(null AS TEXT) AS ordering_routine_name,
           CAST(null AS TEXT) AS reference_type,
           CAST(null AS TEXT) AS data_type,
           CAST(null AS INT) AS character_maximum_length,
           CAST(null AS INT) AS character_octet_length,
           CAST(null AS TEXT) AS character_set_catalog,
           CAST(null AS TEXT) AS character_set_schema,
           CAST(null AS TEXT) AS character_set_name,
           CAST(null AS TEXT) AS collation_catalog,
           CAST(null AS TEXT) AS collation_schema,
           CAST(null AS TEXT) AS collation_name,
           CAST(null AS INT) AS numeric_precision,
           CAST(null AS INT) AS numeric_precision_radix,
           CAST(null AS INT) AS numeric_scale,
           CAST(null AS INT) AS datetime_precision,
           CAST(null AS TEXT) AS interval_type,
           CAST(null AS INT) AS interval_precision,
           CAST(null AS TEXT) AS source_dtd_identifier,
           CAST(null AS TEXT) AS ref_dtd_identifier

    FROM pg_namespace n, pg_class c, pg_type t

    WHERE n.oid = c.relnamespace
          AND t.typrelid = c.oid
          AND c.relkind = 'c'
          AND (pg_has_role(t.typowner, 'USAGE')
               OR has_type_privilege(t.oid, 'USAGE'));
`

// InformationSchemaUserPrivileges describes the schema of the
// information_schema.user_privileges table.
const InformationSchemaUserPrivileges = `
CREATE TABLE information_schema.user_privileges (
	GRANTEE        STRING NOT NULL,
	TABLE_CATALOG  STRING NOT NULL,
	PRIVILEGE_TYPE STRING NOT NULL,
	IS_GRANTABLE   STRING
)`

// InformationSchemaTablePrivileges describes the schema of the
// information_schema.table_privileges table.
const InformationSchemaTablePrivileges = `
CREATE TABLE information_schema.table_privileges (
	GRANTOR        STRING,
	GRANTEE        STRING NOT NULL,
	TABLE_CATALOG  STRING NOT NULL,
	TABLE_SCHEMA   STRING NOT NULL,
	TABLE_NAME     STRING NOT NULL,
	PRIVILEGE_TYPE STRING NOT NULL,
	IS_GRANTABLE   STRING,
	WITH_HIERARCHY STRING NOT NULL
)`

// InformationSchemaViews describes the schema of the
// information_schema.views table.
const InformationSchemaViews = `
CREATE TABLE information_schema.views (
    TABLE_CATALOG              STRING NOT NULL,
    TABLE_SCHEMA               STRING NOT NULL,
    TABLE_NAME                 STRING NOT NULL,
    VIEW_DEFINITION            STRING NOT NULL,
    CHECK_OPTION               STRING,
    IS_UPDATABLE               STRING NOT NULL,
    IS_INSERTABLE_INTO         STRING NOT NULL,
    IS_TRIGGER_UPDATABLE       STRING NOT NULL,
    IS_TRIGGER_DELETABLE       STRING NOT NULL,
    IS_TRIGGER_INSERTABLE_INTO STRING NOT NULL
)`

// InformationSchemaEnabledRoles describes the schema of the
// information_schema.enabled_roles table.
const InformationSchemaEnabledRoles = `
CREATE TABLE information_schema.enabled_roles (
	ROLE_NAME STRING NOT NULL
)`

// InformationSchemaForeignDataWrapperOptions is an empty table in the information_schema that is not implemented yet
const InformationSchemaForeignDataWrapperOptions = `
CREATE TABLE information_schema.foreign_data_wrapper_options (
	foreign_data_wrapper_catalog STRING,
	foreign_data_wrapper_name STRING,
	option_name STRING,
	option_value STRING
)`

// InformationSchemaTransforms is an empty table in the information_schema that is not implemented yet
const InformationSchemaTransforms = `
CREATE TABLE information_schema.transforms (
	udt_catalog STRING,
	udt_schema STRING,
	udt_name STRING,
	specific_catalog STRING,
	specific_schema STRING,
	specific_name STRING,
	group_name STRING,
	transform_type STRING
)`

// InformationSchemaViewColumnUsage is an empty table in the information_schema that is not implemented yet
const InformationSchemaViewColumnUsage = `
CREATE TABLE information_schema.view_column_usage (
	view_catalog STRING,
	view_schema STRING,
	view_name STRING,
	table_catalog STRING,
	table_schema STRING,
	table_name STRING,
	column_name STRING
)`

// InformationSchemaRoutinePrivileges is an empty table in the information_schema that is not implemented yet
const InformationSchemaRoutinePrivileges = `
CREATE TABLE information_schema.routine_privileges (
	grantor STRING,
	grantee STRING,
	specific_catalog STRING,
	specific_schema STRING,
	specific_name STRING,
	routine_catalog STRING,
	routine_schema STRING,
	routine_name STRING,
	privilege_type STRING,
	is_grantable STRING
)`

// InformationSchemaRoleRoutineGrants is an empty table in the information_schema that is not implemented yet
const InformationSchemaRoleRoutineGrants = `
CREATE TABLE information_schema.role_routine_grants (
	grantor STRING,
	grantee STRING,
	specific_catalog STRING,
	specific_schema STRING,
	specific_name STRING,
	routine_catalog STRING,
	routine_schema STRING,
	routine_name STRING,
	privilege_type STRING,
	is_grantable STRING
)`

// InformationSchemaElementTypes is an empty table in the information_schema that is not implemented yet
const InformationSchemaElementTypes = `
CREATE TABLE information_schema.element_types (
	object_catalog STRING,
	object_schema STRING,
	object_name STRING,
	object_type STRING,
	collection_type_identifier STRING,
	data_type STRING,
	character_maximum_length INT,
	character_octet_length INT,
	character_set_catalog STRING,
	character_set_schema STRING,
	character_set_name STRING,
	collation_catalog STRING,
	collation_schema STRING,
	collation_name STRING,
	numeric_precision INT,
	numeric_precision_radix INT,
	numeric_scale INT,
	datetime_precision INT,
	interval_type STRING,
	interval_precision INT,
	domain_default STRING,
	udt_catalog STRING,
	udt_schema STRING,
	udt_name STRING,
	scope_catalog STRING,
	scope_schema STRING,
	scope_name STRING,
	maximum_cardinality INT,
	dtd_identifier STRING
)`

// InformationSchemaRoleUdtGrants is an empty table in the information_schema that is not implemented yet
const InformationSchemaRoleUdtGrants = `
CREATE TABLE information_schema.role_udt_grants (
	grantor STRING,
	grantee STRING,
	udt_catalog STRING,
	udt_schema STRING,
	udt_name STRING,
	privilege_type STRING,
	is_grantable STRING
)`

// InformationSchemaColumnOptions is an empty table in the information_schema that is not implemented yet
const InformationSchemaColumnOptions = `
CREATE TABLE information_schema.column_options (
	table_catalog STRING,
	table_schema STRING,
	table_name STRING,
	column_name STRING,
	option_name STRING,
	option_value STRING
)`

// InformationSchemaForeignTables is an empty table in the information_schema that is not implemented yet
const InformationSchemaForeignTables = `
CREATE TABLE information_schema.foreign_tables (
	foreign_table_catalog STRING,
	foreign_table_schema STRING,
	foreign_table_name STRING,
	foreign_server_catalog STRING,
	foreign_server_name STRING
)`

// InformationSchemaInformationSchemaCatalogName is an empty table in the information_schema that is not implemented yet
const InformationSchemaInformationSchemaCatalogName = `
CREATE TABLE information_schema.information_schema_catalog_name (
	catalog_name STRING
)`

// InformationSchemaCheckConstraintRoutineUsage is an empty table in the information_schema that is not implemented yet
const InformationSchemaCheckConstraintRoutineUsage = `
CREATE TABLE information_schema.check_constraint_routine_usage (
	constraint_catalog STRING,
	constraint_schema STRING,
	constraint_name STRING,
	specific_catalog STRING,
	specific_schema STRING,
	specific_name STRING
)`

// InformationSchemaColumnDomainUsage is an empty table in the information_schema that is not implemented yet
const InformationSchemaColumnDomainUsage = `
CREATE TABLE information_schema.column_domain_usage (
	domain_catalog STRING,
	domain_schema STRING,
	domain_name STRING,
	table_catalog STRING,
	table_schema STRING,
	table_name STRING,
	column_name STRING
)`

// InformationSchemaForeignDataWrappers is an empty table in the information_schema that is not implemented yet
const InformationSchemaForeignDataWrappers = `
CREATE TABLE information_schema.foreign_data_wrappers (
	foreign_data_wrapper_catalog STRING,
	foreign_data_wrapper_name STRING,
	authorization_identifier STRING,
	library_name STRING,
	foreign_data_wrapper_language STRING
)`

// InformationSchemaViewRoutineUsage is an empty table in the information_schema that is not implemented yet
const InformationSchemaViewRoutineUsage = `
CREATE TABLE information_schema.view_routine_usage (
	table_catalog STRING,
	table_schema STRING,
	table_name STRING,
	specific_catalog STRING,
	specific_schema STRING,
	specific_name STRING
)`

// InformationSchemaRoleColumnGrants is an empty table in the information_schema that is not implemented yet
const InformationSchemaRoleColumnGrants = `
CREATE TABLE information_schema.role_column_grants (
	grantor STRING,
	grantee STRING,
	table_catalog STRING,
	table_schema STRING,
	table_name STRING,
	column_name STRING,
	privilege_type STRING,
	is_grantable STRING
)`

// InformationSchemaDomainConstraints is an empty table in the information_schema that is not implemented yet
const InformationSchemaDomainConstraints = `
CREATE TABLE information_schema.domain_constraints (
	constraint_catalog STRING,
	constraint_schema STRING,
	constraint_name STRING,
	domain_catalog STRING,
	domain_schema STRING,
	domain_name STRING,
	is_deferrable STRING,
	initially_deferred STRING
)`

// InformationSchemaUserMappings is an empty table in the information_schema that is not implemented yet
const InformationSchemaUserMappings = `
CREATE TABLE information_schema.user_mappings (
	authorization_identifier STRING,
	foreign_server_catalog STRING,
	foreign_server_name STRING
)`

// InformationSchemaSQLSizing is an empty table in the information_schema that is not implemented yet
const InformationSchemaSQLSizing = `
CREATE TABLE information_schema.sql_sizing (
	sizing_id INT,
	sizing_name STRING,
	supported_value INT,
	comments STRING
)`

// InformationSchemaColumnColumnUsage is an empty table in the information_schema that is not implemented yet
const InformationSchemaColumnColumnUsage = `
CREATE TABLE information_schema.column_column_usage (
	table_catalog STRING,
	table_schema STRING,
	table_name STRING,
	column_name STRING,
	dependent_column STRING
)`

// InformationSchemaUdtPrivileges is an empty table in the information_schema that is not implemented yet
const InformationSchemaUdtPrivileges = `
CREATE TABLE information_schema.udt_privileges (
	grantor STRING,
	grantee STRING,
	udt_catalog STRING,
	udt_schema STRING,
	udt_name STRING,
	privilege_type STRING,
	is_grantable STRING
)`

// InformationSchemaForeignServerOptions is an empty table in the information_schema that is not implemented yet
const InformationSchemaForeignServerOptions = `
CREATE TABLE information_schema.foreign_server_options (
	foreign_server_catalog STRING,
	foreign_server_name STRING,
	option_name STRING,
	option_value STRING
)`

// InformationSchemaConstraintTableUsage is an empty table in the information_schema that is not implemented yet
const InformationSchemaConstraintTableUsage = `
CREATE TABLE information_schema.constraint_table_usage (
	table_catalog STRING,
	table_schema STRING,
	table_name STRING,
	constraint_catalog STRING,
	constraint_schema STRING,
	constraint_name STRING
)`

// InformationSchemaUsagePrivileges is an empty table in the information_schema that is not implemented yet
const InformationSchemaUsagePrivileges = `
CREATE TABLE information_schema.usage_privileges (
	grantor STRING,
	grantee STRING,
	object_catalog STRING,
	object_schema STRING,
	object_name STRING,
	object_type STRING,
	privilege_type STRING,
	is_grantable STRING
)`

// InformationSchemaDomains is an empty table in the information_schema that is not implemented yet
const InformationSchemaDomains = `
CREATE TABLE information_schema.domains (
	domain_catalog STRING,
	domain_schema STRING,
	domain_name STRING,
	data_type STRING,
	character_maximum_length INT,
	character_octet_length INT,
	character_set_catalog STRING,
	character_set_schema STRING,
	character_set_name STRING,
	collation_catalog STRING,
	collation_schema STRING,
	collation_name STRING,
	numeric_precision INT,
	numeric_precision_radix INT,
	numeric_scale INT,
	datetime_precision INT,
	interval_type STRING,
	interval_precision INT,
	domain_default STRING,
	udt_catalog STRING,
	udt_schema STRING,
	udt_name STRING,
	scope_catalog STRING,
	scope_schema STRING,
	scope_name STRING,
	maximum_cardinality INT,
	dtd_identifier STRING
)`

// InformationSchemaSQLImplementationInfo is an empty table in the information_schema that is not implemented yet
const InformationSchemaSQLImplementationInfo = `
CREATE TABLE information_schema.sql_implementation_info (
	implementation_info_id STRING,
	implementation_info_name STRING,
	integer_value INT,
	character_value STRING,
	comments STRING
)`

// InformationSchemaForeignTableOptions is an empty table in the information_schema that is not implemented yet
const InformationSchemaForeignTableOptions = `
CREATE TABLE information_schema.foreign_table_options (
	foreign_table_catalog STRING,
	foreign_table_schema STRING,
	foreign_table_name STRING,
	option_name STRING,
	option_value STRING
)`

// InformationSchemaDomainUdtUsage is an empty table in the information_schema that is not implemented yet
const InformationSchemaDomainUdtUsage = `
CREATE TABLE information_schema.domain_udt_usage (
	udt_catalog STRING,
	udt_schema STRING,
	udt_name STRING,
	domain_catalog STRING,
	domain_schema STRING,
	domain_name STRING
)`

// InformationSchemaUserMappingOptions is an empty table in the information_schema that is not implemented yet
const InformationSchemaUserMappingOptions = `
CREATE TABLE information_schema.user_mapping_options (
	authorization_identifier STRING,
	foreign_server_catalog STRING,
	foreign_server_name STRING,
	option_name STRING,
	option_value STRING
)`

// InformationSchemaDataTypePrivileges is an empty table in the information_schema that is not implemented yet
const InformationSchemaDataTypePrivileges = `
CREATE TABLE information_schema.data_type_privileges (
	object_catalog STRING,
	object_schema STRING,
	object_name STRING,
	object_type STRING,
	dtd_identifier STRING
)`

// InformationSchemaRoleUsageGrants is an empty table in the information_schema that is not implemented yet
const InformationSchemaRoleUsageGrants = `
CREATE TABLE information_schema.role_usage_grants (
	grantor STRING,
	grantee STRING,
	object_catalog STRING,
	object_schema STRING,
	object_name STRING,
	object_type STRING,
	privilege_type STRING,
	is_grantable STRING
)`

// InformationSchemaSQLFeatures is an empty table in the information_schema that is not implemented yet
const InformationSchemaSQLFeatures = `
CREATE TABLE information_schema.sql_features (
	feature_id STRING,
	feature_name STRING,
	sub_feature_id STRING,
	sub_feature_name STRING,
	is_supported STRING,
	is_verified_by STRING,
	comments STRING
)`

// InformationSchemaSQLParts is an empty table in the information_schema that is not implemented yet
const InformationSchemaSQLParts = `
CREATE TABLE information_schema.sql_parts (
	feature_id STRING,
	feature_name STRING,
	is_supported STRING,
	is_verified_by STRING,
	comments STRING
)`

// InformationSchemaTablespaces is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaTablespaces = `
CREATE TABLE information_schema.tablespaces (
	extent_size INT,
	logfile_group_name STRING,
	nodegroup_id INT,
	tablespace_type STRING,
	autoextend_size INT,
	engine STRING,
	maximum_size INT,
	tablespace_comment STRING,
	tablespace_name STRING
)`

// InformationSchemaStSpatialReferenceSystems is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaStSpatialReferenceSystems = `
CREATE TABLE information_schema.st_spatial_reference_systems (
	srs_id INT,
	srs_name STRING,
	definition STRING,
	description STRING,
	organization STRING,
	organization_coordsys_id INT
)`

// InformationSchemaProcesslist is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaProcesslist = `
CREATE TABLE information_schema.processlist (
	host STRING,
	id INT,
	info STRING,
	state STRING,
	time INT,
	"user" STRING,
	command STRING,
	db STRING
)`

// InformationSchemaTriggeredUpdateColumns is an empty table in the information_schema that is not implemented yet
const InformationSchemaTriggeredUpdateColumns = `
CREATE TABLE information_schema.triggered_update_columns (
	trigger_catalog STRING,
	trigger_schema STRING,
	trigger_name STRING,
	event_object_catalog STRING,
	event_object_schema STRING,
	event_object_table STRING,
	event_object_column STRING
)`

// InformationSchemaTriggers is an empty table in the information_schema that is not implemented yet
const InformationSchemaTriggers = `
CREATE TABLE information_schema.triggers (
	trigger_catalog STRING,
	trigger_schema STRING,
	trigger_name STRING,
	event_manipulation STRING,
	event_object_catalog STRING,
	event_object_schema STRING,
	event_object_table STRING,
	action_order INT,
	action_condition STRING,
	action_statement STRING,
	action_orientation STRING,
	action_timing STRING,
	action_reference_old_table STRING,
	action_reference_new_table STRING,
	action_reference_old_row STRING,
	action_reference_new_row STRING,
	created TIMESTAMPTZ
)`

// InformationSchemaTablesExtensions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaTablesExtensions = `
CREATE TABLE information_schema.tables_extensions (
	engine_attribute STRING,
	secondary_engine_attribute STRING,
	table_catalog STRING,
	table_name STRING,
	table_schema STRING
)`

// InformationSchemaViewTableUsage is an empty table in the information_schema that is not implemented yet
const InformationSchemaViewTableUsage = `
CREATE TABLE information_schema.view_table_usage (
	view_catalog STRING,
	view_schema STRING,
	view_name STRING,
	table_catalog STRING,
	table_schema STRING,
	table_name STRING
)`

// InformationSchemaProfiling is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaProfiling = `
CREATE TABLE information_schema.profiling (
	cpu_system DECIMAL,
	messages_sent INT,
	swaps INT,
	block_ops_in INT,
	block_ops_out INT,
	context_voluntary INT,
	cpu_user DECIMAL,
	query_id INT,
	source_function STRING,
	context_involuntary INT,
	duration DECIMAL,
	page_faults_major INT,
	page_faults_minor INT,
	seq INT,
	source_file STRING,
	state STRING,
	messages_received INT,
	source_line INT
)`

// InformationSchemaColumnStatistics is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaColumnStatistics = `
CREATE TABLE information_schema.column_statistics (
	column_name STRING,
	histogram STRING,
	schema_name STRING,
	table_name STRING
)`

// InformationSchemaResourceGroups is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaResourceGroups = `
CREATE TABLE information_schema.resource_groups (
	resource_group_enabled INT2,
	resource_group_name STRING,
	resource_group_type STRING,
	thread_priority INT,
	vcpu_ids BYTES
)`

// InformationSchemaPartitions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaPartitions = `
CREATE TABLE information_schema.partitions (
	data_free INT,
	partition_name STRING,
	subpartition_expression STRING,
	table_name STRING,
	table_rows INT,
	avg_row_length INT,
	check_time TIMESTAMPTZ,
	create_time TIMESTAMPTZ,
	index_length INT,
	nodegroup STRING,
	partition_comment STRING,
	partition_description STRING,
	table_schema STRING,
	checksum INT,
	partition_expression STRING,
	partition_method STRING,
	subpartition_name STRING,
	tablespace_name STRING,
	update_time TIMESTAMPTZ,
	data_length INT,
	max_data_length INT,
	partition_ordinal_position INT,
	subpartition_method STRING,
	subpartition_ordinal_position INT,
	table_catalog STRING
)`

// InformationSchemaTablespacesExtensions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaTablespacesExtensions = `
CREATE TABLE information_schema.tablespaces_extensions (
	engine_attribute STRING,
	tablespace_name STRING
)`

// InformationSchemaSchemataExtensions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaSchemataExtensions = `
CREATE TABLE information_schema.schemata_extensions (
	catalog_name STRING,
	options STRING,
	schema_name STRING
)`

// InformationSchemaStUnitsOfMeasure is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaStUnitsOfMeasure = `
CREATE TABLE information_schema.st_units_of_measure (
	conversion_factor FLOAT,
	description STRING,
	unit_name STRING,
	unit_type STRING
)`

// InformationSchemaFiles is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaFiles = `
CREATE TABLE information_schema.files (
	last_update_time BYTES,
	table_rows BYTES,
	autoextend_size INT,
	check_time BYTES,
	checksum BYTES,
	extra STRING,
	file_id INT,
	table_name BYTES,
	avg_row_length BYTES,
	extent_size INT,
	file_name STRING,
	free_extents INT,
	max_data_length BYTES,
	table_schema BYTES,
	update_time BYTES,
	data_length BYTES,
	tablespace_name STRING,
	version INT,
	create_time BYTES,
	initial_size INT,
	logfile_group_name STRING,
	maximum_size INT,
	status STRING,
	update_count BYTES,
	creation_time BYTES,
	engine STRING,
	fulltext_keys BYTES,
	row_format STRING,
	total_extents INT,
	data_free INT,
	index_length BYTES,
	last_access_time BYTES,
	table_catalog STRING,
	transaction_counter BYTES,
	file_type STRING,
	logfile_group_number INT,
	recover_time BYTES,
	deleted_rows BYTES
)`

// InformationSchemaEngines is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaEngines = `
CREATE TABLE information_schema.engines (
	support STRING,
	transactions STRING,
	xa STRING,
	comment STRING,
	engine STRING,
	savepoints STRING
)`

// InformationSchemaColumnsExtensions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaColumnsExtensions = `
CREATE TABLE information_schema.columns_extensions (
	engine_attribute STRING,
	secondary_engine_attribute STRING,
	table_catalog STRING,
	table_name STRING,
	table_schema STRING,
	column_name STRING
)`

// InformationSchemaStGeometryColumns is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaStGeometryColumns = `
CREATE TABLE information_schema.st_geometry_columns (
	srs_id INT,
	srs_name STRING,
	table_catalog STRING,
	table_name STRING,
	table_schema STRING,
	column_name STRING,
	geometry_type_name STRING
)`

// InformationSchemaPlugins is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaPlugins = `
CREATE TABLE information_schema.plugins (
	plugin_version STRING,
	load_option STRING,
	plugin_description STRING,
	plugin_library_version STRING,
	plugin_status STRING,
	plugin_type STRING,
	plugin_type_version STRING,
	plugin_author STRING,
	plugin_library STRING,
	plugin_license STRING,
	plugin_name STRING
)`

// InformationSchemaEvents is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaEvents = `
CREATE TABLE information_schema.events (
	definer STRING,
	event_definition STRING,
	event_name STRING,
	interval_value STRING,
	last_altered TIMESTAMPTZ,
	on_completion STRING,
	originator INT,
	collation_connection STRING,
	database_collation STRING,
	event_body STRING,
	event_schema STRING,
	execute_at TIMESTAMPTZ,
	interval_field STRING,
	starts TIMESTAMPTZ,
	time_zone STRING,
	character_set_client STRING,
	ends TIMESTAMPTZ,
	event_catalog STRING,
	event_comment STRING,
	event_type STRING,
	last_executed TIMESTAMPTZ,
	sql_mode STRING[],
	status STRING,
	created TIMESTAMPTZ
)`

// InformationSchemaUserAttributes is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaUserAttributes = `
CREATE TABLE information_schema.user_attributes (
	attribute STRING,
	host STRING,
	"user" STRING
)`

// InformationSchemaKeywords is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaKeywords = `
CREATE TABLE information_schema.keywords (
	word STRING,
	reserved INT
)`

// InformationSchemaOptimizerTrace is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaOptimizerTrace = `
CREATE TABLE information_schema.optimizer_trace (
	insufficient_privileges INT2,
	missing_bytes_beyond_max_mem_size INT,
	query STRING,
	trace STRING
)`

// InformationSchemaForeignServers is an empty table in the information_schema that is not implemented yet
const InformationSchemaForeignServers = `
CREATE TABLE information_schema.foreign_servers (
	foreign_server_catalog STRING,
	foreign_server_name STRING,
	foreign_data_wrapper_catalog STRING,
	foreign_data_wrapper_name STRING,
	foreign_server_type STRING,
	foreign_server_version STRING,
	authorization_identifier STRING
)`

// InformationSchemaTableConstraintsExtensions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaTableConstraintsExtensions = `
CREATE TABLE information_schema.table_constraints_extensions (
	constraint_catalog STRING,
	constraint_name STRING,
	constraint_schema STRING,
	engine_attribute STRING,
	secondary_engine_attribute STRING,
	table_name STRING
)`
