// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package vtable

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
const InformationSchemaParameters = `CREATE TABLE information_schema.parameters (
	SPECIFIC_CATALOG STRING,
	SPECIFIC_SCHEMA STRING,
	SPECIFIC_NAME STRING,
	ORDINAL_POSITION INT,
	PARAMETER_MODE STRING,
	IS_RESULT STRING,
	AS_LOCATOR STRING,
	PARAMETER_NAME STRING,
	DATA_TYPE STRING,
	CHARACTER_MAXIMUM_LENGTH INT,
	CHARACTER_OCTET_LENGTH INT,
	CHARACTER_SET_CATALOG STRING,
	CHARACTER_SET_SCHEMA STRING,
	CHARACTER_SET_NAME STRING,
	COLLATION_CATALOG STRING,
	COLLATION_SCHEMA STRING,
	COLLATION_NAME STRING,
	NUMERIC_PRECISION INT,
	NUMERIC_PRECISION_RADIX INT,
	NUMERIC_SCALE INT,
	DATETIME_PRECISION INT,
	INTERVAL_TYPE STRING,
	INTERVAL_PRECISION INT,
	UDT_CATALOG STRING,
	UDT_SCHEMA STRING,
	UDT_NAME STRING,
	SCOPE_CATALOG STRING,
	SCOPE_SCHEMA STRING,
	SCOPE_NAME STRING,
	MAXIMUM_CARDINALITY INT,
	DTD_IDENTIFIER STRING,
	PARAMETER_DEFAULT STRING
)`

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
// information_schema.routines table.
const InformationSchemaRoutines = `
CREATE TABLE information_schema.routines (
	SPECIFIC_CATALOG STRING,
	SPECIFIC_SCHEMA STRING,
	SPECIFIC_NAME STRING,
	ROUTINE_CATALOG STRING,
	ROUTINE_SCHEMA STRING,
	ROUTINE_NAME STRING,
	ROUTINE_TYPE STRING,
	MODULE_CATALOG STRING,
	MODULE_SCHEMA STRING,
	MODULE_NAME STRING,
	UDT_CATALOG STRING,
	UDT_SCHEMA STRING,
	UDT_NAME STRING,
	DATA_TYPE STRING,
	CHARACTER_MAXIMUM_LENGTH INT,
	CHARACTER_OCTET_LENGTH INT,
	CHARACTER_SET_CATALOG STRING,
	CHARACTER_SET_SCHEMA STRING,
	CHARACTER_SET_NAME STRING,
	COLLATION_CATALOG STRING,
	COLLATION_SCHEMA STRING,
	COLLATION_NAME STRING,
	NUMERIC_PRECISION INT,
	NUMERIC_PRECISION_RADIX INT,
	NUMERIC_SCALE INT,
	DATETIME_PRECISION INT,
	INTERVAL_TYPE STRING,
	INTERVAL_PRECISION INT,
	TYPE_UDT_CATALOG STRING,
	TYPE_UDT_SCHEMA STRING,
	TYPE_UDT_NAME STRING,
	SCOPE_CATALOG STRING,
	SCOPE_NAME STRING,
	MAXIMUM_CARDINALITY INT,
	DTD_IDENTIFIER STRING,
	ROUTINE_BODY STRING,
	ROUTINE_DEFINITION STRING,
	EXTERNAL_NAME STRING,
	EXTERNAL_LANGUAGE STRING,
	PARAMETER_STYLE STRING,
	IS_DETERMINISTIC STRING,
	SQL_DATA_ACCESS STRING,
	IS_NULL_CALL STRING,
	SQL_PATH STRING,
	SCHEMA_LEVEL_ROUTINE STRING,
	MAX_DYNAMIC_RESULT_SETS INT,
	IS_USER_DEFINED_CAST STRING,
	IS_IMPLICITLY_INVOCABLE STRING,
	SECURITY_TYPE STRING,
	TO_SQL_SPECIFIC_CATALOG STRING,
	TO_SQL_SPECIFIC_SCHEMA STRING,
	TO_SQL_SPECIFIC_NAME STRING,
	AS_LOCATOR STRING,
	CREATED  TIMESTAMPTZ,
	LAST_ALTERED TIMESTAMPTZ,
	NEW_SAVEPOINT_LEVEL  STRING,
	IS_UDT_DEPENDENT STRING,
	RESULT_CAST_FROM_DATA_TYPE STRING,
	RESULT_CAST_AS_LOCATOR STRING,
	RESULT_CAST_CHAR_MAX_LENGTH  INT,
	RESULT_CAST_CHAR_OCTET_LENGTH INT,
	RESULT_CAST_CHAR_SET_CATALOG STRING,
	RESULT_CAST_CHAR_SET_SCHEMA  STRING,
	RESULT_CAST_CHAR_SET_NAME STRING,
	RESULT_CAST_COLLATION_CATALOG STRING,
	RESULT_CAST_COLLATION_SCHEMA STRING,
	RESULT_CAST_COLLATION_NAME STRING,
	RESULT_CAST_NUMERIC_PRECISION INT,
	RESULT_CAST_NUMERIC_PRECISION_RADIX INT,
	RESULT_CAST_NUMERIC_SCALE INT,
	RESULT_CAST_DATETIME_PRECISION INT,
	RESULT_CAST_INTERVAL_TYPE STRING,
	RESULT_CAST_INTERVAL_PRECISION INT,
	RESULT_CAST_TYPE_UDT_CATALOG STRING,
	RESULT_CAST_TYPE_UDT_SCHEMA  STRING,
	RESULT_CAST_TYPE_UDT_NAME STRING,
	RESULT_CAST_SCOPE_CATALOG STRING,
	RESULT_CAST_SCOPE_SCHEMA STRING,
	RESULT_CAST_SCOPE_NAME STRING,
	RESULT_CAST_MAXIMUM_CARDINALITY INT,
	RESULT_CAST_DTD_IDENTIFIER STRING
)`

// InformationSchemaTypePrivileges describes the schema of the
// information_schema.type_privileges table.
const InformationSchemaTypePrivileges = `
CREATE TABLE information_schema.type_privileges (
	GRANTEE         STRING NOT NULL,
	TYPE_CATALOG    STRING NOT NULL,
	TYPE_SCHEMA     STRING NOT NULL,
	TYPE_NAME       STRING NOT NULL,
	PRIVILEGE_TYPE  STRING NOT NULL
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
	IMPLICIT      STRING NOT NULL
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
//// information_schema.views table.
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

//InformationSchemaDataTypePrivileges is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaDataTypePrivileges = `
CREATE TABLE information_schema.data_type_privileges (
	object_schema STRING,
	object_type STRING,
	dtd_identifier STRING,
	object_catalog STRING,
	object_name STRING
)`

//InformationSchemaSQLImplementationInfo is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaSQLImplementationInfo = `
CREATE TABLE information_schema.sql_implementation_info (
	character_value STRING,
	comments STRING,
	implementation_info_id STRING,
	implementation_info_name STRING,
	integer_value INT
)`

//InformationSchemaForeignTableOptions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaForeignTableOptions = `
CREATE TABLE information_schema.foreign_table_options (
	foreign_table_catalog STRING,
	foreign_table_name STRING,
	foreign_table_schema STRING,
	option_name STRING,
	option_value STRING
)`

//InformationSchemaRoleRoutineGrants is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaRoleRoutineGrants = `
CREATE TABLE information_schema.role_routine_grants (
	privilege_type STRING,
	routine_catalog STRING,
	specific_name STRING,
	specific_schema STRING,
	grantee STRING,
	grantor STRING,
	routine_schema STRING,
	specific_catalog STRING,
	is_grantable STRING,
	routine_name STRING
)`

//InformationSchemaInformationSchemaCatalogName is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaInformationSchemaCatalogName = `
CREATE TABLE information_schema.information_schema_catalog_name (
	catalog_name STRING
)`

//InformationSchemaUserDefinedTypes is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaUserDefinedTypes = `
CREATE TABLE information_schema.user_defined_types (
	character_maximum_length INT,
	character_set_name STRING,
	interval_type STRING,
	ordering_routine_catalog STRING,
	reference_type STRING,
	source_dtd_identifier STRING,
	data_type STRING,
	numeric_precision_radix INT,
	ordering_routine_name STRING,
	user_defined_type_catalog STRING,
	collation_catalog STRING,
	user_defined_type_schema STRING,
	character_set_schema STRING,
	collation_name STRING,
	datetime_precision INT,
	is_instantiable STRING,
	ordering_category STRING,
	ordering_routine_schema STRING,
	ref_dtd_identifier STRING,
	numeric_precision INT,
	numeric_scale INT,
	ordering_form STRING,
	user_defined_type_category STRING,
	collation_schema STRING,
	is_final STRING,
	character_octet_length INT,
	character_set_catalog STRING,
	interval_precision INT,
	user_defined_type_name STRING
)`

//InformationSchemaForeignServerOptions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaForeignServerOptions = `
CREATE TABLE information_schema.foreign_server_options (
	option_value STRING,
	foreign_server_catalog STRING,
	foreign_server_name STRING,
	option_name STRING
)`

//InformationSchemaAttributes is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaAttributes = `
CREATE TABLE information_schema.attributes (
	dtd_identifier STRING,
	interval_precision INT,
	maximum_cardinality INT,
	numeric_precision_radix INT,
	character_set_catalog STRING,
	character_set_name STRING,
	data_type STRING,
	scope_schema STRING,
	udt_catalog STRING,
	attribute_udt_schema STRING,
	character_set_schema STRING,
	scope_catalog STRING,
	attribute_udt_catalog STRING,
	character_maximum_length INT,
	is_derived_reference_attribute STRING,
	udt_schema STRING,
	ordinal_position INT,
	scope_name STRING,
	character_octet_length INT,
	datetime_precision INT,
	numeric_precision INT,
	interval_type STRING,
	attribute_default STRING,
	attribute_name STRING,
	attribute_udt_name STRING,
	is_nullable STRING,
	numeric_scale INT,
	udt_name STRING,
	collation_catalog STRING,
	collation_name STRING,
	collation_schema STRING
)`

//InformationSchemaUserMappingOptions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaUserMappingOptions = `
CREATE TABLE information_schema.user_mapping_options (
	authorization_identifier STRING,
	foreign_server_catalog STRING,
	foreign_server_name STRING,
	option_name STRING,
	option_value STRING
)`

//InformationSchemaColumnColumnUsage is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaColumnColumnUsage = `
CREATE TABLE information_schema.column_column_usage (
	column_name STRING,
	dependent_column STRING,
	table_catalog STRING,
	table_name STRING,
	table_schema STRING
)`

//InformationSchemaDomainUdtUsage is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaDomainUdtUsage = `
CREATE TABLE information_schema.domain_udt_usage (
	udt_catalog STRING,
	udt_name STRING,
	udt_schema STRING,
	domain_catalog STRING,
	domain_name STRING,
	domain_schema STRING
)`

//InformationSchemaDomains is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaDomains = `
CREATE TABLE information_schema.domains (
	scope_catalog STRING,
	data_type STRING,
	numeric_precision_radix INT,
	udt_catalog STRING,
	character_set_name STRING,
	maximum_cardinality INT,
	numeric_precision INT,
	scope_schema STRING,
	domain_name STRING,
	dtd_identifier STRING,
	interval_precision INT,
	udt_schema STRING,
	character_octet_length INT,
	domain_schema STRING,
	numeric_scale INT,
	collation_schema STRING,
	collation_name STRING,
	domain_default STRING,
	interval_type STRING,
	scope_name STRING,
	character_maximum_length INT,
	character_set_schema STRING,
	domain_catalog STRING,
	udt_name STRING,
	character_set_catalog STRING,
	datetime_precision INT,
	collation_catalog STRING
)`

//InformationSchemaViewRoutineUsage is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaViewRoutineUsage = `
CREATE TABLE information_schema.view_routine_usage (
	specific_catalog STRING,
	specific_name STRING,
	specific_schema STRING,
	table_catalog STRING,
	table_name STRING,
	table_schema STRING
)`

//InformationSchemaColumnDomainUsage is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaColumnDomainUsage = `
CREATE TABLE information_schema.column_domain_usage (
	domain_name STRING,
	domain_schema STRING,
	table_catalog STRING,
	table_name STRING,
	table_schema STRING,
	column_name STRING,
	domain_catalog STRING
)`

//InformationSchemaForeignDataWrappers is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaForeignDataWrappers = `
CREATE TABLE information_schema.foreign_data_wrappers (
	authorization_identifier STRING,
	foreign_data_wrapper_catalog STRING,
	foreign_data_wrapper_language STRING,
	foreign_data_wrapper_name STRING,
	library_name STRING
)`

//InformationSchemaSQLFeatures is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaSQLFeatures = `
CREATE TABLE information_schema.sql_features (
	is_verified_by STRING,
	sub_feature_id STRING,
	sub_feature_name STRING,
	comments STRING,
	feature_id STRING,
	feature_name STRING,
	is_supported STRING
)`

//InformationSchemaTransforms is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaTransforms = `
CREATE TABLE information_schema.transforms (
	specific_catalog STRING,
	specific_name STRING,
	specific_schema STRING,
	transform_type STRING,
	udt_catalog STRING,
	udt_name STRING,
	udt_schema STRING,
	group_name STRING
)`

//InformationSchemaSQLSizing is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaSQLSizing = `
CREATE TABLE information_schema.sql_sizing (
	comments STRING,
	sizing_id INT,
	sizing_name STRING,
	supported_value INT
)`

//InformationSchemaElementTypes is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaElementTypes = `
CREATE TABLE information_schema.element_types (
	udt_name STRING,
	character_maximum_length INT,
	collation_schema STRING,
	numeric_precision_radix INT,
	scope_name STRING,
	scope_schema STRING,
	character_octet_length INT,
	character_set_catalog STRING,
	collation_name STRING,
	object_schema STRING,
	character_set_name STRING,
	dtd_identifier STRING,
	interval_type STRING,
	udt_catalog STRING,
	collation_catalog STRING,
	maximum_cardinality INT,
	character_set_schema STRING,
	interval_precision INT,
	udt_schema STRING,
	datetime_precision INT,
	numeric_scale INT,
	object_type STRING,
	collection_type_identifier STRING,
	data_type STRING,
	numeric_precision INT,
	domain_default STRING,
	object_catalog STRING,
	object_name STRING,
	scope_catalog STRING
)`

//InformationSchemaRoutinePrivileges is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaRoutinePrivileges = `
CREATE TABLE information_schema.routine_privileges (
	grantor STRING,
	routine_name STRING,
	specific_catalog STRING,
	specific_name STRING,
	specific_schema STRING,
	grantee STRING,
	is_grantable STRING,
	privilege_type STRING,
	routine_catalog STRING,
	routine_schema STRING
)`

//InformationSchemaConstraintTableUsage is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaConstraintTableUsage = `
CREATE TABLE information_schema.constraint_table_usage (
	table_name STRING,
	table_schema STRING,
	constraint_catalog STRING,
	constraint_name STRING,
	constraint_schema STRING,
	table_catalog STRING
)`

//InformationSchemaColumnOptions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaColumnOptions = `
CREATE TABLE information_schema.column_options (
	column_name STRING,
	option_name STRING,
	option_value STRING,
	table_catalog STRING,
	table_name STRING,
	table_schema STRING
)`

//InformationSchemaForeignServers is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaForeignServers = `
CREATE TABLE information_schema.foreign_servers (
	authorization_identifier STRING,
	foreign_data_wrapper_catalog STRING,
	foreign_data_wrapper_name STRING,
	foreign_server_catalog STRING,
	foreign_server_name STRING,
	foreign_server_type STRING,
	foreign_server_version STRING
)`

//InformationSchemaTriggers is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaTriggers = `
CREATE TABLE information_schema.triggers (
	action_timing STRING,
	event_manipulation STRING,
	trigger_catalog STRING,
	trigger_schema STRING,
	event_object_table STRING,
	trigger_name STRING,
	action_condition STRING,
	action_reference_old_row STRING,
	action_statement STRING,
	created TIMESTAMPTZ,
	event_object_catalog STRING,
	event_object_schema STRING,
	action_order INT,
	action_orientation STRING,
	action_reference_new_row STRING,
	action_reference_new_table STRING,
	action_reference_old_table STRING
)`

//InformationSchemaDomainConstraints is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaDomainConstraints = `
CREATE TABLE information_schema.domain_constraints (
	initially_deferred STRING,
	is_deferrable STRING,
	constraint_catalog STRING,
	constraint_name STRING,
	constraint_schema STRING,
	domain_catalog STRING,
	domain_name STRING,
	domain_schema STRING
)`

//InformationSchemaUdtPrivileges is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaUdtPrivileges = `
CREATE TABLE information_schema.udt_privileges (
	grantor STRING,
	is_grantable STRING,
	privilege_type STRING,
	udt_catalog STRING,
	udt_name STRING,
	udt_schema STRING,
	grantee STRING
)`

//InformationSchemaCheckConstraintRoutineUsage is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaCheckConstraintRoutineUsage = `
CREATE TABLE information_schema.check_constraint_routine_usage (
	constraint_catalog STRING,
	constraint_name STRING,
	constraint_schema STRING,
	specific_catalog STRING,
	specific_name STRING,
	specific_schema STRING
)`

//InformationSchemaRoleColumnGrants is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaRoleColumnGrants = `
CREATE TABLE information_schema.role_column_grants (
	table_schema STRING,
	column_name STRING,
	grantee STRING,
	grantor STRING,
	is_grantable STRING,
	privilege_type STRING,
	table_catalog STRING,
	table_name STRING
)`

//InformationSchemaTriggeredUpdateColumns is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaTriggeredUpdateColumns = `
CREATE TABLE information_schema.triggered_update_columns (
	event_object_catalog STRING,
	event_object_column STRING,
	event_object_schema STRING,
	event_object_table STRING,
	trigger_catalog STRING,
	trigger_name STRING,
	trigger_schema STRING
)`

//InformationSchemaForeignTables is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaForeignTables = `
CREATE TABLE information_schema.foreign_tables (
	foreign_server_catalog STRING,
	foreign_server_name STRING,
	foreign_table_catalog STRING,
	foreign_table_name STRING,
	foreign_table_schema STRING
)`

//InformationSchemaUsagePrivileges is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaUsagePrivileges = `
CREATE TABLE information_schema.usage_privileges (
	is_grantable STRING,
	object_catalog STRING,
	object_name STRING,
	object_schema STRING,
	object_type STRING,
	privilege_type STRING,
	grantee STRING,
	grantor STRING
)`

//InformationSchemaUserMappings is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaUserMappings = `
CREATE TABLE information_schema.user_mappings (
	authorization_identifier STRING,
	foreign_server_catalog STRING,
	foreign_server_name STRING
)`

//InformationSchemaRoleUdtGrants is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaRoleUdtGrants = `
CREATE TABLE information_schema.role_udt_grants (
	udt_schema STRING,
	grantee STRING,
	grantor STRING,
	is_grantable STRING,
	privilege_type STRING,
	udt_catalog STRING,
	udt_name STRING
)`

//InformationSchemaViewColumnUsage is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaViewColumnUsage = `
CREATE TABLE information_schema.view_column_usage (
	column_name STRING,
	table_catalog STRING,
	table_name STRING,
	table_schema STRING,
	view_catalog STRING,
	view_name STRING,
	view_schema STRING
)`

//InformationSchemaForeignDataWrapperOptions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaForeignDataWrapperOptions = `
CREATE TABLE information_schema.foreign_data_wrapper_options (
	option_name STRING,
	option_value STRING,
	foreign_data_wrapper_catalog STRING,
	foreign_data_wrapper_name STRING
)`

//InformationSchemaViewTableUsage is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaViewTableUsage = `
CREATE TABLE information_schema.view_table_usage (
	view_catalog STRING,
	view_name STRING,
	view_schema STRING,
	table_catalog STRING,
	table_name STRING,
	table_schema STRING
)`

//InformationSchemaSQLParts is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaSQLParts = `
CREATE TABLE information_schema.sql_parts (
	comments STRING,
	feature_id STRING,
	feature_name STRING,
	is_supported STRING,
	is_verified_by STRING
)`

//InformationSchemaRoleUsageGrants is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaRoleUsageGrants = `
CREATE TABLE information_schema.role_usage_grants (
	object_name STRING,
	object_schema STRING,
	object_type STRING,
	privilege_type STRING,
	grantee STRING,
	grantor STRING,
	is_grantable STRING,
	object_catalog STRING
)`

//InformationSchemaTablespaces is an empty table in the pg_catalog that is not implemented yet
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

//InformationSchemaStSpatialReferenceSystems is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaStSpatialReferenceSystems = `
CREATE TABLE information_schema.st_spatial_reference_systems (
	srs_id INT,
	srs_name STRING,
	definition STRING,
	description STRING,
	organization STRING,
	organization_coordsys_id INT
)`

//InformationSchemaProcesslist is an empty table in the pg_catalog that is not implemented yet
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

//InformationSchemaTablesExtensions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaTablesExtensions = `
CREATE TABLE information_schema.tables_extensions (
	engine_attribute STRING,
	secondary_engine_attribute STRING,
	table_catalog STRING,
	table_name STRING,
	table_schema STRING
)`

//InformationSchemaProfiling is an empty table in the pg_catalog that is not implemented yet
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

//InformationSchemaColumnStatistics is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaColumnStatistics = `
CREATE TABLE information_schema.column_statistics (
	column_name STRING,
	histogram STRING,
	schema_name STRING,
	table_name STRING
)`

//InformationSchemaResourceGroups is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaResourceGroups = `
CREATE TABLE information_schema.resource_groups (
	resource_group_enabled INT2,
	resource_group_name STRING,
	resource_group_type STRING,
	thread_priority INT,
	vcpu_ids BYTES
)`

//InformationSchemaPartitions is an empty table in the pg_catalog that is not implemented yet
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

//InformationSchemaTablespacesExtensions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaTablespacesExtensions = `
CREATE TABLE information_schema.tablespaces_extensions (
	engine_attribute STRING,
	tablespace_name STRING
)`

//InformationSchemaSchemataExtensions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaSchemataExtensions = `
CREATE TABLE information_schema.schemata_extensions (
	catalog_name STRING,
	options STRING,
	schema_name STRING
)`

//InformationSchemaStUnitsOfMeasure is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaStUnitsOfMeasure = `
CREATE TABLE information_schema.st_units_of_measure (
	conversion_factor FLOAT,
	description STRING,
	unit_name STRING,
	unit_type STRING
)`

//InformationSchemaFiles is an empty table in the pg_catalog that is not implemented yet
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

//InformationSchemaEngines is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaEngines = `
CREATE TABLE information_schema.engines (
	support STRING,
	transactions STRING,
	xa STRING,
	comment STRING,
	engine STRING,
	savepoints STRING
)`

//InformationSchemaColumnsExtensions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaColumnsExtensions = `
CREATE TABLE information_schema.columns_extensions (
	engine_attribute STRING,
	secondary_engine_attribute STRING,
	table_catalog STRING,
	table_name STRING,
	table_schema STRING,
	column_name STRING
)`

//InformationSchemaStGeometryColumns is an empty table in the pg_catalog that is not implemented yet
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

//InformationSchemaPlugins is an empty table in the pg_catalog that is not implemented yet
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

//InformationSchemaEvents is an empty table in the pg_catalog that is not implemented yet
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

//InformationSchemaUserAttributes is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaUserAttributes = `
CREATE TABLE information_schema.user_attributes (
	attribute STRING,
	host STRING,
	"user" STRING
)`

//InformationSchemaKeywords is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaKeywords = `
CREATE TABLE information_schema.keywords (
	word STRING,
	reserved INT
)`

//InformationSchemaOptimizerTrace is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaOptimizerTrace = `
CREATE TABLE information_schema.optimizer_trace (
	insufficient_privileges INT2,
	missing_bytes_beyond_max_mem_size INT,
	query STRING,
	trace STRING
)`

//InformationSchemaTableConstraintsExtensions is an empty table in the pg_catalog that is not implemented yet
const InformationSchemaTableConstraintsExtensions = `
CREATE TABLE information_schema.table_constraints_extensions (
	constraint_catalog STRING,
	constraint_name STRING,
	constraint_schema STRING,
	engine_attribute STRING,
	secondary_engine_attribute STRING,
	table_name STRING
)`
