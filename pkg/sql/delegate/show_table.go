// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

func (d *delegator) delegateShowCreate(n *tree.ShowCreate) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Create)

	switch n.Mode {
	case tree.ShowCreateModeTable, tree.ShowCreateModeView, tree.ShowCreateModeSequence:
		return d.delegateShowCreateTable(n)
	case tree.ShowCreateModeDatabase:
		return d.delegateShowCreateDatabase(n)
	case tree.ShowCreateModeIndexes, tree.ShowCreateModeSecondaryIndexes:
		return d.delegateShowCreateIndexes(n)
	default:
		return nil, errors.Newf("unknown show create mode: %d", n.Mode)
	}
}

func (d *delegator) delegateShowCreateDatabase(n *tree.ShowCreate) (tree.Statement, error) {
	const showCreateQuery = `
SELECT
	name AS database_name,
	create_statement
FROM crdb_internal.databases
WHERE name = %s
;
`

	// Checking if the database exists before running the sql.
	_, err := d.getSpecifiedOrCurrentDatabase(tree.Name(n.Name.Object()))
	if err != nil {
		return nil, err
	}

	return d.parse(fmt.Sprintf(showCreateQuery, lexbase.EscapeSQLString(n.Name.Object())))
}

func (d *delegator) delegateShowCreateTable(n *tree.ShowCreate) (tree.Statement, error) {
	createField := "create_statement"
	switch n.FmtOpt {
	case tree.ShowCreateFormatOptionRedactedValues:
		createField = "crdb_internal.redact(create_redactable)"
	case tree.ShowCreateFormatOptionIgnoreFKs:
		createField = "create_nofks"
	}

	showCreateQuery := `
WITH zone_configs AS (
		SELECT
			string_agg(
				raw_config_sql,
				e';\n'
				ORDER BY partition_name, index_name
			) AS raw,
			string_agg(
				crdb_internal.filter_multiregion_fields_from_zone_config_sql(raw_config_sql),
				e';\n'
				ORDER BY partition_name, index_name
			) AS mr
		FROM crdb_internal.zones
    WHERE database_name = %[1]s
    AND schema_name = %[5]s
    AND table_name = %[2]s
    AND raw_config_yaml IS NOT NULL
    AND raw_config_sql IS NOT NULL
)
SELECT
    %[3]s AS table_name,
    concat(` + createField + `,
        CASE
				WHEN is_multi_region THEN
					CASE
						WHEN (SELECT mr FROM zone_configs) IS NULL THEN NULL
						ELSE concat(e';\n', (SELECT mr FROM zone_configs))
					END
        WHEN (SELECT raw FROM zone_configs) IS NOT NULL THEN
					concat(e';\n', (SELECT raw FROM zone_configs))
        WHEN NOT has_partitions
          THEN NULL
				ELSE
					e'\n-- Warning: Partitioned table with no zone configurations.\n'
        END
    ) AS create_statement
FROM
    %[4]s.crdb_internal.create_statements
WHERE
    descriptor_id = %[6]d
ORDER BY
    1, 2;`

	return d.showTableDetails(n.Name, showCreateQuery)
}

func (d *delegator) delegateShowCreateIndexes(n *tree.ShowCreate) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Indexes)

	showCreateIndexesQuery := `
SELECT
	index_name,
	create_statement
FROM %[4]s.crdb_internal.table_indexes
WHERE descriptor_id = %[3]s::regclass::int`

	// Add additional conditions based on desired index types.
	switch n.Mode {
	// This case is intentionally empty since it should return all types of indexes.
	case tree.ShowCreateModeIndexes:
	case tree.ShowCreateModeSecondaryIndexes:
		showCreateIndexesQuery += `
	AND index_type != 'primary'`
	default:
		return nil, errors.Newf("unknown show create indexes mode: %d", n.Mode)
	}

	return d.showTableDetails(n.Name, showCreateIndexesQuery)
}

// delegateShowIndexes implements SHOW INDEX FROM, SHOW INDEXES FROM, SHOW KEYS
// FROM which returns all the indexes in the given table.
func (d *delegator) delegateShowIndexes(n *tree.ShowIndexes) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Indexes)
	getIndexesQuery := `
SELECT
    s.table_name,
    s.index_name,
    non_unique::BOOL,
    seq_in_index,
    column_name,
    CASE
      -- array_positions(i.indkey, 0) returns the 1-based indexes of the indkey elements that are 0.
      -- array_position(arr, seq_in_index) returns the 1-based index of the value seq_in_index in arr.
      -- indkey is an int2vector, which is accessed with 0-based indexes.
      -- indexprs is a string[], which is accessed with 1-based indexes.
      -- To put this all together, for the k-th 0 value inside of indkey, this will find the k-th indexpr.
      WHEN i.indkey[seq_in_index-1] = 0 THEN (indexprs::STRING[])[array_position(array_positions(i.indkey, 0), seq_in_index)]
      ELSE column_name
    END AS definition,
    direction,
    storing::BOOL,
    implicit::BOOL,
    is_visible::BOOL AS visible,
    visibility`

	if n.WithComment {
		getIndexesQuery += `,
    obj_description(c.oid) AS comment`
	}

	getIndexesQuery += `
FROM
    %[4]s.information_schema.statistics AS s
    JOIN %[4]s.pg_catalog.pg_class c ON c.relname = s.index_name
    JOIN %[4]s.pg_catalog.pg_class c_table ON c_table.relname = s.table_name
    JOIN %[4]s.pg_catalog.pg_namespace n ON c.relnamespace = n.oid AND c_table.relnamespace = n.oid AND n.nspname = s.index_schema
    JOIN %[4]s.pg_catalog.pg_index i ON i.indexrelid = c.oid AND i.indrelid = c_table.oid
`

	getIndexesQuery += `
WHERE
    table_catalog=%[1]s
    AND table_schema=%[5]s
    AND table_name=%[2]s
ORDER BY
    1, 2, 4;`

	return d.showTableDetails(n.Table, getIndexesQuery)
}

func (d *delegator) delegateShowColumns(n *tree.ShowColumns) (tree.Statement, error) {
	getColumnsQuery := `
SELECT
    column_name AS column_name,
    crdb_sql_type AS data_type,
    is_nullable::BOOL,
    column_default,
    generation_expression,
    IF(inames[1] IS NULL, ARRAY[]:::STRING[], inames) AS indices,
    is_hidden::BOOL`

	if n.WithComment {
		getColumnsQuery += `,
    col_description(%[6]d, attnum) AS comment`
	}

	getColumnsQuery += `
FROM
    (
        SELECT column_name, crdb_sql_type, is_nullable, column_default, generation_expression,
            ordinal_position, is_hidden, array_agg(index_name ORDER BY index_name) AS inames
        FROM
        (
            SELECT column_name, crdb_sql_type, is_nullable, column_default, generation_expression,
                ordinal_position, is_hidden
            FROM %[4]s.information_schema.columns
            WHERE (length(%[1]s)=0 OR table_catalog=%[1]s) AND table_schema=%[5]s AND table_name=%[2]s
        )
        LEFT OUTER JOIN
        (
            SELECT column_name, index_name
            FROM %[4]s.information_schema.statistics
            WHERE (length(%[1]s)=0 OR table_catalog=%[1]s) AND table_schema=%[5]s AND table_name=%[2]s
        )
        USING(column_name)
        GROUP BY column_name, crdb_sql_type, is_nullable, column_default, generation_expression,
            ordinal_position, is_hidden
   )`

	if n.WithComment {
		getColumnsQuery += `
    LEFT OUTER JOIN pg_attribute
        ON column_name = pg_attribute.attname
        AND attrelid = %[6]d`
	}

	getColumnsQuery += `
ORDER BY
    ordinal_position, 1, 2, 3, 4, 5, 6, 7;`

	return d.showTableDetails(n.Table, getColumnsQuery)
}

func (d *delegator) delegateShowConstraints(n *tree.ShowConstraints) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Constraints)
	getConstraintsQuery := `
    SELECT
        t.relname AS table_name,
        c.conname AS constraint_name,
        CASE c.contype
           WHEN 'p' THEN 'PRIMARY KEY'
           WHEN 'u' THEN 'UNIQUE'
           WHEN 'c' THEN 'CHECK'
           WHEN 'f' THEN 'FOREIGN KEY'
           ELSE c.contype::TEXT
        END AS constraint_type,
        c.condef AS details,
        c.convalidated AS validated`

	if n.WithComment {
		getConstraintsQuery += `,
	obj_description(c.oid) AS comment`
	}
	getConstraintsQuery += `
FROM
       %[4]s.pg_catalog.pg_class t,
       %[4]s.pg_catalog.pg_namespace n,
       %[4]s.pg_catalog.pg_constraint c
    WHERE t.relname = %[2]s
      AND n.nspname = %[5]s AND t.relnamespace = n.oid
      AND t.oid = c.conrelid
    ORDER BY 1, 2, 3, 4, 5`

	return d.showTableDetails(n.Table, getConstraintsQuery)
}

func (d *delegator) delegateShowCreateAllTables() (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Create)

	const showCreateAllTablesQuery = `
	SELECT crdb_internal.show_create_all_tables(%[1]s) AS create_statement;
`
	databaseLiteral := d.evalCtx.SessionData().Database

	query := fmt.Sprintf(showCreateAllTablesQuery,
		lexbase.EscapeSQLString(databaseLiteral),
	)

	return d.parse(query)
}

// showTableDetails returns the AST of a query which extracts information about
// the given table using the given query patterns in SQL. The query pattern must
// accept the following formatting parameters:
//
//	%[1]s the database name as SQL string literal.
//	%[2]s the unqualified table name as SQL string literal.
//	%[3]s the given table name as SQL string literal.
//	%[4]s the database name as SQL identifier.
//	%[5]s the schema name as SQL string literal.
//	%[6]s the table ID.
func (d *delegator) showTableDetails(
	name *tree.UnresolvedObjectName, query string,
) (tree.Statement, error) {

	dataSource, resName, err := d.resolveAndModifyUnresolvedObjectName(name)
	if err != nil {
		return nil, err
	}
	fullQuery := fmt.Sprintf(query,
		lexbase.EscapeSQLString(resName.Catalog()),
		lexbase.EscapeSQLString(resName.Table()),
		lexbase.EscapeSQLString(resName.String()),
		resName.CatalogName.String(), // note: CatalogName.String() != Catalog()
		lexbase.EscapeSQLString(resName.Schema()),
		dataSource.PostgresDescriptorID(),
	)

	return d.parse(fullQuery)
}
