// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

func (d *delegator) delegateShowIndexes(n *tree.ShowIndexes) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Indexes)
	getIndexesQuery := `
SELECT
    s.table_name,
    s.index_name,
    non_unique::BOOL,
    seq_in_index,
    column_name,
    direction,
    storing::BOOL,
    implicit::BOOL`

	if n.WithComment {
		getIndexesQuery += `,
    obj_description(pg_indexes.crdb_oid) AS comment`
	}

	getIndexesQuery += `
FROM
    %[4]s.information_schema.statistics AS s`

	if n.WithComment {
		getIndexesQuery += `
    LEFT JOIN pg_indexes ON
        pg_indexes.tablename = s.table_name AND
        pg_indexes.indexname = s.index_name`
	}

	getIndexesQuery += `
WHERE
    table_catalog=%[1]s
    AND table_schema=%[5]s
    AND table_name=%[2]s
ORDER BY
    1, 2, 3, 4, 5, 6, 7, 8;`

	return d.populateShowCreateObjectQueryDetails(n.Table, getIndexesQuery)
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

	return d.populateShowCreateObjectQueryDetails(n.Table, getColumnsQuery)
}

func (d *delegator) delegateShowConstraints(n *tree.ShowConstraints) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Constraints)
	const getConstraintsQuery = `
    SELECT
        t.relname AS table_name,
        c.conname AS constraint_name,
        CASE c.contype
           WHEN 'p' THEN 'PRIMARY KEY'
           WHEN 'u' THEN 'UNIQUE'
           WHEN 'c' THEN 'CHECK'
           WHEN 'f' THEN 'FOREIGN KEY'
           ELSE c.contype
        END AS constraint_type,
        c.condef AS details,
        c.convalidated AS validated
    FROM
       %[4]s.pg_catalog.pg_class t,
       %[4]s.pg_catalog.pg_namespace n,
       %[4]s.pg_catalog.pg_constraint c
    WHERE t.relname = %[2]s
      AND n.nspname = %[5]s AND t.relnamespace = n.oid
      AND t.oid = c.conrelid
    ORDER BY 1, 2, 3, 4, 5`

	return d.populateShowCreateObjectQueryDetails(n.Table, getConstraintsQuery)
}

func (d *delegator) delegateShowCreateTables(n *tree.ShowCreateTable) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Create)

	const showCreateQuery = `
WITH zone_configs AS (
    SELECT string_agg(raw_config_sql, e';\n') FROM crdb_internal.zones
    WHERE database_name = %[1]s
    AND table_name IN (%[2]s)
    AND raw_config_yaml IS NOT NULL
    AND raw_config_sql IS NOT NULL
)
SELECT
    descriptor_name AS table_name,
    concat(concat(create_statement, ';'),
        CASE
        WHEN NOT has_partitions
            THEN NULL
        WHEN (SELECT * FROM zone_configs) IS NULL
            THEN e'\n-- Warning: Partitioned table with no zone configurations.'
        ELSE concat(e'\n', (SELECT * FROM zone_configs))
        END
    ) AS create_statement
FROM
    %[3]s.crdb_internal.create_statements
WHERE
    descriptor_id IN (%[4]s)
ORDER BY
    1, 2;`

	return d.populateShowCreateTableQueryDetails(n.Names, showCreateQuery)
}

func (d *delegator) delegateShowCreateAllTables() (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Create)

	const showCreateAllTablesQuery = `
	SELECT crdb_internal.show_create_all_tables(%[1]s);
`
	databaseLiteral := d.evalCtx.SessionData.Database

	query := fmt.Sprintf(showCreateAllTablesQuery,
		lex.EscapeSQLString(databaseLiteral),
	)

	return parse(query)
}

// populateShowCreateObjectQueryDetails returns the AST of a query which extracts
// information about the given table using the given query patterns in SQL.
// populateShowCreateObjectQueryDetails is used for SHOW CREATE INDEX / COLUMNS/
// CONSTRAINTS.
// The query pattern must accept the following formatting parameters:
//   %[1]s the database name as SQL string literal.
//   %[2]s the unqualified table name as SQL string literal.
//   %[3]s the given table name as SQL string literal.
//   %[4]s the database name as SQL identifier.
//   %[5]s the schema name as SQL string literal.
//   %[6]d the table ID.
func (d *delegator) populateShowCreateObjectQueryDetails(
	name *tree.UnresolvedObjectName, query string,
) (tree.Statement, error) {

	resName, id, err := d.getNameAndID(name)
	if err != nil {
		return nil, err
	}

	fullQuery := fmt.Sprintf(query,
		lex.EscapeSQLString(resName.Catalog()),
		lex.EscapeSQLString(resName.Table()),
		lex.EscapeSQLString(resName.String()),
		resName.CatalogName.String(), // note: CatalogName.String() != Catalog()
		lex.EscapeSQLString(resName.Schema()),
		id,
	)

	return parse(fullQuery)
}

// populateShowCreateTableQueryDetails returns the AST of a query which
// extracts information about the given table using the given query patterns
// in SQL.
// populateShowCreateTableQueryDetails is used for SHOW CREATE TABLE.
// The query pattern must accept the following formatting parameters:
//   %[1]s the database name as SQL string literal.
//   %[2]s the unqualified table name(s) as SQL string literal.
//   %[3]s the database name as SQL identifier.
//   %[4]s the table ID(s)
func (d *delegator) populateShowCreateTableQueryDetails(
	names tree.TableNames, query string,
) (tree.Statement, error) {
	var tableNameLiterals []string
	var descriptorIDLiterals []string
	for _, name := range names {
		resName, id, err := d.getNameAndID(name.ToUnresolvedObjectName())
		if err != nil {
			return nil, err
		}

		tableNameLiterals = append(tableNameLiterals, resName.String())
		descriptorIDLiterals = append(descriptorIDLiterals, strconv.Itoa(int(id)))
	}

	tableNames := strings.Join(tableNameLiterals, ",")
	descriptorIDs := strings.Join(descriptorIDLiterals, ",")

	databaseLiteral := d.evalCtx.SessionData.Database

	var buf bytes.Buffer
	lexbase.EncodeEscapedSQLIdent(&buf, databaseLiteral)
	// databaseIdentifier is used in the query, double quote are needed
	// in the case where the database name includes spaces.
	databaseIdentifier := buf.String()

	fullQuery := fmt.Sprintf(query,
		lex.EscapeSQLString(databaseLiteral),
		lex.EscapeSQLString(tableNames),
		databaseIdentifier,
		descriptorIDs,
	)

	return parse(fullQuery)
}

// getNameAndID takes a table name in the form of a tree.UnresolvedObjectName
// performs a lookup for the table and returns the resolved name and id of the
// the table.
func (d *delegator) getNameAndID(
	name *tree.UnresolvedObjectName,
) (cat.DataSourceName, cat.StableID, error) {
	// We avoid the cache so that we can observe the details without
	// taking a lease, like other SHOW commands.
	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	tn := name.ToTableName()
	dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, &tn)
	if err != nil {
		return cat.DataSourceName{}, 0, err
	}
	if err := d.catalog.CheckAnyPrivilege(d.ctx, dataSource); err != nil {
		return cat.DataSourceName{}, 0, err
	}
	return resName, dataSource.PostgresDescriptorID(), nil
}
