// Copyright 2019 The Cockroach Authors.
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

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowCreate(n *tree.ShowCreate) (tree.Statement, error) {
	const showCreateQuery = `
     SELECT %[3]s AS table_name,
            create_statement
       FROM %[4]s.crdb_internal.create_statements
      WHERE (database_name IS NULL OR database_name = %[1]s)
        AND schema_name = %[5]s
        AND descriptor_name = %[2]s`

	return d.showTableDetails(&n.Name, showCreateQuery)
}

func (d *delegator) delegateShowIndex(n *tree.ShowIndex) (tree.Statement, error) {
	const getIndexesQuery = `
    SELECT table_name,
           index_name,
           non_unique::BOOL,
           seq_in_index,
           column_name,
           direction,
           storing::BOOL,
           implicit::BOOL
    FROM %[4]s.information_schema.statistics
    WHERE table_catalog=%[1]s AND table_schema=%[5]s AND table_name=%[2]s`

	return d.showTableDetails(&n.Table, getIndexesQuery)
}

func (d *delegator) delegateShowColumns(n *tree.ShowColumns) (tree.Statement, error) {
	const getColumnsQuery = `
SELECT
  column_name AS column_name,
  crdb_sql_type AS data_type,
  is_nullable::BOOL,
  column_default,
  generation_expression,
  IF(inames[1] IS NULL, ARRAY[]:::STRING[], inames) AS indices,
  is_hidden::BOOL
FROM
  (SELECT column_name, crdb_sql_type, is_nullable, column_default, generation_expression,
	        ordinal_position, is_hidden, array_agg(index_name) AS inames
     FROM
         (SELECT column_name, crdb_sql_type, is_nullable, column_default, generation_expression,
				         ordinal_position, is_hidden
            FROM %[4]s.information_schema.columns
           WHERE (length(%[1]s)=0 OR table_catalog=%[1]s) AND table_schema=%[5]s AND table_name=%[2]s)
         LEFT OUTER JOIN
         (SELECT column_name, index_name
            FROM %[4]s.information_schema.statistics
           WHERE (length(%[1]s)=0 OR table_catalog=%[1]s) AND table_schema=%[5]s AND table_name=%[2]s)
         USING(column_name)
    GROUP BY column_name, crdb_sql_type, is_nullable, column_default, generation_expression,
		         ordinal_position, is_hidden
   )
ORDER BY ordinal_position`

	return d.showTableDetails(&n.Table, getColumnsQuery)
}

func (d *delegator) delegateShowConstraints(n *tree.ShowConstraints) (tree.Statement, error) {
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
    ORDER BY 1, 2`

	return d.showTableDetails(&n.Table, getConstraintsQuery)
}

// showTableDetails returns the AST of a query which extracts information about
// the given table using the given query patterns in SQL. The query pattern must
// accept the following formatting parameters:
//   %[1]s the database name as SQL string literal.
//   %[2]s the unqualified table name as SQL string literal.
//   %[3]s the given table name as SQL string literal.
//   %[4]s the database name as SQL identifier.
//   %[5]s the schema name as SQL string literal.
func (d *delegator) showTableDetails(tn *tree.TableName, query string) (tree.Statement, error) {
	// We avoid the cache so that we can observe the details without
	// taking a lease, like other SHOW commands.
	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, tn)
	if err != nil {
		return nil, err
	}
	if err := d.catalog.CheckAnyPrivilege(d.ctx, dataSource); err != nil {
		return nil, err
	}

	fullQuery := fmt.Sprintf(query,
		lex.EscapeSQLString(resName.Catalog()),
		lex.EscapeSQLString(resName.Table()),
		lex.EscapeSQLString(resName.String()),
		resName.CatalogName.String(), // note: CatalogName.String() != Catalog()
		lex.EscapeSQLString(resName.Schema()),
	)

	return parse(fullQuery)
}
