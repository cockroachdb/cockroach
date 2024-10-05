// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// delegateShowDatabaseIndexes implements SHOW INDEX FROM DATABASE, SHOW INDEXES
// FROM DATABASE, SHOW KEYS FROM DATABASE which returns all the indexes in the
// given or current database.
func (d *delegator) delegateShowDatabaseIndexes(
	n *tree.ShowDatabaseIndexes,
) (tree.Statement, error) {
	name, err := d.getSpecifiedOrCurrentDatabase(n.Database)
	if err != nil {
		return nil, err
	}

	getAllIndexesQuery := `
SELECT
	table_name,
	index_name,
	index_schema,
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
		getAllIndexesQuery += `,
	obj_description(c.oid) AS comment`
	}

	getAllIndexesQuery += `
FROM
    %[1]s.information_schema.statistics AS s
    JOIN %[1]s.pg_catalog.pg_class c ON c.relname = s.index_name
    JOIN %[1]s.pg_catalog.pg_class c_table ON c_table.relname = s.table_name
    JOIN %[1]s.pg_catalog.pg_namespace n ON c.relnamespace = n.oid AND c_table.relnamespace = n.oid AND n.nspname = s.index_schema
    JOIN %[1]s.pg_catalog.pg_index i ON i.indexrelid = c.oid AND i.indrelid = c_table.oid
`

	getAllIndexesQuery += `
ORDER BY 1, 2, 4`

	return d.parse(fmt.Sprintf(getAllIndexesQuery, name.String()))
}
