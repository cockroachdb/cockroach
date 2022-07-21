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
	non_unique::BOOL,
	seq_in_index,
	column_name,
	direction,
	storing::BOOL,
	implicit::BOOL,
	is_visible::BOOL AS visible`

	if n.WithComment {
		getAllIndexesQuery += `,
	obj_description(pg_class.oid) AS comment`
	}

	getAllIndexesQuery += `
FROM
	%s.information_schema.statistics`

	if n.WithComment {
		getAllIndexesQuery += `
	LEFT JOIN pg_class ON
		statistics.index_name = pg_class.relname`
	}

	getAllIndexesQuery += `
ORDER BY 1, 2, 4`

	return parse(fmt.Sprintf(getAllIndexesQuery, name.String()))
}
