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

func (d *delegator) delegateShowDatabaseIndexes(
	n *tree.ShowDatabaseIndexes,
) (tree.Statement, error) {
	getAllIndexesQuery := `
SELECT
	table_name,
	index_name,
	non_unique::BOOL,
	seq_in_index,
	column_name,
	direction,
	storing::BOOL,
	implicit::BOOL`

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

	return parse(fmt.Sprintf(getAllIndexesQuery, n.Database.String()))
}
