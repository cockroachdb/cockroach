// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowDatabaseIndexes(
	n *tree.ShowDatabaseIndexes,
) (tree.Statement, error) {
	const getAllIndexesQuery = `
    SELECT table_name,
           index_name,
           non_unique::BOOL,
           seq_in_index,
           column_name,
           direction,
           storing::BOOL,
           implicit::BOOL
      FROM %s.information_schema.statistics`
	return parse(fmt.Sprintf(getAllIndexesQuery, n.Database.String()))
}
