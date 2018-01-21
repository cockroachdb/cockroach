// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ShowIndex returns all the indexes for a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW INDEXES statement.
//          mysql requires some privilege for any column.
func (p *planner) ShowIndex(ctx context.Context, n *tree.ShowIndex) (planNode, error) {
	const getIndexes = `
				SELECT
					TABLE_NAME AS "Table",
					INDEX_NAME AS "Name",
					NOT NON_UNIQUE::BOOL AS "Unique",
					SEQ_IN_INDEX AS "Seq",
					COLUMN_NAME AS "Column",
					DIRECTION AS "Direction",
					STORING::BOOL AS "Storing",
					IMPLICIT::BOOL AS "Implicit"
				FROM "".information_schema.statistics
				WHERE TABLE_SCHEMA=%[1]s AND TABLE_NAME=%[2]s`
	return p.showTableDetails(ctx, "SHOW INDEX", n.Table, getIndexes)
}
