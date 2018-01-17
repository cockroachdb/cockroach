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

// ShowColumns of a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW COLUMNS statement.
//          mysql only returns columns you have privileges on.
func (p *planner) ShowColumns(ctx context.Context, n *tree.ShowColumns) (planNode, error) {
	const getColumnsQuery = `
				SELECT
					COLUMN_NAME AS "Field",
					DATA_TYPE AS "Type",
					(IS_NULLABLE != 'NO') AS "Null",
					COLUMN_DEFAULT AS "Default",
					IF(inames[1] IS NULL, ARRAY[]:::STRING[], inames) AS "Indices"
				FROM
					(SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION,
									ARRAY_AGG(INDEX_NAME) AS inames
						 FROM
								 (SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION
										FROM "".information_schema.columns
									 WHERE TABLE_SCHEMA=%[1]s AND TABLE_NAME=%[2]s)
								 LEFT OUTER JOIN
								 (SELECT COLUMN_NAME, INDEX_NAME
										FROM "".information_schema.statistics
									 WHERE TABLE_SCHEMA=%[1]s AND TABLE_NAME=%[2]s)
								 USING(COLUMN_NAME)
						GROUP BY COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION
					 )
				ORDER BY ORDINAL_POSITION`
	return p.showTableDetails(ctx, "SHOW COLUMNS", n.Table, getColumnsQuery)
}
