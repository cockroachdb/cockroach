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
					column_name AS "Field",
					data_type AS "Type",
					(is_nullable != 'NO') AS "Null",
					column_default AS "Default",
					IF(inames[1] IS NULL, ARRAY[]:::STRING[], inames) AS "Indices"
				FROM
					(SELECT column_name, data_type, is_nullable, column_default, ordinal_position,
									array_agg(index_name) AS inames
						 FROM
								 (SELECT column_name, data_type, is_nullable, column_default, ordinal_position
										FROM %[4]s.information_schema.columns
									 WHERE (length(%[1]s)=0 OR table_catalog=%[1]s) AND table_schema=%[5]s AND table_name=%[2]s)
								 LEFT OUTER JOIN
								 (SELECT column_name, index_name
										FROM %[4]s.information_schema.statistics
									 WHERE (length(%[1]s)=0 OR table_catalog=%[1]s) AND table_schema=%[5]s AND table_name=%[2]s)
								 USING(column_name)
						GROUP BY column_name, data_type, is_nullable, column_default, ordinal_position
					 )
				ORDER BY ordinal_position`
	return p.showTableDetails(ctx, "SHOW COLUMNS", n.Table, getColumnsQuery)
}
