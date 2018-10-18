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

// ShowConstraints returns all the constraints for a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW CONSTRAINTS statement.
//          mysql requires some privilege for any column.
func (p *planner) ShowConstraints(ctx context.Context, n *tree.ShowConstraints) (planNode, error) {
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
    ORDER BY 1, 2
   `
	return p.showTableDetails(ctx, "SHOW CONSTRAINTS", &n.Table, getConstraintsQuery)
}
