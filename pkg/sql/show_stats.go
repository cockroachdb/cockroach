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
	"bytes"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// ShowTableStats returns a SHOW STATISTICS statement for the specified table.
// Privileges: Any privilege on table.
func (p *planner) ShowTableStats(ctx context.Context, n *tree.ShowTableStats) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	columns := sqlbase.ResultColumns{
		{Name: "Name", Typ: types.String},
		{Name: "Columns", Typ: types.String},
		{Name: "Created At", Typ: types.Timestamp},
		{Name: "Row Count", Typ: types.Int},
		{Name: "Distinct Count", Typ: types.Int},
		{Name: "Null Count", Typ: types.Int},
		{Name: "Histogram", Typ: types.Int},
	}

	return &delayedNode{
		name:    "SHOW STATISTICS FOR TABLE " + n.Table.String(),
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			desc, err := MustGetTableDesc(ctx, p.txn, p.getVirtualTabler(), tn, true /* allowAdding */)
			if err != nil {
				return nil, err
			}
			if err := p.CheckAnyPrivilege(desc); err != nil {
				return nil, err
			}

			ie := InternalExecutor{LeaseManager: p.LeaseMgr()}
			rows, err := ie.QueryRowsInTransaction(
				ctx, "get-stats", p.txn,
				`SELECT "statisticID",
					      name,
					      "columnIDs",
					      "createdAt",
					      "rowCount",
					      "distinctCount",
					      "nullCount",
					      histogram IS NOT NULL
				 FROM system.table_statistics
				 WHERE "tableID" = $1`,
				desc.ID,
			)
			if err != nil {
				return nil, err
			}
			v := p.newContainerValuesNode(columns, 0)
			for _, r := range rows {
				if len(r) != 8 {
					return nil, errors.Errorf("incorrect columns from internal query")
				}

				// List columns by name.
				var buf bytes.Buffer
				for i, d := range r[2].(*tree.DArray).Array {
					if i > 0 {
						buf.WriteString(",")
					}
					id := sqlbase.ColumnID(*d.(*tree.DInt))
					colDesc, err := desc.FindActiveColumnByID(id)
					if err != nil {
						buf.WriteString("<unknown>") // This can happen if a column was removed.
					} else {
						buf.WriteString(colDesc.Name)
					}
				}
				colNames := tree.NewDString(buf.String())

				histogram := tree.DNull
				if r[7] == tree.DBoolTrue {
					histogram = r[0]
				}

				res := tree.Datums{r[1], colNames, r[3], r[4], r[5], r[6], histogram}
				if _, err := v.rows.AddRow(ctx, res); err != nil {
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
}
