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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

var showTableStatsColumns = sqlbase.ResultColumns{
	{Name: "name", Typ: types.String},
	{Name: "columns", Typ: types.String},
	{Name: "created_at", Typ: types.Timestamp},
	{Name: "row_count", Typ: types.Int},
	{Name: "distinct_count", Typ: types.Int},
	{Name: "null_count", Typ: types.Int},
	{Name: "histogram_id", Typ: types.Int},
}

// ShowTableStats returns a SHOW STATISTICS statement for the specified table.
// Privileges: Any privilege on table.
func (p *planner) ShowTableStats(ctx context.Context, n *tree.ShowTableStats) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.SessionData().Database)
	if err != nil {
		return nil, err
	}

	desc, err := MustGetTableDesc(ctx, p.txn, p.getVirtualTabler(), tn, true /* allowAdding */)
	if err != nil {
		return nil, err
	}
	if err := p.CheckAnyPrivilege(ctx, desc); err != nil {
		return nil, err
	}

	return &delayedNode{
		name:    "SHOW STATISTICS FOR TABLE " + n.Table.String(),
		columns: showTableStatsColumns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			// We need to query the table_statistics and then do some post-processing:
			//  - convert column IDs to a list of column names
			//  - if the statistic has a histogram, we return the statistic ID as a
			//    "handle" which can be used with SHOW HISTOGRAM.
			rows, _ /* cols */, err := p.queryRows(
				ctx,
				`SELECT "statisticID",
					      name,
					      "columnIDs",
					      "createdAt",
					      "rowCount",
					      "distinctCount",
					      "nullCount",
					      histogram IS NOT NULL
				 FROM system.public.table_statistics
				 WHERE "tableID" = $1
				 ORDER BY "createdAt"`,
				desc.ID,
			)
			if err != nil {
				return nil, err
			}

			const (
				statIDIdx = iota
				nameIdx
				columnIDsIdx
				createdAtIdx
				rowCountIdx
				distinctCountIdx
				nullCountIdx
				hasHistogramIdx
				numCols
			)
			v := p.newContainerValuesNode(showTableStatsColumns, 0)
			for _, r := range rows {
				if len(r) != numCols {
					v.Close(ctx)
					return nil, errors.Errorf("incorrect columns from internal query")
				}

				// List columns by name.
				var buf bytes.Buffer
				for i, d := range r[columnIDsIdx].(*tree.DArray).Array {
					if i > 0 {
						buf.WriteString(",")
					}
					id := sqlbase.ColumnID(*d.(*tree.DInt))
					colDesc, err := desc.FindColumnByID(id)
					if err != nil {
						buf.WriteString("<unknown>") // This can happen if a column was removed.
					} else {
						buf.WriteString(colDesc.Name)
					}
				}
				colNames := tree.NewDString(buf.String())

				histogramID := tree.DNull
				if r[hasHistogramIdx] == tree.DBoolTrue {
					histogramID = r[statIDIdx]
				}

				res := tree.Datums{
					r[nameIdx],
					colNames,
					r[createdAtIdx],
					r[rowCountIdx],
					r[distinctCountIdx],
					r[nullCountIdx],
					histogramID,
				}
				if _, err := v.rows.AddRow(ctx, res); err != nil {
					v.Close(ctx)
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
}
