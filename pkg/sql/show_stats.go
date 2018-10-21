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
	encjson "encoding/json"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/pkg/errors"
)

var showTableStatsColumns = sqlbase.ResultColumns{
	{Name: "table_name", Typ: types.String},
	{Name: "column_names", Typ: types.TArray{Typ: types.String}},
	{Name: "created", Typ: types.Timestamp},
	{Name: "row_count", Typ: types.Int},
	{Name: "distinct_count", Typ: types.Int},
	{Name: "null_count", Typ: types.Int},
	{Name: "histogram_id", Typ: types.Int},
}

var showTableStatsJSONColumns = sqlbase.ResultColumns{
	{Name: "statistics", Typ: types.JSON},
}

// ShowTableStats returns a SHOW STATISTICS statement for the specified table.
// Privileges: Any privilege on table.
func (p *planner) ShowTableStats(ctx context.Context, n *tree.ShowTableStats) (planNode, error) {
	// We avoid the cache so that we can observe the stats without
	// taking a lease, like other SHOW commands.
	desc, err := p.ResolveUncachedTableDescriptor(ctx, &n.Table, true /*required*/, requireTableDesc)
	if err != nil {
		return nil, err
	}
	if err := p.CheckAnyPrivilege(ctx, desc); err != nil {
		return nil, err
	}
	columns := showTableStatsColumns
	if n.UsingJSON {
		columns = showTableStatsJSONColumns
	}

	return &delayedNode{
		name:    n.String(),
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			// We need to query the table_statistics and then do some post-processing:
			//  - convert column IDs to column names
			//  - if the statistic has a histogram, we return the statistic ID as a
			//    "handle" which can be used with SHOW HISTOGRAM.
			rows, _ /* cols */, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
				ctx,
				"read-table-stats",
				p.txn,
				`SELECT "statisticID",
					      name,
					      "columnIDs",
					      "createdAt",
					      "rowCount",
					      "distinctCount",
					      "nullCount",
					      histogram
				 FROM system.table_statistics
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
				histogramIdx
				numCols
			)

			v := p.newContainerValuesNode(columns, 0)
			if n.UsingJSON {
				result := make([]stats.JSONStatistic, len(rows))
				for i, r := range rows {
					result[i].CreatedAt = tree.AsStringWithFlags(r[createdAtIdx], tree.FmtBareStrings)
					result[i].RowCount = (uint64)(*r[rowCountIdx].(*tree.DInt))
					result[i].DistinctCount = (uint64)(*r[distinctCountIdx].(*tree.DInt))
					result[i].NullCount = (uint64)(*r[nullCountIdx].(*tree.DInt))
					if r[nameIdx] != tree.DNull {
						result[i].Name = string(*r[nameIdx].(*tree.DString))
					}
					colIDs := r[columnIDsIdx].(*tree.DArray).Array
					result[i].Columns = make([]string, len(colIDs))
					for j, d := range colIDs {
						result[i].Columns[j] = statColumnString(desc, d)
					}
					if err := result[i].DecodeAndSetHistogram(r[histogramIdx]); err != nil {
						v.Close(ctx)
						return nil, err
					}
				}
				encoded, err := encjson.Marshal(result)
				if err != nil {
					v.Close(ctx)
					return nil, err
				}
				jsonResult, err := json.ParseJSON(string(encoded))
				if err != nil {
					v.Close(ctx)
					return nil, err
				}
				if _, err := v.rows.AddRow(ctx, tree.Datums{tree.NewDJSON(jsonResult)}); err != nil {
					v.Close(ctx)
					return nil, err
				}
				return v, nil
			}

			for _, r := range rows {
				if len(r) != numCols {
					v.Close(ctx)
					return nil, errors.Errorf("incorrect columns from internal query")
				}

				colIDs := r[columnIDsIdx].(*tree.DArray).Array
				colNames := tree.NewDArray(types.String)
				colNames.Array = make(tree.Datums, len(colIDs))
				for i, d := range colIDs {
					colNames.Array[i] = tree.NewDString(statColumnString(desc, d))
				}

				histogramID := tree.DNull
				if r[histogramIdx] != tree.DNull {
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

func statColumnString(desc *TableDescriptor, colID tree.Datum) string {
	id := sqlbase.ColumnID(*colID.(*tree.DInt))
	colDesc, err := desc.FindColumnByID(id)
	if err != nil {
		// This can happen if a column was removed.
		return "<unknown>"
	}
	return colDesc.Name
}
