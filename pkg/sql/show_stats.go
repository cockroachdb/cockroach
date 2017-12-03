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
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

// ShowTableStats returns a SHOW STATISTICS statement for the specified table.
// Privileges: Any privilege on table.
func (p *planner) ShowTableStats(ctx context.Context, n *tree.ShowTableStats) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	desc, err := MustGetTableDesc(ctx, p.txn, p.getVirtualTabler(), tn, true /* allowAdding */)
	if err != nil {
		return nil, err
	}
	if err := p.CheckAnyPrivilege(desc); err != nil {
		return nil, err
	}

	columns := sqlbase.ResultColumns{
		{Name: "name", Typ: types.String},
		{Name: "columns", Typ: types.String},
		{Name: "created_at", Typ: types.Timestamp},
		{Name: "row_count", Typ: types.Int},
		{Name: "distinct_count", Typ: types.Int},
		{Name: "null_count", Typ: types.Int},
		{Name: "histogram_id", Typ: types.Int},
	}

	return &delayedNode{
		name:    "SHOW STATISTICS FOR TABLE " + n.Table.String(),
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			// We need to query the table_statistics and then do some post-processing:
			//  - convert column IDs to a list of column names
			//  - if the statistic has a histogram, we return the statistic ID as a
			//    "handle" which can be used with SHOW HISTOGRAM.
			rows, err := p.queryRows(
				ctx,
				`SELECT "statisticID",
					      name,
					      "columnIDs",
					      "createdAt",
					      "rowCount",
					      "distinctCount",
					      "nullCount",
					      histogram IS NOT NULL
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
				hasHistogramIdx
				numCols
			)
			v := p.newContainerValuesNode(columns, 0)
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

// ShowHistogram returns a SHOW HISTOGRAM statement.
// Privileges: Any privilege on the respective table.
func (p *planner) ShowHistogram(ctx context.Context, n *tree.ShowHistogram) (planNode, error) {
	// Ideally, we would want upper_bound to have the type of the column the
	// histogram is on. However, we don't want to have a SHOW statement for which
	// the schema depends on its parameters.
	columns := sqlbase.ResultColumns{
		{Name: "upper_bound", Typ: types.String},
		{Name: "num_range", Typ: types.Int},
		{Name: "num_eq", Typ: types.Int},
	}

	return &delayedNode{
		name:    fmt.Sprintf("SHOW HISTOGRAM %d", n.HistogramID),
		columns: columns,

		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			rows, err := p.queryRows(
				ctx,
				`SELECT "tableID", "columnIDs", histogram
	 	 FROM system.table_statistics
		 WHERE "statisticID" = $1`,
				n.HistogramID,
			)
			if err != nil {
				return nil, err
			}
			if len(rows) == 0 {
				return nil, errors.Errorf("histogram %d not found", n.HistogramID)
			}
			if len(rows) == 2 {
				// This should never happen, because we use unique_rowid() to generate
				// statisticIDs.
				return nil, errors.Errorf("multiple histograms with id %d", n.HistogramID)
			}
			row := rows[0]
			const (
				tableColIdx = iota
				colIDsColIdx
				histoColIdx
				numCols
			)
			if len(row) != numCols {
				return nil, errors.Errorf("expected %d columns from internal query", numCols)
			}
			if row[histoColIdx] == tree.DNull {
				// We found a statistic, but it has no histogram.
				return nil, errors.Errorf("histogram %d not found", n.HistogramID)
			}
			tableID := sqlbase.ID(*row[tableColIdx].(*tree.DInt))
			var desc sqlbase.TableDescriptor
			if err := getDescriptorByID(ctx, p.txn, tableID, &desc); err != nil {
				return nil, errors.Wrap(err, "unknown table for histogram")
			}
			colIDs := row[colIDsColIdx].(*tree.DArray).Array
			if len(colIDs) == 0 {
				return nil, errors.Errorf("statistic with no column IDs")
			}
			// Get information about the histogram column (the first one).
			id := sqlbase.ColumnID(*colIDs[0].(*tree.DInt))
			colDesc, err := desc.FindColumnByID(id)
			if err != nil {
				// The only error here is that the column ID is unknown. We tolerate
				// unknown columns except for the first one.
				return nil, err
			}

			histogram := &stats.HistogramData{}
			if err := protoutil.Unmarshal([]byte(*row[histoColIdx].(*tree.DBytes)), histogram); err != nil {
				return nil, err
			}

			v := p.newContainerValuesNode(columns, 0)
			for _, b := range histogram.Buckets {
				ed, _, err := sqlbase.EncDatumFromBuffer(
					&colDesc.Type, sqlbase.DatumEncoding_ASCENDING_KEY, b.UpperBound,
				)
				if err != nil {
					v.Close(ctx)
					return nil, err
				}
				row := tree.Datums{
					tree.NewDString(ed.String(&colDesc.Type)),
					tree.NewDInt(tree.DInt(b.NumRange)),
					tree.NewDInt(tree.DInt(b.NumEq)),
				}
				if _, err := v.rows.AddRow(ctx, row); err != nil {
					v.Close(ctx)
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
}
