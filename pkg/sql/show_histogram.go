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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

// Ideally, we would want upper_bound to have the type of the column the
// histogram is on. However, we don't want to have a SHOW statement for which
// the schema depends on its parameters.
var showHistogramColumns = sqlbase.ResultColumns{
	{Name: "upper_bound", Typ: types.String},
	{Name: "num_range", Typ: types.Int},
	{Name: "num_eq", Typ: types.Int},
}

// ShowHistogram returns a SHOW HISTOGRAM statement.
// Privileges: Any privilege on the respective table.
func (p *planner) ShowHistogram(ctx context.Context, n *tree.ShowHistogram) (planNode, error) {
	return &delayedNode{
		name:    fmt.Sprintf("SHOW HISTOGRAM %d", n.HistogramID),
		columns: showHistogramColumns,

		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			rows, _ /* cols */, err := p.queryRows(
				ctx,
				`SELECT "tableID", "columnIDs", histogram
				 FROM system.public.table_statistics
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
			histData := *row[histoColIdx].(*tree.DBytes)
			if err := protoutil.Unmarshal([]byte(histData), histogram); err != nil {
				return nil, err
			}

			v := p.newContainerValuesNode(showHistogramColumns, 0)
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
