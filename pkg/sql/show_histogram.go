// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// Ideally, we would want upper_bound to have the type of the column the
// histogram is on. However, we don't want to have a SHOW statement for which
// the schema depends on its parameters.
var showHistogramColumns = colinfo.ResultColumns{
	{Name: "upper_bound", Typ: types.String},
	{Name: "range_rows", Typ: types.Int},
	{Name: "distinct_range_rows", Typ: types.Float},
	{Name: "equal_rows", Typ: types.Int},
}

// ShowHistogram returns a SHOW HISTOGRAM statement.
// Privileges: Any privilege on the respective table.
func (p *planner) ShowHistogram(ctx context.Context, n *tree.ShowHistogram) (planNode, error) {
	return &delayedNode{
		name:    fmt.Sprintf("SHOW HISTOGRAM %d", n.HistogramID),
		columns: showHistogramColumns,

		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			row, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
				ctx,
				"read-histogram",
				p.txn,
				sessiondata.InternalExecutorOverride{User: security.RootUserName()},
				`SELECT histogram
				 FROM system.table_statistics
				 WHERE "statisticID" = $1`,
				n.HistogramID,
			)
			if err != nil {
				return nil, err
			}
			if row == nil {
				return nil, fmt.Errorf("histogram %d not found", n.HistogramID)
			}
			if len(row) != 1 {
				return nil, errors.AssertionFailedf("expected 1 column from internal query")
			}
			if row[0] == tree.DNull {
				// We found a statistic, but it has no histogram.
				return nil, fmt.Errorf("histogram %d not found", n.HistogramID)
			}

			histogram := &stats.HistogramData{}
			histData := *row[0].(*tree.DBytes)
			if err := protoutil.Unmarshal([]byte(histData), histogram); err != nil {
				return nil, err
			}

			v := p.newContainerValuesNode(showHistogramColumns, 0)
			for _, b := range histogram.Buckets {
				ed, _, err := rowenc.EncDatumFromBuffer(
					histogram.ColumnType, descpb.DatumEncoding_ASCENDING_KEY, b.UpperBound,
				)
				if err != nil {
					v.Close(ctx)
					return nil, err
				}
				row := tree.Datums{
					tree.NewDString(ed.String(histogram.ColumnType)),
					tree.NewDInt(tree.DInt(b.NumRange)),
					tree.NewDFloat(tree.DFloat(b.DistinctRange)),
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
