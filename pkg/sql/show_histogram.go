// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
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

		constructor: func(ctx context.Context, p *planner) (_ planNode, err error) {
			row, err := p.InternalSQLTxn().QueryRowEx(
				ctx,
				"read-histogram",
				p.txn,
				sessiondata.NodeUserSessionDataOverride,
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

			// Guard against crashes in the code below .
			defer func() {
				if r := recover(); r != nil {
					// Avoid crashing the process in case of a "safe" panic. This is only
					// possible because the code does not update shared state and does not
					// manipulate locks.
					if ok, e := errorutil.ShouldCatch(r); ok {
						err = e
					} else {
						// Other panic objects can't be considered "safe" and thus are
						// propagated as crashes that terminate the session.
						panic(r)
					}
				}
			}()

			histogram := &stats.HistogramData{}
			histData := *row[0].(*tree.DBytes)
			if err := protoutil.Unmarshal([]byte(histData), histogram); err != nil {
				return nil, err
			}

			v := p.newContainerValuesNode(showHistogramColumns, len(histogram.Buckets))
			resolver := descs.NewDistSQLTypeResolver(p.descCollection, p.InternalSQLTxn().KV())
			if err := typedesc.EnsureTypeIsHydrated(ctx, histogram.ColumnType, &resolver); err != nil {
				return nil, err
			}
			var a tree.DatumAlloc
			for _, b := range histogram.Buckets {
				var upperBound string
				datum, err := stats.DecodeUpperBound(histogram.Version, histogram.ColumnType, &a, b.UpperBound)
				if err != nil {
					upperBound = fmt.Sprintf("<error: %v>", err)
				} else {
					upperBound = datum.String()
				}
				row := tree.Datums{
					tree.NewDString(upperBound),
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
