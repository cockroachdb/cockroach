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
	encjson "encoding/json"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

var showTableStatsColumns = colinfo.ResultColumns{
	{Name: "statistics_name", Typ: types.String},
	{Name: "column_names", Typ: types.StringArray},
	{Name: "created", Typ: types.Timestamp},
	{Name: "row_count", Typ: types.Int},
	{Name: "distinct_count", Typ: types.Int},
	{Name: "null_count", Typ: types.Int},
	{Name: "avg_size", Typ: types.Int},
	{Name: "histogram_id", Typ: types.Int},
}

var showTableStatsJSONColumns = colinfo.ResultColumns{
	{Name: "statistics", Typ: types.Jsonb},
}

const showTableStatsOptForecast = "forecast"

var showTableStatsOptValidate = map[string]KVStringOptValidate{
	showTableStatsOptForecast: KVStringOptRequireNoValue,
}

// ShowTableStats returns a SHOW STATISTICS statement for the specified table.
// Privileges: Any privilege on table.
func (p *planner) ShowTableStats(ctx context.Context, n *tree.ShowTableStats) (planNode, error) {
	optsFn, err := p.TypeAsStringOpts(ctx, n.Options, showTableStatsOptValidate)
	if err != nil {
		return nil, err
	}
	opts, err := optsFn()
	if err != nil {
		return nil, err
	}

	// We avoid the cache so that we can observe the stats without
	// taking a lease, like other SHOW commands.
	desc, err := p.ResolveUncachedTableDescriptorEx(ctx, n.Table, true /*required*/, tree.ResolveRequireTableDesc)
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
		constructor: func(ctx context.Context, p *planner) (_ planNode, err error) {
			// We need to query the table_statistics and then do some post-processing:
			//  - convert column IDs to column names
			//  - if the statistic has a histogram, we return the statistic ID as a
			//    "handle" which can be used with SHOW HISTOGRAM.
			// TODO(yuzefovich): refactor the code to use the iterator API
			// (currently it is not possible due to a panic-catcher below).
			const stmt = `SELECT
							"tableID",
							"statisticID",
							name,
							"columnIDs",
							"createdAt",
							"rowCount",
							"distinctCount",
							"nullCount",
							"avgSize",
							histogram
						FROM system.table_statistics
						WHERE "tableID" = $1
						ORDER BY "createdAt", "columnIDs", "statisticID"`
			rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryBuffered(
				ctx,
				"read-table-stats",
				p.txn,
				stmt,
				desc.GetID(),
			)
			if err != nil {
				return nil, err
			}

			const (
				tableIDIdx = iota
				statIDIdx
				nameIdx
				columnIDsIdx
				createdAtIdx
				rowCountIdx
				distinctCountIdx
				nullCountIdx
				avgSizeIdx
				histogramIdx
				numCols
			)

			// Guard against crashes in the code below (e.g. #56356).
			defer func() {
				if r := recover(); r != nil {
					// This code allows us to propagate internal errors without having to add
					// error checks everywhere throughout the code. This is only possible
					// because the code does not update shared state and does not manipulate
					// locks.
					if ok, e := errorutil.ShouldCatch(r); ok {
						err = e
					} else {
						// Other panic objects can't be considered "safe" and thus are
						// propagated as crashes that terminate the session.
						panic(r)
					}
				}
			}()

			if _, withForecast := opts[showTableStatsOptForecast]; withForecast {
				observed := make([]*stats.TableStatistic, 0, len(rows))
				for _, row := range rows {
					// Skip stats on dropped columns.
					colIDs := row[columnIDsIdx].(*tree.DArray).Array
					ignoreStatsRowWithDroppedColumn := false
					for _, colID := range colIDs {
						cid := descpb.ColumnID(*colID.(*tree.DInt))
						if _, err := desc.FindColumnWithID(cid); err != nil {
							if sqlerrors.IsUndefinedColumnError(err) {
								ignoreStatsRowWithDroppedColumn = true
								break
							} else {
								return nil, err
							}
						}
					}
					if ignoreStatsRowWithDroppedColumn {
						continue
					}
					stat, err := stats.NewTableStatisticProto(row)
					if err != nil {
						return nil, err
					}
					obs := &stats.TableStatistic{TableStatisticProto: *stat}
					if obs.HistogramData != nil && !obs.HistogramData.ColumnType.UserDefined() {
						if err := stats.DecodeHistogramBuckets(obs); err != nil {
							return nil, err
						}
					}
					observed = append(observed, obs)
				}

				// Reverse the list to sort by CreatedAt descending.
				for i := 0; i < len(observed)/2; i++ {
					j := len(observed) - i - 1
					observed[i], observed[j] = observed[j], observed[i]
				}

				forecasts := stats.ForecastTableStatistics(ctx, p.EvalContext(), observed)

				// Iterate in reverse order to match the ORDER BY "columnIDs".
				for i := len(forecasts) - 1; i >= 0; i-- {
					forecastRow, err := tableStatisticProtoToRow(&forecasts[i].TableStatisticProto)
					if err != nil {
						return nil, err
					}
					rows = append(rows, forecastRow)
				}
			}

			v := p.newContainerValuesNode(columns, 0)
			if n.UsingJSON {
				result := make([]stats.JSONStatistic, 0, len(rows))
				for _, r := range rows {
					var statsRow stats.JSONStatistic
					colIDs := r[columnIDsIdx].(*tree.DArray).Array
					statsRow.Columns = make([]string, len(colIDs))
					ignoreStatsRowWithDroppedColumn := false
					for j, d := range colIDs {
						statsRow.Columns[j], err = statColumnString(desc, d)
						if err != nil && sqlerrors.IsUndefinedColumnError(err) {
							ignoreStatsRowWithDroppedColumn = true
							break
						}
					}
					if ignoreStatsRowWithDroppedColumn {
						continue
					}
					statsRow.CreatedAt = tree.AsStringWithFlags(r[createdAtIdx], tree.FmtBareStrings)
					statsRow.RowCount = (uint64)(*r[rowCountIdx].(*tree.DInt))
					statsRow.DistinctCount = (uint64)(*r[distinctCountIdx].(*tree.DInt))
					statsRow.NullCount = (uint64)(*r[nullCountIdx].(*tree.DInt))
					statsRow.AvgSize = (uint64)(*r[avgSizeIdx].(*tree.DInt))
					if r[nameIdx] != tree.DNull {
						statsRow.Name = string(*r[nameIdx].(*tree.DString))
					}
					if err := statsRow.DecodeAndSetHistogram(ctx, &p.semaCtx, r[histogramIdx]); err != nil {
						v.Close(ctx)
						return nil, err
					}
					result = append(result, statsRow)
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
				ignoreStatsRowWithDroppedColumn := false
				var colName string
				for i, d := range colIDs {
					colName, err = statColumnString(desc, d)
					if err != nil && sqlerrors.IsUndefinedColumnError(err) {
						ignoreStatsRowWithDroppedColumn = true
						break
					}
					colNames.Array[i] = tree.NewDString(colName)
				}
				if ignoreStatsRowWithDroppedColumn {
					continue
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
					r[avgSizeIdx],
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

func statColumnString(desc catalog.TableDescriptor, colID tree.Datum) (colName string, err error) {
	id := descpb.ColumnID(*colID.(*tree.DInt))
	colDesc, err := desc.FindColumnWithID(id)
	if err != nil {
		// This can happen if a column was removed.
		return "<unknown>", err
	}
	return colDesc.GetName(), nil
}

func tableStatisticProtoToRow(stat *stats.TableStatisticProto) (tree.Datums, error) {
	name := tree.DNull
	if stat.Name != "" {
		name = tree.NewDString(stat.Name)
	}
	columnIDs := tree.NewDArray(types.Int)
	for _, c := range stat.ColumnIDs {
		if err := columnIDs.Append(tree.NewDInt(tree.DInt(c))); err != nil {
			return nil, err
		}
	}
	row := tree.Datums{
		tree.NewDInt(tree.DInt(stat.TableID)),
		tree.NewDInt(tree.DInt(stat.StatisticID)),
		name,
		columnIDs,
		&tree.DTimestamp{Time: stat.CreatedAt},
		tree.NewDInt(tree.DInt(stat.RowCount)),
		tree.NewDInt(tree.DInt(stat.DistinctCount)),
		tree.NewDInt(tree.DInt(stat.NullCount)),
		tree.NewDInt(tree.DInt(stat.AvgSize)),
	}
	if stat.HistogramData == nil {
		row = append(row, tree.DNull)
	} else {
		histogram, err := protoutil.Marshal(stat.HistogramData)
		if err != nil {
			return nil, err
		}
		row = append(row, tree.NewDBytes(tree.DBytes(histogram)))
	}
	return row, nil
}
