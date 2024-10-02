// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	encjson "encoding/json"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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
	{Name: "created", Typ: types.TimestampTZ},
	{Name: "row_count", Typ: types.Int},
	{Name: "distinct_count", Typ: types.Int},
	{Name: "null_count", Typ: types.Int},
	{Name: "avg_size", Typ: types.Int},
	{Name: "partial_predicate", Typ: types.String},
	{Name: "histogram_id", Typ: types.Int},
	{Name: "full_histogram_id", Typ: types.Int},
}

var showTableStatsJSONColumns = colinfo.ResultColumns{
	{Name: "statistics", Typ: types.Jsonb},
}

const showTableStatsOptForecast = "forecast"

const showTableStatsOptMerge = "merge"

var showTableStatsOptValidate = map[string]exprutil.KVStringOptValidate{
	showTableStatsOptForecast: exprutil.KVStringOptRequireNoValue,
	showTableStatsOptMerge:    exprutil.KVStringOptRequireNoValue,
}

func containsDroppedColumn(colIDs tree.Datums, desc catalog.TableDescriptor) bool {
	for _, colID := range colIDs {
		cid := descpb.ColumnID(*colID.(*tree.DInt))
		if catalog.FindColumnByID(desc, cid) == nil {
			return true
		}
	}
	return false
}

// ShowTableStats returns a SHOW STATISTICS statement for the specified table.
// Privileges: Any privilege on table.
func (p *planner) ShowTableStats(ctx context.Context, n *tree.ShowTableStats) (planNode, error) {
	opts, err := p.ExprEvaluator("SHOW STATISTICS").KVOptions(
		ctx, n.Options, showTableStatsOptValidate,
	)
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
			stmt := `SELECT
							"tableID",
							"statisticID",
							name,
							"columnIDs",
							"createdAt",
							"rowCount",
							"distinctCount",
							"nullCount",
							"avgSize",
							"partialPredicate",
							histogram,
							"fullStatisticID"
						FROM system.table_statistics
						WHERE "tableID" = $1
						ORDER BY "createdAt", "columnIDs", "statisticID"`

			// There is a privilege check above to make sure the user has any
			// privilege on the table being inspected. We use the node user to execute
			// the query against the system table so that SHOW STATISTICS does not
			// _also_ need privileges on the system.table_statistics table.
			rows, err := p.InternalSQLTxn().QueryBufferedEx(
				ctx,
				"read-table-stats",
				p.txn,
				sessiondata.NodeUserSessionDataOverride,
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
				partialPredicateIdx
				histogramIdx
				fullStatisticIDIdx
				numCols
			)

			histIdx := histogramIdx
			nCols := numCols

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

			_, withMerge := opts[showTableStatsOptMerge]
			_, withForecast := opts[showTableStatsOptForecast]
			if withMerge || withForecast {
				statsList := make([]*stats.TableStatistic, 0, len(rows))
				for _, row := range rows {
					// Skip stats on dropped columns.
					colIDs := row[columnIDsIdx].(*tree.DArray).Array
					ignoreStatsRowWithDroppedColumn := containsDroppedColumn(colIDs, desc)
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
					statsList = append(statsList, obs)
				}

				// Reverse the list to sort by CreatedAt descending.
				for i := 0; i < len(statsList)/2; i++ {
					j := len(statsList) - i - 1
					statsList[i], statsList[j] = statsList[j], statsList[i]
				}

				if withMerge {
					merged := stats.MergedStatistics(ctx, statsList, p.ExtendedEvalContext().Settings)
					statsList = append(merged, statsList...)
					// Iterate in reverse order to match the ORDER BY "columnIDs".
					for i := len(merged) - 1; i >= 0; i-- {
						mergedRow, err := tableStatisticProtoToRow(&merged[i].TableStatisticProto)
						if err != nil {
							return nil, err
						}
						rows = append(rows, mergedRow)
					}
				}

				if withForecast {
					forecasts := stats.ForecastTableStatistics(ctx, p.ExtendedEvalContext().Settings, statsList)
					forecastRows := make([]tree.Datums, 0, len(forecasts))
					// Iterate in reverse order to match the ORDER BY "columnIDs".
					for i := len(forecasts) - 1; i >= 0; i-- {
						forecastRow, err := tableStatisticProtoToRow(&forecasts[i].TableStatisticProto)
						if err != nil {
							return nil, err
						}
						forecastRows = append(forecastRows, forecastRow)
					}
					rows = append(forecastRows, rows...)
					// Some forecasts could have a CreatedAt time before or after some
					// collected stats, so make sure the list is sorted in ascending
					// CreatedAt order.
					sort.SliceStable(rows, func(i, j int) bool {
						iTime := rows[i][createdAtIdx].(*tree.DTimestamp).Time
						jTime := rows[j][createdAtIdx].(*tree.DTimestamp).Time
						return iTime.Before(jTime)
					})
				}
			}

			rowContainerCap := len(rows)
			if n.UsingJSON {
				rowContainerCap = 1
			}
			v := p.newContainerValuesNode(columns, rowContainerCap)
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
					statsRow.ID = (uint64)(*r[statIDIdx].(*tree.DInt))
					statsRow.CreatedAt = tree.AsStringWithFlags(r[createdAtIdx], tree.FmtBareStrings)
					statsRow.RowCount = (uint64)(*r[rowCountIdx].(*tree.DInt))
					statsRow.DistinctCount = (uint64)(*r[distinctCountIdx].(*tree.DInt))
					statsRow.NullCount = (uint64)(*r[nullCountIdx].(*tree.DInt))
					statsRow.AvgSize = (uint64)(*r[avgSizeIdx].(*tree.DInt))
					if r[nameIdx] != tree.DNull {
						statsRow.Name = string(*r[nameIdx].(*tree.DString))
					}
					if r[partialPredicateIdx] != tree.DNull && r[fullStatisticIDIdx] != tree.DNull {
						statsRow.PartialPredicate = string(*r[partialPredicateIdx].(*tree.DString))
						statsRow.FullStatisticID = (uint64)(*r[fullStatisticIDIdx].(*tree.DInt))
					}
					if err := statsRow.DecodeAndSetHistogram(ctx, &p.semaCtx, r[histIdx]); err != nil {
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
				if len(r) != nCols {
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

				createdAt := r[createdAtIdx].(*tree.DTimestamp)
				createdAtTZ, err := createdAt.AddTimeZone(time.UTC, time.Microsecond)
				if err != nil {
					return nil, err
				}

				histogramID := tree.DNull
				if r[histIdx] != tree.DNull {
					histogramID = r[statIDIdx]
				}

				res := tree.Datums{
					r[nameIdx],
					colNames,
					createdAtTZ,
					r[rowCountIdx],
					r[distinctCountIdx],
					r[nullCountIdx],
					r[avgSizeIdx],
					r[partialPredicateIdx],
					histogramID,
					r[fullStatisticIDIdx],
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
	colDesc, err := catalog.MustFindColumnByID(desc, id)
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
	partialPredicate := tree.DNull
	FullStatisticID := tree.DNull
	if stat.PartialPredicate != "" {
		partialPredicate = tree.NewDString(stat.PartialPredicate)
	}
	if stat.FullStatisticID != 0 {
		FullStatisticID = tree.NewDInt(tree.DInt(stat.FullStatisticID))
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
		partialPredicate,
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
	row = append(row, FullStatisticID)
	return row, nil
}
