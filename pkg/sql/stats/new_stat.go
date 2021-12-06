// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// InsertNewStats inserts a slice of statistics at the current time into the
// system table.
func InsertNewStats(
	ctx context.Context,
	settings *cluster.Settings,
	executor sqlutil.InternalExecutor,
	txn *kv.Txn,
	tableStats []*TableStatisticProto,
) error {
	var err error
	for _, statistic := range tableStats {
		err = InsertNewStat(
			ctx,
			settings,
			executor,
			txn,
			statistic.TableID,
			statistic.Name,
			statistic.ColumnIDs,
			int64(statistic.RowCount),
			int64(statistic.DistinctCount),
			int64(statistic.NullCount),
			int64(statistic.AvgSize),
			statistic.HistogramData,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// InsertNewStat inserts a new statistic in the system table.
//
// The stats cache will automatically update asynchronously (as well as the
// stats caches on all other nodes).
func InsertNewStat(
	ctx context.Context,
	settings *cluster.Settings,
	executor sqlutil.InternalExecutor,
	txn *kv.Txn,
	tableID descpb.ID,
	name string,
	columnIDs []descpb.ColumnID,
	rowCount, distinctCount, nullCount, avgSize int64,
	h *HistogramData,
) error {
	// We must pass a nil interface{} if we want to insert a NULL.
	var nameVal, histogramVal interface{}
	if name != "" {
		nameVal = name
	}
	if h != nil {
		var err error
		histogramVal, err = protoutil.Marshal(h)
		if err != nil {
			return err
		}
	}

	columnIDsVal := tree.NewDArray(types.Int)
	for _, c := range columnIDs {
		if err := columnIDsVal.Append(tree.NewDInt(tree.DInt(int(c)))); err != nil {
			return err
		}
	}
	if !settings.Version.IsActive(ctx, clusterversion.AlterSystemTableStatisticsAddAvgSizeCol) {
		_, err := executor.Exec(
			ctx, "insert-statistic", txn,
			`INSERT INTO system.table_statistics (
					"tableID",
					"name",
					"columnIDs",
					"rowCount",
					"distinctCount",
					"nullCount",
					histogram
				) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			tableID,
			nameVal,
			columnIDsVal,
			rowCount,
			distinctCount,
			nullCount,
			histogramVal,
		)
		return err
	}
	_, err := executor.Exec(
		ctx, "insert-statistic", txn,
		`INSERT INTO system.table_statistics (
					"tableID",
					"name",
					"columnIDs",
					"rowCount",
					"distinctCount",
					"nullCount",
					"avgSize",
					histogram
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		tableID,
		nameVal,
		columnIDsVal,
		rowCount,
		distinctCount,
		nullCount,
		avgSize,
		histogramVal,
	)
	return err
}
