// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// InsertNewStats inserts a slice of statistics at the current time into the
// system table.
func InsertNewStats(
	ctx context.Context, settings *cluster.Settings, txn isql.Txn, tableStats []*TableStatisticProto,
) error {
	var err error
	for _, statistic := range tableStats {
		err = InsertNewStat(
			ctx,
			settings,
			txn,
			statistic.TableID,
			statistic.Name,
			statistic.ColumnIDs,
			int64(statistic.RowCount),
			int64(statistic.DistinctCount),
			int64(statistic.NullCount),
			int64(statistic.AvgSize),
			statistic.HistogramData,
			statistic.PartialPredicate,
			statistic.FullStatisticID,
			"", // createdAt - empty string uses default timestamp
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteStatWithRetention implements the "DELETE staled stats -> INSERT new stats" 
// retention logic used by sampleAggregator.writeResults(). It deletes old statistics 
// for the given columns (if not partial) and then inserts the new statistic.
func WriteStatWithRetention(
	ctx context.Context,
	settings *cluster.Settings,
	txn isql.Txn,
	tableID descpb.ID,
	name string,
	columnIDs []descpb.ColumnID,
	rowCount, distinctCount, nullCount, avgSize int64,
	h *HistogramData,
	partialPredicate string,
	fullStatisticID uint64,
	createdAt string,
) error {
	// Delete old stats that have been superseded, if the new statistic
	// is not partial.
	if partialPredicate == "" {
		if err := DeleteOldStatsForColumns(
			ctx,
			txn,
			tableID,
			columnIDs,
		); err != nil {
			return err
		}
	}

	// Insert the new stat.
	return InsertNewStat(
		ctx,
		settings,
		txn,
		tableID,
		name,
		columnIDs,
		rowCount,
		distinctCount,
		nullCount,
		avgSize,
		h,
		partialPredicate,
		fullStatisticID,
		createdAt,
	)
}

// InsertNewStat inserts a new statistic in the system table.
//
// The stats cache will automatically update asynchronously (as well as the
// stats caches on all other nodes).
func InsertNewStat(
	ctx context.Context,
	settings *cluster.Settings,
	txn isql.Txn,
	tableID descpb.ID,
	name string,
	columnIDs []descpb.ColumnID,
	rowCount, distinctCount, nullCount, avgSize int64,
	h *HistogramData,
	partialPredicate string,
	fullStatisticID uint64,
	createdAt string,
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

	// Need to assign to a nil interface{} to be able to insert NULL value.
	var predicateValue interface{}
	if partialPredicate != "" {
		predicateValue = partialPredicate
	}

	if createdAt == "" {
		// Insert without specifying createdAt - let the database use default (now())
		_, err := txn.Exec(
			ctx, "insert-statistic", txn.KV(),
			`INSERT INTO system.table_statistics (
						"tableID",
						"name",
						"columnIDs",
						"rowCount",
						"distinctCount",
						"nullCount",
						"avgSize",
						histogram,
						"partialPredicate",
						"fullStatisticID"
					) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			tableID,
			nameVal,
			columnIDsVal,
			rowCount,
			distinctCount,
			nullCount,
			avgSize,
			histogramVal,
			predicateValue,
			fullStatisticID,
		)
		return err
	}

	// Insert with explicit createdAt timestamp
	_, err := txn.Exec(
		ctx, "insert-statistic", txn.KV(),
		`INSERT INTO system.table_statistics (
					"tableID",
					"name",
					"columnIDs",
					"createdAt",
					"rowCount",
					"distinctCount",
					"nullCount",
					"avgSize",
					histogram,
					"partialPredicate",
					"fullStatisticID"
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		tableID,
		nameVal,
		columnIDsVal,
		createdAt,
		rowCount,
		distinctCount,
		nullCount,
		avgSize,
		histogramVal,
		predicateValue,
		fullStatisticID,
	)
	return err
}
