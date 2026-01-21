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
		_, _, err = InsertNewStat(
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
	txn isql.Txn,
	tableID descpb.ID,
	name string,
	columnIDs []descpb.ColumnID,
	rowCount, distinctCount, nullCount, avgSize int64,
	h *HistogramData,
	partialPredicate string,
	fullStatisticID uint64,
) (int64, int64, error) {
	// We must pass a nil interface{} if we want to insert a NULL.
	var nameVal, histogramVal interface{}
	if name != "" {
		nameVal = name
	}
	if h != nil {
		var err error
		histogramVal, err = protoutil.Marshal(h)
		if err != nil {
			return 0, 0, err
		}
	}

	columnIDsVal := tree.NewDArray(types.Int)
	for _, c := range columnIDs {
		if err := columnIDsVal.Append(tree.NewDInt(tree.DInt(int(c)))); err != nil {
			return 0, 0, err
		}
	}

	// Need to assign to a nil interface{} to be able to insert NULL value.
	var predicateValue interface{}
	if partialPredicate != "" {
		predicateValue = partialPredicate
	}

	res, err := txn.QueryRow(
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
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING "createdAt", "statisticID"`,
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

	ts := res[0].(*tree.DTimestamp)
	statsID := res[1].(*tree.DInt)
	return ts.UnixNano(), int64(*statsID), err
}
