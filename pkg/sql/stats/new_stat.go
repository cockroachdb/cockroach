// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// InsertNewStats inserts a slice of statistics at the current time into the
// system table.
func InsertNewStats(ctx context.Context, txn isql.Txn, tableStats []*TableStatisticProto) error {
	var err error
	for _, statistic := range tableStats {
		err = InsertNewStat(
			ctx,
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
			"", /* createdAt */
			0,  /* statisticID */
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteStatsWithOldDeleted implements the "DELETE stale stats -> INSERT
// new stats" retention rule. It deletes old statistics for the given
// columns if the new statistics is not partial and then inserts the new
// statistic. For createdAt and statisticID, if the provided values are
// empty, we use the default values defined in the
// system.table_statistics schema for the INSERT.
func WriteStatsWithOldDeleted(
	ctx context.Context,
	txn isql.Txn,
	tableID descpb.ID,
	name string,
	columnIDs []descpb.ColumnID,
	rowCount, distinctCount, nullCount, avgSize int64,
	h *HistogramData,
	partialPredicate string,
	fullStatisticID uint64,
	createdAt string,
	statisticID uint64,
	canaryEnabled bool,
) error {
	// Delete old stats that have been superseded, if the new statistic
	// is not partial.
	if partialPredicate == "" {
		if canaryEnabled {
			// We don't immediately delete the "stale" stats, but keep it
			// until another canary stats is collected. It is because if a
			// query picked the stable path for stats selection, we will
			// need to reuse the stale stats.
			if err := DeleteExpiredStats(
				ctx,
				txn,
				tableID,
				columnIDs,
			); err != nil {
				return errors.Wrapf(err, "fail to delete expired stats for table %d columns %v", tableID, columnIDs)
			}

			if err := MarkDelayDelete(ctx, txn, tableID, columnIDs); err != nil {
				return errors.Wrapf(err, "failed to mark rows for delayed deletion for table %d columns %v", tableID, columnIDs)
			}
		} else {
			if err := DeleteOldStatsForColumns(
				ctx,
				txn,
				tableID,
				columnIDs,
			); err != nil {
				return err
			}
		}
	}

	return InsertNewStat(
		ctx,
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
		statisticID,
	)
}

// InsertNewStat inserts a new statistic in the system table.
//
// The stats cache will automatically update asynchronously (as well as the
// stats caches on all other nodes).
func InsertNewStat(
	ctx context.Context,
	txn isql.Txn,
	tableID descpb.ID,
	name string,
	columnIDs []descpb.ColumnID,
	rowCount, distinctCount, nullCount, avgSize int64,
	h *HistogramData,
	partialPredicate string,
	fullStatisticID uint64,
	createdAt string,
	statisticID uint64,
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

	paramList := []string{
		`"tableID"`,
		`"name"`,
		`"columnIDs"`,
		`"rowCount"`,
		`"distinctCount"`,
		`"nullCount"`,
		`"avgSize"`,
		`histogram`,
		`"partialPredicate"`,
	}

	valList := []interface{}{
		tableID,
		nameVal,
		columnIDsVal,
		rowCount,
		distinctCount,
		nullCount,
		avgSize,
		histogramVal,
		predicateValue,
	}

	if createdAt != "" {
		paramList = append(paramList, `"createdAt"`)
		valList = append(valList, createdAt)
	}

	if statisticID != 0 {
		paramList = append(paramList, `"statisticID"`)
		valList = append(valList, statisticID)
	}

	if fullStatisticID != 0 {
		paramList = append(paramList, `"fullStatisticID"`)
		valList = append(valList, fullStatisticID)
	}

	var placeholdersStr strings.Builder
	for i := range paramList {
		placeholdersStr.WriteString(fmt.Sprintf("$%d", i+1))
		if i != len(paramList)-1 {
			placeholdersStr.WriteString(", ")
		}
	}

	stmt := fmt.Sprintf(
		"INSERT INTO system.table_statistics (%s) VALUES (%s)",
		strings.Join(paramList, ","),
		placeholdersStr.String(),
	)
	_, err := txn.Exec(ctx, "insert-statistic", txn.KV(), stmt, valList...)
	return err
}
