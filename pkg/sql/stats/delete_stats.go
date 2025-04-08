// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

const (
	// keepCount is the number of automatic statistics to keep for a given
	// table and set of columns when deleting old stats. The purpose of keeping
	// several old automatic statistics is to be able to track the amount of
	// time between refreshes. See comments in automatic_stats.go for more
	// details.
	keepCount = 4

	// defaultKeepTime is the default time to keep around old stats for columns that are
	// not collected by default.
	defaultKeepTime = 24 * time.Hour
)

// TableStatisticsRetentionPeriod controls the cluster setting for the
// retention period of statistics that are not collected by default.
var TableStatisticsRetentionPeriod = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.stats.non_default_columns.min_retention_period",
	"minimum retention period for table statistics collected on non-default columns",
	defaultKeepTime,
	settings.WithPublic)

// DeleteOldStatsForColumns deletes old statistics from the
// system.table_statistics table. For the given tableID and columnIDs,
// DeleteOldStatsForColumns keeps the most recent keepCount automatic
// statistics and deletes all the others.
func DeleteOldStatsForColumns(
	ctx context.Context, txn isql.Txn, tableID descpb.ID, columnIDs []descpb.ColumnID,
) error {
	columnIDsVal := tree.NewDArray(types.Int)
	for _, c := range columnIDs {
		if err := columnIDsVal.Append(tree.NewDInt(tree.DInt(int(c)))); err != nil {
			return err
		}
	}

	// This will delete all old statistics for the given table and columns,
	// including stats created manually (except for a few automatic statistics,
	// which are identified by the name AutoStatsName).
	_, err := txn.Exec(
		ctx, "delete-statistics", txn.KV(),
		`DELETE FROM system.table_statistics
               WHERE "tableID" = $1
               AND "columnIDs" = $3
               AND "statisticID" NOT IN (
                   SELECT "statisticID" FROM system.table_statistics
                   WHERE "tableID" = $1
                   AND "name" = $2
                   AND "columnIDs" = $3
                   ORDER BY "createdAt" DESC
                   LIMIT $4
               )`,
		tableID,
		jobspb.AutoStatsName,
		columnIDsVal,
		keepCount,
	)
	return err
}

// DeleteOldStatsForOtherColumns deletes statistics from the
// system.table_statistics table for columns *not* in the given set of column
// IDs that are older than keepTime.
func DeleteOldStatsForOtherColumns(
	ctx context.Context,
	txn isql.Txn,
	tableID descpb.ID,
	columnIDs [][]descpb.ColumnID,
	keepTime time.Duration,
) error {
	var columnIDsPlaceholders bytes.Buffer
	placeholderVals := make([]interface{}, 0, len(columnIDs)+2)
	placeholderVals = append(placeholderVals, tableID, keepTime)
	columnIDsPlaceholdersStart := len(placeholderVals) + 1
	for i := range columnIDs {
		columnIDsVal := tree.NewDArray(types.Int)
		for _, c := range columnIDs[i] {
			if err := columnIDsVal.Append(tree.NewDInt(tree.DInt(int(c)))); err != nil {
				return err
			}
		}
		if i > 0 {
			columnIDsPlaceholders.WriteString(", ")
		}
		columnIDsPlaceholders.WriteString(fmt.Sprintf("$%d::string", i+columnIDsPlaceholdersStart))
		placeholderVals = append(placeholderVals, columnIDsVal)
	}

	// This will delete all statistics for the given table that are not
	// on the given columns and are older than keepTime.
	_, err := txn.Exec(
		ctx, "delete-statistics", txn.KV(),
		fmt.Sprintf(`DELETE FROM system.table_statistics
               WHERE "tableID" = $1
               AND "columnIDs"::string NOT IN (%s)
               AND "createdAt" < now() - $2`, columnIDsPlaceholders.String()),
		placeholderVals...,
	)
	return err
}

// deleteStatsForDroppedTables deletes all statistics for at most 'limit' number
// of dropped tables.
func deleteStatsForDroppedTables(ctx context.Context, db isql.DB, limit int64) error {
	_, err := db.Executor().Exec(
		ctx, "delete-statistics-for-dropped-tables", nil, /* txn */
		fmt.Sprintf(`DELETE FROM system.table_statistics
                            WHERE "tableID" NOT IN (SELECT table_id FROM crdb_internal.tables)
                            LIMIT %d`, limit),
	)
	return err
}
