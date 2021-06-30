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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

const (
	// keepCount is the number of automatic statistics to keep for a given
	// table and set of columns when deleting old stats. The purpose of keeping
	// several old automatic statistics is to be able to track the amount of
	// time between refreshes. See comments in automatic_stats.go for more
	// details.
	keepCount = 4
)

// DeleteOldStatsForColumns deletes old statistics from the
// system.table_statistics table. For the given tableID and columnIDs,
// DeleteOldStatsForColumns keeps the most recent keepCount automatic
// statistics and deletes all the others.
func DeleteOldStatsForColumns(
	ctx context.Context,
	executor sqlutil.InternalExecutor,
	txn *kv.Txn,
	tableID descpb.ID,
	columnIDs []descpb.ColumnID,
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
	_, err := executor.Exec(
		ctx, "delete-statistics", txn,
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
