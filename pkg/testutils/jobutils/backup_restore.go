// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobutils

import (
	"context"
	gosql "database/sql"
	"fmt"

	"github.com/cockroachdb/errors"
)

// CheckDownloadCompletenessOnDatabase checks the download job actually
// completed over the whole database.
func CheckDownloadCompletenessOnDatabase(
	ctx context.Context, conn *gosql.DB, dbName string,
) (uint64, error) {
	var databaseID int
	row := conn.QueryRowContext(ctx, `SELECT id FROM system.namespace WHERE name = $1`, dbName)
	if err := row.Scan(&databaseID); err != nil {
		return 0, errors.New("could not get database descriptor id")
	}
	rows, err := conn.QueryContext(ctx, `SELECT id FROM system.namespace where "parentID" = $1`, databaseID)
	if err != nil {
		return 0, errors.New("could not get database table name and tableIDs")
	}
	defer rows.Close()

	var tableIDs []int
	for rows.Next() {
		var tableID int
		if err := rows.Scan(&tableID); err != nil {
			return 0, err
		}
		tableIDs = append(tableIDs, tableID)
	}
	var externalBytes uint64
	for _, tableID := range tableIDs {
		externalBytesForTable, err := CheckDownloadCompletenessOnTable(ctx, conn, databaseID, tableID)
		if err != nil {
			return 0, errors.Wrapf(err, "failed download check on table %d", tableID)
		}
		externalBytes += externalBytesForTable
	}
	return externalBytes, nil
}

// CheckDownloadCompletenessOnTable checks the download job actually completed
// over the whole table.
func CheckDownloadCompletenessOnTable(
	ctx context.Context, conn *gosql.DB, databaseID, tableID int,
) (uint64, error) {

	statsQuery := fmt.Sprintf(`SELECT stats->>'external_file_bytes' FROM crdb_internal.tenant_span_stats(
		ARRAY(SELECT(crdb_internal.table_span(%d)[1], crdb_internal.table_span(%d)[2])))`, tableID, tableID)
	var stats uint64
	if err := conn.QueryRowContext(ctx, statsQuery).Scan(&stats); err != nil {
		return 0, err
	}
	return stats, nil
}
