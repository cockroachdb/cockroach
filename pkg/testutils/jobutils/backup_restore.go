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
)

// GetExternalBytesOnNode computes the number of external bytes on the node
// we're connected to.
func GetExternalBytesOnNode(ctx context.Context, conn *gosql.DB) (uint64, error) {
	var stats uint64
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(stats->>'external_file_bytes','0') FROM crdb_internal.tenant_span_stats(
		ARRAY(SELECT(crdb_internal.tenant_span()[1], crdb_internal.tenant_span()[2])))`).Scan(&stats); err != nil {
		return 0, err
	}
	return stats, nil
}
