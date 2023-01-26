// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keyvisstorage

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// DeleteSamplesBeforeTime deletes collected samples that were taken before time.
func DeleteSamplesBeforeTime(ctx context.Context, ie *sql.InternalExecutor, t time.Time) error {

	// Delete samples and sample buckets that have expired.
	stmt := fmt.Sprintf(
		"WITH "+
			"	deleted_rows AS ("+
			"		DELETE FROM system.span_stats_samples "+
			"		WHERE sample_time < CAST(%d AS TIMESTAMP)"+
			"		RETURNING id"+
			"	)"+
			"DELETE FROM system.span_stats_buckets "+
			"WHERE sample_id IN (SELECT id FROM deleted_rows)",
		t.Unix())

	_, err := ie.ExecEx(ctx, "delete-expired-samples", nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()}, stmt)

	if err != nil {
		return err
	}

	// Delete keys that are no longer referenced by any buckets.
	deleteKeysStmt := "" +
		"DELETE FROM system.span_stats_unique_keys " +
		"WHERE " +
		"	NOT EXISTS (" +
		"		SELECT * " +
		"		FROM system.span_stats_buckets " +
		"		WHERE " +
		"			system.span_stats_buckets.start_key_id = system.span_stats_unique_keys.id " +
		"			OR system.span_stats_buckets.end_key_id = system.span_stats_unique_keys.id" +
		"	)"

	_, err = ie.ExecEx(
		ctx,
		"delete-unused-start-keys",
		nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		deleteKeysStmt,
	)
	return err
}
