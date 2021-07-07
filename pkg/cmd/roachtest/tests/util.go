// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// WaitFor3XReplication waits until all ranges in the system are on at least
// three voters.
//
// TODO(nvanbenschoten): this function should take a context and be responsive
// to context cancellation.
func WaitFor3XReplication(t test.Test, db *gosql.DB) {
	t.L().Printf("waiting for up-replication...")
	tStart := timeutil.Now()
	for ok := false; !ok; time.Sleep(time.Second) {
		if err := db.QueryRow(
			"SELECT min(array_length(replicas, 1)) >= 3 FROM crdb_internal.ranges",
		).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		if timeutil.Since(tStart) > 30*time.Second {
			t.L().Printf("still waiting for full replication")
		}
	}
}

// WaitForUpdatedReplicationReport waits for an updated replication report.
func WaitForUpdatedReplicationReport(ctx context.Context, t test.Test, db *gosql.DB) {
	t.L().Printf("waiting for updated replication report...")

	// Temporarily drop the replication report interval down.
	if _, err := db.ExecContext(
		ctx, `SET CLUSTER setting kv.replication_reports.interval = '2s'`,
	); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := db.ExecContext(
			ctx, `RESET CLUSTER setting kv.replication_reports.interval`,
		); err != nil {
			t.Fatal(err)
		}
	}()

	// Wait for a new report with a timestamp after tStart to ensure
	// that the report picks up any new tables or zones.
	tStart := timeutil.Now()
	for r := retry.StartWithCtx(ctx, retry.Options{}); r.Next(); {
		var gen time.Time
		if err := db.QueryRowContext(
			ctx, `SELECT generated FROM system.reports_meta ORDER BY 1 DESC LIMIT 1`,
		).Scan(&gen); err != nil {
			if !errors.Is(err, gosql.ErrNoRows) {
				t.Fatal(err)
			}
			// No report generated yet.
		} else if tStart.Before(gen) {
			// New report generated.
			return
		}
		if timeutil.Since(tStart) > 30*time.Second {
			t.L().Printf("still waiting for updated replication report")
		}
	}
}
