// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"context"
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// WaitFor3XReplication is like WaitForReplication but specifically requires
// three as the minimum number of voters a range must be replicated on.
func WaitFor3XReplication(ctx context.Context, l *logger.Logger, db *gosql.DB) error {
	return WaitForReplication(ctx, l, db, 3 /* replicationFactor */, AtLeastReplicationFactor)
}

type waitForReplicationType int

const (
	_ waitForReplicationType = iota

	// atleastReplicationFactor indicates all ranges in the system should have
	// at least the replicationFactor number of replicas.
	AtLeastReplicationFactor

	// exactlyReplicationFactor indicates that all ranges in the system should
	// have exactly the replicationFactor number of replicas.
	ExactlyReplicationFactor
)

// WaitForReplication waits until all ranges in the system are on at least or
// exactly replicationFactor number of voters, depending on the supplied
// waitForReplicationType.
// N.B. When using a multi-tenant cluster, you'll want `db` to be a connection to the _system_ tenant, not a secondary
// tenant. (Unless you _really_ just want to wait for replication of the secondary tenant's keyspace, not the whole
// system's.)
func WaitForReplication(
	ctx context.Context,
	l *logger.Logger,
	db *gosql.DB,
	replicationFactor int,
	waitForReplicationType waitForReplicationType,
) error {
	// N.B. We must ensure SQL session is fully initialized before attempting to execute any SQL commands.
	if err := WaitForSQLReady(ctx, db); err != nil {
		return errors.Wrap(err, "failed to wait for SQL to be ready")
	}

	l.Printf("waiting for initial up-replication...")
	tStart := timeutil.Now()
	var compStr string
	switch waitForReplicationType {
	case ExactlyReplicationFactor:
		compStr = "!="
	case AtLeastReplicationFactor:
		compStr = "<"
	default:
		return fmt.Errorf("unknown type %v", waitForReplicationType)
	}
	var oldN int
	for {
		var n int
		if err := db.QueryRowContext(
			ctx,
			fmt.Sprintf(
				"SELECT count(1) FROM crdb_internal.ranges WHERE array_length(replicas, 1) %s %d",
				compStr,
				replicationFactor,
			),
		).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			l.Printf("up-replication complete")
			return nil
		}
		if timeutil.Since(tStart) > 30*time.Second || oldN != n {
			l.Printf("still waiting for full replication (%d ranges left)", n)
		}
		oldN = n
		time.Sleep(time.Second)
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
		var count int
		var gen gosql.NullTime
		if err := db.QueryRowContext(
			ctx, `SELECT count(*), min(generated) FROM system.reports_meta`,
		).Scan(&count, &gen); err != nil {
			if !errors.Is(err, gosql.ErrNoRows) {
				t.Fatal(err)
			}
			// No report generated yet. There are 3 types of reports. We want to
			// see a result for all of them.
		} else if count == 3 && tStart.Before(gen.Time) {
			// New report generated.
			return
		}
		if timeutil.Since(tStart) > 30*time.Second {
			t.L().Printf("still waiting for updated replication report")
		}
	}
}
