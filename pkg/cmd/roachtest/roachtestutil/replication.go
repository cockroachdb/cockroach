// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

// WaitFor3XReplication is like WaitForReplication but specifically requires
// three as the minimum number of voters a range must be replicated on.
//
// TODO(tbg): consider replacing callers with WaitForFullReplication,
// which checks each range against its own resolved zone config rather than
// a hardcoded replication factor.
func WaitFor3XReplication(ctx context.Context, l *logger.Logger, db *gosql.DB) error {
	return WaitForReplication(ctx, l, db, 3 /* replicationFactor */, roachprod.AtLeastReplicationFactor)
}

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
	waitForReplicationType roachprod.WaitForReplicationType,
) error {
	// N.B. We must ensure SQL session is fully initialized before attempting to execute any SQL commands.
	if err := WaitForSQLReady(ctx, db); err != nil {
		return errors.Wrap(err, "failed to wait for SQL to be ready")
	}
	return roachprod.WaitForReplication(ctx, db, l, replicationFactor, waitForReplicationType, install.RetryEveryDuration(time.Second))
}

// WaitForFullReplication polls until every range matching the optional
// predicate has at least as many replicas as its resolved zone config demands.
// It uses SHOW CLUSTER RANGES WITH ZONE to compare each range's actual replica
// count against the numReplicas field from its (inheritance-resolved) zone
// configuration.
//
// The predicate, if non-empty, is ANDed into the WHERE clause and may reference
// any column from SHOW CLUSTER RANGES WITH TABLES, ZONE (e.g.
// "database_name = 'system'"). Pass "" to check all ranges.
func WaitForFullReplication(
	ctx context.Context, l *logger.Logger, db *gosql.DB, predicate string,
) error {
	where := `WHERE array_length(replicas, 1) < (zone_config->>'numReplicas')::INT`
	if predicate != "" {
		where += "\nAND (" + predicate + ")"
	}
	q := `SELECT range_id, start_key,
	array_length(replicas, 1) AS actual,
	(zone_config->>'numReplicas')::INT AS desired,
	count(*) OVER () AS total
FROM [SHOW CLUSTER RANGES WITH TABLES, ZONE] ` + where + `
ORDER BY range_id LIMIT 10`

	const interval = 10 * time.Second
	const logEvery = 6 // log every 6 iterations = 60s
	for i := 0; ; i++ {
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			return errors.Wrap(err, "querying replication status")
		}
		var total int
		var buf strings.Builder
		for rows.Next() {
			var rangeID, actual, desired int
			var startKey string
			if err := rows.Scan(&rangeID, &startKey, &actual, &desired, &total); err != nil {
				rows.Close()
				return errors.Wrap(err, "scanning replication status")
			}
			fmt.Fprintf(&buf, "\n  r%d %s (%d/%d replicas)", rangeID, startKey, actual, desired)
		}
		if err := rows.Close(); err != nil {
			return errors.Wrap(err, "querying replication status")
		}
		if total == 0 {
			l.Printf("all ranges fully replicated")
			return nil
		}
		if i > 0 && i%logEvery == 0 {
			l.Printf("waiting for %d ranges to reach desired replication factor%s",
				total, buf.String())
		}
		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(),
				"%d ranges still under-replicated%s", total, buf.String())
		case <-time.After(interval):
		}
	}
}

// WaitForUpdatedReplicationReport waits for an updated replication report.
func WaitForUpdatedReplicationReport(ctx context.Context, t test.Test, db *gosql.DB) {
	if err := roachprod.WaitForUpdatedReplicationReport(ctx, db, t.L(), install.WithMaxRetries(0)); err != nil {
		t.Fatal(err)
	}
}
