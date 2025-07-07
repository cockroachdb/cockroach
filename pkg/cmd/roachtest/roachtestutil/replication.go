// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"context"
	gosql "database/sql"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

// WaitFor3XReplication is like WaitForReplication but specifically requires
// three as the minimum number of voters a range must be replicated on.
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

// WaitForUpdatedReplicationReport waits for an updated replication report.
func WaitForUpdatedReplicationReport(ctx context.Context, t test.Test, db *gosql.DB) {
	if err := roachprod.WaitForUpdatedReplicationReport(ctx, db, t.L(), install.WithMaxRetries(0)); err != nil {
		t.Fatal(err)
	}
}
