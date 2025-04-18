// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprod

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func makeRetryOpts(opts ...install.RetryOptionFunc) retry.Options {
	defaultConfig := &retry.Options{
		MaxRetries: 2,
	}
	for _, opt := range opts {
		opt(defaultConfig)
	}

	return *defaultConfig
}

// WaitForSQLReady waits until the corresponding node's SQL subsystem is fully initialized and ready
// to serve SQL clients.
func WaitForSQLReady(ctx context.Context, db *gosql.DB, opts ...install.RetryOptionFunc) error {
	config := makeRetryOpts(opts...)
	return config.Do(ctx, func(ctx context.Context) error {
		_, err := db.ExecContext(ctx, "SELECT 1")
		return err
	})
}

// WaitForSQLUnavailable waits until we fail to establish a SQL connection to the corresponding node.
func WaitForSQLUnavailable(
	ctx context.Context, db *gosql.DB, l *logger.Logger, opts ...install.RetryOptionFunc,
) error {
	config := makeRetryOpts(opts...)
	return config.Do(ctx, func(ctx context.Context) error {
		if _, err := db.ExecContext(ctx, "SELECT 1"); err != nil {
			l.Printf("SQL connection was unavailable: %s", err)
			//nolint:returnerrcheck
			return nil
		}
		err := errors.New("able to establish SQL connection to node")
		l.Printf(err.Error())
		return err
	})
}

// WaitForProcessDeath checks systemd until the cockroach process is no longer marked
// as active.
func WaitForProcessDeath(
	ctx context.Context,
	c install.SyncedCluster,
	l *logger.Logger,
	node install.Nodes,
	opts ...install.RetryOptionFunc,
) error {
	config := makeRetryOpts(opts...)
	start := timeutil.Now()
	return config.Do(ctx, func(ctx context.Context) error {
		res, err := c.RunWithDetails(ctx, l, install.WithNodes(node), "WaitForProcessDeath", "systemctl is-active cockroach-system.service")
		if err != nil {
			return err
		}
		status := strings.TrimSpace(res[0].Stdout)
		if status != "active" {
			l.Printf("n%d cockroach process exited after %s: %s", node, timeutil.Since(start), status)
			return nil
		}
		err = errors.Newf("systemd reported n%d cockroach process as %s", node, status)
		l.Printf(err.Error())
		return err
	})
}

type WaitForReplicationType int

const (
	_ WaitForReplicationType = iota

	// AtLeastReplicationFactor indicates all ranges in the system should have
	// at least the replicationFactor number of replicas.
	AtLeastReplicationFactor

	// ExactlyReplicationFactor indicates that all ranges in the system should
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
	db *gosql.DB,
	l *logger.Logger,
	replicationFactor int,
	waitForReplicationType WaitForReplicationType,
	opts ...install.RetryOptionFunc,
) error {
	config := makeRetryOpts(opts...)

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
	return config.Do(ctx, func(ctx context.Context) error {
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
		return errors.Newf("waiting for %d ranges to be replicated", n)
	})
}

// WaitForUpdatedReplicationReport waits for an updated replication report.
func WaitForUpdatedReplicationReport(
	ctx context.Context, db *gosql.DB, l *logger.Logger, opts ...install.RetryOptionFunc,
) (err error) {
	config := makeRetryOpts(opts...)

	l.Printf("waiting for updated replication report...")

	// Temporarily drop the replication report interval down.
	if _, err = db.ExecContext(
		ctx, `SET CLUSTER setting kv.replication_reports.interval = '2s'`,
	); err != nil {
		return err
	}
	defer func() {
		if _, resetErr := db.ExecContext(
			ctx, `RESET CLUSTER setting kv.replication_reports.interval`,
		); resetErr != nil {
			err = errors.CombineErrors(err, resetErr)
		}
	}()

	// Wait for a new report with a timestamp after tStart to ensure
	// that the report picks up any new tables or zones.
	tStart := timeutil.Now()
	return config.Do(ctx, func(ctx context.Context) error {
		var count int
		var gen gosql.NullTime
		if err := db.QueryRowContext(
			ctx, `SELECT count(*), min(generated) FROM system.reports_meta`,
		).Scan(&count, &gen); err != nil {
			if !errors.Is(err, gosql.ErrNoRows) {
				return err
			}
			// No report generated yet. There are 3 types of reports. We want to
			// see a result for all of them.
		} else if count == 3 && tStart.Before(gen.Time) {
			// New report generated.
			return nil
		}
		if timeutil.Since(tStart) > 30*time.Second {
			l.Printf("still waiting for updated replication report")
		}
		return errors.Newf("waiting for updated replication report")
	})
}
