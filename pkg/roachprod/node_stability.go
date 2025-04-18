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

type RetryConfig struct {
	retry.Options
	timeout       time.Duration
	verboseLogger *logger.Logger
}

func (c *RetryConfig) logVerbose(format string, args ...any) {
	if c.verboseLogger != nil {
		c.verboseLogger.Printf(format, args...)
	}
}

type RetryConfigFunc func(*RetryConfig)

// WithRetryOpts sets the retry options for the function. Used if the caller
// wants to set more custom options not provided by the common opts below.
func WithRetryOpts(options retry.Options) RetryConfigFunc {
	return func(config *RetryConfig) {
		config.Options = options
	}
}

// WithMaxRetries will retry the function up to maxRetries times.
func WithMaxRetries(maxRetries int) RetryConfigFunc {
	return func(config *RetryConfig) {
		config.Options.MaxRetries = maxRetries
	}
}

// RetryEveryDuration will retry the function every duration until it succeeds
// or the context is cancelled. This is useful for when we want to see incremental
// progress that is not subject to the backoff/jitter.
func RetryEveryDuration(duration time.Duration) RetryConfigFunc {
	return func(config *RetryConfig) {
		config.Options.MaxRetries = 0
		config.Options.Multiplier = 1
		config.Options.InitialBackoff = duration
	}
}

// WithTimeout sets the timeout for the function until the context is cancelled.
func WithTimeout(timeout time.Duration) RetryConfigFunc {
	return func(config *RetryConfig) {
		config.timeout = timeout
	}
}

// WithVerboseLogger will log additional information. This is default nil as these
// functions may be called by, e.g. cluster startup, and we don't want to pollute the
// logs.
func WithVerboseLogger(logger *logger.Logger) RetryConfigFunc {
	return func(config *RetryConfig) {
		config.verboseLogger = logger
	}
}

func getRetryConfig(opts ...RetryConfigFunc) RetryConfig {
	defaultConfig := RetryConfig{
		Options: retry.Options{
			MaxRetries: 2,
		},
	}

	for _, opt := range opts {
		opt(&defaultConfig)
	}

	return defaultConfig
}

func maybeSetTimeout(ctx context.Context, config RetryConfig) (context.Context, func()) {
	if config.timeout > 0 {
		return context.WithTimeout(ctx, config.timeout)
	}
	return ctx, func() {}
}

// WaitForSQLReady waits until the corresponding node's SQL subsystem is fully initialized and ready
// to serve SQL clients.
func WaitForSQLReady(ctx context.Context, db *gosql.DB, opts ...RetryConfigFunc) error {
	config := getRetryConfig(opts...)
	timeoutCtx, cancel := maybeSetTimeout(ctx, config)
	defer cancel()
	return config.Do(timeoutCtx, func(ctx context.Context) error {
		_, err := db.ExecContext(ctx, "SELECT 1")
		return err
	})
}

// WaitForSQLUnavailable waits until we fail to establish a SQL connection to the corresponding node.
func WaitForSQLUnavailable(ctx context.Context, db *gosql.DB, opts ...RetryConfigFunc) error {
	config := getRetryConfig(opts...)
	timeoutCtx, cancel := maybeSetTimeout(ctx, config)
	defer cancel()
	return config.Do(timeoutCtx, func(ctx context.Context) error {
		if _, err := db.ExecContext(ctx, "SELECT 1"); err != nil {
			config.logVerbose("SQL connection was unavailable: %s", err)
			//nolint:returnerrcheck
			return nil
		}
		err := errors.New("able to establish SQL connection to node")
		config.logVerbose(err.Error())
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
	opts ...RetryConfigFunc,
) error {
	config := getRetryConfig(opts...)
	timeoutCtx, cancel := maybeSetTimeout(ctx, config)
	defer cancel()

	start := timeutil.Now()
	return config.Do(timeoutCtx, func(ctx context.Context) error {
		res, err := c.RunWithDetails(ctx, l, install.WithNodes(node), "WaitForProcessDeath", "systemctl is-active cockroach-system.service")
		if err != nil {
			return err
		}
		status := strings.TrimSpace(res[0].Stdout)
		if status != "active" {
			config.logVerbose("n%d cockroach process exited after %s: %s", node, timeutil.Since(start), status)
			return nil
		}
		err = errors.Newf("systemd reported n%d cockroach process as %s", node, status)
		config.logVerbose(err.Error())
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
	replicationFactor int,
	waitForReplicationType WaitForReplicationType,
	opts ...RetryConfigFunc,
) error {
	config := getRetryConfig(opts...)
	timeoutCtx, cancel := maybeSetTimeout(ctx, config)
	defer cancel()

	config.logVerbose("waiting for initial up-replication...")
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
	return config.Do(timeoutCtx, func(ctx context.Context) error {
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
			config.logVerbose("up-replication complete")
			return nil
		}
		if timeutil.Since(tStart) > 30*time.Second || oldN != n {
			config.logVerbose("still waiting for full replication (%d ranges left)", n)
		}
		oldN = n
		return errors.Newf("waiting for %d ranges to be replicated", n)
	})
}

// WaitForUpdatedReplicationReport waits for an updated replication report.
func WaitForUpdatedReplicationReport(
	ctx context.Context, db *gosql.DB, opts ...RetryConfigFunc,
) (err error) {
	config := getRetryConfig(opts...)
	timeoutCtx, cancel := maybeSetTimeout(ctx, config)
	defer cancel()

	config.logVerbose("waiting for updated replication report...")

	// Temporarily drop the replication report interval down.
	if _, err = db.ExecContext(
		timeoutCtx, `SET CLUSTER setting kv.replication_reports.interval = '2s'`,
	); err != nil {
		return err
	}
	defer func() {
		if _, resetErr := db.ExecContext(
			timeoutCtx, `RESET CLUSTER setting kv.replication_reports.interval`,
		); resetErr != nil {
			err = errors.CombineErrors(err, resetErr)
		}
	}()

	// Wait for a new report with a timestamp after tStart to ensure
	// that the report picks up any new tables or zones.
	tStart := timeutil.Now()
	return config.Do(timeoutCtx, func(ctx context.Context) error {
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
			config.logVerbose("still waiting for updated replication report")
		}
		return errors.Newf("waiting for updated replication report")
	})
}
