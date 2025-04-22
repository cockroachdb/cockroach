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

type unbalancedRanges struct {
	// The store id of the unbalanced stores.
	storeIDs []int
	// Range counts for each store in storeIDs.
	rangeCounts []int
	// Average range count across all stores, not just the unbalanced ones.
	avgRangeCount float64
}

func findUnbalancedStores(
	ctx context.Context, db *gosql.DB, threshold float64,
) (unbalancedRanges, error) {
	lowerBound := 1 - threshold
	upperBound := 1 + threshold

	query := fmt.Sprintf(`WITH stats AS (
    SELECT avg(range_count) AS mean_val
    FROM crdb_internal.kv_store_status
)
SELECT store_id, range_count, stats.mean_val
FROM crdb_internal.kv_store_status, stats
WHERE range_count < mean_val * %f
   OR range_count > mean_val * %f;
`, lowerBound, upperBound)

	var unablancedStores []int
	var rangeCounts []int
	var avgRanges float64
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return unbalancedRanges{}, err
	}
	for rows.Next() {
		var storeID, ranges int
		if err = rows.Scan(&storeID, &ranges, &avgRanges); err != nil {
			return unbalancedRanges{}, err
		}
		unablancedStores = append(unablancedStores, storeID)
		rangeCounts = append(rangeCounts, ranges)
	}
	return unbalancedRanges{
		storeIDs:      unablancedStores,
		rangeCounts:   rangeCounts,
		avgRangeCount: avgRanges,
	}, nil
}

// WaitForBalancedReplicas blocks until the replica count across each store is less than
// `range_rebalance_threshold` percent from the mean. Note that this doesn't wait for
// rebalancing to _fully_ finish; there can still be range events that happen after this.
// We don't know what kind of background workloads may be running concurrently and creating
// range events, so lets just get to a state "close enough", i.e. a state that the allocator
// would consider balanced.
func WaitForBalancedReplicas(
	ctx context.Context, db *gosql.DB, l *logger.Logger, opts ...install.RetryOptionFunc,
) error {
	config := makeRetryOpts(opts...)

	// This is the threshold the db uses to determine that a store is under or overfull
	// and needs to be rebalanced.
	var threshold float64
	if err := db.QueryRowContext(
		ctx, "SHOW CLUSTER SETTING kv.allocator.range_rebalance_threshold",
	).Scan(&threshold); err != nil {
		return err
	}

	// If we query too soon after a node is added to the cluster, our calculation may
	// not include those stores. Make sure we observe a few consecutive intervals that confirm
	// our ranges are stable.
	consecutiveStableIntervals := 0
	return config.Do(ctx, func(ctx context.Context) error {
		unbalanced, err := findUnbalancedStores(ctx, db, threshold)
		if err != nil {
			return err
		}

		if len(unbalanced.storeIDs) == 0 {
			consecutiveStableIntervals++
			l.Printf("all stores have range count within %.2f%% of the mean", threshold*100)
			if consecutiveStableIntervals > 2 {
				return nil
			}
		} else {
			l.Printf("unbalanced stores: %v, ranges: %v, avg: %f", unbalanced.storeIDs, unbalanced.rangeCounts, unbalanced.avgRangeCount)
			consecutiveStableIntervals = 0
		}
		return errors.Newf("only %d consecutive intervals of balanced stores", consecutiveStableIntervals)
	})
}
