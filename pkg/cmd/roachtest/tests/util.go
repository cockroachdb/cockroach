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
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// WaitFor3XReplication is like WaitForReplication but specifically requires
// three as the minimum number of voters a range must be replicated on.
func WaitFor3XReplication(ctx context.Context, t test.Test, db *gosql.DB) error {
	return WaitForReplication(ctx, t, db, 3 /* replicationFactor */, atLeastReplicationFactor)
}

// WaitForReady waits until the given nodes report ready via health checks.
// This implies that the node has completed server startup, is heartbeating its
// liveness record, and can serve SQL clients.
func WaitForReady(
	ctx context.Context, t test.Test, c cluster.Cluster, nodes option.NodeListOption,
) {
	checkReady := func(ctx context.Context, url string) error {
		resp, err := httputil.Get(ctx, url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("HTTP %d: %s", resp.StatusCode, body)
		}
		return nil
	}

	adminAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), nodes)
	require.NoError(t, err)

	require.NoError(t, timeutil.RunWithTimeout(
		ctx, "waiting for ready", time.Minute, func(ctx context.Context) error {
			for i, adminAddr := range adminAddrs {
				url := fmt.Sprintf(`http://%s/health?ready=1`, adminAddr)

				for err := checkReady(ctx, url); err != nil; err = checkReady(ctx, url) {
					t.L().Printf("n%d not ready, retrying: %s", nodes[i], err)
					time.Sleep(time.Second)
				}
				t.L().Printf("n%d is ready", nodes[i])
			}
			return nil
		},
	))
}

type waitForReplicationType int

const (
	_ waitForReplicationType = iota

	// atleastReplicationFactor indicates all ranges in the system should have
	// at least the replicationFactor number of replicas.
	atLeastReplicationFactor

	// exactlyReplicationFactor indicates that all ranges in the system should
	// have exactly the replicationFactor number of replicas.
	exactlyReplicationFactor
)

// WaitForReplication waits until all ranges in the system are on at least or
// exactly replicationFactor number of voters, depending on the supplied
// waitForReplicationType.
func WaitForReplication(
	ctx context.Context,
	t test.Test,
	db *gosql.DB,
	replicationFactor int,
	waitForReplicationType waitForReplicationType,
) error {
	t.L().Printf("waiting for initial up-replication... (<%s)", 2*time.Minute)
	tStart := timeutil.Now()
	var compStr string
	switch waitForReplicationType {
	case exactlyReplicationFactor:
		compStr = "!="
	case atLeastReplicationFactor:
		compStr = "<"
	default:
		t.Fatalf("unknown type %v", waitForReplicationType)
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
			t.L().Printf("up-replication complete")
			return nil
		}
		if timeutil.Since(tStart) > 30*time.Second || oldN != n {
			t.L().Printf("still waiting for full replication (%d ranges left)", n)
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

// setAdmissionControl sets the admission control cluster settings on the
// given cluster.
func setAdmissionControl(ctx context.Context, t test.Test, c cluster.Cluster, enabled bool) {
	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()
	val := "true"
	if !enabled {
		val = "false"
	}
	for _, setting := range []string{
		"admission.kv.enabled",
		"admission.sql_kv_response.enabled",
		"admission.sql_sql_response.enabled",
		"admission.elastic_cpu.enabled",
	} {
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING "+setting+" = '"+val+"'"); err != nil {
			t.Fatalf("failed to set admission control to %t: %v", enabled, err)
		}
	}
	if !enabled {
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING admission.kv.pause_replication_io_threshold = 0.0"); err != nil {
			t.Fatalf("failed to set admission control to %t: %v", enabled, err)
		}
	}
}

// maybeUseBuildWithEnabledAssertions stages the cockroach-short binary with
// enabled assertions with eaProb probability if that binary is available,
// otherwise stages the regular cockroach binary, and starts the cluster.
func maybeUseBuildWithEnabledAssertions(
	ctx context.Context, t test.Test, c cluster.Cluster, rng *rand.Rand, eaProb float64,
) {
	if rng.Float64() < eaProb {
		// Check whether the cockroach-short binary is available.
		if t.CockroachShort() != "" {
			randomSeed := rng.Int63()
			t.Status(
				"using cockroach-short binary compiled with --crdb_test "+
					"build tag and COCKROACH_RANDOM_SEED=", randomSeed,
			)
			c.Put(ctx, t.CockroachShort(), "./cockroach")
			// We need to ensure that all nodes in the cluster start with the
			// same random seed (if not, some assumptions can be violated - for
			// example that coldata.BatchSize() values are the same on all
			// nodes).
			settings := install.MakeClusterSettings()
			settings.Env = append(settings.Env, fmt.Sprintf("COCKROACH_RANDOM_SEED=%d", randomSeed))
			c.Start(ctx, t.L(), option.DefaultStartOpts(), settings)
			return
		}
	}
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
}
