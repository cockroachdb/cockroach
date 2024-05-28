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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// WaitFor3XReplication is like WaitForReplication but specifically requires
// three as the minimum number of voters a range must be replicated on.
func WaitFor3XReplication(ctx context.Context, t test.Test, l *logger.Logger, db *gosql.DB) error {
	return WaitForReplication(ctx, t, l, db, 3 /* replicationFactor */, atLeastReplicationFactor)
}

// WaitForReady waits until the given nodes report ready via health checks.
// This implies that the node has completed server startup, is heartbeating its
// liveness record, and can serve SQL clients.
func WaitForReady(
	ctx context.Context, t test.Test, c cluster.Cluster, nodes option.NodeListOption,
) {
	client := roachtestutil.DefaultHTTPClient(c, t.L())
	checkReady := func(ctx context.Context, url string) error {
		resp, err := client.Get(ctx, url)
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
				url := fmt.Sprintf(`https://%s/health?ready=1`, adminAddr)

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
	l *logger.Logger,
	db *gosql.DB,
	replicationFactor int,
	waitForReplicationType waitForReplicationType,
) error {
	l.Printf("waiting for initial up-replication... (<%s)", 2*time.Minute)
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

// UsingRuntimeAssertions returns true if calls to `t.Cockroach()` for
// this test will return the cockroach build with runtime
// assertions. Note that calling this function only makes sense if the
// test uploads cockroach using `t.Cockroach` (instead of calling
// t.StandardCockroach or t.RuntimeAssertionsCockroach directly).
func UsingRuntimeAssertions(t test.Test) bool {
	return t.Cockroach() == t.RuntimeAssertionsCockroach()
}

// maybeUseMemoryBudget returns a StartOpts with the specified --max-sql-memory
// if runtime assertions are enabled, and the default values otherwise.
// A scheduled backup will not begin at the start of the roachtest.
func maybeUseMemoryBudget(t test.Test, budget int) option.StartOpts {
	startOpts := option.NewStartOpts(option.NoBackupSchedule)
	if UsingRuntimeAssertions(t) {
		// When running tests with runtime assertions enabled, increase
		// SQL's memory budget to avoid 'budget exceeded' failures.
		startOpts.RoachprodOpts.ExtraArgs = append(
			startOpts.RoachprodOpts.ExtraArgs,
			fmt.Sprintf("--max-sql-memory=%d%%", budget),
		)
	}
	return startOpts
}

// Returns the mean over the last n samples. If n > len(items), returns the mean
// over the entire items slice.
func getMeanOverLastN(n int, items []float64) float64 {
	count := n
	if len(items) < n {
		count = len(items)
	}
	sum := float64(0)
	i := 0
	for i < count {
		sum += items[len(items)-1-i]
		i++
	}
	return sum / float64(count)
}

// profileTopStatements enables profile collection on the top statements from
// the cluster that exceed 10ms latency. Top statements are defined as ones that
// have executed frequently enough to matter.
// minDuration is the minimum duration a statement to be included in profiling. Typically
// set this close to the 99.9 percentile for good results.
func profileTopStatements(
	ctx context.Context, cluster cluster.Cluster, logger *logger.Logger, minDuration time.Duration,
) error {
	db := cluster.Conn(ctx, logger, 1)
	defer db.Close()

	// Enable continuous statement diagnostics rather than just the first one.
	sql := "SET CLUSTER SETTING sql.stmt_diagnostics.collect_continuously.enabled=true"
	if _, err := db.Exec(sql); err != nil {
		return err
	}

	// The probability that a statement will be included in the profile.
	probabilityToInclude := .001

	// The minimum number of times the statement must be executed to be
	// included. By using a count here it removes the need to explicitly list
	// out all the statements we need to capture.
	minNumExpectedStmts := 1000

	sql = fmt.Sprintf(`
SELECT
    crdb_internal.request_statement_bundle(statement, %f, '%s'::INTERVAL, '12h'::INTERVAL )
FROM (
	SELECT DISTINCT statement FROM (
		SELECT metadata->>'query' AS statement, 
			CAST(statistics->'execution_statistics'->>'cnt' AS int) AS cnt 
			FROM crdb_internal.statement_statistics
		) 
	WHERE cnt > %d
)`, probabilityToInclude, minDuration, minNumExpectedStmts)
	if _, err := db.Exec(sql); err != nil {
		return err
	}
	return nil
}

// downloadProfiles downloads all profiles from the cluster and saves them to
// the given artifacts directory to the stmtbundle sub-directory.
func downloadProfiles(
	ctx context.Context, cluster cluster.Cluster, logger *logger.Logger, outputDir string,
) error {
	stmtDir := filepath.Join(outputDir, "stmtbundle")
	if err := os.MkdirAll(stmtDir, os.ModePerm); err != nil {
		return err
	}
	query := "SELECT id, collected_at FROM system.statement_diagnostics"
	db := cluster.Conn(ctx, logger, 1)
	defer db.Close()
	idRow, err := db.Query(query)
	if err != nil {
		return err
	}
	adminUIAddrs, err := cluster.ExternalAdminUIAddr(ctx, logger, cluster.Node(1))
	if err != nil {
		return err
	}

	client := roachtestutil.DefaultHTTPClient(cluster, logger)
	urlPrefix := `https://` + adminUIAddrs[0] + `/_admin/v1/stmtbundle/`

	var diagID string
	var collectedAt time.Time
	for idRow.Next() {
		if err := idRow.Scan(&diagID, &collectedAt); err != nil {
			return err
		}
		url := urlPrefix + diagID
		filename := fmt.Sprintf("%s-%s.zip", collectedAt.Format("2006-01-02T15_04_05Z07:00"), diagID)
		logger.Printf("downloading profile %s", filename)
		if err := client.Download(ctx, url, filepath.Join(stmtDir, filename)); err != nil {
			return err
		}
	}
	return nil
}

type IP struct {
	Query string
}

// getListenAddr returns the public IP address of the machine running the test.
func getListenAddr(ctx context.Context) (string, error) {
	req, err := httputil.Get(ctx, "http://ip-api.com/json/")
	if err != nil {
		return "", err
	}
	defer req.Body.Close()

	var ip IP
	if err := json.NewDecoder(req.Body).Decode(&ip); err != nil {
		return "", err
	}

	return ip.Query, nil
}
