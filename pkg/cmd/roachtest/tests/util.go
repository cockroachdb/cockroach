// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

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
// assertions.
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
// the cluster that exceed 10ms latency and are more than 10x the P99 up to this
// point. Top statements are defined as ones that have executed frequently
// enough to matter.
// NB: This doesn't really work well. The data in statement_statistics is not
// very accurate so we often use the minimum latency of 10ms.
func profileTopStatements(
	ctx context.Context, cluster cluster.Cluster, logger *logger.Logger, dbName string,
) error {
	db := cluster.Conn(ctx, logger, 1)
	defer db.Close()

	// Enable continuous statement diagnostics rather than just the first one.
	sql := "SET CLUSTER SETTING sql.stmt_diagnostics.collect_continuously.enabled=true"
	if _, err := db.Exec(sql); err != nil {
		return err
	}

	// TODO(baptist): Make these 4 constants configurable using options.
	// The probability that a statement will be included in the profile.
	probabilityToInclude := .001

	// The minimum number of times the statement must be executed to be
	// included. By using a count here it removes the need to explicitly list
	// out all the statements we need to capture.
	minNumExpectedStmts := 1000

	// minimumLatency is the minimum P99 latency in seconds. Anything lower than
	// this is rounded up to this value.
	minimumLatency := 0.001

	// multipleFromP99 is the multiple of the P99 latency that the statement
	// must exceed in order to be collected.
	multipleFromP99 := 10

	sql = fmt.Sprintf(`
SELECT
    crdb_internal.request_statement_bundle(
		statement,
		%f,
		(%d*CASE WHEN latency::FLOAT > %f THEN LATENCY::FLOAT ELSE %f END)::INTERVAL,
		'12h'::INTERVAL
	)
FROM (
	SELECT max(latency) as latency, statement FROM (
		SELECT
			metadata->>'db' AS db,
			metadata->>'query' AS statement,
			CAST(statistics->'execution_statistics'->>'cnt' AS int) AS cnt,
			statistics->'statistics'->'latencyInfo'->'p99' AS latency
			FROM crdb_internal.statement_statistics
		)
	WHERE db = '%s' AND cnt > %d AND latency::FLOAT > 0
	GROUP BY statement
)`,
		probabilityToInclude,
		multipleFromP99,
		minimumLatency,
		minimumLatency,
		dbName,
		minNumExpectedStmts,
	)
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

// EnvWorkloadDurationFlag - environment variable to override
// default run time duration of workload set in tests.
// Usage: ROACHTEST_PERF_WORKLOAD_DURATION="5m".
const EnvWorkloadDurationFlag = "ROACHTEST_PERF_WORKLOAD_DURATION"

var workloadDurationRegex = regexp.MustCompile(`^\d+[mhsMHS]$`)

// getEnvWorkloadDurationValueOrDefault validates EnvWorkloadDurationFlag and
// returns value set if valid else returns default duration.
func getEnvWorkloadDurationValueOrDefault(defaultDuration string) string {
	envWorkloadDurationFlag := os.Getenv(EnvWorkloadDurationFlag)
	if envWorkloadDurationFlag != "" && workloadDurationRegex.MatchString(envWorkloadDurationFlag) {
		return " --duration=" + envWorkloadDurationFlag
	}
	return " --duration=" + defaultDuration
}
