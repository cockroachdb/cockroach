// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// fkTxnTableNames returns the table names fktxn will produce for numTables.
// The workload uses randgen.RandCreateTables with prefix "t" and a 1-based
// table index, so names are deterministic and independent of the seed.
func fkTxnTableNames(numTables int) []string {
	names := make([]string, numTables)
	for i := range names {
		names[i] = fmt.Sprintf("t%d", i+1)
	}
	return names
}

// newFKTxnSchemaSeedSource returns a function that yields a schema seed per
// call. When COCKROACH_RANDOM_SEED is set, every call returns that seed — the
// retry loop will burn its budget on the same schema, which is what we want
// when reproducing a specific failure. Otherwise each call draws a fresh seed
// from a shared rng so the loop tries different schemas.
func newFKTxnSchemaSeedSource() func() int64 {
	if v, ok := os.LookupEnv("COCKROACH_RANDOM_SEED"); ok {
		if seed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return func() int64 { return seed }
		}
	}
	rng, _ := randutil.NewPseudoRand()
	return func() int64 { return rng.Int63() }
}

func TestLDRFKTxn(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, ldrConfig ldrConfig,
) {
	const numIterations = 5
	for iteration := 1; iteration <= numIterations; iteration++ {
		t.L().Printf("=== iteration %d/%d ===", iteration, numIterations)
		runLDRFKTxnIteration(ctx, t, c, setup, ldrConfig)
	}
}

// runLDRFKTxnIteration executes one full pass of the fktxn workload: it
// resets both clusters' fktxn database, finds a schema seed accepted by
// LDR, runs the workload, and verifies that left and right fingerprints
// match. Each call is self-contained so TestLDRFKTxn can invoke it
// repeatedly on the same cluster.
func runLDRFKTxnIteration(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, ldrConfig ldrConfig,
) {
	duration := 10 * time.Minute
	const numTables = 10
	workers := 16
	if c.IsLocal() {
		duration = 2 * time.Minute
		workers = 8
	}

	const dbName = "fktxn"
	const fkDensity = 0.4
	const maxSeedAttempts = 50

	tableNames := fkTxnTableNames(numTables)

	getSeed := newFKTxnSchemaSeedSource()
	var seed int64
	var leftJobID, rightJobID int
	var lastErr error
	// The workload itself has some notion of what isn't allowed in LDR txn mode and disallows
	// it in schema creation. We perform a second validation here by attempting to spin up
	// an LDR job and making sure it doesn't immediately error. The distinction between what should
	// be retried instead of explicit filters in the workload are features that we expect to eventually
	// support. This way when we do add support, we get testing coverage for free without having to
	// remember
	for attempt := 1; attempt <= maxSeedAttempts; attempt++ {
		seed = getSeed()
		t.L().Printf("attempt %d: trying fktxn schema with seed=%d", attempt, seed)

		// Reset both clusters to a clean slate. Cancel any LDR jobs from a
		// prior attempt before dropping the database — CASCADE drops tables
		// but won't stop the jobs that reference them.
		cancelAllLDRJobs(t, setup.left.sysSQL)
		cancelAllLDRJobs(t, setup.right.sysSQL)
		setup.left.sysSQL.Exec(t, fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", dbName))
		setup.right.sysSQL.Exec(t, fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", dbName))
		setup.left.sysSQL.Exec(t, fmt.Sprintf("CREATE DATABASE %s", dbName))
		setup.right.sysSQL.Exec(t, fmt.Sprintf("CREATE DATABASE %s", dbName))

		var err error
		leftJobID, rightJobID, err = initFKTxnAndStartLDR(ctx, t, c, setup, dbName, tableNames, seed, numTables, fkDensity, ldrConfig)
		if err != nil {
			lastErr = err
			t.L().Printf("attempt %d failed: %v", attempt, err)
			continue
		}
		t.L().Printf("attempt %d succeeded with seed=%d", attempt, seed)
		break
	}
	if leftJobID == 0 && rightJobID == 0 {
		t.Fatalf("no fktxn schema accepted by LDR after %d attempts; last error: %v",
			maxSeedAttempts, lastErr)
	}

	workloadDoneCh := make(chan struct{})
	// We are not too concerned with latency as long as we are making _some_ progress
	// and eventually finish.
	maxExpectedLatency := 6 * time.Minute
	group := t.NewGroup()
	validateLatency := setupLatencyVerifiersWithGroup(t, c, group, leftJobID, rightJobID, setup, workloadDoneCh, maxExpectedLatency)

	group.Go(func(ctx context.Context, _ *logger.Logger) error {
		defer close(workloadDoneCh)
		cmd := fmt.Sprintf("./workload run fktxn --workers=%d --duration=%s --row-pool-size=100 --db=%s {pgurl%s:system}",
			workers, duration, dbName, setup.left.nodes)
		c.Run(ctx, option.WithNodes(setup.workloadNode), cmd)
		return nil
	})

	group.Wait()
	validateLatency()

	// Wait for both sides to catch up and assert that per-table fingerprints
	// match. We skip the DLQ check used by VerifyCorrectness because
	// transactional-mode LDR doesn't create DLQ tables.
	now := timeutil.Now()
	t.Status("waiting for replicated times to catchup before verifying left and right clusters")
	if leftJobID != 0 {
		waitForReplicatedTimeToReachTimestamp(t, leftJobID, setup.left.db, getLogicalDataReplicationJobInfo, 10*time.Minute, now)
	}
	waitForReplicatedTimeToReachTimestamp(t, rightJobID, setup.right.db, getLogicalDataReplicationJobInfo, 10*time.Minute, now)

	t.Status("verifying equality of left and right clusters")
	for _, tableName := range tableNames {
		fpQuery := fmt.Sprintf("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s.%s", dbName, tableName)
		left := setup.left.sysSQL.QueryStr(t, fpQuery)
		right := setup.right.sysSQL.QueryStr(t, fpQuery)
		require.Equal(t, left, right, "fingerprint mismatch for table %s", tableName)
	}
}

// initFKTxnAndStartLDR runs `workload init fktxn` on both clusters and
// issues a unidirectional CREATE LOGICAL REPLICATION STREAM.
func initFKTxnAndStartLDR(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	setup multiClusterSetup,
	dbName string,
	tableNames []string,
	seed int64,
	numTables int,
	fkDensity float64,
	ldrConfig ldrConfig,
) (leftJobID, rightJobID int, _ error) {
	for _, side := range []*clusterInfo{setup.left, setup.right} {
		initCmd := fmt.Sprintf(
			"./workload init fktxn --seed=%d --num-tables=%d --fk-density=%g --db=%s {pgurl:%d:system}",
			seed, numTables, fkDensity, dbName, side.nodes[0],
		)
		if err := c.RunE(ctx, option.WithNodes(setup.workloadNode), initCmd); err != nil {
			return 0, 0, errors.Wrap(err, "fktxn init")
		}
	}

	tableNamesStr := "(" + dbName + "." + tableNames[0]
	for _, name := range tableNames[1:] {
		tableNamesStr += ", " + dbName + "." + name
	}
	tableNamesStr += ")"

	externalConnCmd := "CREATE EXTERNAL CONNECTION IF NOT EXISTS '%s' AS '%s'"
	setup.right.sysSQL.Exec(t, fmt.Sprintf(externalConnCmd, leftExternalConn.Host, setup.left.PgURLForDatabase(dbName)))

	options := ""
	if ldrConfig.mode != Default {
		options = fmt.Sprintf("WITH mode='%s'", ldrConfig.mode)
	}
	if _, err := setup.right.db.ExecContext(ctx, fmt.Sprintf("USE %s", dbName)); err != nil {
		return 0, 0, errors.Wrap(err, "USE database on right")
	}
	ldrCmd := fmt.Sprintf("CREATE LOGICAL REPLICATION STREAM FROM TABLES %s ON $1 INTO TABLES %s %s",
		tableNamesStr, tableNamesStr, options)
	if err := setup.right.db.QueryRowContext(ctx, ldrCmd, leftExternalConn.String()).Scan(&rightJobID); err != nil {
		return 0, 0, errors.Wrap(err, "CREATE LOGICAL REPLICATION STREAM")
	}

	initialScanTimeout := 2 * time.Minute
	if ldrConfig.initialScanTimeout != 0 {
		initialScanTimeout = ldrConfig.initialScanTimeout
	}
	if err := waitForReplicatedTimeE(ctx, rightJobID, setup.right.db, initialScanTimeout); err != nil {
		return 0, 0, errors.Wrap(err, "waiting for right initial scan")
	}
	return 0, rightJobID, nil
}

// cancelAllLDRJobs cancels every running LDR job on the cluster. Used
// between attempts in TestLDRFKTxn so a failed CREATE LOGICAL REPLICATION
// STREAM can leave behind a partial job without blocking the next attempt.
func cancelAllLDRJobs(t test.Test, sql *sqlutils.SQLRunner) {
	rows := sql.QueryStr(t, "SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'LOGICAL REPLICATION' AND status IN ('running', 'pending', 'paused')")
	for _, row := range rows {
		sql.Exec(t, fmt.Sprintf("CANCEL JOB %s", row[0]))
	}
}

// waitForReplicatedTimeE polls the LDR job's progress until it reports a
// non-zero high-water mark or wait elapses. Returns an error on timeout
// instead of failing the test, so the caller can retry with a different
// seed.
func waitForReplicatedTimeE(
	ctx context.Context, jobID int, db *gosql.DB, wait time.Duration,
) error {
	deadline := timeutil.Now().Add(wait)
	for {
		info, err := getLogicalDataReplicationJobInfo(db, jobID)
		if err == nil && !info.GetHighWater().IsZero() {
			return nil
		}
		if timeutil.Now().After(deadline) {
			if err != nil {
				return errors.Wrapf(err, "job %d never reached non-zero high-water within %s", jobID, wait)
			}
			return errors.Newf("job %d never reached non-zero high-water within %s", jobID, wait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
