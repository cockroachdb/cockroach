// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package roachtestutil

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// CheckReplicaDivergenceOnDB runs a consistency check via the provided DB. It
// ignores transient errors that can result from the implementation of
// crdb_internal.check_consistency, so a nil result does not prove anything.
//
// This will take storage checkpoints and terminate nodes if an inconsistency is
// found, since all roachtests set COCKROACH_INTERNAL_CHECK_CONSISTENCY_FATAL.
// These checkpoints will be collected in the test artifacts under
// "checkpoints". An error is also returned in this case.
//
// The consistency check may not get enough time to complete, but will return
// any inconsistencies that it did find before timing out.
func CheckReplicaDivergenceOnDB(ctx context.Context, l *logger.Logger, db *gosql.DB) error {
	// Cancel context if we error out, so the check doesn't keep running.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Speed up consistency checks. The test is done, so let's go full throttle.
	_, err := db.ExecContext(ctx, "SET CLUSTER SETTING server.consistency_check.max_rate = '1GB'")
	if err != nil {
		return errors.Wrap(err, "unable to set 'server.consistency_check.max_rate'")
	}

	// NB: we set a statement_timeout since context cancellation won't work here.
	// We've seen the consistency checks hang indefinitely in some cases.
	// https://github.com/cockroachdb/cockroach/pull/34520
	//
	// TODO(erikgrinaker): avoid result set buffering. We seem to be receiving
	// results in batches of 64 rows, regardless of results_buffer_size or the
	// row size (e.g. with 16 KB ballast per row). Not clear where this buffering
	// is happening or how to disable it.
	started := timeutil.Now()
	rows, err := db.QueryContext(ctx, `
SET statement_timeout = '20m';
SELECT t.range_id, t.start_key_pretty, t.status, t.detail
FROM crdb_internal.check_consistency(false, '', '') as t;`)
	if err != nil {
		// TODO(tbg): the checks can fail for silly reasons like missing gossiped
		// descriptors, etc. -- not worth failing the test for. Ideally this would
		// be rock solid.
		l.Printf("consistency check failed with %v; ignoring", err)
		return nil
	}
	defer rows.Close()

	logEvery := Every(time.Minute)
	logEvery.ShouldLog() // don't immediately log

	var ranges int
	for rows.Next() {
		var rangeID int32
		var prettyKey, status, detail string
		if scanErr := rows.Scan(&rangeID, &prettyKey, &status, &detail); scanErr != nil {
			l.Printf("consistency check failed with %v; ignoring", scanErr)
			return nil
		}
		// Only detect replica inconsistencies, and ignore MVCC stats mismatches
		// since these can happen in rare cases due to lease requests not respecting
		// latches: https://github.com/cockroachdb/cockroach/issues/93896
		if status == kvpb.CheckConsistencyResponse_RANGE_INCONSISTENT.String() {
			return errors.Newf("r%d (%s) is inconsistent: %s %s\n", rangeID, prettyKey, status, detail)
		}

		ranges++
		if logEvery.ShouldLog() {
			l.Printf("consistency checked %d ranges (at key %s)", ranges, prettyKey)
		}
	}
	l.Printf("consistency checked %d ranges in %s",
		ranges, timeutil.Since(started).Round(time.Second))

	if err := rows.Err(); err != nil {
		l.Printf("consistency check failed with %v; ignoring", err)
	}
	return nil
}

// CheckInvalidDescriptors returns an error if there exists any descriptors in
// the crdb_internal.invalid_objects virtual table.
func CheckInvalidDescriptors(ctx context.Context, db *gosql.DB) error {
	var invalidIDs string
	if err := timeutil.RunWithTimeout(ctx, "descriptor validation", time.Minute, func(ctx context.Context) error {
		// Because crdb_internal.invalid_objects is a virtual table, by default, the
		// query will take a lease on the database sqlDB is connected to and only run
		// the query on the given database. The "" prefix prevents this lease
		// acquisition and allows the query to fetch all descriptors in the cluster.
		rows, err := db.QueryContext(ctx, `SELECT id, obj_name, error FROM "".crdb_internal.invalid_objects`)
		if err != nil {
			return err
		}
		invalidIDs, err = sqlutils.RowsToDataDrivenOutput(rows)
		return err
	}); err != nil {
		return err
	}

	if invalidIDs != "" {
		return errors.Errorf("the following descriptor ids are invalid\n%v", invalidIDs)
	}
	return nil
}

// CheckInspectDatabase runs INSPECT DATABASE in parallel on user databases
// to verify consistency. System databases (system, postgres, defaultdb) are
// excluded. All INSPECT jobs run concurrently under a time budget. Once the
// budget is exceeded, we check if any errors were found. And return success if
// none.
//
// If the cluster version does not support INSPECT (requires v25.4+), the
// check is skipped and returns nil without error.
func CheckInspectDatabase(
	ctx context.Context, l *logger.Logger, db *gosql.DB, timeout time.Duration,
) error {
	databases, err := discoverUserDatabases(ctx, db)
	if err != nil {
		return err
	}
	if len(databases) == 0 {
		l.Printf("No user databases found, skipping INSPECT check")
		return nil
	}

	l.Printf("Found %d user databases to INSPECT: %v", len(databases), databases)
	l.Printf("INSPECT timeout budget: %s total", timeout)

	if err := launchInspectJobs(ctx, l, db, databases); err != nil {
		// If INSPECT is not supported due to cluster version, skip the check.
		if isFeatureNotSupportedError(err) {
			l.Printf("INSPECT not supported on this cluster version (requires v25.4+), skipping validation")
			return nil
		}
		return err
	}

	jobIDs, err := fetchInspectJobIDs(ctx, db, len(databases))
	if err != nil {
		return err
	}
	l.Printf("Found %d INSPECT jobs: %v", len(jobIDs), jobIDs)

	waitForInspectJobCompletion(ctx, l, db, jobIDs, timeout)

	totalErrors, errorDetails, err := collectInspectErrors(ctx, db, jobIDs)
	if err != nil {
		return err
	}

	if totalErrors > 0 {
		return errors.Errorf("INSPECT found %d consistency errors:%s", totalErrors, errorDetails)
	}

	l.Printf("INSPECT validation completed successfully (0 errors across %d jobs)", len(jobIDs))
	return nil
}

// discoverUserDatabases queries pg_database to find all user databases,
// excluding the system database.
func discoverUserDatabases(ctx context.Context, db *gosql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT datname FROM pg_database
		WHERE datname NOT IN ('system')
		ORDER BY datname`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query databases")
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, err
		}
		databases = append(databases, dbName)
	}
	return databases, rows.Err()
}

// isStatementTimeoutError returns true if the error is a statement timeout error.
// Statement timeout errors are expected when launching INSPECT jobs since we only
// want to start the job, not wait for it to complete.
func isStatementTimeoutError(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pgcode.MakeCode(string(pqErr.Code)) == pgcode.QueryCanceled
	}
	return false
}

// isFeatureNotSupportedError returns true if the error is a feature not supported error.
// This can occur when the cluster version is not yet upgraded to support INSPECT.
func isFeatureNotSupportedError(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pgcode.MakeCode(string(pqErr.Code)) == pgcode.FeatureNotSupported
	}
	return false
}

// launchInspectJobs launches INSPECT DATABASE commands in parallel for all
// provided databases using task manager for concurrency control. Each INSPECT
// command enables the inspect command for that connection. Statement timeout
// errors are ignored as they indicate the job was successfully started.
// Feature not supported errors are returned to the caller, indicating the
// cluster version does not support INSPECT.
func launchInspectJobs(
	ctx context.Context, l *logger.Logger, db *gosql.DB, databases []string,
) error {
	// Make the INSPECT jobs go as fast as possible.
	if _, err := db.ExecContext(ctx,
		"SET CLUSTER SETTING sql.inspect.admission_control.enabled = off"); err != nil {
		return errors.Wrap(err, "failed to disable INSPECT admission control")
	}

	statementTimeout := 5 * time.Second
	tm := task.NewManager(ctx, l)
	g := tm.NewErrorGroup()

	for _, dbName := range databases {
		dbName := dbName
		g.Go(func(ctx context.Context, l *logger.Logger) error {
			l.Printf("Launching INSPECT DATABASE %s", dbName)

			statements := []string{
				"SET enable_inspect_command = true",
				fmt.Sprintf("SET statement_timeout = '%s'", statementTimeout.String()),
				fmt.Sprintf("INSPECT DATABASE %s", lexbase.EscapeSQLIdent(dbName)),
			}

			var stmtErr error
			for _, stmt := range statements {
				if _, err := db.ExecContext(ctx, stmt); err != nil {
					stmtErr = err
					break
				}
			}

			// Always reset statement timeout back to default.
			if _, err := db.ExecContext(ctx, "RESET statement_timeout"); err != nil {
				l.Printf("Warning: failed to reset statement timeout: %v", err)
			}

			// Check for errors from the statements loop.
			if stmtErr != nil {
				// Statement timeout is expected - it means the job started but didn't complete
				// within the timeout. The job is still running in the background.
				if !isStatementTimeoutError(stmtErr) {
					l.Printf("INSPECT DATABASE %s failed to start: %v", dbName, stmtErr)
					return errors.Wrapf(stmtErr, "failed to start INSPECT DATABASE %s", dbName)
				}
			}

			l.Printf("INSPECT DATABASE %s started", dbName)
			return nil
		})
	}
	return g.WaitE()
}

// fetchInspectJobIDs queries SHOW JOBS to retrieve the job IDs of the most
// recently started INSPECT jobs.
func fetchInspectJobIDs(ctx context.Context, db *gosql.DB, expectedCount int) ([]int64, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT job_id
		FROM [SHOW JOBS]
		WHERE job_type = 'INSPECT'
		ORDER BY started DESC
		LIMIT %d`, expectedCount))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query INSPECT job IDs")
	}
	defer rows.Close()

	var jobIDs []int64
	for rows.Next() {
		var jobID int64
		if err := rows.Scan(&jobID); err != nil {
			return nil, err
		}
		jobIDs = append(jobIDs, jobID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(jobIDs) == 0 {
		return nil, errors.New("no INSPECT jobs were found after launching")
	}
	return jobIDs, nil
}

// waitForInspectJobCompletion polls SHOW JOBS until all INSPECT jobs complete
// or the timeout is reached. Logs progress periodically.
func waitForInspectJobCompletion(
	ctx context.Context, l *logger.Logger, db *gosql.DB, jobIDs []int64, timeout time.Duration,
) {
	deadline := timeutil.Now().Add(timeout)
	pollInterval := 5 * time.Second
	logEvery := Every(30 * time.Second)

	for timeutil.Now().Before(deadline) {
		time.Sleep(pollInterval)

		placeholders := make([]string, len(jobIDs))
		args := make([]interface{}, len(jobIDs))
		for i, jobID := range jobIDs {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = jobID
		}

		var pendingCount int
		countQuery := fmt.Sprintf(`
			SELECT count(*)
			FROM [SHOW JOBS]
			WHERE job_id IN (%s)
			  AND status NOT IN ('succeeded', 'failed', 'canceled')`,
			strings.Join(placeholders, ", "))

		if err := db.QueryRowContext(ctx, countQuery, args...).Scan(&pendingCount); err != nil {
			l.Printf("Warning: failed to check job status: %v", err)
			return
		}

		if pendingCount == 0 {
			l.Printf("All INSPECT jobs completed")
			return
		}

		if logEvery.ShouldLog() {
			l.Printf("Waiting for %d INSPECT jobs to complete (%s remaining)",
				pendingCount, deadline.Sub(timeutil.Now()).Round(time.Second))
		}
	}

	l.Printf("INSPECT timeout reached, checking for errors anyway")
}

// collectInspectErrors queries SHOW INSPECT ERRORS for each job and returns
// the total error count and formatted error details.
func collectInspectErrors(ctx context.Context, db *gosql.DB, jobIDs []int64) (int, string, error) {
	totalErrors := 0
	var errorDetails strings.Builder

	for _, jobID := range jobIDs {
		var errorCount int
		if err := db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT count(*) FROM [SHOW INSPECT ERRORS FOR JOB %d]", jobID),
		).Scan(&errorCount); err != nil {
			return 0, "", errors.Wrapf(err, "failed to query INSPECT errors for job %d", jobID)
		}

		if errorCount > 0 {
			totalErrors += errorCount
			if err := func() error {
				rows, err := db.QueryContext(ctx, fmt.Sprintf(`
					SELECT database_name, schema_name, table_name, error_type, details
					FROM [SHOW INSPECT ERRORS FOR JOB %d WITH DETAILS]`, jobID))
				if err != nil {
					return errors.Wrapf(err, "failed to fetch error details for job %d", jobID)
				}
				defer rows.Close()

				fmt.Fprintf(&errorDetails, "\n  Job %d:", jobID)
				for rows.Next() {
					var dbName, schemaName, tableName, errorType, details string
					if err := rows.Scan(&dbName, &schemaName, &tableName, &errorType, &details); err != nil {
						return err
					}
					fmt.Fprintf(&errorDetails, "\n    - %s.%s.%s: %s (%s)",
						dbName, schemaName, tableName, errorType, details)
				}
				return rows.Err()
			}(); err != nil {
				return 0, "", err
			}
		}
	}
	return totalErrors, errorDetails.String(), nil
}

// validateTokensReturned ensures that all RACv2 tokens are returned to the pool
// at the end of the test.
func ValidateTokensReturned(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	nodes option.NodeListOption,
	waitTime time.Duration,
) {
	t.L().Printf("validating all tokens returned")
	for _, node := range nodes {
		// Wait for the tokens to be returned to the pool. Normally this will
		// pass immediately however it is possible that there is still some
		// recovery so loop a few times.
		testutils.SucceedsWithin(t, func() error {
			db := c.Conn(ctx, t.L(), node)
			defer db.Close()
			for _, sType := range []string{"send", "eval"} {
				for _, tType := range []string{"elastic", "regular"} {
					statPrefix := fmt.Sprintf("kvflowcontrol.tokens.%s.%s", sType, tType)
					query := fmt.Sprintf(`
		SELECT d.value::INT8 AS deducted, r.value::INT8 AS returned
		FROM
		  crdb_internal.node_metrics d,
		  crdb_internal.node_metrics r
		WHERE
		  d.name='%s.deducted' AND
		  r.name='%s.returned'`,
						statPrefix, statPrefix)
					rows, err := db.QueryContext(ctx, query)
					require.NoError(t, err)
					require.True(t, rows.Next())
					var deducted, returned int64
					if err := rows.Scan(&deducted, &returned); err != nil {
						return err
					}
					if deducted != returned {
						return errors.Newf("tokens not returned for %s: deducted %d returned %d", statPrefix, deducted, returned)
					}
				}
			}
			return nil
			// We wait up to waitTime for the tokens to be returned. In tests which
			// purposefully create a send queue towards a node, the queue may take a
			// while to drain. The tokens will not be returned until the queue is
			// empty and there are no inflight requests.
		}, waitTime)
	}
}

// Fingerprint returns a fingerprint of `db.table`.
func Fingerprint(ctx context.Context, conn *gosql.DB, db, table string) (string, error) {
	var b strings.Builder

	query := fmt.Sprintf("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s.%s", db, table)
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var name, fp string
		if err := rows.Scan(&name, &fp); err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "%s: %s\n", name, fp)
	}

	return b.String(), rows.Err()
}
