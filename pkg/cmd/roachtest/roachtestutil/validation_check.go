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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
		return err
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
