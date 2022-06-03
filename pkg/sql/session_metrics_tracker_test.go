// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestSessionMetricsTracker tests that the long-running query and transaction
// metrics get sane values.
func TestSessionMetricsTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "too slow to make it stable")

	ctx := context.Background()
	threshold := 10 * time.Millisecond
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SessionMetricTrackerKnobs: &sql.SessionMetricsTrackerTestingKnobs{
				MinUpdateInterval:    0,
				LongRunningThreshold: threshold,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY)")

	tx, err := sqlDB.Begin()
	require.NoError(t, err)
	sqlutils.MakeSQLRunner(tx).Exec(t, "INSERT INTO t VALUES (1)")

	const maxN = 16 // arbitrary
	n := rand.Intn(maxN)
	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < n; i++ {
		g.Go(func() error {
			_, err := sqlDB.ExecContext(gCtx, "SELECT * FROM t")
			return err
		})
	}

	tdb.CheckQueryResultsRetry(t, `
SELECT value
  FROM crdb_internal.node_metrics
 WHERE name = 'sql.txn.long_running.count'`,
		[][]string{{strconv.Itoa(n + 1)}})
	tdb.CheckQueryResultsRetry(t, `
SELECT value > $1
  FROM crdb_internal.node_metrics
 WHERE name = 'sql.txn.long_running.max_duration'`,
		[][]string{{"true"}}, threshold.Nanoseconds())
	tdb.CheckQueryResultsRetry(t, `
SELECT value
  FROM crdb_internal.node_metrics
 WHERE name = 'sql.query.long_running.count'`,
		[][]string{{strconv.Itoa(n)}})

	ie := s.InternalExecutor().(sqlutil.InternalExecutor)
	nInternal := rand.Intn(maxN)
	for i := 0; i < nInternal; i++ {
		g.Go(func() error {
			_, err := ie.Exec(ctx, "test", nil, "SELECT * FROM defaultdb.t")
			return err
		})
	}

	tdb.CheckQueryResultsRetry(t, `
SELECT value
  FROM crdb_internal.node_metrics
 WHERE name = 'sql.query.long_running.count.internal'`,
		[][]string{{strconv.Itoa(nInternal)}})
	tdb.CheckQueryResultsRetry(t, `
SELECT value
  FROM crdb_internal.node_metrics
 WHERE name = 'sql.txn.long_running.count.internal'`,
		[][]string{{strconv.Itoa(nInternal)}})
	tdb.CheckQueryResultsRetry(t, `
SELECT txn.value > stmt.value,
    txn.value > internal.value,
    txn.value > internal_txn.value
  FROM (
        SELECT value
          FROM crdb_internal.node_metrics
         WHERE name = 'sql.query.long_running.max_duration'
       ) AS stmt,
       (
        SELECT value
          FROM crdb_internal.node_metrics
         WHERE name = 'sql.txn.long_running.max_duration'
       ) AS txn,
       (
        SELECT value
          FROM crdb_internal.node_metrics
         WHERE name = 'sql.query.long_running.max_duration.internal'
       ) AS internal,
       (
        SELECT value
          FROM crdb_internal.node_metrics
         WHERE name = 'sql.txn.long_running.max_duration.internal'
       ) AS internal_txn;`,
		[][]string{{"true", "true", "true"}})
	require.NoError(t, tx.Rollback())
	require.NoError(t, g.Wait())
}
