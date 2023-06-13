// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// TestStatsAreDeletedForDroppedTables ensures that statistics for dropped
// tables are automatically deleted.
func TestStatsAreDeletedForDroppedTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	params.ScanMaxIdleTime = time.Millisecond // speed up MVCC GC queue scans
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Disable auto stats so that it doesn't interfere.
	runner.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;")
	// Lower the garbage collection interval to speed up the test.
	runner.Exec(t, "SET CLUSTER SETTING sql.stats.garbage_collection_interval = '1s';")
	// Poll for MVCC GC more frequently.
	runner.Exec(t, "SET CLUSTER SETTING sql.gc_job.wait_for_gc.interval = '1s';")
	// Cached protected timestamp state delays MVCC GC, update it every second.
	runner.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '1s';")

	// Create a table with short TTL and collect stats on it.
	runner.Exec(t, "CREATE TABLE t (k PRIMARY KEY) AS SELECT 1;")
	runner.Exec(t, "ALTER TABLE t CONFIGURE ZONE USING gc.ttlseconds = 1;")
	runner.Exec(t, "ANALYZE t;")

	r := runner.QueryRow(t, "SELECT 't'::regclass::oid")
	var tableID int
	r.Scan(&tableID)

	// Ensure that we see a single statistic for the table.
	var count int
	runner.QueryRow(t, `SELECT count(*) FROM system.table_statistics WHERE "tableID" = $1;`, tableID).Scan(&count)
	if count != 1 {
		t.Fatalf("expected a single statistic for table 't', found %d", count)
	}

	// Now drop the table and make sure that the table statistic is deleted
	// promptly.
	runner.Exec(t, "DROP TABLE t;")
	testutils.SucceedsSoon(t, func() error {
		runner.QueryRow(t, `SELECT count(*) FROM system.table_statistics WHERE "tableID" = $1;`, tableID).Scan(&count)
		if count != 0 {
			return errors.Newf("expected no stats for the dropped table, found %d statistics", count)
		}
		return nil
	})
}
