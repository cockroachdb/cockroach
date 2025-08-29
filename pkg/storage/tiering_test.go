// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// TestTieringPolicy is a test that sets up a table with tiering and allows
// manually checking the span policy reported to Pebble (via debugSpanPolicy logs).
func TestTieringPolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer tc.Stopper().Stop(context.Background())
	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Shorten the closed timestamp target duration and span config reconciliation
	// interval so that span configs propagate more rapidly.
	runner.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
	runner.Exec(t, `SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '100ms'`)
	runner.Exec(t, `SET CLUSTER SETTING spanconfig.reconciliation_job.checkpoint_interval = '100ms'`)

	fmt.Printf("\n\ncreating table\n")
	runner.Exec(t, "CREATE TABLE tieredtable (k INT PRIMARY KEY, v STRING, ts TIMESTAMPTZ)")

	// Wait for the span config to be applied.
	fmt.Printf("\n\nsleeping\n")
	time.Sleep(2 * time.Second)

	fmt.Printf("\n\ninserting\n")
	runner.Exec(t, `INSERT INTO tieredtable VALUES
		(1, 'foo', '1970-01-01 00:10:00'),
		(2, 'bar', '1970-01-01 00:00:01'),
		(3, 'baz', '1970-01-01 00:11:00'),
		(4, 'qux', '1970-01-01 00:00:02')`)

	fmt.Printf("\n\nquerying:\n")
	runner.CheckQueryResults(t, `SELECT k, ts, crdb_internal_tiering_attr FROM tieredtable`, [][]string{
		{"1", "1970-01-01 00:10:00 +0000 UTC", "600"},
		{"2", "1970-01-01 00:00:01 +0000 UTC", "1"},
		{"3", "1970-01-01 00:11:00 +0000 UTC", "660"},
		{"4", "1970-01-01 00:00:02 +0000 UTC", "2"},
	})

	fmt.Printf("\n\nrunning compaction\n")
	runner.Exec(t, `SELECT crdb_internal.compact_engine_span(1, 1,
		(SELECT raw_start_key FROM [SHOW RANGES FROM TABLE tieredtable WITH KEYS] ORDER BY start_key LIMIT 1),
		(SELECT raw_end_key FROM [SHOW RANGES FROM TABLE tieredtable WITH KEYS] ORDER BY end_key DESC LIMIT 1))`)

	fmt.Printf("\n\ndone\n")
}

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}
