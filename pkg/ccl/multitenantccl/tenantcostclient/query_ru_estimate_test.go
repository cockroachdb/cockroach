// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostclient_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl" // ccl init hooks
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostserver"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestEstimateQueryRUConsumption verifies that EXPLAIN ANALYZE produces
// reasonable per-query RU estimates for a tenant. Each query type is checked
// against an expected value with a tolerance, rather than comparing against
// measured tenant consumption.
func TestEstimateQueryRUConsumption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "slow multi-tenant test")

	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()
	stats.AutomaticStatisticsClusterMode.Override(ctx, &st.SV, false)
	stats.UseStatisticsOnSystemTables.Override(ctx, &st.SV, false)
	stats.AutomaticStatisticsOnSystemTables.Override(ctx, &st.SV, false)

	params := base.TestServerArgs{
		Settings:          st,
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	}

	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	tenant1, tenantDB1 := serverutils.StartTenant(t, s, base.TestTenantArgs{
		TenantID: serverutils.TestTenantID(),
		TestingKnobs: base.TestingKnobs{
			SQLEvalContext: &eval.TestingKnobs{
				ForceProductionValues: true,
			},
		},
	})
	defer tenant1.AppStopper().Stop(ctx)
	defer tenantDB1.Close()
	tdb := sqlutils.MakeSQLRunner(tenantDB1)
	tdb.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false")
	tdb.Exec(t, "CREATE TABLE abcd (a INT, b INT, c INT, d INT, INDEX foo (a, b, c))")
	tdb.Exec(t, "INSERT INTO abcd (SELECT t%2, t%3, t, -t FROM generate_series(1,20000) g(t))")

	// Scan both indexes to resolve all intents.
	tdb.Exec(t, "SELECT count(*) FROM abcd@primary")
	tdb.Exec(t, "SELECT count(*) FROM abcd@foo")

	// Each test case specifies a baseRU covering the deterministic portion
	// of the RU estimate (KV reads/writes and network egress). The CPU
	// portion is computed from the observed SQL CPU time using the default
	// PodCPUSecond cost (333.3333 RU/s). The total expected RU is
	// baseRU + cpuRU, and the actual estimate must be within 10%.
	const tolerancePct = 0.1
	podCPUPerSec := tenantcostmodel.SQLCPUSecondCost.Default()

	type testCase struct {
		name   string
		sql    string
		baseRU float64
	}
	testCases := []testCase{
		{
			name:   "point query",
			sql:    "SELECT a FROM abcd WHERE (a, b) = (1, 1)",
			baseRU: 34.5,
		},
		{
			name:   "range query",
			sql:    "SELECT a FROM abcd WHERE (a, b) = (1, 1) AND c > 0 AND c < 10000",
			baseRU: 17.3,
		},
		{
			name:   "aggregate",
			sql:    "SELECT count(*) FROM abcd",
			baseRU: 13,
		},
		{
			name:   "distinct",
			sql:    "SELECT DISTINCT ON (a, b) * FROM abcd",
			baseRU: 15.2,
		},
		{
			name:   "full table scan",
			sql:    "SELECT a FROM abcd",
			baseRU: 208.2,
		},
		{
			name:   "lookup join",
			sql:    "SELECT a FROM (VALUES (1, 1), (0, 2)) v(x, y) INNER LOOKUP JOIN abcd ON (a, b) = (x, y)",
			baseRU: 70,
		},
		{
			name:   "index join",
			sql:    "SELECT * FROM abcd WHERE (a, b) = (0, 0)",
			baseRU: 525.9,
		},
		{
			name:   "no kv IO, network egress",
			sql:    "SELECT 'deadbeef' FROM generate_series(1, 50000)",
			baseRU: 830.1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output := tdb.QueryStr(t, "EXPLAIN ANALYZE (VERBOSE) "+tc.sql)
			var estimatedRU float64
			var cpuTime time.Duration
			for _, row := range output {
				if len(row) != 1 {
					t.Fatalf("expected one column")
				}
				if estimatedRU == 0 && strings.Contains(row[0], "estimated RUs consumed") {
					fields := strings.Fields(row[0])
					ruStr := strings.ReplaceAll(fields[len(fields)-1], ",", "")
					var err error
					estimatedRU, err = strconv.ParseFloat(ruStr, 64)
					require.NoError(t, err, "failed to parse estimated RUs from %q", row[0])
				}
				if cpuTime == 0 && strings.Contains(row[0], "sql cpu time") {
					fields := strings.Fields(row[0])
					var err error
					cpuTime, err = time.ParseDuration(fields[len(fields)-1])
					require.NoError(t, err, "failed to parse sql cpu time from %q", row[0])
				}
			}

			// Compute expected RU as base (deterministic) + CPU portion
			// derived from the observed SQL CPU time.
			cpuRU := cpuTime.Seconds() * podCPUPerSec
			expectedRU := tc.baseRU + cpuRU
			minRU := expectedRU * (1 - tolerancePct)
			maxRU := expectedRU * (1 + tolerancePct)
			t.Logf("estimated=%.2f expected=%.2f base=%.2f cpuRU=%.2f cpuTime=%s",
				estimatedRU, expectedRU, tc.baseRU, cpuRU, cpuTime)
			require.GreaterOrEqualf(t, estimatedRU, minRU,
				"estimated RUs (%.2f) below minimum (%.2f)", estimatedRU, minRU)
			require.LessOrEqualf(t, estimatedRU, maxRU,
				"estimated RUs (%.2f) above maximum (%.2f)", estimatedRU, maxRU)
		})
	}
}
