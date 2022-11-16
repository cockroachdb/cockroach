// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl" // ccl init hooks
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostclient"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestEstimateQueryRUConsumption is a sanity check for the RU estimates
// produced for queries that are run by a tenant under EXPLAIN ANALYZE. The RU
// consumption of a query is not deterministic, since it depends on inexact
// quantities like the (already estimated) CPU usage. Therefore, the test runs
// each query multiple times and then checks that the total estimated RU
// consumption is within reasonable distance from the actual measured RUs for
// the tenant.
func TestEstimateQueryRUConsumption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test becomes flaky when the machine/cluster is under significant
	// background load, so it should only be run manually.
	skip.IgnoreLint(t, "intended to be manually run as a sanity test")

	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()
	stats.AutomaticStatisticsClusterMode.Override(ctx, &st.SV, false)
	stats.UseStatisticsOnSystemTables.Override(ctx, &st.SV, false)
	stats.AutomaticStatisticsOnSystemTables.Override(ctx, &st.SV, false)

	// Lower the target duration for reporting tenant usage so that it can be
	// measured accurately. Avoid decreasing too far, since doing so can add
	// measurable overhead.
	tenantcostclient.TargetPeriodSetting.Override(ctx, &st.SV, time.Millisecond*500)

	params := base.TestServerArgs{
		Settings:                 st,
		DisableDefaultTestTenant: true,
	}

	s, mainDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	sysDB := sqlutils.MakeSQLRunner(mainDB)

	tenantID := serverutils.TestTenantID()
	tenant1, tenantDB1 := serverutils.StartTenant(t, s, base.TestTenantArgs{
		TenantID: tenantID,
		Settings: st,
	})
	defer tenant1.Stopper().Stop(ctx)
	defer tenantDB1.Close()
	tdb := sqlutils.MakeSQLRunner(tenantDB1)
	tdb.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false")
	tdb.Exec(t, "CREATE TABLE abcd (a INT, b INT, c INT, d INT, INDEX (a, b, c))")

	type testCase struct {
		sql   string
		count int
	}
	testCases := []testCase{
		{ // Insert statement
			sql:   "INSERT INTO abcd (SELECT t%2, t%3, t, -t FROM generate_series(1,50000) g(t))",
			count: 1,
		},
		{ // Point query
			sql:   "SELECT a FROM abcd WHERE (a, b) = (1, 1)",
			count: 10,
		},
		{ // Range query
			sql:   "SELECT a FROM abcd WHERE (a, b) = (1, 1) AND c > 0 AND c < 10000",
			count: 10,
		},
		{ // Aggregate
			sql:   "SELECT count(*) FROM abcd",
			count: 10,
		},
		{ // Distinct
			sql:   "SELECT DISTINCT ON (a, b) * FROM abcd",
			count: 10,
		},
		{ // Full table scan
			sql:   "SELECT a FROM abcd",
			count: 10,
		},
		{ // Lookup join
			sql:   "SELECT a FROM (VALUES (1, 1), (0, 2)) v(x, y) INNER LOOKUP JOIN abcd ON (a, b) = (x, y)",
			count: 10,
		},
		{ // Index join
			sql:   "SELECT * FROM abcd WHERE (a, b) = (0, 0)",
			count: 10,
		},
		{ // No kv IO, lots of network egress.
			sql:   "SELECT 'deadbeef' FROM generate_series(1, 50000)",
			count: 10,
		},
	}

	var err error
	var tenantEstimatedRUs int
	for _, tc := range testCases {
		for i := 0; i < tc.count; i++ {
			output := tdb.QueryStr(t, "EXPLAIN ANALYZE "+tc.sql)
			var estimatedRU int
			for _, row := range output {
				if len(row) != 1 {
					t.Fatalf("expected one column")
				}
				val := row[0]
				if strings.Contains(val, "estimated RUs consumed") {
					substr := strings.Split(val, " ")
					require.Equalf(t, 4, len(substr), "expected RU consumption message to have four words")
					ruCountStr := strings.Replace(strings.TrimSpace(substr[3]), ",", "", -1)
					estimatedRU, err = strconv.Atoi(ruCountStr)
					require.NoError(t, err, "failed to retrieve estimated RUs")
					break
				}
			}
			tenantEstimatedRUs += estimatedRU
		}
	}

	getTenantRUs := func() float64 {
		// Sleep to ensure the measured RU consumption gets recorded in the
		// tenant_usage table.
		time.Sleep(time.Second)
		var consumptionBytes []byte
		var consumption roachpb.TenantConsumption
		var tenantRUs float64
		rows := sysDB.Query(t,
			fmt.Sprintf(
				"SELECT total_consumption FROM system.tenant_usage WHERE tenant_id = %d AND instance_id = 0",
				tenantID.ToUint64(),
			),
		)
		for rows.Next() {
			require.NoError(t, rows.Scan(&consumptionBytes))
			if len(consumptionBytes) == 0 {
				continue
			}
			require.NoError(t, protoutil.Unmarshal(consumptionBytes, &consumption))
			tenantRUs += consumption.RU
		}
		return tenantRUs
	}
	tenantStartRUs := getTenantRUs()

	var tenantMeasuredRUs float64
	for _, tc := range testCases {
		for i := 0; i < tc.count; i++ {
			tdb.QueryStr(t, tc.sql)
		}
	}

	// Check the estimated RU aggregate for all the queries against the actual
	// measured RU consumption for the tenant.
	tenantMeasuredRUs = getTenantRUs() - tenantStartRUs
	const deltaFraction = 0.25
	allowedDelta := tenantMeasuredRUs * deltaFraction
	require.InDeltaf(t, tenantMeasuredRUs, tenantEstimatedRUs, allowedDelta,
		"estimated RUs (%d) were not within %f RUs of the expected value (%f)",
		tenantEstimatedRUs,
		allowedDelta,
		tenantMeasuredRUs,
	)
}
