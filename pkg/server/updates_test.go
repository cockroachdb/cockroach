// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
)

func stubURL(target **url.URL, stubURL *url.URL) func() {
	realURL := *target
	*target = stubURL
	return func() {
		*target = realURL
	}
}

func TestCheckVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()

	r := makeMockRecorder(t)
	defer r.Close()
	defer stubURL(&updatesURL, r.url)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	s.(*TestServer).checkForUpdates(ctx, time.Minute)
	r.Close()
	s.Stopper().Stop(ctx)

	t.Run("expected-reporting", func(t *testing.T) {
		r.Lock()
		defer r.Unlock()

		if expected, actual := 1, r.requests; actual != expected {
			t.Fatalf("expected %v update checks, got %v", expected, actual)
		}

		if expected, actual := s.(*TestServer).ClusterID().String(), r.last.uuid; expected != actual {
			t.Errorf("expected uuid %v, got %v", expected, actual)
		}

		if expected, actual := build.GetInfo().Tag, r.last.version; expected != actual {
			t.Errorf("expected version tag %v, got %v", expected, actual)
		}

		if expected, actual := "OSS", r.last.licenseType; expected != actual {
			t.Errorf("expected license type %v, got %v", expected, actual)
		}

		if expected, actual := "false", r.last.internal; expected != actual {
			t.Errorf("expected internal to be %v, got %v", expected, actual)
		}
	})

	t.Run("npe", func(t *testing.T) {
		// ensure nil, which happens when an empty env override URL is used, does not
		// cause a crash. We've deferred a cleanup of the original pointer above.
		updatesURL = nil
		s.(*TestServer).checkForUpdates(ctx, time.Minute)
	})
}

func TestUsageQuantization(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := makeMockRecorder(t)
	defer stubURL(&reportingURL, r.url)()
	defer r.Close()

	st := cluster.MakeTestingClusterSettings()
	ctx := context.TODO()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{Settings: st})
	defer s.Stopper().Stop(ctx)
	ts := s.(*TestServer)

	if _, err := db.Exec(`SET application_name = 'test'`); err != nil {
		t.Fatal(err)
	}

	// Issue some queries against the test app name.
	for i := 0; i < 8; i++ {
		if _, err := db.Exec(`SELECT 1`); err != nil {
			t.Fatal(err)
		}
	}
	// Between 10 and 100 queries is quantized to 10.
	for i := 0; i < 30; i++ {
		if _, err := db.Exec(`SELECT 1,2`); err != nil {
			t.Fatal(err)
		}
	}
	// Between 100 and 10000 gets quantized to 100.
	for i := 0; i < 200; i++ {
		if _, err := db.Exec(`SELECT 1,2,3`); err != nil {
			t.Fatal(err)
		}
	}
	// Above 10000 gets quantized to 10000.
	for i := 0; i < 10010; i++ {
		if _, err := db.Exec(`SHOW application_name`); err != nil {
			t.Fatal(err)
		}
	}

	// Flush the SQL stat pool.
	ts.Server.pgServer.SQLServer.ResetSQLStats(ctx)

	// Collect a round of statistics.
	ts.reportDiagnostics(ctx)

	// The stats "hide" the application name by hashing it. To find the
	// test app name, we need to hash the ref string too prior to the
	// comparison.
	clusterSecret := sql.ClusterSecret.Get(&st.SV)
	hashedAppName := sql.HashForReporting(clusterSecret, "test")
	if hashedAppName == sql.FailedHashedValue {
		t.Fatalf("expected hashedAppName to not be 'unknown'")
	}

	testData := []struct {
		query         string
		expectedCount int64
	}{
		{`SELECT _`, 8},
		{`SELECT _, _`, 10},
		{`SELECT _, _, _`, 100},
		{`SHOW application_name`, 10000},
	}

	for _, test := range testData {
		found := false
		for _, s := range r.last.SqlStats {
			if s.Key.App == hashedAppName && s.Key.Query == test.query {
				if s.Stats.Count != test.expectedCount {
					t.Errorf("quantization incorrect for query %q: expected %d, got %d",
						test.query, test.expectedCount, s.Stats.Count)
				}
				found = true
				break
			}
		}
		if !found {
			t.Errorf("query %q missing from stats", test.query)
		}
	}
}

func TestCBOReportUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const elemName = "somestring"
	ctx := context.TODO()

	r := makeMockRecorder(t)
	defer stubURL(&reportingURL, r.url)()
	defer r.Close()

	st := cluster.MakeTestingClusterSettings()

	storeSpec := base.DefaultTestStoreSpec
	storeSpec.Attributes = roachpb.Attributes{Attrs: []string{elemName}}
	params := base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			storeSpec,
			base.DefaultTestStoreSpec,
		},
		Settings: st,
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "east"},
				{Key: "zone", Value: elemName},
				{Key: "state", Value: "ny"},
				{Key: "city", Value: "nyc"},
			},
		},
		Knobs: base.TestingKnobs{
			SQLLeaseManager: &sql.LeaseManagerTestingKnobs{
				// Disable SELECT called for delete orphaned leases to keep
				// query stats stable.
				DisableDeleteOrphanedLeases: true,
			},
		},
	}

	s, db, _ := serverutils.StartServer(t, params)
	// Stopper will wait for the update/report loop to finish too.
	defer s.Stopper().Stop(context.TODO())
	defer db.Close()
	ts := s.(*TestServer)

	// make sure the test's generated activity is the only activity we measure.
	telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)

	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %s`, elemName))
	sqlDB.Exec(t, `CREATE TABLE x (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE STATISTICS stats FROM x`)
	// Run a variety of hinted queries.
	sqlDB.Exec(t, `SELECT * FROM x@primary`)
	sqlDB.Exec(t, `EXPLAIN SELECT * FROM x`)
	sqlDB.Exec(t, `EXPLAIN (distsql) SELECT * FROM x`)
	sqlDB.Exec(t, `EXPLAIN ANALYZE SELECT * FROM x`)
	sqlDB.Exec(t, `EXPLAIN (opt) SELECT * FROM x`)
	sqlDB.Exec(t, `EXPLAIN (opt, verbose) SELECT * FROM x`)
	// Do joins different numbers of times to disambiguate them in the expected,
	// output but make sure the query strings are different each time so that the
	// plan cache doesn't affect our results.
	sqlDB.Exec(
		t,
		`SELECT x FROM (VALUES (1)) AS a(x) INNER HASH JOIN (VALUES (1)) AS b(y) ON x = y`,
	)
	for i := 0; i < 2; i++ {
		sqlDB.Exec(
			t,
			fmt.Sprintf(
				`SELECT x FROM (VALUES (%d)) AS a(x) INNER MERGE JOIN (VALUES (1)) AS b(y) ON x = y`,
				i,
			),
		)
	}
	for i := 0; i < 3; i++ {
		sqlDB.Exec(
			t,
			fmt.Sprintf(
				`SELECT a FROM (VALUES (%d)) AS b(y) INNER LOOKUP JOIN x ON y = a`,
				i,
			),
		)
	}

	for i := 0; i < 20; i += 3 {
		sqlDB.Exec(
			t,
			fmt.Sprintf(
				`SET CLUSTER SETTING sql.defaults.reorder_joins_limit = %d`,
				i,
			),
		)
	}

	sqlDB.Exec(
		t,
		`SET CLUSTER SETTING sql.stats.automatic_collection.enabled = on`,
	)

	sqlDB.Exec(
		t,
		`SET CLUSTER SETTING sql.stats.automatic_collection.enabled = off`,
	)

	if _, err := db.Exec(`RESET CLUSTER SETTING sql.stats.automatic_collection.enabled`); err != nil {
		t.Fatal(err)
	}

	ts.reportDiagnostics(ctx)

	expectedFeatureUsage := map[string]int32{
		"sql.plan.hints.hash-join":              1,
		"sql.plan.hints.merge-join":             2,
		"sql.plan.hints.lookup-join":            3,
		"sql.plan.hints.index":                  1,
		"sql.plan.reorder-joins.set-limit-0":    1,
		"sql.plan.reorder-joins.set-limit-3":    1,
		"sql.plan.reorder-joins.set-limit-6":    1,
		"sql.plan.reorder-joins.set-limit-9":    1,
		"sql.plan.reorder-joins.set-limit-more": 3,
		"sql.plan.automatic-stats.enabled":      2,
		"sql.plan.automatic-stats.disabled":     1,
		"sql.plan.stats.created":                1,
		"sql.plan.explain":                      1,
		"sql.plan.explain-analyze":              1,
		"sql.plan.explain-opt":                  1,
		"sql.plan.explain-opt-verbose":          1,
		"sql.plan.explain-distsql":              1,
	}

	for key, expected := range expectedFeatureUsage {
		if got, ok := r.last.FeatureUsage[key]; !ok {
			t.Fatalf("expected report of feature %q", key)
		} else if got != expected {
			t.Fatalf("expected reported value of feature %q to be %d not %d", key, expected, got)
		}
	}
}

func TestReportUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const elemName = "somestring"
	const internalAppName = sqlbase.ReportableAppNamePrefix + "foo"
	ctx := context.TODO()

	r := makeMockRecorder(t)
	defer stubURL(&reportingURL, r.url)()
	defer r.Close()

	st := cluster.MakeTestingClusterSettings()

	storeSpec := base.DefaultTestStoreSpec
	storeSpec.Attributes = roachpb.Attributes{Attrs: []string{elemName}}
	params := base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			storeSpec,
			base.DefaultTestStoreSpec,
		},
		Settings: st,
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "east"},
				{Key: "zone", Value: elemName},
				{Key: "state", Value: "ny"},
				{Key: "city", Value: "nyc"},
			},
		},
		Knobs: base.TestingKnobs{
			SQLLeaseManager: &sql.LeaseManagerTestingKnobs{
				// Disable SELECT called for delete orphaned leases to keep
				// query stats stable.
				DisableDeleteOrphanedLeases: true,
			},
		},
	}

	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO()) // stopper will wait for the update/report loop to finish too.
	ts := s.(*TestServer)

	// make sure the test's generated activity is the only activity we measure.
	telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)

	if _, err := db.Exec(fmt.Sprintf(`CREATE DATABASE %s`, elemName)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`SET CLUSTER SETTING server.time_until_store_dead = '90s'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`SET CLUSTER SETTING diagnostics.reporting.send_crash_reports = false`); err != nil {
		t.Fatal(err)
	}

	telemetry.Count("test.a")
	telemetry.Count("test.b")
	telemetry.Count("test.b")
	c := telemetry.GetCounter("test.c")
	telemetry.Inc(c)
	telemetry.Inc(c)
	telemetry.Inc(c)

	for _, cmd := range []struct {
		resource string
		config   string
	}{
		{"TABLE system.rangelog", fmt.Sprintf(`constraints: [+zone=%[1]s, +%[1]s]`, elemName)},
		{"TABLE system.rangelog", `{gc: {ttlseconds: 1}}`},
		{"DATABASE system", `num_replicas: 5`},
		{"DATABASE system", fmt.Sprintf(`constraints: {"+zone=%[1]s,+%[1]s": 2, +%[1]s: 1}`, elemName)},
		{"DATABASE system", fmt.Sprintf(`experimental_lease_preferences: [[+zone=%[1]s,+%[1]s], [+%[1]s]]`, elemName)},
	} {
		testutils.SucceedsSoon(t, func() error {
			if _, err := db.Exec(
				fmt.Sprintf(`ALTER %s CONFIGURE ZONE = '%s'`, cmd.resource, cmd.config),
			); err != nil {
				// Work around gossip asynchronicity.
				return errors.Errorf("error applying zone config %q to %q: %v", cmd.config, cmd.resource, err)
			}
			return nil
		})
	}
	if _, err := db.Exec(`INSERT INTO system.zones (id, config) VALUES (10000, null)`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		fmt.Sprintf(`CREATE TABLE %[1]s.%[1]s (%[1]s INT8 CONSTRAINT %[1]s CHECK (%[1]s > 1))`, elemName),
	); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		fmt.Sprintf(`CREATE TABLE %[1]s.%[1]s_s (%[1]s SERIAL2)`, elemName),
	); err != nil {
		t.Fatal(err)
	}

	// Run some queries so we have some query statistics collected.
	for i := 0; i < 10; i++ {
		// Run some sample queries. Each are passed a string and int by Exec.
		// Note placeholders aren't allowed in some positions, including names.
		for _, q := range []string{
			`SELECT * FROM %[1]s.%[1]s WHERE %[1]s = length($1::string) OR %[1]s = $2`,
			`INSERT INTO %[1]s.%[1]s VALUES (length($1::string)), ($2)`,
		} {
			if _, err := db.Exec(fmt.Sprintf(q, elemName), elemName, 10003); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.Exec(`some non-parsing garbage`); !testutils.IsError(
			err, "syntax",
		) {
			t.Fatal(err)
		}
		if _, err := db.Exec(`SELECT crdb_internal.force_error('blah', $1)`, elemName); !testutils.IsError(
			err, elemName,
		) {
			t.Fatal(err)
		}
		if _, err := db.Exec(`SELECT crdb_internal.force_error('', $1)`, elemName); !testutils.IsError(
			err, elemName,
		) {
			t.Fatal(err)
		}
		if _, err := db.Exec(`SELECT crdb_internal.set_vmodule('invalid')`); !testutils.IsError(
			err, "comma-separated list",
		) {
			t.Fatal(err)
		}
		// If the function ever gets supported, change to pick one that is not supported yet.
		if _, err := db.Exec(`SELECT json_object_agg()`); !testutils.IsError(
			err, "this function is not supported",
		) {
			t.Fatal(err)
		}
		// If the vtable ever gets supported, change to pick one that is not supported yet.
		if _, err := db.Exec(`SELECT * FROM pg_catalog.pg_stat_wal_receiver`); !testutils.IsError(
			err, "virtual schema table not implemented",
		) {
			t.Fatal(err)
		}
		if _, err := db.Exec(`SELECT 2/0`); !testutils.IsError(
			err, "division by zero",
		) {
			t.Fatal(err)
		}
		// pass args to force a prepare/exec path as that may differ.
		if _, err := db.Exec(`SELECT 2/$1`, 0); !testutils.IsError(
			err, "division by zero",
		) {
			t.Fatal(err)
		}
		if _, err := db.Exec(`SELECT crdb_internal.force_assertion_error('woo')`); !testutils.IsError(
			err, "internal error",
		) {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE TABLE somestring.foo (a INT8 PRIMARY KEY, b INT8, INDEX (b) INTERLEAVE IN PARENT foo (b))`); !testutils.IsError(
			err, "unimplemented: use CREATE INDEX to make interleaved indexes",
		) {
			t.Fatal(err)
		}
		// Even queries that don't use placeholders and contain literal strings
		// should still not cause those strings to appear in reports.
		for _, q := range []string{
			`SELECT * FROM %[1]s.%[1]s WHERE %[1]s = 1 AND '%[1]s' = '%[1]s'`,
			`INSERT INTO %[1]s.%[1]s VALUES (6), (7), (8)`,
			`INSERT INTO %[1]s.%[1]s SELECT unnest(ARRAY[9, 10, 11, 12])`,
			`SELECT (1, 20, 30, 40) = (SELECT %[1]s, 1, 2, 3 FROM %[1]s.%[1]s LIMIT 1)`,
			`SET application_name = '%[1]s'`,
			`SELECT %[1]s FROM %[1]s.%[1]s WHERE %[1]s = 1 AND lower('%[1]s') = lower('%[1]s')`,
			`UPDATE %[1]s.%[1]s SET %[1]s = %[1]s + 1`,
		} {
			if _, err := db.Exec(fmt.Sprintf(q, elemName)); err != nil {
				t.Fatal(err)
			}
		}
		// Application names with the '$ ' prefix should not be scrubbed.
		if _, err := db.Exec(`SET application_name = $1`, internalAppName); err != nil {
			t.Fatal(err)
		}
		// Set cluster to an internal testing cluster
		q := `SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing'`
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`RESET application_name`); err != nil {
			t.Fatal(err)
		}
		// Try some esoteric operators unlikely to be executed in background activity.
		if _, err := db.Exec(`SELECT '1.2.3.4'::STRING::INET, '{"a":"b","c":123}'::JSON - 'a', ARRAY (SELECT 1)[1]`); err != nil {
			t.Fatal(err)
		}
		// Try a CTE to check CTE feature reporting.
		if _, err := db.Exec(`WITH a AS (SELECT 1) SELECT * FROM a`); err != nil {
			t.Fatal(err)
		}
		// Try a recursive CTE to check recursive CTE feature reporting.
		if _, err := db.Exec(`WITH RECURSIVE a AS (SELECT 1 UNION ALL SELECT * FROM a WHERE false) SELECT * FROM a`); err != nil {
			t.Fatal(err)
		}
		// Try a correlated subquery to check that feature reporting.
		if _, err := db.Exec(`SELECT x FROM (VALUES (1)) AS b(x) WHERE EXISTS(SELECT * FROM (VALUES (1)) AS a(x) WHERE a.x = b.x)`); err != nil {
			t.Fatal(err)
		}
		// Try queries that use LATERAL.
		if _, err := db.Exec(`SELECT * FROM (VALUES (1), (2)) AS a(x), LATERAL (SELECT a.x+1)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`SELECT * FROM (VALUES (1), (2)) AS a(x) JOIN LATERAL (SELECT a.x+1 AS x) AS b ON a.x < b.x`); err != nil {
			t.Fatal(err)
		}
	}

	tables, err := ts.collectSchemaInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if actual := len(tables); actual != 2 {
		t.Fatalf("unexpected table count %d", actual)
	}
	for _, table := range tables {
		if expected, actual := "_", table.Name; expected != actual {
			t.Fatalf("unexpected table name, expected %q got %q", expected, actual)
		}
	}

	expectedUsageReports := 0

	clusterSecret := sql.ClusterSecret.Get(&st.SV)
	testutils.SucceedsSoon(t, func() error {
		expectedUsageReports++

		node := ts.node.recorder.GenerateNodeStatus(ctx)
		// Clear the SQL stat pool before getting diagnostics.
		ts.pgServer.SQLServer.ResetSQLStats(ctx)
		ts.reportDiagnostics(ctx)

		keyCounts := make(map[roachpb.StoreID]int64)
		rangeCounts := make(map[roachpb.StoreID]int64)
		totalKeys := int64(0)
		totalRanges := int64(0)

		for _, store := range node.StoreStatuses {
			if keys, ok := store.Metrics["keycount"]; ok {
				totalKeys += int64(keys)
				keyCounts[store.Desc.StoreID] = int64(keys)
			} else {
				t.Fatal("keycount not in metrics")
			}
			if replicas, ok := store.Metrics["replicas"]; ok {
				totalRanges += int64(replicas)
				rangeCounts[store.Desc.StoreID] = int64(replicas)
			} else {
				t.Fatal("replicas not in metrics")
			}
		}

		r.Lock()
		defer r.Unlock()

		if expected, actual := expectedUsageReports, r.requests; expected != actual {
			t.Fatalf("expected %v reports, got %v", expected, actual)
		}
		if expected, actual := ts.ClusterID().String(), r.last.uuid; expected != actual {
			return errors.Errorf("expected cluster id %v got %v", expected, actual)
		}
		if expected, actual := ts.node.Descriptor.NodeID, r.last.Node.NodeID; expected != actual {
			return errors.Errorf("expected node id %v got %v", expected, actual)
		}

		if r.last.Node.Hardware.Mem.Total == 0 {
			return errors.Errorf("expected non-zero total mem")
		}
		if r.last.Node.Hardware.Mem.Available == 0 {
			return errors.Errorf("expected non-zero available mem")
		}
		if actual, expected := r.last.Node.Hardware.Cpu.Numcpu, runtime.NumCPU(); int(actual) != expected {
			return errors.Errorf("expected %d num cpu, got %d", expected, actual)
		}
		if r.last.Node.Hardware.Cpu.Sockets == 0 {
			return errors.Errorf("expected non-zero sockets")
		}
		if r.last.Node.Hardware.Cpu.Mhz == 0.0 {
			return errors.Errorf("expected non-zero speed")
		}
		if r.last.Node.Os.Platform == "" {
			return errors.Errorf("expected non-empty OS")
		}

		if minExpected, actual := totalKeys, r.last.Node.KeyCount; minExpected > actual {
			return errors.Errorf("expected node keys at least %v got %v", minExpected, actual)
		}
		if minExpected, actual := totalRanges, r.last.Node.RangeCount; minExpected > actual {
			return errors.Errorf("expected node ranges at least %v got %v", minExpected, actual)
		}
		if minExpected, actual := len(params.StoreSpecs), len(r.last.Stores); minExpected > actual {
			return errors.Errorf("expected at least %v stores got %v", minExpected, actual)
		}
		if expected, actual := "true", r.last.internal; expected != actual {
			t.Errorf("expected internal to be %v, got %v", expected, actual)
		}
		if expected, actual := len(params.Locality.Tiers), len(r.last.Node.Locality.Tiers); expected != actual {
			t.Errorf("expected locality to have %d tier, got %d", expected, actual)
		}
		for i := range params.Locality.Tiers {
			if expected, actual := sql.HashForReporting(clusterSecret, params.Locality.Tiers[i].Key),
				r.last.Node.Locality.Tiers[i].Key; expected != actual {
				t.Errorf("expected locality tier %d key to be %s, got %s", i, expected, actual)
			}
			if expected, actual := sql.HashForReporting(clusterSecret, params.Locality.Tiers[i].Value),
				r.last.Node.Locality.Tiers[i].Value; expected != actual {
				t.Errorf("expected locality tier %d value to be %s, got %s", i, expected, actual)
			}
		}

		for _, store := range r.last.Stores {
			if minExpected, actual := keyCounts[store.StoreID], store.KeyCount; minExpected > actual {
				return errors.Errorf("expected at least %v keys in store %v got %v", minExpected, store.StoreID, actual)
			}
			if minExpected, actual := rangeCounts[store.StoreID], store.RangeCount; minExpected > actual {
				return errors.Errorf("expected at least %v ranges in store %v got %v", minExpected, store.StoreID, actual)
			}
		}
		return nil
	})

	// This check isn't clean, since the body is a raw proto binary and thus could
	// easily contain some encoded form of elemName, but *if* it ever does fail,
	// that is probably very interesting.
	if strings.Contains(r.last.rawReportBody, elemName) {
		t.Fatalf("%q should not appear in %q", elemName, r.last.rawReportBody)
	}

	if expected, actual := len(tables), len(r.last.Schema); expected != actual {
		t.Fatalf("expected %d tables in schema, got %d", expected, actual)
	}
	reportedByID := make(map[sqlbase.ID]sqlbase.TableDescriptor, len(tables))
	for _, tbl := range r.last.Schema {
		reportedByID[tbl.ID] = tbl
	}
	for _, tbl := range tables {
		r, ok := reportedByID[tbl.ID]
		if !ok {
			t.Fatalf("expected table %d to be in reported schema", tbl.ID)
		}
		if !reflect.DeepEqual(r, tbl) {
			t.Fatalf("reported table %d does not match: expected\n%+v got\n%+v", tbl.ID, tbl, r)
		}
	}

	// This test would be infuriating if it had to be updated on every
	// edit to the Go code that changed the line number of the trace
	// produced by force_error, so just scrub the trace
	// here.
	for k := range r.last.FeatureUsage {
		if strings.HasPrefix(k, "othererror.builtins.go") {
			r.last.FeatureUsage["othererror.builtins.go"] = r.last.FeatureUsage[k]
			delete(r.last.FeatureUsage, k)
			break
		}
	}

	expectedFeatureUsage := map[string]int32{
		"test.a": 1,
		"test.b": 2,
		"test.c": 3,

		// SERIAL normalization.
		"sql.schema.serial.rowid.int2": 1,

		// Although the query is executed 10 times, due to plan caching
		// keyed by the SQL text, the planning only occurs once.
		// TODO(radu): fix this (#39361).
		"sql.plan.ops.cast.string::inet":                                            1,
		"sql.plan.ops.bin.jsonb - string":                                           1,
		"sql.plan.builtins.crdb_internal.force_assertion_error(msg: string) -> int": 1,
		"sql.plan.ops.array.ind":                                                    1,
		"sql.plan.ops.array.cons":                                                   1,
		"sql.plan.ops.array.flatten":                                                1,
		// The CTE counter is exercised by `WITH a AS (SELECT 1) ...` and
		// `WITH RECURSIVE a AS ...` queries.
		"sql.plan.cte":           2,
		"sql.plan.cte.recursive": 1,

		// The lateral join counter is exercised by the two queries that use the
		// LATERAL keyword.
		"sql.plan.lateral-join": 2,

		// The subquery counter is exercised by `(1, 20, 30, 40) = (SELECT ...)`.
		"sql.plan.subquery": 1,
		// The correlated sq counter is exercised by `WHERE EXISTS ( ... )` above.
		"sql.plan.subquery.correlated": 1,

		"unimplemented.#33285.json_object_agg":          10,
		"unimplemented.pg_catalog.pg_stat_wal_receiver": 10,
		"unimplemented.#9148":                           10,
		"othererror." +
			pgcode.Uncategorized +
			".crdb_internal.set_vmodule()": 10,
		"errorcodes.blah":                          10,
		"errorcodes." + pgcode.Internal:            10,
		"errorcodes." + pgcode.Syntax:              10,
		"errorcodes." + pgcode.FeatureNotSupported: 10,
		"errorcodes." + pgcode.DivisionByZero:      10,
	}

	if expected, actual := len(expectedFeatureUsage), len(r.last.FeatureUsage); actual < expected {
		t.Fatalf("expected at least %d feature usage counts, got %d: %v", expected, actual, r.last.FeatureUsage)
	}
	t.Logf("%# v", pretty.Formatter(r.last.FeatureUsage))
	for key, expected := range expectedFeatureUsage {
		if got, ok := r.last.FeatureUsage[key]; !ok {
			t.Fatalf("expected report of feature %q", key)
		} else if got != expected {
			t.Fatalf("expected reported value of feature %q to be %d not %d", key, expected, got)
		}
	}

	// 3 + 3 = 6: set 3 initially and org is set mid-test for 3 altered settings,
	// plus version, reporting and secret settings are set in startup
	// migrations.
	if expected, actual := 6, len(r.last.AlteredSettings); expected != actual {
		t.Fatalf("expected %d changed settings, got %d: %v", expected, actual, r.last.AlteredSettings)
	}
	for key, expected := range map[string]string{
		"cluster.organization":                     "<redacted>",
		"diagnostics.reporting.enabled":            "true",
		"diagnostics.reporting.send_crash_reports": "false",
		"server.time_until_store_dead":             "1m30s",
		"version":                                  clusterversion.TestingBinaryVersion.String(),
		"cluster.secret":                           "<redacted>",
	} {
		if got, ok := r.last.AlteredSettings[key]; !ok {
			t.Fatalf("expected report of altered setting %q", key)
		} else if got != expected {
			t.Fatalf("expected reported value of setting %q to be %q not %q", key, expected, got)
		}
	}

	// Verify that we receive the four auto-populated zone configs plus the two
	// modified above, and that their values are as expected.
	for _, expectedID := range []int64{
		keys.RootNamespaceID,
		keys.LivenessRangesID,
		keys.MetaRangesID,
		keys.RangeEventTableID,
		keys.SystemDatabaseID,
	} {
		if _, ok := r.last.ZoneConfigs[expectedID]; !ok {
			t.Errorf("didn't find expected ID %d in reported ZoneConfigs: %+v",
				expectedID, r.last.ZoneConfigs)
		}
	}
	hashedElemName := sql.HashForReporting(clusterSecret, elemName)
	hashedZone := sql.HashForReporting(clusterSecret, "zone")
	for id, zone := range r.last.ZoneConfigs {
		if id == keys.RootNamespaceID {
			if defZone := ts.Cfg.DefaultZoneConfig; !reflect.DeepEqual(zone, defZone) {
				t.Errorf("default zone config does not match: expected\n%+v got\n%+v", defZone, zone)
			}
		}
		if id == keys.RangeEventTableID {
			if a, e := zone.GC.TTLSeconds, int32(1); a != e {
				t.Errorf("expected zone %d GC.TTLSeconds = %d; got %d", id, e, a)
			}
			if a, e := zone.Constraints, []zonepb.Constraints{
				{
					Constraints: []zonepb.Constraint{
						{Key: hashedZone, Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
						{Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
					},
				},
			}; !reflect.DeepEqual(a, e) {
				t.Errorf("expected zone %d Constraints = %+v; got %+v", id, e, a)
			}
		}
		if id == keys.SystemDatabaseID {
			if a, e := zone.Constraints, []zonepb.Constraints{
				{
					NumReplicas: 1,
					Constraints: []zonepb.Constraint{{Value: hashedElemName, Type: zonepb.Constraint_REQUIRED}},
				},
				{
					NumReplicas: 2,
					Constraints: []zonepb.Constraint{
						{Key: hashedZone, Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
						{Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
					},
				},
			}; !reflect.DeepEqual(a, e) {
				t.Errorf("expected zone %d Constraints = %+v; got %+v", id, e, a)
			}
			if a, e := zone.LeasePreferences, []zonepb.LeasePreference{
				{
					Constraints: []zonepb.Constraint{
						{Key: hashedZone, Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
						{Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
					},
				},
				{
					Constraints: []zonepb.Constraint{{Value: hashedElemName, Type: zonepb.Constraint_REQUIRED}},
				},
			}; !reflect.DeepEqual(a, e) {
				t.Errorf("expected zone %d LeasePreferences = %+v; got %+v", id, e, a)
			}
		}
	}

	var foundKeys []string
	for _, s := range r.last.SqlStats {
		if strings.HasPrefix(s.Key.App, sqlbase.InternalAppNamePrefix) {
			// Let's ignore all internal queries for this test.
			continue
		}
		var tags []string
		if s.Key.Opt {
			tags = append(tags, "opt")
		}

		if s.Key.DistSQL {
			tags = append(tags, "dist")
		} else {
			tags = append(tags, "nodist")
		}

		if s.Key.Failed {
			tags = append(tags, "failed")
		} else {
			tags = append(tags, "ok")
		}

		foundKeys = append(foundKeys, fmt.Sprintf("[%s] %s", strings.Join(tags, ","), s.Key.Query))
	}
	sort.Strings(foundKeys)
	expectedKeys := []string{
		`[opt,nodist,ok] ALTER DATABASE _ CONFIGURE ZONE = _`,
		`[opt,nodist,ok] ALTER TABLE _ CONFIGURE ZONE = _`,
		`[opt,nodist,ok] CREATE DATABASE _`,
		`[opt,nodist,ok] SET CLUSTER SETTING "cluster.organization" = _`,
		`[opt,nodist,ok] SET CLUSTER SETTING "diagnostics.reporting.send_crash_reports" = _`,
		`[opt,nodist,ok] SET CLUSTER SETTING "server.time_until_store_dead" = _`,
		`[opt,nodist,ok] SET application_name = $1`,
		`[opt,nodist,ok] SET application_name = DEFAULT`,
		`[opt,nodist,ok] SET application_name = _`,
		`[opt,nodist,ok] CREATE TABLE _ (_ INT8 NOT NULL DEFAULT unique_rowid())`,
		`[opt,nodist,ok] CREATE TABLE _ (_ INT8, CONSTRAINT _ CHECK (_ > _))`,
		`[opt,nodist,ok] INSERT INTO _ SELECT unnest(ARRAY[_, _, __more2__])`,
		`[opt,nodist,ok] INSERT INTO _ VALUES (_), (__more2__)`,
		`[opt,nodist,ok] INSERT INTO _ VALUES (length($1::STRING)), (__more1__)`,
		`[opt,nodist,ok] INSERT INTO _(_, _) VALUES (_, _)`,
		`[opt,nodist,ok] SELECT (_, _, __more2__) = (SELECT _, _, _, _ FROM _ LIMIT _)`,
		`[opt,nodist,ok] SELECT _ FROM (VALUES (_)) AS _ (_) WHERE EXISTS (SELECT * FROM (VALUES (_)) AS _ (_) WHERE _._ = _._)`,
		"[opt,nodist,ok] SELECT _::STRING::INET, _::JSONB - _, ARRAY (SELECT _)[_]",
		`[opt,nodist,ok] UPDATE _ SET _ = _ + _`,
		"[opt,nodist,ok] WITH _ AS (SELECT _) SELECT * FROM _",
		`[opt,nodist,ok] WITH RECURSIVE _ AS (SELECT _ UNION ALL SELECT * FROM _ WHERE _) SELECT * FROM _`,
		`[opt,nodist,ok] SELECT * FROM (VALUES (_), (__more1__)) AS _ (_), LATERAL (SELECT _._ + _)`,
		`[opt,nodist,ok] SELECT * FROM (VALUES (_), (__more1__)) AS _ (_) JOIN LATERAL (SELECT _._ + _ AS _) AS _ ON _._ < _._`,
		`[opt,nodist,failed] CREATE TABLE _ (_ INT8 PRIMARY KEY, _ INT8, INDEX (_) INTERLEAVE IN PARENT _ (_))`,
		`[opt,nodist,failed] SELECT _ / $1`,
		`[opt,nodist,failed] SELECT _ / _`,
		`[opt,nodist,failed] SELECT crdb_internal.force_assertion_error(_)`,
		`[opt,nodist,failed] SELECT crdb_internal.force_error(_, $1)`,
		`[opt,nodist,failed] SELECT crdb_internal.set_vmodule(_)`,
		`[opt,dist,ok] SELECT * FROM _ WHERE (_ = _) AND (_ = _)`,
		`[opt,dist,ok] SELECT * FROM _ WHERE (_ = length($1::STRING)) OR (_ = $2)`,
		`[opt,dist,ok] SELECT _ FROM _ WHERE (_ = _) AND (lower(_) = lower(_))`,
	}
	sort.Strings(expectedKeys)
	t.Logf("expected:\n%s\ngot:\n%s", pretty.Sprint(expectedKeys), pretty.Sprint(foundKeys))
	for i, found := range foundKeys {
		if i >= len(expectedKeys) {
			t.Fatalf("extraneous stat entry: %q", found)
		}
		if expectedKeys[i] != found {
			t.Fatalf("expected entry %d to be %q, got %q", i, expectedKeys[i], found)
		}
	}
	if len(foundKeys) < len(expectedKeys) {
		t.Fatalf("expected %d stat entries, found %d", len(expectedKeys), len(foundKeys))
	}

	bucketByApp := make(map[string][]roachpb.CollectedStatementStatistics)
	for _, s := range r.last.SqlStats {
		if strings.HasPrefix(s.Key.App, sqlbase.InternalAppNamePrefix) {
			// Let's ignore all internal queries for this test.
			continue
		}
		bucketByApp[s.Key.App] = append(bucketByApp[s.Key.App], s)
	}

	if expected, actual := 3, len(bucketByApp); expected != actual {
		t.Fatalf("expected %d apps in stats report, got %d", expected, actual)
	}

	for appName, expectedStatements := range map[string][]string{
		"": {
			`ALTER DATABASE _ CONFIGURE ZONE = _`,
			`ALTER TABLE _ CONFIGURE ZONE = _`,
			`CREATE DATABASE _`,
			`CREATE TABLE _ (_ INT8, CONSTRAINT _ CHECK (_ > _))`,
			`CREATE TABLE _ (_ INT8 NOT NULL DEFAULT unique_rowid())`,
			`CREATE TABLE _ (_ INT8 PRIMARY KEY, _ INT8, INDEX (_) INTERLEAVE IN PARENT _ (_))`,
			`INSERT INTO _ VALUES (length($1::STRING)), (__more1__)`,
			`INSERT INTO _ VALUES (_), (__more2__)`,
			`INSERT INTO _ SELECT unnest(ARRAY[_, _, __more2__])`,
			`INSERT INTO _(_, _) VALUES (_, _)`,
			`SELECT (_, _, __more2__) = (SELECT _, _, _, _ FROM _ LIMIT _)`,
			`SELECT _ FROM (VALUES (_)) AS _ (_) WHERE EXISTS (SELECT * FROM (VALUES (_)) AS _ (_) WHERE _._ = _._)`,
			`SELECT _::STRING::INET, _::JSONB - _, ARRAY (SELECT _)[_]`,
			`SELECT * FROM _ WHERE (_ = length($1::STRING)) OR (_ = $2)`,
			`SELECT * FROM _ WHERE (_ = _) AND (_ = _)`,
			`SELECT _ / $1`,
			`SELECT _ / _`,
			`SELECT crdb_internal.force_assertion_error(_)`,
			`SELECT crdb_internal.force_error(_, $1)`,
			`SELECT crdb_internal.set_vmodule(_)`,
			`SET CLUSTER SETTING "server.time_until_store_dead" = _`,
			`SET CLUSTER SETTING "diagnostics.reporting.send_crash_reports" = _`,
			`SET application_name = _`,
			`WITH _ AS (SELECT _) SELECT * FROM _`,
			`WITH RECURSIVE _ AS (SELECT _ UNION ALL SELECT * FROM _ WHERE _) SELECT * FROM _`,
			`SELECT * FROM (VALUES (_), (__more1__)) AS _ (_), LATERAL (SELECT _._ + _)`,
			`SELECT * FROM (VALUES (_), (__more1__)) AS _ (_) JOIN LATERAL (SELECT _._ + _ AS _) AS _ ON _._ < _._`,
		},
		elemName: {
			`SELECT _ FROM _ WHERE (_ = _) AND (lower(_) = lower(_))`,
			`UPDATE _ SET _ = _ + _`,
			`SET application_name = $1`,
		},
		internalAppName: {
			`SET CLUSTER SETTING "cluster.organization" = _`,
			`SET application_name = DEFAULT`,
		},
	} {
		maybeHashedAppName := sql.HashForReporting(clusterSecret, appName)
		if appName == internalAppName {
			// Exempted from hashing due to '$ ' prefix.
			maybeHashedAppName = internalAppName
		}
		if maybeHashedAppName == sql.FailedHashedValue {
			t.Fatalf("expected maybeHashedAppName to not be 'unknown'")
		}
		if app, ok := bucketByApp[maybeHashedAppName]; !ok {
			t.Fatalf("missing stats for app %q %+v", appName, bucketByApp)
		} else {
			if actual, expected := len(app), len(expectedStatements); expected != actual {
				for _, q := range app {
					t.Log(q.Key.Query, q.Key)
				}
				t.Fatalf("expected %d statements in app %q report, got %d: %+v", expected, appName, actual, app)
			}
			keys := make(map[string]struct{})
			for _, q := range app {
				keys[q.Key.Query] = struct{}{}
			}
			for _, expected := range expectedStatements {
				if _, ok := keys[expected]; !ok {
					t.Fatalf("expected %q in app %s: %s", expected, pretty.Sprint(appName), pretty.Sprint(keys))
				}
			}
		}
	}
}

type mockRecorder struct {
	*httptest.Server
	url *url.URL

	syncutil.Mutex
	requests int
	last     struct {
		uuid        string
		version     string
		licenseType string
		internal    string
		diagnosticspb.DiagnosticReport
		rawReportBody string
	}
}

func makeMockRecorder(t *testing.T) *mockRecorder {
	rec := &mockRecorder{}

	rec.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		rec.Lock()
		defer rec.Unlock()

		rec.requests++
		rec.last.uuid = r.URL.Query().Get("uuid")
		rec.last.version = r.URL.Query().Get("version")
		rec.last.licenseType = r.URL.Query().Get("licensetype")
		rec.last.internal = r.URL.Query().Get("internal")
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		rec.last.rawReportBody = string(body)
		// TODO(dt): switch on the request path to handle different request types.
		if err := protoutil.Unmarshal(body, &rec.last.DiagnosticReport); err != nil {
			panic(err)
		}
	}))

	u, err := url.Parse(rec.URL)
	if err != nil {
		t.Fatal(err)
	}
	rec.url = u

	return rec
}
