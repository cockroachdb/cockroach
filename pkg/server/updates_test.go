// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	s.(*TestServer).checkForUpdates(time.Minute)
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
		s.(*TestServer).checkForUpdates(time.Minute)
	})
}

func TestReportUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()

	r := makeMockRecorder(t)
	defer stubURL(&reportingURL, r.url)()
	defer r.Close()

	st := cluster.MakeTestingClusterSettings()

	params := base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
		},
		Settings: st,
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "east"},
				{Key: "state", Value: "ny"},
				{Key: "city", Value: "nyc"},
			},
		},
	}
	s, db, _ := serverutils.StartServer(t, params)
	ts := s.(*TestServer)

	if err := ts.WaitForInitialSplits(); err != nil {
		t.Fatal(err)
	}

	ts.sqlExecutor.ResetStatementStats(ctx)

	const elemName = "somestring"
	if _, err := db.Exec(fmt.Sprintf(`CREATE DATABASE %s`, elemName)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`SET CLUSTER SETTING server.time_until_store_dead = '20s'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`SET CLUSTER SETTING diagnostics.reporting.send_crash_reports = false`); err != nil {
		t.Fatal(err)
	}

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
		if _, err := db.Exec(
			fmt.Sprintf(`ALTER %s EXPERIMENTAL CONFIGURE ZONE '%s'`, cmd.resource, cmd.config),
		); err != nil {
			t.Fatalf("error applying zone config %q to %q: %v", cmd.config, cmd.resource, err)
		}
	}

	if _, err := db.Exec(
		fmt.Sprintf(`CREATE TABLE %[1]s.%[1]s (%[1]s INT CONSTRAINT %[1]s CHECK (%[1]s > 1))`, elemName),
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
		if _, err := db.Exec(`ALTER TABLE foo RENAME CONSTRAINT x TO y`); !testutils.IsError(
			err, "unimplemented",
		) {
			t.Fatal(err)
		}
		// pass args to force a prepare/exec path as that may differ.
		if _, err := db.Exec(`SELECT 1::INTERVAL(1), $1`, 1); !testutils.IsError(
			err, "unimplemented",
		) {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE TABLE somestring.foo (a INT PRIMARY KEY, b INT, INDEX (b) INTERLEAVE IN PARENT foo (b))`); !testutils.IsError(
			err, "unimplemented: use CREATE INDEX to make interleaved indexes",
		) {
			t.Fatal(err)
		}
		// Even queries that don't use placeholders and contain literal strings
		// should still not cause those strings to appear in reports.
		for _, q := range []string{
			`SELECT * FROM %[1]s.%[1]s WHERE %[1]s = 1 AND '%[1]s' = '%[1]s'`,
			`INSERT INTO %[1]s.%[1]s VALUES (6), (7)`,
			`SET application_name = '%[1]s'`,
			`SELECT %[1]s FROM %[1]s.%[1]s WHERE %[1]s = 1 AND lower('%[1]s') = lower('%[1]s')`,
			`UPDATE %[1]s.%[1]s SET %[1]s = %[1]s + 1`,
		} {
			if _, err := db.Exec(fmt.Sprintf(q, elemName)); err != nil {
				t.Fatal(err)
			}
		}
		// Set cluster to an internal testing cluster
		q := `SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing'`
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`RESET application_name`); err != nil {
			t.Fatal(err)
		}
	}

	tables, err := ts.collectSchemaInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if actual := len(tables); actual != 1 {
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

		node := ts.node.recorder.GetStatusSummary(ctx)
		ts.reportDiagnostics(0)

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

	if expected, actual := 5, len(r.last.ErrorCounts); expected != actual {
		t.Fatalf("expected %d error codes counts in report, got %d (%v)", expected, actual, r.last.ErrorCounts)
	}

	// this test would be infuriating if it had to be updated on every edit to
	// builtins.go that changed the line number of force_error, so just scrub the
	// line number here.
	for k := range r.last.ErrorCounts {
		if strings.HasPrefix(k, "builtins.go") {
			r.last.ErrorCounts["builtins.go"] = r.last.ErrorCounts[k]
			delete(r.last.ErrorCounts, k)
			break
		}
	}

	for code, expected := range map[string]int64{
		pgerror.CodeSyntaxError:              10,
		pgerror.CodeFeatureNotSupportedError: 30,
		pgerror.CodeDivisionByZeroError:      20,
		"blah":        10,
		"builtins.go": 10,
	} {
		if actual := r.last.ErrorCounts[code]; expected != actual {
			t.Fatalf(
				"unexpected %d hits to error code %q, got %d from %v",
				expected, code, actual, r.last.ErrorCounts,
			)
		}
	}

	if expected, actual := 3, len(r.last.UnimplementedErrors); expected != actual {
		t.Fatalf("expected %d unimplemented feature errors, got %d", expected, actual)
	}

	for _, feat := range []string{"alter table rename constraint", "simple_type const_interval", "#9148"} {
		if expected, actual := int64(10), r.last.UnimplementedErrors[feat]; expected != actual {
			t.Fatalf(
				"unexpected %d hits to unimplemented %q, got %d from %v",
				expected, feat, actual, r.last.UnimplementedErrors,
			)
		}
	}

	// 3 + 4 = 7: set 3 initially and org is set mid-test for 3 altered settings,
	// plus version, reporting, trace and secret settings are set in startup
	// migrations.
	if expected, actual := 7, len(r.last.AlteredSettings); expected != actual {
		t.Fatalf("expected %d changed settings, got %d: %v", expected, actual, r.last.AlteredSettings)
	}
	for key, expected := range map[string]string{
		"cluster.organization":                     "<redacted>",
		"diagnostics.reporting.enabled":            "true",
		"diagnostics.reporting.send_crash_reports": "false",
		"server.time_until_store_dead":             "20s",
		"trace.debug.enable":                       "false",
		"version":                                  "2.0-2",
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
		keys.JobsTableID,
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
			if !reflect.DeepEqual(zone, config.DefaultZoneConfig()) {
				t.Errorf("default zone config does not match: expected\n%+v got\n%+v",
					config.DefaultZoneConfig(), zone)
			}
		}
		if id == keys.RangeEventTableID {
			if a, e := zone.GC.TTLSeconds, int32(1); a != e {
				t.Errorf("expected zone %d GC.TTLSeconds = %d; got %d", id, e, a)
			}
			if a, e := zone.Constraints, []config.Constraints{
				{
					Constraints: []config.Constraint{
						{Key: hashedZone, Value: hashedElemName, Type: config.Constraint_REQUIRED},
						{Value: hashedElemName, Type: config.Constraint_REQUIRED},
					},
				},
			}; !reflect.DeepEqual(a, e) {
				t.Errorf("expected zone %d Constraints = %+v; got %+v", id, e, a)
			}
		}
		if id == keys.SystemDatabaseID {
			if a, e := zone.Constraints, []config.Constraints{
				{
					NumReplicas: 1,
					Constraints: []config.Constraint{{Value: hashedElemName, Type: config.Constraint_REQUIRED}},
				},
				{
					NumReplicas: 2,
					Constraints: []config.Constraint{
						{Key: hashedZone, Value: hashedElemName, Type: config.Constraint_REQUIRED},
						{Value: hashedElemName, Type: config.Constraint_REQUIRED},
					},
				},
			}; !reflect.DeepEqual(a, e) {
				t.Errorf("expected zone %d Constraints = %+v; got %+v", id, e, a)
			}
			if a, e := zone.LeasePreferences, []config.LeasePreference{
				{
					Constraints: []config.Constraint{
						{Key: hashedZone, Value: hashedElemName, Type: config.Constraint_REQUIRED},
						{Value: hashedElemName, Type: config.Constraint_REQUIRED},
					},
				},
				{
					Constraints: []config.Constraint{{Value: hashedElemName, Type: config.Constraint_REQUIRED}},
				},
			}; !reflect.DeepEqual(a, e) {
				t.Errorf("expected zone %d LeasePreferences = %+v; got %+v", id, e, a)
			}
		}
	}

	if expected, actual := 15, len(r.last.SqlStats); expected != actual {
		t.Fatalf("expected %d queries in stats report, got %d :\n %v", expected, actual, r.last.SqlStats)
	}

	bucketByApp := make(map[string][]roachpb.CollectedStatementStatistics)
	for _, s := range r.last.SqlStats {
		bucketByApp[s.Key.App] = append(bucketByApp[s.Key.App], s)
	}

	if expected, actual := 2, len(bucketByApp); expected != actual {
		t.Fatalf("expected %d apps in stats report, got %d", expected, actual)
	}

	for appName, expectedStatements := range map[string][]string{
		"": {
			`CREATE DATABASE _`,
			`CREATE TABLE _ (_ INT, CONSTRAINT _ CHECK (_ > _))`,
			`CREATE TABLE _ (_ INT PRIMARY KEY, _ INT, INDEX (_) INTERLEAVE IN PARENT _ (_))`,
			`INSERT INTO _ VALUES (length($1::STRING))`,
			`INSERT INTO _ VALUES (_)`,
			`SELECT * FROM _ WHERE (_ = length($1::STRING)) OR (_ = $2)`,
			`SELECT * FROM _ WHERE (_ = _) AND (_ = _)`,
			`SELECT _ / $1`,
			`SELECT _ / _`,
			`SELECT crdb_internal.force_error(_, $1)`,
			`SET CLUSTER SETTING "server.time_until_store_dead" = _`,
			`SET CLUSTER SETTING "diagnostics.reporting.send_crash_reports" = _`,
		},
		elemName: {
			`SELECT _ FROM _ WHERE (_ = _) AND (lower(_) = lower(_))`,
			`UPDATE _ SET _ = _ + _`,
			`SET CLUSTER SETTING "cluster.organization" = _`,
		},
	} {
		hashedAppName := sql.HashForReporting(clusterSecret, appName)
		if hashedAppName == sql.FailedHashedValue {
			t.Fatalf("expected hashedAppName to not be 'unknown'")
		}
		if app, ok := bucketByApp[hashedAppName]; !ok {
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
					t.Fatalf("expected %q in app %s: %+v", expected, appName, keys)
				}
			}
		}
	}

	ts.Stopper().Stop(context.TODO()) // stopper will wait for the update/report loop to finish too.
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
