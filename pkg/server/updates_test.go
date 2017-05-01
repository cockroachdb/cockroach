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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

	updateChecks := int32(0)
	uuid := ""
	version := ""

	recorder := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		atomic.AddInt32(&updateChecks, 1)
		uuid = r.URL.Query().Get("uuid")
		version = r.URL.Query().Get("version")
	}))
	u, err := url.Parse(recorder.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer stubURL(&updatesURL, u)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	s.(*TestServer).checkForUpdates(time.Minute)
	recorder.Close()
	s.Stopper().Stop(context.TODO())

	if expected, actual := int32(1), atomic.LoadInt32(&updateChecks); actual != expected {
		t.Fatalf("expected %v update checks, got %v", expected, actual)
	}

	if expected, actual := s.(*TestServer).node.ClusterID.String(), uuid; expected != actual {
		t.Errorf("expected uuid %v, got %v", expected, actual)
	}

	if expected, actual := build.GetInfo().Tag, version; expected != actual {
		t.Errorf("expected version tag %v, got %v", expected, actual)
	}
}

func TestReportUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()

	usageReports := int32(0)
	var uuid, rawReportBody string
	reported := reportingInfo{}

	recorder := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		atomic.AddInt32(&usageReports, 1)
		uuid = r.URL.Query().Get("uuid")
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewDecoder(bytes.NewReader(body)).Decode(&reported); err != nil {
			t.Fatal(err)
		}
		rawReportBody = string(body)
	}))
	u, err := url.Parse(recorder.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer stubURL(&reportingURL, u)()

	params := base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
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

	var expectedUsageReports int32
	testutils.SucceedsSoon(t, func() error {
		expectedUsageReports++

		node := ts.node.recorder.GetStatusSummary()
		ts.reportDiagnostics(0)

		keyCounts := make(map[roachpb.StoreID]int)
		rangeCounts := make(map[roachpb.StoreID]int)
		totalKeys := 0
		totalRanges := 0

		for _, store := range node.StoreStatuses {
			if keys, ok := store.Metrics["keycount"]; ok {
				totalKeys += int(keys)
				keyCounts[store.Desc.StoreID] = int(keys)
			} else {
				t.Fatal("keycount not in metrics")
			}
			if replicas, ok := store.Metrics["replicas"]; ok {
				totalRanges += int(replicas)
				rangeCounts[store.Desc.StoreID] = int(replicas)
			} else {
				t.Fatal("replicas not in metrics")
			}
		}

		if expected, actual := expectedUsageReports, atomic.LoadInt32(&usageReports); expected != actual {
			t.Fatalf("expected %v reports, got %v", expected, actual)
		}
		if expected, actual := ts.node.ClusterID.String(), uuid; expected != actual {
			return errors.Errorf("expected cluster id %v got %v", expected, actual)
		}
		if expected, actual := ts.node.Descriptor.NodeID, reported.Node.NodeID; expected != actual {
			return errors.Errorf("expected node id %v got %v", expected, actual)
		}
		if minExpected, actual := totalKeys, reported.Node.KeyCount; minExpected > actual {
			return errors.Errorf("expected node keys at least %v got %v", minExpected, actual)
		}
		if minExpected, actual := totalRanges, reported.Node.RangeCount; minExpected > actual {
			return errors.Errorf("expected node ranges at least %v got %v", minExpected, actual)
		}
		if minExpected, actual := len(params.StoreSpecs), len(reported.Stores); minExpected > actual {
			return errors.Errorf("expected at least %v stores got %v", minExpected, actual)
		}

		for _, store := range reported.Stores {
			if minExpected, actual := keyCounts[store.StoreID], store.KeyCount; minExpected > actual {
				return errors.Errorf("expected at least %v keys in store %v got %v", minExpected, store.StoreID, actual)
			}
			if minExpected, actual := rangeCounts[store.StoreID], store.RangeCount; minExpected > actual {
				return errors.Errorf("expected at least %v ranges in store %v got %v", minExpected, store.StoreID, actual)
			}
		}
		return nil
	})

	if strings.Contains(rawReportBody, elemName) {
		t.Fatalf("%q should not appear in %q", elemName, rawReportBody)
	}

	if expected, actual := len(tables), len(reported.Schema); expected != actual {
		t.Fatalf("expected %d tables in schema, got %d", expected, actual)
	}
	reportedByID := make(map[sqlbase.ID]sqlbase.TableDescriptor, len(tables))
	for _, r := range reported.Schema {
		reportedByID[r.ID] = r
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

	if expected, actual := 2, len(reported.QueryStats); expected != actual {
		t.Fatalf("expected %d apps in stats report, got %d", expected, actual)
	}

	for appName, expectedStatements := range map[string][]string{
		"": {
			`CREATE DATABASE _`,
			`CREATE TABLE _ (_ INT, CONSTRAINT _ CHECK (_ > _))`,
			`INSERT INTO _ VALUES (length(_::STRING))`,
			`INSERT INTO _ VALUES (_)`,
			`SELECT * FROM _ WHERE (_ = length(_::STRING)) OR (_ = $2)`,
			`SELECT * FROM _ WHERE (_ = _) AND (_ = _)`,
		},
		elemName: {
			`SELECT _ FROM _ WHERE (_ = _) AND (lower(_) = lower(_))`,
			`UPDATE _ SET _ = _ + _`,
		},
	} {
		if app, ok := reported.QueryStats[sql.HashAppName(appName)]; !ok {
			t.Fatalf("missing stats for default app")
		} else {
			if actual, expected := len(app), len(expectedStatements); expected != actual {
				t.Fatalf("expected %d statements in app %s report, got %d", expected, appName, actual)
			}
			keys := make(map[string]struct{})
			for k := range app {
				keys[k] = struct{}{}
			}
			for _, expected := range expectedStatements {
				if _, ok := app[expected]; !ok {
					t.Fatalf("expected %q in app %s: %+v", expected, appName, keys)
				}
			}
		}
	}

	ts.Stopper().Stop(context.TODO()) // stopper will wait for the update/report loop to finish too.
	recorder.Close()
}
