// Copyright 2014 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func getAdminJSONProto(
	ts serverutils.TestServerInterface, path string, response proto.Message,
) error {
	return serverutils.GetJSONProto(ts, adminPrefix+path, response)
}

func postAdminJSONProto(
	ts serverutils.TestServerInterface, path string, request, response proto.Message,
) error {
	return serverutils.PostJSONProto(ts, adminPrefix+path, request, response)
}

// getText fetches the HTTP response body as text in the form of a
// byte slice from the specified URL.
func getText(ts serverutils.TestServerInterface, url string) ([]byte, error) {
	httpClient, err := ts.GetHTTPClient()
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

// getJSON fetches the JSON from the specified URL and returns
// it as unmarshaled JSON. Returns an error on any failure to fetch
// or unmarshal response body.
func getJSON(ts serverutils.TestServerInterface, url string) (interface{}, error) {
	body, err := getText(ts, url)
	if err != nil {
		return nil, err
	}
	var jI interface{}
	if err := json.Unmarshal(body, &jI); err != nil {
		return nil, err
	}
	return jI, nil
}

// debugURL returns the root debug URL.
func debugURL(s serverutils.TestServerInterface) string {
	return s.AdminURL() + debugEndpoint
}

// TestAdminDebugExpVar verifies that cmdline and memstats variables are
// available via the /debug/vars link.
func TestAdminDebugExpVar(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	jI, err := getJSON(s, debugURL(s)+"vars")
	if err != nil {
		t.Fatalf("failed to fetch JSON: %v", err)
	}
	j := jI.(map[string]interface{})
	if _, ok := j["cmdline"]; !ok {
		t.Error("cmdline not found in JSON response")
	}
	if _, ok := j["memstats"]; !ok {
		t.Error("memstats not found in JSON response")
	}
}

// TestAdminDebugMetrics verifies that cmdline and memstats variables are
// available via the /debug/metrics link.
func TestAdminDebugMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	jI, err := getJSON(s, debugURL(s)+"metrics")
	if err != nil {
		t.Fatalf("failed to fetch JSON: %v", err)
	}
	j := jI.(map[string]interface{})
	if _, ok := j["cmdline"]; !ok {
		t.Error("cmdline not found in JSON response")
	}
	if _, ok := j["memstats"]; !ok {
		t.Error("memstats not found in JSON response")
	}
}

// TestAdminDebugPprof verifies that pprof tools are available.
// via the /debug/pprof/* links.
func TestAdminDebugPprof(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	body, err := getText(s, debugURL(s)+"pprof/block")
	if err != nil {
		t.Fatal(err)
	}
	if exp := "contention:\ncycles/second="; !bytes.Contains(body, []byte(exp)) {
		t.Errorf("expected %s to contain %s", body, exp)
	}
}

// TestAdminDebugTrace verifies that the net/trace endpoints are available
// via /debug/{requests,events}.
func TestAdminDebugTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	tc := []struct {
		segment, search string
	}{
		{"requests", "<title>/debug/requests</title>"},
		{"events", "<title>events</title>"},
	}

	for _, c := range tc {
		body, err := getText(s, debugURL(s)+c.segment)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(body, []byte(c.search)) {
			t.Errorf("expected %s to be contained in %s", c.search, body)
		}
	}
}

// TestAdminDebugRedirect verifies that the /debug/ endpoint is redirected to on
// incorrect /debug/ paths.
func TestAdminDebugRedirect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	expURL := debugURL(s)
	origURL := expURL + "incorrect"

	// There are no particular permissions on admin endpoints, TestUser is fine.
	client, err := testutils.NewTestBaseContext(TestUser).GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	// Don't follow redirects automatically.
	redirectAttemptedError := errors.New("redirect")
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return redirectAttemptedError
	}

	resp, err := client.Get(origURL)
	if urlError, ok := err.(*url.Error); ok && urlError.Err == redirectAttemptedError {
		// Ignore the redirectAttemptedError.
		err = nil
	}
	if err != nil {
		t.Fatal(err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusMovedPermanently {
			t.Errorf("expected status code %d; got %d", http.StatusMovedPermanently, resp.StatusCode)
		}
		if redirectURL, err := resp.Location(); err != nil {
			t.Error(err)
		} else if foundURL := redirectURL.String(); foundURL != expURL {
			t.Errorf("expected location %s; got %s", expURL, foundURL)
		}
	}
}

func TestAdminAPIDatabases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)

	ac := log.AmbientContext{Tracer: tracing.NewTracer()}
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()

	// Test databases endpoint.
	const testdb = "test"
	session := sql.NewSession(
		ctx, sql.SessionArgs{User: security.RootUser}, ts.sqlExecutor, nil, &sql.MemoryMetrics{})
	session.StartUnlimitedMonitor()
	defer session.Finish(ts.sqlExecutor)
	query := "CREATE DATABASE " + testdb
	createRes := ts.sqlExecutor.ExecuteStatements(session, query, nil)
	defer createRes.Close(ctx)

	if createRes.ResultList[0].Err != nil {
		t.Fatal(createRes.ResultList[0].Err)
	}

	var resp serverpb.DatabasesResponse
	if err := getAdminJSONProto(s, "databases", &resp); err != nil {
		t.Fatal(err)
	}

	expectedDBs := []string{"system", testdb}
	if a, e := len(resp.Databases), len(expectedDBs); a != e {
		t.Fatalf("length of result %d != expected %d", a, e)
	}

	sort.Strings(resp.Databases)
	for i, e := range expectedDBs {
		if a := resp.Databases[i]; a != e {
			t.Fatalf("database name %s != expected %s", a, e)
		}
	}

	// Test database details endpoint.
	privileges := []string{"SELECT", "UPDATE"}
	testuser := "testuser"
	grantQuery := "GRANT " + strings.Join(privileges, ", ") + " ON DATABASE " + testdb + " TO " + testuser
	grantRes := s.(*TestServer).sqlExecutor.ExecuteStatements(session, grantQuery, nil)
	defer grantRes.Close(ctx)
	if grantRes.ResultList[0].Err != nil {
		t.Fatal(grantRes.ResultList[0].Err)
	}

	var details serverpb.DatabaseDetailsResponse
	if err := getAdminJSONProto(s, "databases/"+testdb, &details); err != nil {
		t.Fatal(err)
	}

	if a, e := len(details.Grants), 3; a != e {
		t.Fatalf("# of grants %d != expected %d", a, e)
	}

	userGrants := make(map[string][]string)
	for _, grant := range details.Grants {
		switch grant.User {
		case security.RootUser, testuser:
			userGrants[grant.User] = append(userGrants[grant.User], grant.Privileges...)
		default:
			t.Fatalf("unknown grant to user %s", grant.User)
		}
	}
	for u, p := range userGrants {
		switch u {
		case security.RootUser:
			if !reflect.DeepEqual(p, []string{"ALL"}) {
				t.Fatalf("privileges %v != expected %v", p, privileges)
			}
		case testuser:
			sort.Strings(p)
			if !reflect.DeepEqual(p, privileges) {
				t.Fatalf("privileges %v != expected %v", p, privileges)
			}
		default:
			t.Fatalf("unknown grant to user %s", u)
		}
	}

	// Verify Descriptor ID.
	path, err := ts.admin.queryDescriptorIDPath(ctx, session, []string{testdb})
	if err != nil {
		t.Fatal(err)
	}
	if a, e := details.DescriptorID, int64(path[1]); a != e {
		t.Fatalf("db had descriptorID %d, expected %d", a, e)
	}
}

func TestAdminAPIDatabaseDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	const errPattern = "database.+does not exist"
	if err := getAdminJSONProto(s, "databases/i_do_not_exist", nil); !testutils.IsError(err, errPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPIDatabaseVirtual(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	const errPattern = `\\"information_schema\\" is a virtual schema`
	if err := getAdminJSONProto(s, "databases/information_schema", nil); !testutils.IsError(err, errPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPIDatabaseSQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	const fakedb = "system;DROP DATABASE system;"
	const path = "databases/" + fakedb
	const errPattern = `database \\"` + fakedb + `\\" does not exist`
	if err := getAdminJSONProto(s, path, nil); !testutils.IsError(err, errPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPITableDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	const fakename = "i_do_not_exist"
	const badDBPath = "databases/" + fakename + "/tables/foo"
	const dbErrPattern = `database \\"` + fakename + `\\" does not exist`
	if err := getAdminJSONProto(s, badDBPath, nil); !testutils.IsError(err, dbErrPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, dbErrPattern)
	}

	const badTablePath = "databases/system/tables/" + fakename
	const tableErrPattern = `table \\"system.` + fakename + `\\" does not exist`
	if err := getAdminJSONProto(s, badTablePath, nil); !testutils.IsError(err, tableErrPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, tableErrPattern)
	}
}

func TestAdminAPITableVirtual(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	const virtual = "information_schema"
	const badDBPath = "databases/" + virtual + "/tables/tables"
	const dbErrPattern = `\\"` + virtual + `\\" is a virtual schema`
	if err := getAdminJSONProto(s, badDBPath, nil); !testutils.IsError(err, dbErrPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, dbErrPattern)
	}
}

func TestAdminAPITableSQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	const fakeTable = "users;DROP DATABASE system;"
	const path = "databases/system/tables/" + fakeTable
	const errPattern = `table \"system.\\\"` + fakeTable + `\\\"\" does not exist`
	if err := getAdminJSONProto(s, path, nil); !testutils.IsError(err, regexp.QuoteMeta(errPattern)) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPITableDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name, dbName, tblName string
	}{
		{name: "lower", dbName: "test", tblName: "tbl"},
		{name: "lower with space", dbName: "test test", tblName: "tbl tbl"},
		{name: "upper", dbName: "TEST", tblName: "TBL"}, // Regression test for issue #14056
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(context.TODO())
			ts := s.(*TestServer)

			escDBName := parser.Name(tc.dbName).String()
			escTblName := parser.Name(tc.tblName).String()

			ac := log.AmbientContext{Tracer: tracing.NewTracer()}
			ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
			defer span.Finish()

			session := sql.NewSession(
				ctx, sql.SessionArgs{User: security.RootUser}, ts.sqlExecutor, nil, &sql.MemoryMetrics{})
			session.StartUnlimitedMonitor()
			defer session.Finish(ts.sqlExecutor)
			setupQueries := []string{
				fmt.Sprintf("CREATE DATABASE %s", escDBName),
				fmt.Sprintf(`CREATE TABLE %s.%s (
							nulls_allowed INT,
							nulls_not_allowed INT NOT NULL DEFAULT 1000,
							default2 INT DEFAULT 2,
							string_default STRING DEFAULT 'default_string'
						)`, escDBName, escTblName),
				fmt.Sprintf("GRANT SELECT ON %s.%s TO readonly", escDBName, escTblName),
				fmt.Sprintf("GRANT SELECT,UPDATE,DELETE ON %s.%s TO app", escDBName, escTblName),
				fmt.Sprintf("CREATE INDEX descidx ON %s.%s (default2 DESC)", escDBName, escTblName),
			}

			for _, q := range setupQueries {
				res := ts.sqlExecutor.ExecuteStatements(session, q, nil)
				defer res.Close(ctx)
				if res.ResultList[0].Err != nil {
					t.Fatalf("error executing '%s': %s", q, res.ResultList[0].Err)
				}
			}

			// Perform API call.
			var resp serverpb.TableDetailsResponse
			url := fmt.Sprintf("databases/%s/tables/%s", tc.dbName, tc.tblName)
			if err := getAdminJSONProto(s, url, &resp); err != nil {
				t.Fatal(err)
			}

			// Verify columns.
			expColumns := []serverpb.TableDetailsResponse_Column{
				{Name: "nulls_allowed", Type: "INT", Nullable: true, DefaultValue: ""},
				{Name: "nulls_not_allowed", Type: "INT", Nullable: false, DefaultValue: "1000:::INT"},
				{Name: "default2", Type: "INT", Nullable: true, DefaultValue: "2:::INT"},
				{Name: "string_default", Type: "STRING", Nullable: true, DefaultValue: "'default_string':::STRING"},
			}
			testutils.SortStructs(expColumns, "Name")
			testutils.SortStructs(resp.Columns, "Name")
			if a, e := len(resp.Columns), len(expColumns); a != e {
				t.Fatalf("# of result columns %d != expected %d (got: %#v)", a, e, resp.Columns)
			}
			for i, a := range resp.Columns {
				e := expColumns[i]
				if a.String() != e.String() {
					t.Fatalf("mismatch at column %d: actual %#v != %#v", i, a, e)
				}
			}

			// Verify grants.
			expGrants := []serverpb.TableDetailsResponse_Grant{
				{User: security.RootUser, Privileges: []string{"ALL"}},
				{User: "app", Privileges: []string{"DELETE"}},
				{User: "app", Privileges: []string{"SELECT"}},
				{User: "app", Privileges: []string{"UPDATE"}},
				{User: "readonly", Privileges: []string{"SELECT"}},
			}
			testutils.SortStructs(expGrants, "User")
			testutils.SortStructs(resp.Grants, "User")
			if a, e := len(resp.Grants), len(expGrants); a != e {
				t.Fatalf("# of grant columns %d != expected %d (got: %#v)", a, e, resp.Grants)
			}
			for i, a := range resp.Grants {
				e := expGrants[i]
				sort.Strings(a.Privileges)
				sort.Strings(e.Privileges)
				if a.String() != e.String() {
					t.Fatalf("mismatch at index %d: actual %#v != %#v", i, a, e)
				}
			}

			// Verify indexes.
			expIndexes := []serverpb.TableDetailsResponse_Index{
				{Name: "primary", Column: "rowid", Direction: "ASC", Unique: true, Seq: 1},
				{Name: "descidx", Column: "rowid", Direction: "ASC", Unique: false, Seq: 2, Implicit: true},
				{Name: "descidx", Column: "default2", Direction: "DESC", Unique: false, Seq: 1},
			}
			testutils.SortStructs(expIndexes, "Name", "Seq")
			testutils.SortStructs(resp.Indexes, "Name", "Seq")
			for i, a := range resp.Indexes {
				e := expIndexes[i]
				if a.String() != e.String() {
					t.Fatalf("mismatch at index %d: actual %#v != %#v", i, a, e)
				}
			}

			// Verify range count.
			if a, e := resp.RangeCount, int64(1); a != e {
				t.Fatalf("# of ranges %d != expected %d", a, e)
			}

			// Verify Create Table Statement.
			{

				const createTableCol = "CreateTable"
				showCreateTableQuery := fmt.Sprintf("SHOW CREATE TABLE %s.%s", escDBName, escTblName)

				resSet := ts.sqlExecutor.ExecuteStatements(session, showCreateTableQuery, nil)
				defer resSet.Close(ctx)
				res := resSet.ResultList[0]
				if res.Err != nil {
					t.Fatalf("error executing '%s': %s", showCreateTableQuery, res.Err)
				}

				scanner := makeResultScanner(res.Columns)
				var createStmt string
				if err := scanner.Scan(res.Rows.At(0), createTableCol, &createStmt); err != nil {
					t.Fatal(err)
				}

				if a, e := resp.CreateTableStatement, createStmt; a != e {
					t.Fatalf("mismatched create table statement; expected %s, got %s", e, a)
				}
			}

			// Verify Descriptor ID.
			path, err := ts.admin.queryDescriptorIDPath(ctx, session, []string{tc.dbName, tc.tblName})
			if err != nil {
				t.Fatal(err)
			}
			if a, e := resp.DescriptorID, int64(path[2]); a != e {
				t.Fatalf("table had descriptorID %d, expected %d", a, e)
			}
		})
	}
}

// TestAdminAPIZoneDetails verifies the zone configuration information returned
// for both DatabaseDetailsResponse AND TableDetailsResponse.
func TestAdminAPIZoneDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)

	// Create database and table.
	ac := log.AmbientContext{Tracer: tracing.NewTracer()}
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()
	session := sql.NewSession(
		ctx, sql.SessionArgs{User: security.RootUser}, ts.sqlExecutor, nil, &sql.MemoryMetrics{})
	session.StartUnlimitedMonitor()
	setupQueries := []string{
		"CREATE DATABASE test",
		"CREATE TABLE test.tbl (val STRING)",
	}
	for _, q := range setupQueries {
		res := ts.sqlExecutor.ExecuteStatements(session, q, nil)
		defer res.Close(ctx)
		if res.ResultList[0].Err != nil {
			t.Fatalf("error executing '%s': %s", q, res.ResultList[0].Err)
		}
	}

	// Function to verify the zone for table "test.tbl" as returned by the Admin
	// API.
	verifyTblZone := func(
		expectedZone config.ZoneConfig, expectedLevel serverpb.ZoneConfigurationLevel,
	) {
		var resp serverpb.TableDetailsResponse
		if err := getAdminJSONProto(s, "databases/test/tables/tbl", &resp); err != nil {
			t.Fatal(err)
		}
		if a, e := &resp.ZoneConfig, &expectedZone; !proto.Equal(a, e) {
			t.Errorf("actual table zone config %v did not match expected value %v", a, e)
		}
		if a, e := resp.ZoneConfigLevel, expectedLevel; a != e {
			t.Errorf("actual table ZoneConfigurationLevel %s did not match expected value %s", a, e)
		}
		if t.Failed() {
			t.FailNow()
		}
	}

	// Function to verify the zone for database "test" as returned by the Admin
	// API.
	verifyDbZone := func(
		expectedZone config.ZoneConfig, expectedLevel serverpb.ZoneConfigurationLevel,
	) {
		var resp serverpb.DatabaseDetailsResponse
		if err := getAdminJSONProto(s, "databases/test", &resp); err != nil {
			t.Fatal(err)
		}
		if a, e := &resp.ZoneConfig, &expectedZone; !proto.Equal(a, e) {
			t.Errorf("actual db zone config %v did not match expected value %v", a, e)
		}
		if a, e := resp.ZoneConfigLevel, expectedLevel; a != e {
			t.Errorf("actual db ZoneConfigurationLevel %s did not match expected value %s", a, e)
		}
		if t.Failed() {
			t.FailNow()
		}
	}

	// Function to store a zone config for a given object ID.
	setZone := func(zoneCfg config.ZoneConfig, id sqlbase.ID) {
		zoneBytes, err := zoneCfg.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		const query = `INSERT INTO system.zones VALUES($1, $2)`
		params := parser.MakePlaceholderInfo()
		params.SetValue(`1`, parser.NewDInt(parser.DInt(id)))
		params.SetValue(`2`, parser.NewDBytes(parser.DBytes(zoneBytes)))
		res := ts.sqlExecutor.ExecuteStatements(session, query, &params)
		defer res.Close(ctx)
		if res.ResultList[0].Err != nil {
			t.Fatalf("error executing '%s': %s", query, res.ResultList[0].Err)
		}
	}

	// Verify zone matches cluster default.
	verifyDbZone(config.DefaultZoneConfig(), serverpb.ZoneConfigurationLevel_CLUSTER)
	verifyTblZone(config.DefaultZoneConfig(), serverpb.ZoneConfigurationLevel_CLUSTER)

	// Get ID path for table. This will be an array of three IDs, containing the ID of the root namespace,
	// the database, and the table (in that order).
	idPath, err := ts.admin.queryDescriptorIDPath(ctx, session, []string{"test", "tbl"})
	if err != nil {
		t.Fatal(err)
	}

	// Apply zone configuration to database and check again.
	dbZone := config.ZoneConfig{
		RangeMinBytes: 456,
	}
	setZone(dbZone, idPath[1])
	verifyDbZone(dbZone, serverpb.ZoneConfigurationLevel_DATABASE)
	verifyTblZone(dbZone, serverpb.ZoneConfigurationLevel_DATABASE)

	// Apply zone configuration to table and check again.
	tblZone := config.ZoneConfig{
		RangeMinBytes: 789,
	}
	setZone(tblZone, idPath[2])
	verifyDbZone(dbZone, serverpb.ZoneConfigurationLevel_DATABASE)
	verifyTblZone(tblZone, serverpb.ZoneConfigurationLevel_TABLE)
}

func TestAdminAPIUsers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)

	// Create sample users.
	ac := log.AmbientContext{Tracer: tracing.NewTracer()}
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()
	session := sql.NewSession(
		ctx, sql.SessionArgs{User: security.RootUser}, ts.sqlExecutor, nil, &sql.MemoryMetrics{})
	session.StartUnlimitedMonitor()
	defer session.Finish(ts.sqlExecutor)
	query := `
INSERT INTO system.users (username, hashedPassword)
VALUES ('admin', 'abc'), ('bob', 'xyz')`
	res := ts.sqlExecutor.ExecuteStatements(session, query, nil)
	defer res.Close(ctx)
	if a, e := len(res.ResultList), 1; a != e {
		t.Fatalf("len(results) %d != %d", a, e)
	} else if res.ResultList[0].Err != nil {
		t.Fatal(res.ResultList[0].Err)
	}

	// Query the API for users.
	var resp serverpb.UsersResponse
	if err := getAdminJSONProto(s, "users", &resp); err != nil {
		t.Fatal(err)
	}
	expResult := serverpb.UsersResponse{
		Users: []serverpb.UsersResponse_User{
			{Username: "admin"},
			{Username: "bob"},
		},
	}

	// Verify results.
	const sortKey = "Username"
	testutils.SortStructs(resp.Users, sortKey)
	testutils.SortStructs(expResult.Users, sortKey)
	if !reflect.DeepEqual(resp, expResult) {
		t.Fatalf("result %v != expected %v", resp, expResult)
	}
}

func TestAdminAPIEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)

	ac := log.AmbientContext{Tracer: tracing.NewTracer()}
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()
	session := sql.NewSession(
		ctx, sql.SessionArgs{User: security.RootUser}, ts.sqlExecutor, nil, &sql.MemoryMetrics{})
	session.StartUnlimitedMonitor()
	defer session.Finish(ts.sqlExecutor)
	setupQueries := []string{
		"CREATE DATABASE api_test",
		"CREATE TABLE api_test.tbl1 (a INT)",
		"CREATE TABLE api_test.tbl2 (a INT)",
		"CREATE TABLE api_test.tbl3 (a INT)",
		"DROP TABLE api_test.tbl1",
		"DROP TABLE api_test.tbl2",
	}
	for _, q := range setupQueries {
		res := ts.sqlExecutor.ExecuteStatements(session, q, nil)
		defer res.Close(ctx)
		if res.ResultList[0].Err != nil {
			t.Fatalf("error executing '%s': %s", q, res.ResultList[0].Err)
		}
	}

	const allEvents = ""
	type testcase struct {
		eventType sql.EventLogType
		expCount  int
	}
	testcases := []testcase{
		{sql.EventLogNodeJoin, 1},
		{sql.EventLogNodeRestart, 0},
		{sql.EventLogDropDatabase, 0},
		{sql.EventLogCreateDatabase, 1},
		{sql.EventLogDropTable, 2},
		{sql.EventLogCreateTable, 3},
	}
	minTotalEvents := 0
	for _, tc := range testcases {
		minTotalEvents += tc.expCount
	}
	testcases = append(testcases, testcase{allEvents, minTotalEvents})

	for i, tc := range testcases {
		url := "events"
		if tc.eventType != allEvents {
			url += "?type=" + string(tc.eventType)
		}
		var resp serverpb.EventsResponse
		if err := getAdminJSONProto(s, url, &resp); err != nil {
			t.Fatal(err)
		}
		if tc.eventType == allEvents {
			// When retrieving all events, we expect that there will be some system
			// database migrations, unrelated to this test, that add to the log entry
			// count. So, we do a looser check here.
			if a, min := len(resp.Events), tc.expCount; a < tc.expCount {
				t.Fatalf("%d: total # of events %d < min %d", i, a, min)
			}
		} else {
			if a, e := len(resp.Events), tc.expCount; a != e {
				t.Fatalf("%d: # of %s events %d != expected %d", i, tc.eventType, a, e)
			}
		}

		// Ensure we don't have blank / nonsensical fields.
		for _, e := range resp.Events {
			if e.Timestamp == (time.Time{}) {
				t.Errorf("%d: missing/empty timestamp", i)
			}

			if len(tc.eventType) > 0 {
				if a, e := e.EventType, string(tc.eventType); a != e {
					t.Errorf("%d: event type %s != expected %s", i, a, e)
				}
			} else {
				if len(e.EventType) == 0 {
					t.Errorf("%d: missing event type in event", i)
				}
			}

			if e.TargetID == 0 {
				t.Errorf("%d: missing/empty TargetID", i)
			}
			if e.ReportingID == 0 {
				t.Errorf("%d: missing/empty ReportingID", i)
			}
			if len(e.Info) == 0 {
				t.Errorf("%d: missing/empty Info", i)
			}
			if len(e.UniqueID) == 0 {
				t.Errorf("%d: missing/empty UniqueID", i)
			}
		}
	}
}

const settingKey = "testing.b"

var _ = settings.RegisterBoolSetting(settingKey, "", true)

func TestAdminAPISettings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := log.Scope(t)
	defer sc.Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	allKeys := settings.Keys()

	checkSetting := func(t *testing.T, k string, v serverpb.SettingsResponse_Value) {
		ref, desc, ok := settings.Lookup(k)
		if !ok {
			t.Fatalf("%s: not found after initial lookup", k)
		}
		typ := ref.Typ()

		if ref.String() != v.Value {
			t.Errorf("%s: expected value %s, got %s", k, ref, v.Value)
		}

		if desc != v.Description {
			t.Errorf("%s: expected description %s, got %s", k, desc, v.Description)
		}
		if typ != v.Type {
			t.Errorf("%s: expected type %s, got %s", k, typ, v.Type)
		}
	}

	t.Run("all", func(t *testing.T) {
		var resp serverpb.SettingsResponse

		if err := getAdminJSONProto(s, "settings", &resp); err != nil {
			t.Fatal(err)
		}

		// Check that all expected keys were returned
		if len(allKeys) != len(resp.KeyValues) {
			t.Fatalf("expected %d keys, got %d", len(allKeys), len(resp.KeyValues))
		}
		for _, k := range allKeys {
			if _, ok := resp.KeyValues[k]; !ok {
				t.Fatalf("expected key %s not found in response", k)
			}
		}

		// Check that the test key is listed and the values come indeed
		// from the settings package unchanged.
		seenRef := false
		for k, v := range resp.KeyValues {
			if k == settingKey {
				seenRef = true
				if v.Value != "true" {
					t.Errorf("%s: expected true, got %s", k, v.Value)
				}
			}

			checkSetting(t, k, v)
		}

		if !seenRef {
			t.Fatalf("failed to observe test setting %s, got %q", settingKey, resp.KeyValues)
		}
	})

	t.Run("one-by-one", func(t *testing.T) {
		var resp serverpb.SettingsResponse

		// All the settings keys must be retrievable, and their
		// type and description must match.
		for _, k := range allKeys {
			q := make(url.Values)
			q.Add("keys", k)
			url := "settings?" + q.Encode()
			if err := getAdminJSONProto(s, url, &resp); err != nil {
				t.Fatalf("%s: %v", k, err)
			}
			if len(resp.KeyValues) != 1 {
				t.Fatalf("%s: expected 1 response, got %d", k, len(resp.KeyValues))
			}
			v, ok := resp.KeyValues[k]
			if !ok {
				t.Fatalf("%s: response does not contain key", k)
			}

			checkSetting(t, k, v)
		}
	})
}

func TestAdminAPIUIData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	start := timeutil.Now()

	mustSetUIData := func(keyValues map[string][]byte) {
		if err := postAdminJSONProto(s, "uidata", &serverpb.SetUIDataRequest{
			KeyValues: keyValues,
		}, &serverpb.SetUIDataResponse{}); err != nil {
			t.Fatal(err)
		}
	}

	expectKeyValues := func(expKeyValues map[string][]byte) {
		var resp serverpb.GetUIDataResponse
		queryValues := make(url.Values)
		for key := range expKeyValues {
			queryValues.Add("keys", key)
		}
		url := "uidata?" + queryValues.Encode()
		if err := getAdminJSONProto(s, url, &resp); err != nil {
			t.Fatal(err)
		}
		// Do a two-way comparison. We can't use reflect.DeepEqual(), because
		// resp.KeyValues has timestamps and expKeyValues doesn't.
		for key, actualVal := range resp.KeyValues {
			if a, e := actualVal.Value, expKeyValues[key]; !bytes.Equal(a, e) {
				t.Fatalf("key %s: value = %v, expected = %v", key, a, e)
			}
		}
		for key, expVal := range expKeyValues {
			if a, e := resp.KeyValues[key].Value, expVal; !bytes.Equal(a, e) {
				t.Fatalf("key %s: value = %v, expected = %v", key, a, e)
			}
		}

		// Sanity check LastUpdated.
		for _, val := range resp.KeyValues {
			now := timeutil.Now()
			if val.LastUpdated.Before(start) {
				t.Fatalf("val.LastUpdated %s < start %s", val.LastUpdated, start)
			}
			if val.LastUpdated.After(now) {
				t.Fatalf("val.LastUpdated %s > now %s", val.LastUpdated, now)
			}
		}
	}

	expectValueEquals := func(key string, expVal []byte) {
		expectKeyValues(map[string][]byte{key: expVal})
	}

	expectKeyNotFound := func(key string) {
		var resp serverpb.GetUIDataResponse
		url := "uidata?keys=" + key
		if err := getAdminJSONProto(s, url, &resp); err != nil {
			t.Fatal(err)
		}
		if len(resp.KeyValues) != 0 {
			t.Fatal("key unexpectedly found")
		}
	}

	// Basic tests.
	var badResp serverpb.GetUIDataResponse
	const errPattern = "400 Bad Request"
	if err := getAdminJSONProto(s, "uidata", &badResp); !testutils.IsError(err, errPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}

	mustSetUIData(map[string][]byte{"k1": []byte("v1")})
	expectValueEquals("k1", []byte("v1"))

	expectKeyNotFound("NON_EXISTENT_KEY")

	mustSetUIData(map[string][]byte{
		"k2": []byte("v2"),
		"k3": []byte("v3"),
	})
	expectValueEquals("k2", []byte("v2"))
	expectValueEquals("k3", []byte("v3"))
	expectKeyValues(map[string][]byte{
		"k2": []byte("v2"),
		"k3": []byte("v3"),
	})

	mustSetUIData(map[string][]byte{"k2": []byte("v2-updated")})
	expectKeyValues(map[string][]byte{
		"k2": []byte("v2-updated"),
		"k3": []byte("v3"),
	})

	// Write a binary blob with all possible byte values, then verify it.
	var buf bytes.Buffer
	for i := 0; i < 997; i++ {
		buf.WriteByte(byte(i % 256))
	}
	mustSetUIData(map[string][]byte{"bin": buf.Bytes()})
	expectValueEquals("bin", buf.Bytes())
}

func TestClusterAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// We need to retry, because the cluster ID isn't set until after
	// bootstrapping.
	testutils.SucceedsSoon(t, func() error {
		var resp serverpb.ClusterResponse
		if err := getAdminJSONProto(s, "cluster", &resp); err != nil {
			return err
		}
		if a, e := resp.ClusterID, s.(*TestServer).node.ClusterID.String(); a != e {
			return errors.Errorf("cluster ID %s != expected %s", a, e)
		}
		return nil
	})
}

func TestHealthAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// We need to retry because the node ID isn't set until after
	// bootstrapping.
	testutils.SucceedsSoon(t, func() error {
		var resp serverpb.HealthResponse
		return getAdminJSONProto(s, "health", &resp)
	})

	// Expire this node's liveness record by pausing heartbeats and advancing the
	// server's clock.
	ts := s.(*TestServer)
	ts.nodeLiveness.PauseHeartbeat(true)
	self, err := ts.nodeLiveness.Self()
	if err != nil {
		t.Fatal(err)
	}
	s.Clock().Update(self.Expiration.Add(1, 0))

	expected := "node is not live"
	var resp serverpb.HealthResponse
	if err := getAdminJSONProto(s, "health", &resp); !testutils.IsError(err, expected) {
		t.Errorf("expected %q error, got %v", expected, err)
	}
}
