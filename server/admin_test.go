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

	"golang.org/x/net/context"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server/serverpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

// getText fetches the HTTP response body as text in the form of a
// byte slice from the specified URL.
func getText(url string) ([]byte, error) {
	// There are no particular permissions on admin endpoints, TestUser is fine.
	client, err := testutils.NewTestBaseContext(TestUser).GetHTTPClient()
	if err != nil {
		return nil, err
	}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

// getJSON fetches the JSON from the specified URL and returns
// it as unmarshaled JSON. Returns an error on any failure to fetch
// or unmarshal response body.
func getJSON(url string) (interface{}, error) {
	body, err := getText(url)
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
	defer s.Stopper().Stop()

	jI, err := getJSON(debugURL(s) + "vars")
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
	defer s.Stopper().Stop()

	jI, err := getJSON(debugURL(s) + "metrics")
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
	defer s.Stopper().Stop()

	body, err := getText(debugURL(s) + "pprof/block")
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
	defer s.Stopper().Stop()

	tc := []struct {
		segment, search string
	}{
		{"requests", "<title>/debug/requests</title>"},
		{"events", "<title>events</title>"},
	}

	for _, c := range tc {
		body, err := getText(debugURL(s) + c.segment)
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
	defer s.Stopper().Stop()

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

// apiGet issues a GET to the provided server using the given API path and
// marshals the result into response.
func apiGet(s serverutils.TestServerInterface, path string, response proto.Message) error {
	apiPath := apiEndpoint + path
	client, err := s.GetHTTPClient()
	if err != nil {
		return err
	}
	return util.GetJSON(client, s.AdminURL()+apiPath, response)
}

// apiPost issues a POST to the provided server using the given API path and
// request, marshalling the result into response.
func apiPost(s serverutils.TestServerInterface, path string, request, response proto.Message) error {
	apiPath := apiEndpoint + path
	client, err := s.GetHTTPClient()
	if err != nil {
		return err
	}
	return util.PostJSON(client, s.AdminURL()+apiPath, request, response)
}

func TestAdminAPIDatabases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	ts := s.(*TestServer)

	// Test databases endpoint.
	const testdb = "test"
	session := sql.NewSession(
		context.Background(), sql.SessionArgs{User: security.RootUser}, ts.sqlExecutor, nil)
	query := "CREATE DATABASE " + testdb
	createRes := ts.sqlExecutor.ExecuteStatements(session, query, nil)
	if createRes.ResultList[0].Err != nil {
		t.Fatal(createRes.ResultList[0].Err)
	}

	var resp serverpb.DatabasesResponse
	if err := apiGet(s, "databases", &resp); err != nil {
		t.Fatal(err)
	}

	// We should have three databases:
	// - system database
	// - information_schema
	// - newly created test database
	if a, e := len(resp.Databases), 3; a != e {
		t.Fatalf("length of result %d != expected %d", a, e)
	}

	sort.Strings(resp.Databases)
	for i, e := range []string{"information_schema", "system", testdb} {
		if a := resp.Databases[i]; a != e {
			t.Fatalf("database name %s != expected %s", a, e)
		}
	}

	// Test database details endpoint.
	privileges := []string{"SELECT", "UPDATE"}
	testuser := "testuser"
	grantQuery := "GRANT " + strings.Join(privileges, ", ") + " ON DATABASE " + testdb + " TO " + testuser
	grantRes := s.(*TestServer).sqlExecutor.ExecuteStatements(session, grantQuery, nil)
	if grantRes.ResultList[0].Err != nil {
		t.Fatal(grantRes.ResultList[0].Err)
	}

	var details serverpb.DatabaseDetailsResponse
	if err := apiGet(s, "databases/"+testdb, &details); err != nil {
		t.Fatal(err)
	}

	if a, e := len(details.Grants), 2; a != e {
		t.Fatalf("# of grants %d != expected %d", a, e)
	}

	for _, grant := range details.Grants {
		switch grant.User {
		case security.RootUser:
			if !reflect.DeepEqual(grant.Privileges, []string{"ALL"}) {
				t.Fatalf("privileges %v != expected %v", details.Grants[0].Privileges, privileges)
			}
		case testuser:
			sort.Strings(grant.Privileges)
			if !reflect.DeepEqual(grant.Privileges, privileges) {
				t.Fatalf("privileges %v != expected %v", grant.Privileges, privileges)
			}
		default:
			t.Fatalf("unknown grant to user %s", grant.User)
		}
	}
}

func TestAdminAPIDatabaseDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	if err := apiGet(s, "databases/I_DO_NOT_EXIST", nil); !testutils.IsError(err, "database.+does not exist") {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestAdminAPIDatabaseSQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	const fakedb = "system;DROP DATABASE system;"
	const path = "databases/" + fakedb
	const errPattern = `database \\"` + fakedb + `\\" does not exist`
	if err := apiGet(s, path, nil); !testutils.IsError(err, errPattern) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestAdminAPITableDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	const fakename = "I_DO_NOT_EXIST"
	const badDBPath = "databases/" + fakename + "/tables/foo"
	const dbErrPattern = `database \\"` + fakename + `\\" does not exist`
	if err := apiGet(s, badDBPath, nil); !testutils.IsError(err, dbErrPattern) {
		t.Fatalf("unexpected error: %s", err)
	}

	const badTablePath = "databases/system/tables/" + fakename
	const tableErrPattern = `table \\"system.` + fakename + `\\" does not exist`
	if err := apiGet(s, badTablePath, nil); !testutils.IsError(err, tableErrPattern) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestAdminAPITableSQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	const fakeTable = "users;DROP DATABASE system;"
	const path = "databases/system/tables/" + fakeTable
	const errPattern = `table \"system.\\\"` + fakeTable + `\\\"\" does not exist`
	if err := apiGet(s, path, nil); !testutils.IsError(err, regexp.QuoteMeta(errPattern)) {
		t.Fatalf("unexpected error: %s\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPITableDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	ts := s.(*TestServer)

	session := sql.NewSession(
		context.Background(), sql.SessionArgs{User: security.RootUser}, ts.sqlExecutor, nil)
	setupQueries := []string{
		"CREATE DATABASE test",
		`
CREATE TABLE test.tbl (
	nulls_allowed INT,
	nulls_not_allowed INT NOT NULL DEFAULT 1000,
	default2 INT DEFAULT 2,
	string_default STRING DEFAULT 'default_string'
)`,
		"GRANT SELECT ON test.tbl TO readonly",
		"GRANT SELECT,UPDATE,DELETE ON test.tbl TO app",
		"CREATE INDEX descIdx ON test.tbl (default2 DESC)",
	}

	for _, q := range setupQueries {
		res := ts.sqlExecutor.ExecuteStatements(session, q, nil)
		if res.ResultList[0].Err != nil {
			t.Fatalf("error executing '%s': %s", q, res.ResultList[0].Err)
		}
	}

	// Perform API call.
	var resp serverpb.TableDetailsResponse
	if err := apiGet(s, "databases/test/tables/tbl", &resp); err != nil {
		t.Fatal(err)
	}

	// Verify columns.
	expColumns := []serverpb.TableDetailsResponse_Column{
		{Name: "nulls_allowed", Type: "INT", Nullable: true, DefaultValue: ""},
		{Name: "nulls_not_allowed", Type: "INT", Nullable: false, DefaultValue: "1000"},
		{Name: "default2", Type: "INT", Nullable: true, DefaultValue: "2"},
		{Name: "string_default", Type: "STRING", Nullable: true, DefaultValue: "'default_string'"},
		{Name: "rowid", Type: "INT", Nullable: false, DefaultValue: "unique_rowid()"},
	}
	testutils.SortStructs(expColumns, "Name")
	testutils.SortStructs(resp.Columns, "Name")
	if a, e := len(resp.Columns), len(expColumns); a != e {
		t.Fatalf("# of result columns %d != expected %d (got: %#v)", a, e, resp.Columns)
	}
	for i, a := range resp.Columns {
		e := expColumns[i]
		if a.String() != e.String() {
			t.Fatalf("mismatch at index %d: actual %#v != %#v", i, a, e)
		}
	}

	// Verify grants.
	expGrants := []serverpb.TableDetailsResponse_Grant{
		{User: security.RootUser, Privileges: []string{"ALL"}},
		{User: "app", Privileges: []string{"DELETE", "SELECT", "UPDATE"}},
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
		{Name: "descIdx", Column: "default2", Direction: "DESC", Unique: false, Seq: 1},
	}
	testutils.SortStructs(expIndexes, "Column")
	testutils.SortStructs(resp.Indexes, "Column")
	for i, a := range resp.Indexes {
		e := expIndexes[i]
		if a.String() != e.String() {
			t.Fatalf("mismatch at index %d: actual %#v != %#v", i, a, e)
		}
	}

	if a, e := resp.RangeCount, int64(1); a != e {
		t.Fatalf("# of ranges %d != expected %d", a, e)
	}

	// Verify Create Table Statement.
	{
		const (
			showCreateTableQuery = "SHOW CREATE TABLE test.tbl"
			createTableCol       = "CreateTable"
		)

		resSet := ts.sqlExecutor.ExecuteStatements(session, showCreateTableQuery, nil)
		res := resSet.ResultList[0]
		if res.Err != nil {
			t.Fatalf("error executing '%s': %s", showCreateTableQuery, res.Err)
		}

		scanner := makeResultScanner(res.Columns)
		var createStmt string
		if err := scanner.Scan(res.Rows[0], createTableCol, &createStmt); err != nil {
			t.Fatal(err)
		}

		if a, e := resp.CreateTableStatement, createStmt; a != e {
			t.Fatalf("mismatched create table statement; expected %s, got %s", e, a)
		}
	}
}

func TestAdminAPITableDetailsZone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	ts := s.(*TestServer)

	// Create database and table.
	session := sql.NewSession(
		context.Background(), sql.SessionArgs{User: security.RootUser}, ts.sqlExecutor, nil)
	setupQueries := []string{
		"CREATE DATABASE test",
		"CREATE TABLE test.tbl (val STRING)",
	}
	for _, q := range setupQueries {
		res := ts.sqlExecutor.ExecuteStatements(session, q, nil)
		if res.ResultList[0].Err != nil {
			t.Fatalf("error executing '%s': %s", q, res.ResultList[0].Err)
		}
	}

	// Function to verify the zone for test.tbl as returned by the Admin API.
	verifyZone := func(expectedZone config.ZoneConfig, expectedLevel serverpb.ZoneConfigurationLevel) {
		var resp serverpb.TableDetailsResponse
		if err := apiGet(s, "databases/test/tables/tbl", &resp); err != nil {
			t.Fatal(err)
		}
		if a, e := &resp.ZoneConfig, &expectedZone; !proto.Equal(a, e) {
			t.Errorf("actual zone config %v did not match expected value %v", a, e)
		}
		if a, e := resp.ZoneConfigLevel, expectedLevel; a != e {
			t.Errorf("actual ZoneConfigurationLevel %s did not match expected value %s", a, e)
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
		params := parser.NewPlaceholderInfo()
		params.SetValue(`1`, parser.NewDInt(parser.DInt(id)))
		params.SetValue(`2`, parser.NewDBytes(parser.DBytes(zoneBytes)))
		res := ts.sqlExecutor.ExecuteStatements(session, query, params)
		if res.ResultList[0].Err != nil {
			t.Fatalf("error executing '%s': %s", query, res.ResultList[0].Err)
		}
	}

	// Verify zone matches cluster default.
	verifyZone(config.DefaultZoneConfig(), serverpb.ZoneConfigurationLevel_CLUSTER)

	// Get ID path for table. This will be an array of three IDs, containing the ID of the root namespace,
	// the database, and the table (in that order).
	idPath, err := ts.admin.queryDescriptorIDPath(session, []string{"test", "tbl"})
	if err != nil {
		t.Fatal(err)
	}

	// Apply zone configuration to database and check again.
	dbZone := config.ZoneConfig{
		RangeMinBytes: 456,
	}
	setZone(dbZone, idPath[1])
	verifyZone(dbZone, serverpb.ZoneConfigurationLevel_DATABASE)

	// Apply zone configuration to table and check again.
	tblZone := config.ZoneConfig{
		RangeMinBytes: 789,
	}
	setZone(tblZone, idPath[2])
	verifyZone(tblZone, serverpb.ZoneConfigurationLevel_TABLE)
}

func TestAdminAPIUsers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	ts := s.(*TestServer)

	// Create sample users.
	session := sql.NewSession(
		context.Background(), sql.SessionArgs{User: security.RootUser}, ts.sqlExecutor, nil)
	query := `
INSERT INTO system.users (username, hashedPassword)
VALUES ('admin', 'abc'), ('bob', 'xyz')`
	res := ts.sqlExecutor.ExecuteStatements(session, query, nil)
	if a, e := len(res.ResultList), 1; a != e {
		t.Fatalf("len(results) %d != %d", a, e)
	} else if res.ResultList[0].Err != nil {
		t.Fatal(res.ResultList[0].Err)
	}

	// Query the API for users.
	var resp serverpb.UsersResponse
	if err := apiGet(s, "users", &resp); err != nil {
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
	defer s.Stopper().Stop()
	ts := s.(*TestServer)

	session := sql.NewSession(
		context.Background(), sql.SessionArgs{User: security.RootUser}, ts.sqlExecutor, nil)
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
		if res.ResultList[0].Err != nil {
			t.Fatalf("error executing '%s': %s", q, res.ResultList[0].Err)
		}
	}

	var zeroTimestamp serverpb.EventsResponse_Event_Timestamp

	testcases := []struct {
		eventType sql.EventLogType
		expCount  int
	}{
		{"", 7},
		{sql.EventLogNodeJoin, 1},
		{sql.EventLogNodeRestart, 0},
		{sql.EventLogDropDatabase, 0},
		{sql.EventLogCreateDatabase, 1},
		{sql.EventLogDropTable, 2},
		{sql.EventLogCreateTable, 3},
	}
	for i, tc := range testcases {
		var url string
		if len(tc.eventType) > 0 {
			url = fmt.Sprintf("events?type=%s", tc.eventType)
		} else {
			url = "events"
		}
		var resp serverpb.EventsResponse
		if err := apiGet(s, url, &resp); err != nil {
			t.Fatal(err)
		}

		if a, e := len(resp.Events), tc.expCount; a != e {
			t.Errorf("%d: # of events %d != expected %d", i, a, e)
		}

		// Ensure we don't have blank / nonsensical fields.
		for _, e := range resp.Events {
			if e.Timestamp == zeroTimestamp {
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

func TestAdminAPIUIData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	start := timeutil.Now()

	mustSetUIData := func(keyValues map[string][]byte) {
		if err := apiPost(s, "uidata", &serverpb.SetUIDataRequest{
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
		url := fmt.Sprintf("uidata?%s", queryValues.Encode())
		if err := apiGet(s, url, &resp); err != nil {
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
			lastUpdated := time.Unix(val.LastUpdated.Sec, int64(val.LastUpdated.Nsec))
			if lastUpdated.Before(start) {
				t.Fatalf("lastUpdated %s < start %s", lastUpdated, start)
			}
			if lastUpdated.After(now) {
				t.Fatalf("lastUpdated %s > now %s", lastUpdated, now)
			}
		}
	}

	expectValueEquals := func(key string, expVal []byte) {
		expectKeyValues(map[string][]byte{key: expVal})
	}

	expectKeyNotFound := func(key string) {
		var resp serverpb.GetUIDataResponse
		url := fmt.Sprintf("uidata?keys=%s", key)
		if err := apiGet(s, url, &resp); err != nil {
			t.Fatal(err)
		}
		if len(resp.KeyValues) != 0 {
			t.Fatal("key unexpectedly found")
		}
	}

	// Basic tests.
	var badResp serverpb.GetUIDataResponse
	if err := apiGet(s, "uidata", &badResp); !testutils.IsError(err, "400 Bad Request") {
		t.Fatalf("unexpected error: %v", err)
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
	defer s.Stopper().Stop()

	// We need to retry, because the cluster ID isn't set until after
	// bootstrapping.
	util.SucceedsSoon(t, func() error {
		var resp serverpb.ClusterResponse
		if err := apiGet(s, "cluster", &resp); err != nil {
			return err
		}
		if a, e := resp.ClusterID, s.(*TestServer).node.ClusterID.String(); a != e {
			return errors.Errorf("cluster ID %s != expected %s", a, e)
		}
		return nil
	})
}

func TestClusterFreeze(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	for _, freeze := range []bool{true, false} {
		req := serverpb.ClusterFreezeRequest{
			Freeze: freeze,
		}

		var resp serverpb.ClusterFreezeResponse
		cb := func(v proto.Message) {
			oldNum := resp.RangesAffected
			resp = *v.(*serverpb.ClusterFreezeResponse)
			if oldNum > resp.RangesAffected {
				resp.RangesAffected = oldNum
			}
		}

		cli, err := s.GetHTTPClient()
		if err != nil {
			t.Fatal(err)
		}
		path := s.AdminURL() + apiEndpoint + "cluster/freeze"

		if err := util.StreamJSON(cli, path, &req, &serverpb.ClusterFreezeResponse{}, cb); err != nil {
			t.Fatal(err)
		}
		if aff := resp.RangesAffected; aff == 0 {
			t.Fatalf("expected affected ranges: %+v", resp)
		}

		if err := util.StreamJSON(cli, path, &req, &serverpb.ClusterFreezeResponse{}, cb); err != nil {
			t.Fatal(err)
		}
	}
}
