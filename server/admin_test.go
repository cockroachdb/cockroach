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
	"io/ioutil"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
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

// TestAdminDebugExpVar verifies that cmdline and memstats variables are
// available via the /debug/vars link.
func TestAdminDebugExpVar(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := StartTestServer(t)
	defer s.Stop()

	jI, err := getJSON(s.Ctx.HTTPRequestScheme() + "://" + s.ServingAddr() + debugEndpoint + "vars")
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
	s := StartTestServer(t)
	defer s.Stop()

	body, err := getText(s.Ctx.HTTPRequestScheme() + "://" + s.ServingAddr() + debugEndpoint + "pprof/block")
	if err != nil {
		t.Fatal(err)
	}
	if exp := "contention:\ncycles/second="; !bytes.Contains(body, []byte(exp)) {
		t.Errorf("expected %s to contain %s", body, exp)
	}
}

// TestAdminDebugTrace verifies that the net/trace endpoints are available
// via /debug/{requests,events}.
func TestAdminNetTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := StartTestServer(t)
	defer s.Stop()

	tc := []struct {
		segment, search string
	}{
		{"requests", "<title>/debug/requests</title>"},
		{"events", "<title>events</title>"},
	}

	for _, c := range tc {
		body, err := getText(s.Ctx.HTTPRequestScheme() + "://" + s.ServingAddr() + debugEndpoint + c.segment)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(body, []byte(c.search)) {
			t.Errorf("expected %s to be contained in %s", c.search, body)
		}
	}
}

// apiGet issues a GET to the provided server using the given API path and marshals the result
// into the v parameter.
func apiGet(s *TestServer, path string, v interface{}) error {
	apiPath := apiEndpoint + path
	client, err := s.Ctx.GetHTTPClient()
	if err != nil {
		return err
	}
	return util.GetJSON(client, s.Ctx.HTTPRequestScheme(), s.ServingAddr(), apiPath, v)
}

func TestAdminAPIDatabases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := StartTestServer(t)
	defer s.Stop()

	// Test databases endpoint.
	const testdb = "test"
	var session sql.Session
	query := "CREATE DATABASE " + testdb
	createRes := s.sqlExecutor.ExecuteStatements(security.RootUser, &session, query, nil)
	if createRes.ResultList[0].PErr != nil {
		t.Fatal(createRes.ResultList[0].PErr)
	}

	type databasesResult struct {
		Databases []string
	}
	var res databasesResult
	if err := apiGet(s, "databases", &res); err != nil {
		t.Fatal(err)
	}

	// We should have the system database and the newly created test database.
	if a, e := len(res.Databases), 2; a != e {
		t.Fatalf("length of result %d != expected %d", a, e)
	}

	sort.Strings(res.Databases)
	if a, e := res.Databases[0], "system"; a != e {
		t.Fatalf("database name %s != expected %s", a, e)
	}
	if a, e := res.Databases[1], testdb; a != e {
		t.Fatalf("database name %s != expected %s", a, e)
	}

	// Test database details endpoint.
	privileges := []string{"SELECT", "UPDATE"}
	testuser := "testuser"
	grantQuery := "GRANT " + strings.Join(privileges, ", ") + " ON DATABASE " + testdb + " TO " + testuser
	grantRes := s.sqlExecutor.ExecuteStatements(security.RootUser, &session, grantQuery, nil)
	if grantRes.ResultList[0].PErr != nil {
		t.Fatal(grantRes.ResultList[0].PErr)
	}

	type databaseDetailsResult struct {
		Grants []struct {
			Database   string
			Privileges []string
			User       string
		}
		Tables []string
	}
	var details databaseDetailsResult
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

		if a, e := grant.Database, testdb; a != e {
			t.Fatalf("database %s != expected %s", grant.Database, testdb)
		}
	}
}

func TestAdminAPIDatabaseDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := StartTestServer(t)
	defer s.Stop()

	if err := apiGet(s, "databases/I_DO_NOT_EXIST", nil); !testutils.IsError(err, "Not Found") {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestAdminAPIDatabaseSQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := StartTestServer(t)
	defer s.Stop()

	fakedb := "system;DROP DATABASE system;"
	path := "databases/" + fakedb
	if err := apiGet(s, path, nil); !testutils.IsError(err, fakedb+".*does not exist") {
		t.Fatalf("unexpected error: %s", err)
	}
}
