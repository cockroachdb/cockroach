// Copyright 2014 The Cockroach Authors.
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
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func getAdminJSONProto(
	ts serverutils.TestServerInterface, path string, response protoutil.Message,
) error {
	return getAdminJSONProtoWithAdminOption(ts, path, response, true)
}

func getAdminJSONProtoWithAdminOption(
	ts serverutils.TestServerInterface, path string, response protoutil.Message, isAdmin bool,
) error {
	return serverutils.GetJSONProtoWithAdminOption(ts, adminPrefix+path, response, isAdmin)
}

func postAdminJSONProto(
	ts serverutils.TestServerInterface, path string, request, response protoutil.Message,
) error {
	return postAdminJSONProtoWithAdminOption(ts, path, request, response, true)
}

func postAdminJSONProtoWithAdminOption(
	ts serverutils.TestServerInterface,
	path string,
	request, response protoutil.Message,
	isAdmin bool,
) error {
	return serverutils.PostJSONProtoWithAdminOption(ts, adminPrefix+path, request, response, isAdmin)
}

// getText fetches the HTTP response body as text in the form of a
// byte slice from the specified URL.
func getText(ts serverutils.TestServerInterface, url string) ([]byte, error) {
	httpClient, err := ts.GetAdminAuthenticatedHTTPClient()
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
		return nil, errors.Wrapf(err, "body is:\n%s", body)
	}
	return jI, nil
}

// debugURL returns the root debug URL.
func debugURL(s serverutils.TestServerInterface) string {
	return s.AdminURL() + debug.Endpoint
}

// TestAdminDebugExpVar verifies that cmdline and memstats variables are
// available via the /debug/vars link.
func TestAdminDebugExpVar(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

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
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

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
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	body, err := getText(s, debugURL(s)+"pprof/block?debug=1")
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
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

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

func TestAdminDebugAuth(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

	url := debugURL(s)

	// Unauthenticated.
	client, err := ts.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status code %d; got %d", http.StatusUnauthorized, resp.StatusCode)
	}

	// Authenticated as non-admin.
	client, err = ts.GetAuthenticatedHTTPClient(false)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status code %d; got %d", http.StatusUnauthorized, resp.StatusCode)
	}

	// Authenticated as admin.
	client, err = ts.GetAuthenticatedHTTPClient(true)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status code %d; got %d", http.StatusOK, resp.StatusCode)
	}
}

// TestAdminDebugRedirect verifies that the /debug/ endpoint is redirected to on
// incorrect /debug/ paths.
func TestAdminDebugRedirect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

	expURL := debugURL(s)
	origURL := expURL + "incorrect"

	// Must be admin to access debug endpoints
	client, err := ts.GetAdminAuthenticatedHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	// Don't follow redirects automatically.
	redirectAttemptedError := errors.New("redirect")
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return redirectAttemptedError
	}

	resp, err := client.Get(origURL)
	if urlError := (*url.Error)(nil); errors.As(err, &urlError) &&
		errors.Is(urlError.Err, redirectAttemptedError) {
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
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

	ac := log.AmbientContext{Tracer: s.ClusterSettings().Tracer}
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()

	const testdb = "test"
	query := "CREATE DATABASE " + testdb
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}

	// We have to create the non-admin user before calling
	// "GRANT ... TO authenticatedUserNameNoAdmin".
	// This is done in "GetAuthenticatedHTTPClient".
	if _, err := ts.GetAuthenticatedHTTPClient(false); err != nil {
		t.Fatal(err)
	}

	// Grant permissions to view the tables for the given viewing user.
	privileges := []string{"SELECT", "UPDATE"}
	query = fmt.Sprintf(
		"GRANT %s ON DATABASE %s TO %s",
		strings.Join(privileges, ", "),
		testdb,
		authenticatedUserNameNoAdmin().SQLIdentifier(),
	)
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		expectedDBs []string
		isAdmin     bool
	}{
		{[]string{"defaultdb", "postgres", "system", testdb}, true},
		{[]string{testdb}, false},
	} {
		t.Run(fmt.Sprintf("isAdmin:%t", tc.isAdmin), func(t *testing.T) {
			// Test databases endpoint.
			var resp serverpb.DatabasesResponse
			if err := getAdminJSONProtoWithAdminOption(
				s,
				"databases",
				&resp,
				tc.isAdmin,
			); err != nil {
				t.Fatal(err)
			}

			if a, e := len(resp.Databases), len(tc.expectedDBs); a != e {
				t.Fatalf("length of result %d != expected %d", a, e)
			}

			sort.Strings(resp.Databases)
			for i, e := range tc.expectedDBs {
				if a := resp.Databases[i]; a != e {
					t.Fatalf("database name %s != expected %s", a, e)
				}
			}

			// Test database details endpoint.
			var details serverpb.DatabaseDetailsResponse
			if err := getAdminJSONProtoWithAdminOption(
				s,
				"databases/"+testdb,
				&details,
				tc.isAdmin,
			); err != nil {
				t.Fatal(err)
			}

			if a, e := len(details.Grants), 4; a != e {
				t.Fatalf("# of grants %d != expected %d", a, e)
			}

			userGrants := make(map[string][]string)
			for _, grant := range details.Grants {
				switch grant.User {
				case security.AdminRole, security.RootUser, authenticatedUserNoAdmin:
					userGrants[grant.User] = append(userGrants[grant.User], grant.Privileges...)
				default:
					t.Fatalf("unknown grant to user %s", grant.User)
				}
			}
			for u, p := range userGrants {
				switch u {
				case security.AdminRole:
					if !reflect.DeepEqual(p, []string{"ALL"}) {
						t.Fatalf("privileges %v != expected %v", p, privileges)
					}
				case security.RootUser:
					if !reflect.DeepEqual(p, []string{"ALL"}) {
						t.Fatalf("privileges %v != expected %v", p, privileges)
					}
				case authenticatedUserNoAdmin:
					sort.Strings(p)
					if !reflect.DeepEqual(p, privileges) {
						t.Fatalf("privileges %v != expected %v", p, privileges)
					}
				default:
					t.Fatalf("unknown grant to user %s", u)
				}
			}

			// Verify Descriptor ID.
			databaseID, err := ts.admin.queryDatabaseID(ctx, security.RootUserName(), testdb)
			if err != nil {
				t.Fatal(err)
			}
			if a, e := details.DescriptorID, int64(databaseID); a != e {
				t.Fatalf("db had descriptorID %d, expected %d", a, e)
			}
		})
	}
}

func TestAdminAPIDatabaseDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	const errPattern = "database.+does not exist"
	if err := getAdminJSONProto(s, "databases/i_do_not_exist", nil); !testutils.IsError(err, errPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPIDatabaseSQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	const fakedb = "system;DROP DATABASE system;"
	const path = "databases/" + fakedb
	const errPattern = `target database or schema does not exist`
	if err := getAdminJSONProto(s, path, nil); !testutils.IsError(err, errPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPINonTableStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())
	s := testCluster.Server(0)

	// Skip TableStatsResponse.Stats comparison, since it includes data which
	// aren't consistent (time, bytes).
	expectedResponse := serverpb.NonTableStatsResponse{
		TimeSeriesStats: &serverpb.TableStatsResponse{
			RangeCount:   1,
			ReplicaCount: 3,
			NodeCount:    3,
		},
		InternalUseStats: &serverpb.TableStatsResponse{
			RangeCount:   10,
			ReplicaCount: 12,
			NodeCount:    3,
		},
	}

	var resp serverpb.NonTableStatsResponse
	if err := getAdminJSONProto(s, "nontablestats", &resp); err != nil {
		t.Fatal(err)
	}

	assertExpectedStatsResponse := func(expected, actual *serverpb.TableStatsResponse) {
		assert.Equal(t, expected.RangeCount, actual.RangeCount)
		assert.Equal(t, expected.ReplicaCount, actual.ReplicaCount)
		assert.Equal(t, expected.NodeCount, actual.NodeCount)
	}

	assertExpectedStatsResponse(expectedResponse.TimeSeriesStats, resp.TimeSeriesStats)
	assertExpectedStatsResponse(expectedResponse.InternalUseStats, resp.InternalUseStats)
}

// Verify that for a cluster with no user data, all the ranges on the Databases
// page consist of:
// 1) the total ranges listed for the system database
// 2) the total ranges listed for the Non-Table data
func TestRangeCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())
	s := testCluster.Server(0)

	// Sum up ranges for non-table parts of the system returned
	// from the "nontablestats" enpoint.
	getNonTableRangeCount := func() (ts, internal int64) {
		var resp serverpb.NonTableStatsResponse
		if err := getAdminJSONProto(s, "nontablestats", &resp); err != nil {
			t.Fatal(err)
		}
		return resp.TimeSeriesStats.RangeCount, resp.InternalUseStats.RangeCount
	}

	// Return map tablename=>count obtained from the
	// "databases/system/tables/{table}" endpoints.
	getSystemTableRangeCount := func() map[string]int64 {
		m := map[string]int64{}
		var dbResp serverpb.DatabaseDetailsResponse
		if err := getAdminJSONProto(s, "databases/system", &dbResp); err != nil {
			t.Fatal(err)
		}
		for _, tableName := range dbResp.TableNames {
			var tblResp serverpb.TableStatsResponse
			path := "databases/system/tables/" + tableName + "/stats"
			if err := getAdminJSONProto(s, path, &tblResp); err != nil {
				t.Fatal(err)
			}
			m[tableName] = tblResp.RangeCount
		}
		return m
	}

	getRangeCountFromFullSpan := func() int64 {
		adminServer := s.(*TestServer).Server.admin
		stats, err := adminServer.statsForSpan(context.Background(), roachpb.Span{
			Key:    keys.LocalMax,
			EndKey: keys.MaxKey,
		})
		if err != nil {
			t.Fatal(err)
		}
		return stats.RangeCount
	}

	exp := getRangeCountFromFullSpan()

	sysDBMap := getSystemTableRangeCount()
	{
		// The tables below sit on the SystemConfigRange. For technical reason,
		// their range count comes back as zero. Let's just use the descriptor
		// table to count this range as they're not picked up by the "non-table
		// data" neither.
		for _, table := range []string{"public.descriptor", "public.settings", "public.zones"} {
			n, ok := sysDBMap[table]
			require.True(t, ok, table)
			require.Zero(t, n, table)
		}

		sysDBMap["public.descriptor"] = 1
	}
	var systemTableRangeCount int64
	for _, n := range sysDBMap {
		systemTableRangeCount += n
	}

	tsCount, internalCount := getNonTableRangeCount()

	act := tsCount + internalCount + systemTableRangeCount

	if !assert.Equal(t,
		exp,
		act,
	) {
		t.Log("did nonTableDescriptorRangeCount() change?")
		t.Logf(
			"claimed numbers:\ntime series = %d\ninternal = %d\nsystemdb = %d (%v)",
			tsCount, internalCount, systemTableRangeCount, sysDBMap,
		)
		db := testCluster.ServerConn(0)
		defer db.Close()

		runner := sqlutils.MakeSQLRunner(db)
		s := sqlutils.MatrixToStr(runner.QueryStr(t, `
select range_id, database_name, table_name, start_pretty, end_pretty from crdb_internal.ranges order by range_id asc`,
		))
		t.Logf("actual ranges:\n%s", s)
	}
}

func TestAdminAPITableDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	const fakename = "i_do_not_exist"
	const badDBPath = "databases/" + fakename + "/tables/foo"
	const dbErrPattern = `relation \\"` + fakename + `.foo\\" does not exist`
	if err := getAdminJSONProto(s, badDBPath, nil); !testutils.IsError(err, dbErrPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, dbErrPattern)
	}

	const badTablePath = "databases/system/tables/" + fakename
	const tableErrPattern = `relation \\"system.` + fakename + `\\" does not exist`
	if err := getAdminJSONProto(s, badTablePath, nil); !testutils.IsError(err, tableErrPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, tableErrPattern)
	}
}

func TestAdminAPITableSQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	const fakeTable = "users;DROP DATABASE system;"
	const path = "databases/system/tables/" + fakeTable
	const errPattern = `relation \"system.` + fakeTable + `\" does not exist`
	if err := getAdminJSONProto(s, path, nil); !testutils.IsError(err, regexp.QuoteMeta(errPattern)) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPITableDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name, dbName, tblName string
	}{
		{name: "lower", dbName: "test", tblName: "tbl"},
		{name: "lower", dbName: "test", tblName: `testschema.tbl`},
		{name: "lower with space", dbName: "test test", tblName: `"tbl tbl"`},
		{name: "upper", dbName: "TEST", tblName: `"TBL"`}, // Regression test for issue #14056
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(context.Background())
			ts := s.(*TestServer)

			escDBName := tree.NameStringP(&tc.dbName)
			tblName := tc.tblName
			schemaName := "testschema"

			ac := log.AmbientContext{Tracer: s.ClusterSettings().Tracer}
			ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
			defer span.Finish()

			tableSchema := `nulls_allowed INT8,
							nulls_not_allowed INT8 NOT NULL DEFAULT 1000,
							default2 INT8 DEFAULT 2,
							string_default STRING DEFAULT 'default_string',
						  INDEX descidx (default2 DESC)`

			setupQueries := []string{
				fmt.Sprintf("CREATE DATABASE %s", escDBName),
				fmt.Sprintf("CREATE SCHEMA %s", schemaName),
				fmt.Sprintf(`CREATE TABLE %s.%s (%s)`, escDBName, tblName, tableSchema),
				"CREATE USER readonly",
				"CREATE USER app",
				fmt.Sprintf("GRANT SELECT ON %s.%s TO readonly", escDBName, tblName),
				fmt.Sprintf("GRANT SELECT,UPDATE,DELETE ON %s.%s TO app", escDBName, tblName),
			}
			pgURL, cleanupGoDB := sqlutils.PGUrl(
				t, s.ServingSQLAddr(), "StartServer" /* prefix */, url.User(security.RootUser))
			defer cleanupGoDB()
			pgURL.Path = tc.dbName
			db, err := gosql.Open("postgres", pgURL.String())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, q := range setupQueries {
				if _, err := db.Exec(q); err != nil {
					t.Fatal(err)
				}
			}

			// Perform API call.
			var resp serverpb.TableDetailsResponse
			url := fmt.Sprintf("databases/%s/tables/%s", tc.dbName, tblName)
			if err := getAdminJSONProto(s, url, &resp); err != nil {
				t.Fatal(err)
			}

			// Verify columns.
			expColumns := []serverpb.TableDetailsResponse_Column{
				{Name: "nulls_allowed", Type: "INT8", Nullable: true, DefaultValue: ""},
				{Name: "nulls_not_allowed", Type: "INT8", Nullable: false, DefaultValue: "1000:::INT8"},
				{Name: "default2", Type: "INT8", Nullable: true, DefaultValue: "2:::INT8"},
				{Name: "string_default", Type: "STRING", Nullable: true, DefaultValue: "'default_string':::STRING"},
				{Name: "rowid", Type: "INT8", Nullable: false, DefaultValue: "unique_rowid()", Hidden: true},
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
				{User: security.AdminRole, Privileges: []string{"ALL"}},
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
				{Name: "primary", Column: "string_default", Direction: "N/A", Unique: true, Seq: 5, Storing: true},
				{Name: "primary", Column: "default2", Direction: "N/A", Unique: true, Seq: 4, Storing: true},
				{Name: "primary", Column: "nulls_not_allowed", Direction: "N/A", Unique: true, Seq: 3, Storing: true},
				{Name: "primary", Column: "nulls_allowed", Direction: "N/A", Unique: true, Seq: 2, Storing: true},
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

				showCreateTableQuery := fmt.Sprintf("SHOW CREATE TABLE %s.%s", escDBName, tblName)

				row := db.QueryRow(showCreateTableQuery)
				var createStmt, tableName string
				if err := row.Scan(&tableName, &createStmt); err != nil {
					t.Fatal(err)
				}

				if a, e := resp.CreateTableStatement, createStmt; a != e {
					t.Fatalf("mismatched create table statement; expected %s, got %s", e, a)
				}
			}

			// Verify Descriptor ID.
			tableID, err := ts.admin.queryTableID(ctx, security.RootUserName(), tc.dbName, tc.tblName)
			if err != nil {
				t.Fatal(err)
			}
			if a, e := resp.DescriptorID, int64(tableID); a != e {
				t.Fatalf("table had descriptorID %d, expected %d", a, e)
			}
		})
	}
}

// TestAdminAPIZoneDetails verifies the zone configuration information returned
// for both DatabaseDetailsResponse AND TableDetailsResponse.
func TestAdminAPIZoneDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

	// Create database and table.
	ac := log.AmbientContext{Tracer: s.ClusterSettings().Tracer}
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()
	setupQueries := []string{
		"CREATE DATABASE test",
		"CREATE TABLE test.tbl (val STRING)",
	}
	for _, q := range setupQueries {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("error executing '%s': %s", q, err)
		}
	}

	// Function to verify the zone for table "test.tbl" as returned by the Admin
	// API.
	verifyTblZone := func(
		expectedZone zonepb.ZoneConfig, expectedLevel serverpb.ZoneConfigurationLevel,
	) {
		var resp serverpb.TableDetailsResponse
		if err := getAdminJSONProto(s, "databases/test/tables/tbl", &resp); err != nil {
			t.Fatal(err)
		}
		if a, e := &resp.ZoneConfig, &expectedZone; !a.Equal(e) {
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
		expectedZone zonepb.ZoneConfig, expectedLevel serverpb.ZoneConfigurationLevel,
	) {
		var resp serverpb.DatabaseDetailsResponse
		if err := getAdminJSONProto(s, "databases/test", &resp); err != nil {
			t.Fatal(err)
		}
		if a, e := &resp.ZoneConfig, &expectedZone; !a.Equal(e) {
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
	setZone := func(zoneCfg zonepb.ZoneConfig, id descpb.ID) {
		zoneBytes, err := protoutil.Marshal(&zoneCfg)
		if err != nil {
			t.Fatal(err)
		}
		const query = `INSERT INTO system.zones VALUES($1, $2)`
		if _, err := db.Exec(query, id, zoneBytes); err != nil {
			t.Fatalf("error executing '%s': %s", query, err)
		}
	}

	// Verify zone matches cluster default.
	verifyDbZone(s.(*TestServer).Cfg.DefaultZoneConfig, serverpb.ZoneConfigurationLevel_CLUSTER)
	verifyTblZone(s.(*TestServer).Cfg.DefaultZoneConfig, serverpb.ZoneConfigurationLevel_CLUSTER)

	databaseID, err := ts.admin.queryDatabaseID(ctx, security.RootUserName(), "test")
	if err != nil {
		t.Fatal(err)
	}
	tableID, err := ts.admin.queryTableID(ctx, security.RootUserName(), "test", "tbl")
	if err != nil {
		t.Fatal(err)
	}

	// Apply zone configuration to database and check again.
	dbZone := zonepb.ZoneConfig{
		RangeMinBytes: proto.Int64(456),
	}
	setZone(dbZone, databaseID)
	verifyDbZone(dbZone, serverpb.ZoneConfigurationLevel_DATABASE)
	verifyTblZone(dbZone, serverpb.ZoneConfigurationLevel_DATABASE)

	// Apply zone configuration to table and check again.
	tblZone := zonepb.ZoneConfig{
		RangeMinBytes: proto.Int64(789),
	}
	setZone(tblZone, tableID)
	verifyDbZone(dbZone, serverpb.ZoneConfigurationLevel_DATABASE)
	verifyTblZone(tblZone, serverpb.ZoneConfigurationLevel_TABLE)
}

func TestAdminAPIUsers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create sample users.
	query := `
INSERT INTO system.users (username, "hashedPassword")
VALUES ('adminUser', 'abc'), ('bob', 'xyz')`
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}

	// Query the API for users.
	var resp serverpb.UsersResponse
	if err := getAdminJSONProto(s, "users", &resp); err != nil {
		t.Fatal(err)
	}
	expResult := serverpb.UsersResponse{
		Users: []serverpb.UsersResponse_User{
			{Username: "adminUser"},
			{Username: "authentic_user"},
			{Username: "bob"},
			{Username: "root"},
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
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	setupQueries := []string{
		"CREATE DATABASE api_test",
		"CREATE TABLE api_test.tbl1 (a INT)",
		"CREATE TABLE api_test.tbl2 (a INT)",
		"CREATE TABLE api_test.tbl3 (a INT)",
		"DROP TABLE api_test.tbl1",
		"DROP TABLE api_test.tbl2",
		"SET CLUSTER SETTING cluster.organization = 'somestring';",
	}
	for _, q := range setupQueries {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("error executing '%s': %s", q, err)
		}
	}

	const allEvents = ""
	type testcase struct {
		eventType  string
		hasLimit   bool
		limit      int
		unredacted bool
		expCount   int
	}
	testcases := []testcase{
		{"node_join", false, 0, false, 1},
		{"node_restart", false, 0, false, 0},
		{"drop_database", false, 0, false, 0},
		{"create_database", false, 0, false, 3},
		{"drop_table", false, 0, false, 2},
		{"create_table", false, 0, false, 3},
		{"set_cluster_setting", false, 0, false, 4},
		// We use limit=true with no limit here because otherwise the
		// expCount will mess up the expected total count below.
		{"set_cluster_setting", true, 0, true, 4},
		{"create_table", true, 0, false, 3},
		{"create_table", true, -1, false, 3},
		{"create_table", true, 2, false, 2},
	}
	minTotalEvents := 0
	for _, tc := range testcases {
		if !tc.hasLimit {
			minTotalEvents += tc.expCount
		}
	}
	testcases = append(testcases, testcase{allEvents, false, 0, false, minTotalEvents})

	for i, tc := range testcases {
		url := "events"
		if tc.eventType != allEvents {
			url += "?type=" + tc.eventType
			if tc.hasLimit {
				url += fmt.Sprintf("&limit=%d", tc.limit)
			}
			if tc.unredacted {
				url += "&unredacted_events=true"
			}
		}

		t.Run(url, func(t *testing.T) {
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
					if a, e := e.EventType, tc.eventType; a != e {
						t.Errorf("%d: event type %s != expected %s", i, a, e)
					}
				} else {
					if len(e.EventType) == 0 {
						t.Errorf("%d: missing event type in event", i)
					}
				}

				isSettingChange := e.EventType == "set_cluster_setting"
				isRoleChange := e.EventType == "create_role" ||
					e.EventType == "drop_role" ||
					e.EventType == "alter_role"

				if e.TargetID == 0 && !isSettingChange && !isRoleChange {
					t.Errorf("%d: missing/empty TargetID", i)
				}
				if e.ReportingID == 0 {
					t.Errorf("%d: missing/empty ReportingID", i)
				}
				if len(e.Info) == 0 {
					t.Errorf("%d: missing/empty Info", i)
				}
				if isSettingChange && strings.Contains(e.Info, "cluster.organization") {
					if tc.unredacted {
						if !strings.Contains(e.Info, "somestring") {
							t.Errorf("%d: require 'somestring' in Info", i)
						}
					} else {
						if strings.Contains(e.Info, "somestring") {
							t.Errorf("%d: un-redacted 'somestring' in Info", i)
						}
					}
				}
				if len(e.UniqueID) == 0 {
					t.Errorf("%d: missing/empty UniqueID", i)
				}
			}
		})
	}
}

func TestAdminAPISettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Any bool that defaults to true will work here.
	const settingKey = "sql.metrics.statement_details.enabled"
	st := s.ClusterSettings()
	allKeys := settings.Keys()

	checkSetting := func(t *testing.T, k string, v serverpb.SettingsResponse_Value) {
		ref, ok := settings.Lookup(k, settings.LookupForReporting)
		if !ok {
			t.Fatalf("%s: not found after initial lookup", k)
		}
		typ := ref.Typ()

		if !settings.TestingIsReportable(ref) {
			if v.Value != "<redacted>" && v.Value != "" {
				t.Errorf("%s: expected redacted value for %v, got %s", k, ref, v.Value)
			}
		} else {
			if ref.String(&st.SV) != v.Value {
				t.Errorf("%s: expected value %v, got %s", k, ref, v.Value)
			}
		}

		if expectedPublic := ref.Visibility() == settings.Public; expectedPublic != v.Public {
			t.Errorf("%s: expected public %v, got %v", k, expectedPublic, v.Public)
		}

		if desc := ref.Description(); desc != v.Description {
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
			t.Fatalf("failed to observe test setting %s, got %+v", settingKey, resp.KeyValues)
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

// TestAdminAPIUIData checks that UI customizations are properly
// persisted for both admin and non-admin users.
func TestAdminAPIUIData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	testutils.RunTrueAndFalse(t, "isAdmin", func(t *testing.T, isAdmin bool) {
		start := timeutil.Now()

		mustSetUIData := func(keyValues map[string][]byte) {
			if err := postAdminJSONProtoWithAdminOption(s, "uidata", &serverpb.SetUIDataRequest{
				KeyValues: keyValues,
			}, &serverpb.SetUIDataResponse{}, isAdmin); err != nil {
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
			if err := getAdminJSONProtoWithAdminOption(s, url, &resp, isAdmin); err != nil {
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
			if err := getAdminJSONProtoWithAdminOption(s, url, &resp, isAdmin); err != nil {
				t.Fatal(err)
			}
			if len(resp.KeyValues) != 0 {
				t.Fatal("key unexpectedly found")
			}
		}

		// Basic tests.
		var badResp serverpb.GetUIDataResponse
		const errPattern = "400 Bad Request"
		if err := getAdminJSONProtoWithAdminOption(s, "uidata", &badResp, isAdmin); !testutils.IsError(err, errPattern) {
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
	})
}

// TestAdminAPIUISeparateData check that separate users have separate customizations.
func TestAdminAPIUISeparateData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Make a setting for an admin user.
	if err := postAdminJSONProtoWithAdminOption(s, "uidata",
		&serverpb.SetUIDataRequest{KeyValues: map[string][]byte{"k": []byte("v1")}},
		&serverpb.SetUIDataResponse{},
		true /*isAdmin*/); err != nil {
		t.Fatal(err)
	}

	// Make a setting for a non-admin user.
	if err := postAdminJSONProtoWithAdminOption(s, "uidata",
		&serverpb.SetUIDataRequest{KeyValues: map[string][]byte{"k": []byte("v2")}},
		&serverpb.SetUIDataResponse{},
		false /*isAdmin*/); err != nil {
		t.Fatal(err)
	}

	var resp serverpb.GetUIDataResponse
	url := "uidata?keys=k"

	if err := getAdminJSONProtoWithAdminOption(s, url, &resp, true /* isAdmin */); err != nil {
		t.Fatal(err)
	}
	if len(resp.KeyValues) != 1 || !bytes.Equal(resp.KeyValues["k"].Value, []byte("v1")) {
		t.Fatalf("unexpected admin values: %+v", resp.KeyValues)
	}
	if err := getAdminJSONProtoWithAdminOption(s, url, &resp, false /* isAdmin */); err != nil {
		t.Fatal(err)
	}
	if len(resp.KeyValues) != 1 || !bytes.Equal(resp.KeyValues["k"].Value, []byte("v2")) {
		t.Fatalf("unexpected non-admin values: %+v", resp.KeyValues)
	}
}

func TestClusterAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	testutils.RunTrueAndFalse(t, "reportingOn", func(t *testing.T, reportingOn bool) {
		testutils.RunTrueAndFalse(t, "enterpriseOn", func(t *testing.T, enterpriseOn bool) {
			// Override server license check.
			if enterpriseOn {
				old := base.CheckEnterpriseEnabled
				base.CheckEnterpriseEnabled = func(_ *cluster.Settings, _ uuid.UUID, _, _ string) error {
					return nil
				}
				defer func() { base.CheckEnterpriseEnabled = old }()
			}

			if _, err := db.Exec(`SET CLUSTER SETTING diagnostics.reporting.enabled = $1`, reportingOn); err != nil {
				t.Fatal(err)
			}

			// We need to retry, because the cluster ID isn't set until after
			// bootstrapping and because setting a cluster setting isn't necessarily
			// instantaneous.
			//
			// Also note that there's a migration that affects `diagnostics.reporting.enabled`,
			// so manipulating the cluster setting var directly is a bad idea.
			testutils.SucceedsSoon(t, func() error {
				var resp serverpb.ClusterResponse
				if err := getAdminJSONProto(s, "cluster", &resp); err != nil {
					return err
				}
				if a, e := resp.ClusterID, s.RPCContext().ClusterID.String(); a != e {
					return errors.Errorf("cluster ID %s != expected %s", a, e)
				}
				if a, e := resp.ReportingEnabled, reportingOn; a != e {
					return errors.Errorf("reportingEnabled = %t, wanted %t", a, e)
				}
				if a, e := resp.EnterpriseEnabled, enterpriseOn; a != e {
					return errors.Errorf("enterpriseEnabled = %t, wanted %t", a, e)
				}
				return nil
			})
		})
	})
}

func TestHealthAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ts := s.(*TestServer)

	// We need to retry because the node ID isn't set until after
	// bootstrapping.
	testutils.SucceedsSoon(t, func() error {
		var resp serverpb.HealthResponse
		return getAdminJSONProto(s, "health", &resp)
	})

	// Make the SQL listener appear unavailable. Verify that health fails after that.
	ts.sqlServer.acceptingClients.Set(false)
	var resp serverpb.HealthResponse
	err := getAdminJSONProto(s, "health?ready=1", &resp)
	if err == nil {
		t.Error("server appears ready even though SQL listener is not")
	}
	ts.sqlServer.acceptingClients.Set(true)
	err = getAdminJSONProto(s, "health?ready=1", &resp)
	if err != nil {
		t.Errorf("server not ready after SQL listener is ready again: %v", err)
	}

	// Expire this node's liveness record by pausing heartbeats and advancing the
	// server's clock.
	defer ts.nodeLiveness.PauseAllHeartbeatsForTest()()
	self, ok := ts.nodeLiveness.Self()
	assert.True(t, ok)
	s.Clock().Update(self.Expiration.ToTimestamp().Add(1, 0).UnsafeToClockTimestamp())

	testutils.SucceedsSoon(t, func() error {
		err := getAdminJSONProto(s, "health?ready=1", &resp)
		if err == nil {
			return errors.New("health OK, still waiting for unhealth")
		}

		t.Logf("observed error: %v", err)
		if !testutils.IsError(err, `(?s)503 Service Unavailable.*"error": "node is not healthy"`) {
			return err
		}
		return nil
	})

	// After the node reports an error with `?ready=1`, the health
	// endpoint must still succeed without error when `?ready=1` is not specified.
	if err := getAdminJSONProto(s, "health", &resp); err != nil {
		t.Fatal(err)
	}
}

// getSystemJobIDs queries the jobs table for all jobs IDs. Sorted by decreasing creation time.
func getSystemJobIDs(t testing.TB, db *sqlutils.SQLRunner) []int64 {
	rows := db.Query(t, `SELECT job_id FROM crdb_internal.jobs ORDER BY created DESC;`)
	defer rows.Close()

	res := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		res = append(res, id)
	}
	return res
}

func TestAdminAPIJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	testutils.RunTrueAndFalse(t, "isAdmin", func(t *testing.T, isAdmin bool) {
		// Creating this client causes a user to be created, which causes jobs
		// to be created, so we do it up-front rather than inside the test.
		_, err := s.GetAuthenticatedHTTPClient(isAdmin)
		if err != nil {
			t.Fatal(err)
		}
	})

	// Get list of existing jobs (migrations). Assumed to all have succeeded.
	existingIDs := getSystemJobIDs(t, sqlDB)

	testJobs := []struct {
		id       int64
		status   jobs.Status
		details  jobspb.Details
		progress jobspb.ProgressDetails
		username security.SQLUsername
	}{
		{1, jobs.StatusRunning, jobspb.RestoreDetails{}, jobspb.RestoreProgress{}, security.RootUserName()},
		{2, jobs.StatusRunning, jobspb.BackupDetails{}, jobspb.BackupProgress{}, security.RootUserName()},
		{3, jobs.StatusSucceeded, jobspb.BackupDetails{}, jobspb.BackupProgress{}, security.RootUserName()},
		{4, jobs.StatusRunning, jobspb.ChangefeedDetails{}, jobspb.ChangefeedProgress{}, security.RootUserName()},
		{5, jobs.StatusSucceeded, jobspb.BackupDetails{}, jobspb.BackupProgress{}, authenticatedUserNameNoAdmin()},
	}
	for _, job := range testJobs {
		payload := jobspb.Payload{UsernameProto: job.username.EncodeProto(), Details: jobspb.WrapPayloadDetails(job.details)}
		payloadBytes, err := protoutil.Marshal(&payload)
		if err != nil {
			t.Fatal(err)
		}

		progress := jobspb.Progress{Details: jobspb.WrapProgressDetails(job.progress)}
		// Populate progress.Progress field with a specific progress type based on
		// the job type.
		if _, ok := job.progress.(jobspb.ChangefeedProgress); ok {
			progress.Progress = &jobspb.Progress_HighWater{
				HighWater: &hlc.Timestamp{},
			}
		} else {
			progress.Progress = &jobspb.Progress_FractionCompleted{
				FractionCompleted: 1.0,
			}
		}

		progressBytes, err := protoutil.Marshal(&progress)
		if err != nil {
			t.Fatal(err)
		}
		sqlDB.Exec(t,
			`INSERT INTO system.jobs (id, status, payload, progress) VALUES ($1, $2, $3, $4)`,
			job.id, job.status, payloadBytes, progressBytes,
		)
	}

	const invalidJobType = math.MaxInt32

	testCases := []struct {
		uri                    string
		expectedIDsViaAdmin    []int64
		expectedIDsViaNonAdmin []int64
	}{
		{"jobs", append([]int64{5, 4, 3, 2, 1}, existingIDs...), []int64{5}},
		{"jobs?limit=1", []int64{5}, []int64{5}},
		{"jobs?status=running", []int64{4, 2, 1}, []int64{}},
		{"jobs?status=succeeded", append([]int64{5, 3}, existingIDs...), []int64{5}},
		{"jobs?status=pending", []int64{}, []int64{}},
		{"jobs?status=garbage", []int64{}, []int64{}},
		{fmt.Sprintf("jobs?type=%d", jobspb.TypeBackup), []int64{5, 3, 2}, []int64{5}},
		{fmt.Sprintf("jobs?type=%d", jobspb.TypeRestore), []int64{1}, []int64{}},
		{fmt.Sprintf("jobs?type=%d", invalidJobType), []int64{}, []int64{}},
		{fmt.Sprintf("jobs?status=running&type=%d", jobspb.TypeBackup), []int64{2}, []int64{}},
	}

	testutils.RunTrueAndFalse(t, "isAdmin", func(t *testing.T, isAdmin bool) {
		for i, testCase := range testCases {
			var res serverpb.JobsResponse
			if err := getAdminJSONProtoWithAdminOption(s, testCase.uri, &res, isAdmin); err != nil {
				t.Fatal(err)
			}
			resIDs := []int64{}
			for _, job := range res.Jobs {
				resIDs = append(resIDs, job.ID)
			}

			expected := testCase.expectedIDsViaAdmin
			if !isAdmin {
				expected = testCase.expectedIDsViaNonAdmin
			}

			if e, a := expected, resIDs; !reflect.DeepEqual(e, a) {
				t.Errorf("%d: expected job IDs %v, but got %v", i, e, a)
			}
		}
	})
}

func TestAdminAPILocations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	testLocations := []struct {
		localityKey   string
		localityValue string
		latitude      float64
		longitude     float64
	}{
		{"city", "Des Moines", 41.60054, -93.60911},
		{"city", "New York City", 40.71427, -74.00597},
		{"city", "Seattle", 47.60621, -122.33207},
	}
	for _, loc := range testLocations {
		sqlDB.Exec(t,
			`INSERT INTO system.locations ("localityKey", "localityValue", latitude, longitude) VALUES ($1, $2, $3, $4)`,
			loc.localityKey, loc.localityValue, loc.latitude, loc.longitude,
		)
	}
	var res serverpb.LocationsResponse
	if err := getAdminJSONProtoWithAdminOption(s, "locations", &res, false /* isAdmin */); err != nil {
		t.Fatal(err)
	}
	for i, loc := range testLocations {
		expLoc := serverpb.LocationsResponse_Location{
			LocalityKey:   loc.localityKey,
			LocalityValue: loc.localityValue,
			Latitude:      loc.latitude,
			Longitude:     loc.longitude,
		}
		if !reflect.DeepEqual(res.Locations[i], expLoc) {
			t.Errorf("%d: expected location %v, but got %v", i, expLoc, res.Locations[i])
		}
	}
}

func TestAdminAPIQueryPlan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `CREATE DATABASE api_test`)
	sqlDB.Exec(t, `CREATE TABLE api_test.t1 (id int primary key, name string)`)
	sqlDB.Exec(t, `CREATE TABLE api_test.t2 (id int primary key, name string)`)

	testCases := []struct {
		query string
		exp   []string
	}{
		{"SELECT sum(id) FROM api_test.t1", []string{"nodeNames\":[\"1\"]", "Out: @1"}},
		{"SELECT sum(1) FROM api_test.t1 JOIN api_test.t2 on t1.id = t2.id", []string{"nodeNames\":[\"1\"]", "Out: @1"}},
	}
	for i, testCase := range testCases {
		var res serverpb.QueryPlanResponse
		queryParam := url.QueryEscape(testCase.query)
		if err := getAdminJSONProto(s, fmt.Sprintf("queryplan?query=%s", queryParam), &res); err != nil {
			t.Errorf("%d: got error %s", i, err)
		}

		for _, exp := range testCase.exp {
			if !strings.Contains(res.DistSQLPhysicalQueryPlan, exp) {
				t.Errorf("%d: expected response %v to contain %s", i, res, exp)
			}
		}
	}

}

func TestAdminAPIRangeLogByRangeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	rangeID := 654321
	testCases := []struct {
		rangeID  int
		hasLimit bool
		limit    int
		expected int
	}{
		{rangeID, true, 0, 2},
		{rangeID, true, -1, 2},
		{rangeID, true, 1, 1},
		{rangeID, false, 0, 2},
		// We'll create one event that has rangeID+1 as the otherRangeID.
		{rangeID + 1, false, 0, 1},
	}

	for _, otherRangeID := range []int{rangeID + 1, rangeID + 2} {
		if _, err := db.Exec(
			`INSERT INTO system.rangelog (
             timestamp, "rangeID", "otherRangeID", "storeID", "eventType"
           ) VALUES (
             now(), $1, $2, $3, $4
          )`,
			rangeID, otherRangeID,
			1, // storeID
			kvserverpb.RangeLogEventType_add_voter.String(),
		); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range testCases {
		url := fmt.Sprintf("rangelog/%d", tc.rangeID)
		if tc.hasLimit {
			url += fmt.Sprintf("?limit=%d", tc.limit)
		}
		t.Run(url, func(t *testing.T) {
			var resp serverpb.RangeLogResponse
			if err := getAdminJSONProto(s, url, &resp); err != nil {
				t.Fatal(err)
			}

			if e, a := tc.expected, len(resp.Events); e != a {
				t.Fatalf("expected %d events, got %d", e, a)
			}

			for _, event := range resp.Events {
				expID := roachpb.RangeID(tc.rangeID)
				if event.Event.RangeID != expID && event.Event.OtherRangeID != expID {
					t.Errorf("expected rangeID or otherRangeID to be %d, got %d and r%d",
						expID, event.Event.RangeID, event.Event.OtherRangeID)
				}
			}
		})
	}
}

// Test the range log API when queries are not filtered by a range ID (like in
// TestAdminAPIRangeLogByRangeID).
func TestAdminAPIFullRangeLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					DisableSplitQueue: true,
				},
			},
		})
	defer s.Stopper().Stop(context.Background())

	// Insert something in the rangelog table, otherwise it's empty for new
	// clusters.
	rows, err := db.Query(`SELECT count(1) FROM system.rangelog`)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("missing row")
	}
	var cnt int
	if err := rows.Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Fatalf("expected 0 rows in system.rangelog, found: %d", cnt)
	}
	const rangeID = 100
	for i := 0; i < 10; i++ {
		if _, err := db.Exec(
			`INSERT INTO system.rangelog (
             timestamp, "rangeID", "storeID", "eventType"
           ) VALUES (now(), $1, 1, $2)`,
			rangeID,
			kvserverpb.RangeLogEventType_add_voter.String(),
		); err != nil {
			t.Fatal(err)
		}
	}
	expectedEvents := 10

	testCases := []struct {
		hasLimit bool
		limit    int
		expected int
	}{
		{false, 0, expectedEvents},
		{true, 0, expectedEvents},
		{true, -1, expectedEvents},
		{true, 1, 1},
	}

	for _, tc := range testCases {
		url := "rangelog"
		if tc.hasLimit {
			url += fmt.Sprintf("?limit=%d", tc.limit)
		}
		t.Run(url, func(t *testing.T) {
			var resp serverpb.RangeLogResponse
			if err := getAdminJSONProto(s, url, &resp); err != nil {
				t.Fatal(err)
			}
			events := resp.Events
			if e, a := tc.expected, len(events); e != a {
				var sb strings.Builder
				for _, ev := range events {
					sb.WriteString(ev.String() + "\n")
				}
				t.Fatalf("expected %d events, got %d:\n%s", e, a, sb.String())
			}
		})
	}
}

func TestAdminAPIDataDistribution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())

	firstServer := testCluster.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	// Create some tables.
	sqlDB.Exec(t, `CREATE DATABASE roachblog`)
	sqlDB.Exec(t, `CREATE TABLE roachblog.posts (id INT PRIMARY KEY, title text, body text)`)
	sqlDB.Exec(t, `CREATE TABLE roachblog.comments (
		id INT PRIMARY KEY,
		post_id INT REFERENCES roachblog.posts,
		body text
	)`)
	sqlDB.Exec(t, `CREATE SCHEMA roachblog."foo bar"`)
	sqlDB.Exec(t, `CREATE TABLE roachblog."foo bar".other_stuff(id INT PRIMARY KEY, body TEXT)`)
	// Test special characters in DB and table names.
	sqlDB.Exec(t, `CREATE DATABASE "sp'ec\ch""ars"`)
	sqlDB.Exec(t, `CREATE TABLE "sp'ec\ch""ars"."more\spec'chars" (id INT PRIMARY KEY)`)

	// Verify that we see their replicas in the DataDistribution response, evenly spread
	// across the test cluster's three nodes.

	expectedDatabaseInfo := map[string]serverpb.DataDistributionResponse_DatabaseInfo{
		"roachblog": {
			TableInfo: map[string]serverpb.DataDistributionResponse_TableInfo{
				"public.posts": {
					ReplicaCountByNodeId: map[roachpb.NodeID]int64{
						1: 1,
						2: 1,
						3: 1,
					},
				},
				"public.comments": {
					ReplicaCountByNodeId: map[roachpb.NodeID]int64{
						1: 1,
						2: 1,
						3: 1,
					},
				},
				`"foo bar".other_stuff`: {
					ReplicaCountByNodeId: map[roachpb.NodeID]int64{
						1: 1,
						2: 1,
						3: 1,
					},
				},
			},
		},
		`sp'ec\ch"ars`: {
			TableInfo: map[string]serverpb.DataDistributionResponse_TableInfo{
				`public."more\spec'chars"`: {
					ReplicaCountByNodeId: map[roachpb.NodeID]int64{
						1: 1,
						2: 1,
						3: 1,
					},
				},
			},
		},
	}

	// Wait for the new tables' ranges to be created and replicated.
	testutils.SucceedsSoon(t, func() error {
		var resp serverpb.DataDistributionResponse
		if err := getAdminJSONProto(firstServer, "data_distribution", &resp); err != nil {
			t.Fatal(err)
		}

		delete(resp.DatabaseInfo, "system") // delete results for system database.
		if !reflect.DeepEqual(resp.DatabaseInfo, expectedDatabaseInfo) {
			return fmt.Errorf("expected %v; got %v", expectedDatabaseInfo, resp.DatabaseInfo)
		}

		// Don't test anything about the zone configs for now; just verify that something is there.
		if len(resp.ZoneConfigs) == 0 {
			return fmt.Errorf("no zone configs returned")
		}

		return nil
	})

	// Verify that the request still works after a table has been dropped,
	// and that dropped_at is set on the dropped table.
	sqlDB.Exec(t, `DROP TABLE roachblog.comments`)

	var resp serverpb.DataDistributionResponse
	if err := getAdminJSONProto(firstServer, "data_distribution", &resp); err != nil {
		t.Fatal(err)
	}

	if resp.DatabaseInfo["roachblog"].TableInfo["public.comments"].DroppedAt == nil {
		t.Fatal("expected roachblog.comments to have dropped_at set but it's nil")
	}

	// Verify that the request still works after a database has been dropped.
	sqlDB.Exec(t, `DROP DATABASE roachblog CASCADE`)

	if err := getAdminJSONProto(firstServer, "data_distribution", &resp); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkAdminAPIDataDistribution(b *testing.B) {
	skip.UnderShort(b, "TODO: fix benchmark")
	testCluster := serverutils.StartNewTestCluster(b, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())

	firstServer := testCluster.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	sqlDB.Exec(b, `CREATE DATABASE roachblog`)

	// Create a bunch of tables.
	for i := 0; i < 200; i++ {
		sqlDB.Exec(
			b,
			fmt.Sprintf(`CREATE TABLE roachblog.t%d (id INT PRIMARY KEY, title text, body text)`, i),
		)
		// TODO(vilterp): split to increase the number of ranges for each table
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var resp serverpb.DataDistributionResponse
		if err := getAdminJSONProto(firstServer, "data_distribution", &resp); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func TestEnqueueRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer testCluster.Stopper().Stop(context.Background())

	// Up-replicate r1 to all 3 nodes. We use manual replication to avoid lease
	// transfers causing temporary conditions in which no store is the
	// leaseholder, which can break the tests below.
	_, err := testCluster.AddVoters(roachpb.KeyMin, testCluster.Target(1), testCluster.Target(2))
	if err != nil {
		t.Fatal(err)
	}

	// RangeID being queued
	const realRangeID = 1
	const fakeRangeID = 999

	// Who we expect responses from.
	const none = 0
	const leaseholder = 1
	const allReplicas = 3

	testCases := []struct {
		nodeID            roachpb.NodeID
		queue             string
		rangeID           roachpb.RangeID
		expectedDetails   int
		expectedNonErrors int
	}{
		// Success cases
		{0, "gc", realRangeID, allReplicas, leaseholder},
		{0, "split", realRangeID, allReplicas, leaseholder},
		{0, "replicaGC", realRangeID, allReplicas, allReplicas},
		{0, "RaFtLoG", realRangeID, allReplicas, allReplicas},
		{0, "RAFTSNAPSHOT", realRangeID, allReplicas, allReplicas},
		{0, "consistencyChecker", realRangeID, allReplicas, leaseholder},
		{0, "TIMESERIESmaintenance", realRangeID, allReplicas, leaseholder},
		{1, "raftlog", realRangeID, leaseholder, leaseholder},
		{2, "raftlog", realRangeID, leaseholder, 1},
		{3, "raftlog", realRangeID, leaseholder, 1},
		// Error cases
		{0, "gv", realRangeID, allReplicas, none},
		{0, "GC", fakeRangeID, allReplicas, none},
	}

	for _, tc := range testCases {
		t.Run(tc.queue, func(t *testing.T) {
			req := &serverpb.EnqueueRangeRequest{
				NodeID:  tc.nodeID,
				Queue:   tc.queue,
				RangeID: tc.rangeID,
			}
			var resp serverpb.EnqueueRangeResponse
			if err := postAdminJSONProto(testCluster.Server(0), "enqueue_range", req, &resp); err != nil {
				t.Fatal(err)
			}
			if e, a := tc.expectedDetails, len(resp.Details); e != a {
				t.Errorf("expected %d details; got %d: %+v", e, a, resp)
			}
			var numNonErrors int
			for _, details := range resp.Details {
				if len(details.Events) > 0 && details.Error == "" {
					numNonErrors++
				}
			}
			if tc.expectedNonErrors != numNonErrors {
				t.Errorf("expected %d non-error details; got %d: %+v", tc.expectedNonErrors, numNonErrors, resp)
			}
		})
	}

	// Finally, test a few more basic error cases.
	reqs := []*serverpb.EnqueueRangeRequest{
		{NodeID: -1, Queue: "gc"},
		{Queue: ""},
		{RangeID: -1, Queue: "gc"},
	}
	for _, req := range reqs {
		t.Run(fmt.Sprint(req), func(t *testing.T) {
			var resp serverpb.EnqueueRangeResponse
			err := postAdminJSONProto(testCluster.Server(0), "enqueue_range", req, &resp)
			if err == nil {
				t.Fatalf("unexpected success: %+v", resp)
			}
			if !testutils.IsError(err, "400 Bad Request") {
				t.Fatalf("unexpected error type: %+v", err)
			}
		})
	}
}

func TestStatsforSpanOnLocalMax(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())
	firstServer := testCluster.Server(0)
	adminServer := firstServer.(*TestServer).Server.admin

	underTest := roachpb.Span{
		Key:    keys.LocalMax,
		EndKey: keys.SystemPrefix,
	}

	_, err := adminServer.statsForSpan(context.Background(), underTest)
	if err != nil {
		t.Fatal(err)
	}
}

// TestEndpointTelemetryBasic tests that the telemetry collection on the usage of
// CRDB's endpoints works as expected by recording the call counts of `Admin` &
// `Status` requests.
func TestEndpointTelemetryBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Check that calls over HTTP are recorded.
	var details serverpb.LocationsResponse
	if err := getAdminJSONProto(s, "locations", &details); err != nil {
		t.Fatal(err)
	}
	require.GreaterOrEqual(t, telemetry.Read(getServerEndpointCounter(
		"/cockroach.server.serverpb.Admin/Locations",
	)), int32(1))

	var resp serverpb.StatementsResponse
	if err := getStatusJSONProto(s, "statements", &resp); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, int32(1), telemetry.Read(getServerEndpointCounter(
		"/cockroach.server.serverpb.Status/Statements",
	)))
}

func TestDecommissionSelf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t) // can't handle 7-node clusters

	// Set up test cluster.
	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 7, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)

	// Decommission several nodes, including the node we're submitting the
	// decommission request to. We use the admin client in order to test the
	// admin server's logic, which involves a subsequent DecommissionStatus
	// call which could fail if used from a node that's just decommissioned.
	adminSrv := tc.Server(4)
	conn, err := adminSrv.RPCContext().GRPCDialNode(
		adminSrv.RPCAddr(), adminSrv.NodeID(), rpc.DefaultClass).Connect(ctx)
	require.NoError(t, err)
	adminClient := serverpb.NewAdminClient(conn)
	decomNodeIDs := []roachpb.NodeID{
		tc.Server(4).NodeID(),
		tc.Server(5).NodeID(),
		tc.Server(6).NodeID(),
	}

	// The DECOMMISSIONING call should return a full status response.
	resp, err := adminClient.Decommission(ctx, &serverpb.DecommissionRequest{
		NodeIDs:          decomNodeIDs,
		TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
	})
	require.NoError(t, err)
	require.Len(t, resp.Status, len(decomNodeIDs))
	for i, nodeID := range decomNodeIDs {
		status := resp.Status[i]
		require.Equal(t, nodeID, status.NodeID)
		// Liveness entries may not have been updated yet.
		require.Contains(t, []livenesspb.MembershipStatus{
			livenesspb.MembershipStatus_ACTIVE,
			livenesspb.MembershipStatus_DECOMMISSIONING,
		}, status.Membership, "unexpected membership status %v for node %v", status, nodeID)
	}

	// The DECOMMISSIONED call should return an empty response, to avoid
	// erroring due to loss of cluster RPC access when decommissioning self.
	resp, err = adminClient.Decommission(ctx, &serverpb.DecommissionRequest{
		NodeIDs:          decomNodeIDs,
		TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONED,
	})
	require.NoError(t, err)
	require.Empty(t, resp.Status)

	// The nodes should now have been (or soon become) decommissioned.
	for i := 0; i < tc.NumServers(); i++ {
		srv := tc.Server(i)
		expect := livenesspb.MembershipStatus_ACTIVE
		for _, nodeID := range decomNodeIDs {
			if srv.NodeID() == nodeID {
				expect = livenesspb.MembershipStatus_DECOMMISSIONED
				break
			}
		}
		require.Eventually(t, func() bool {
			liveness, ok := srv.NodeLiveness().(*liveness.NodeLiveness).GetLiveness(srv.NodeID())
			return ok && liveness.Membership == expect
		}, 5*time.Second, 100*time.Millisecond, "timed out waiting for node %v status %v", i, expect)
	}
}

func TestAdminDecommissionedOperations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
		ServerArgs: base.TestServerArgs{
			Insecure: true, // allows admin client without setting up certs
		},
	})
	defer tc.Stopper().Stop(ctx)

	scratchKey := tc.ScratchRange(t)
	scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
	require.Len(t, scratchRange.InternalReplicas, 1)
	require.Equal(t, tc.Server(0).NodeID(), scratchRange.InternalReplicas[0].NodeID)

	// Decommission server 1 and wait for it to lose cluster access.
	srv := tc.Server(0)
	decomSrv := tc.Server(1)
	for _, status := range []livenesspb.MembershipStatus{
		livenesspb.MembershipStatus_DECOMMISSIONING, livenesspb.MembershipStatus_DECOMMISSIONED,
	} {
		require.NoError(t, srv.Decommission(ctx, status, []roachpb.NodeID{decomSrv.NodeID()}))
	}

	require.Eventually(t, func() bool {
		_, err := decomSrv.DB().Scan(ctx, keys.MinKey, keys.MaxKey, 0)
		s, ok := status.FromError(errors.UnwrapAll(err))
		return ok && s.Code() == codes.PermissionDenied
	}, 10*time.Second, 100*time.Millisecond, "timed out waiting for server to lose cluster access")

	// Set up an admin client.
	conn, err := grpc.Dial(decomSrv.ServingRPCAddr(), grpc.WithInsecure())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close() // nolint:grpcconnclose
	}()
	adminClient := serverpb.NewAdminClient(conn)

	// Run some operations on the decommissioned node. The ones that require
	// access to the cluster should fail, other should succeed. We're mostly
	// concerned with making sure they return rather than hang due to internal
	// retries.
	testcases := []struct {
		name       string
		expectCode codes.Code
		op         func(serverpb.AdminClient) error
	}{
		{"Cluster", codes.OK, func(c serverpb.AdminClient) error {
			_, err := c.Cluster(ctx, &serverpb.ClusterRequest{})
			return err
		}},
		{"Databases", codes.Internal, func(c serverpb.AdminClient) error {
			_, err := c.Databases(ctx, &serverpb.DatabasesRequest{})
			return err
		}},
		{"DatabaseDetails", codes.Internal, func(c serverpb.AdminClient) error {
			_, err := c.DatabaseDetails(ctx, &serverpb.DatabaseDetailsRequest{Database: "foo"})
			return err
		}},
		{"DataDistribution", codes.Internal, func(c serverpb.AdminClient) error {
			_, err := c.DataDistribution(ctx, &serverpb.DataDistributionRequest{})
			return err
		}},
		{"Decommission", codes.Unknown, func(c serverpb.AdminClient) error {
			_, err := c.Decommission(ctx, &serverpb.DecommissionRequest{
				NodeIDs:          []roachpb.NodeID{srv.NodeID(), decomSrv.NodeID()},
				TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONED,
			})
			return err
		}},
		{"DecommissionStatus", codes.Unknown, func(c serverpb.AdminClient) error {
			_, err := c.DecommissionStatus(ctx, &serverpb.DecommissionStatusRequest{
				NodeIDs: []roachpb.NodeID{srv.NodeID(), decomSrv.NodeID()},
			})
			return err
		}},
		{"EnqueueRange", codes.Internal, func(c serverpb.AdminClient) error {
			_, err := c.EnqueueRange(ctx, &serverpb.EnqueueRangeRequest{
				RangeID: scratchRange.RangeID,
				Queue:   "replicaGC",
			})
			return err
		}},
		{"Events", codes.Internal, func(c serverpb.AdminClient) error {
			_, err := c.Events(ctx, &serverpb.EventsRequest{})
			return err
		}},
		{"Health", codes.OK, func(c serverpb.AdminClient) error {
			_, err := c.Health(ctx, &serverpb.HealthRequest{})
			return err
		}},
		{"Jobs", codes.Internal, func(c serverpb.AdminClient) error {
			_, err := c.Jobs(ctx, &serverpb.JobsRequest{})
			return err
		}},
		{"Liveness", codes.OK, func(c serverpb.AdminClient) error {
			_, err := c.Liveness(ctx, &serverpb.LivenessRequest{})
			return err
		}},
		{"Locations", codes.Internal, func(c serverpb.AdminClient) error {
			_, err := c.Locations(ctx, &serverpb.LocationsRequest{})
			return err
		}},
		{"NonTableStats", codes.Internal, func(c serverpb.AdminClient) error {
			_, err := c.NonTableStats(ctx, &serverpb.NonTableStatsRequest{})
			return err
		}},
		{"QueryPlan", codes.OK, func(c serverpb.AdminClient) error {
			_, err := c.QueryPlan(ctx, &serverpb.QueryPlanRequest{Query: "SELECT 1"})
			return err
		}},
		{"RangeLog", codes.Internal, func(c serverpb.AdminClient) error {
			_, err := c.RangeLog(ctx, &serverpb.RangeLogRequest{})
			return err
		}},
		{"Settings", codes.OK, func(c serverpb.AdminClient) error {
			_, err := c.Settings(ctx, &serverpb.SettingsRequest{})
			return err
		}},
		{"TableStats", codes.Internal, func(c serverpb.AdminClient) error {
			_, err := c.TableStats(ctx, &serverpb.TableStatsRequest{Database: "foo", Table: "bar"})
			return err
		}},
		{"TableDetails", codes.Internal, func(c serverpb.AdminClient) error {
			_, err := c.TableDetails(ctx, &serverpb.TableDetailsRequest{Database: "foo", Table: "bar"})
			return err
		}},
		{"Users", codes.Internal, func(c serverpb.AdminClient) error {
			_, err := c.Users(ctx, &serverpb.UsersRequest{})
			return err
		}},
		// We drain at the end, since it may evict us.
		{"Drain", codes.Unknown, func(c serverpb.AdminClient) error {
			stream, err := c.Drain(ctx, &serverpb.DrainRequest{DoDrain: true})
			if err != nil {
				return err
			}
			_, err = stream.Recv()
			return err
		}},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var err error
			require.Eventually(t, func() bool {
				err = tc.op(adminClient)
				if tc.expectCode == codes.OK {
					require.NoError(t, err)
					return true
				}
				s, ok := status.FromError(errors.UnwrapAll(err))
				if s == nil || !ok {
					return false
				}
				require.Equal(t, tc.expectCode, s.Code())
				return true
			}, 10*time.Second, 100*time.Millisecond, "timed out waiting for gRPC error, got %s", err)
		})
	}
}
