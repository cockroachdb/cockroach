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
	"io"
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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/idxusage"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randident"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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
	httpClient, err := ts.GetAdminHTTPClient()
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails with
		// it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails with
		// it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails with
		// it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails with
		// it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
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

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails with
		// it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

	url := debugURL(s)

	// Unauthenticated.
	client, err := ts.GetUnauthenticatedHTTPClient()
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
	client, err = ts.GetAuthenticatedHTTPClient(false, serverutils.SingleTenantSession)
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
	client, err = ts.GetAuthenticatedHTTPClient(true, serverutils.SingleTenantSession)
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails with
		// it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

	expURL := debugURL(s)
	origURL := expURL + "incorrect"

	// Must be admin to access debug endpoints
	client, err := ts.GetAdminHTTPClient()
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

func generateRandomName() string {
	rand, _ := randutil.NewTestRand()
	cfg := randident.DefaultNameGeneratorConfig()
	// REST api can not handle `/`. This is fixed in
	// the UI by using sql-over-http endpoint instead.
	cfg.Punctuate = -1
	cfg.Finalize()

	ng := randident.NewNameGenerator(
		&cfg,
		rand,
		"a b%s-c.d",
	)
	return ng.GenerateOne(42)
}

func TestAdminAPIStatementDiagnosticsBundle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

	query := "EXPLAIN ANALYZE (DEBUG) SELECT 'secret'"
	_, err := db.Exec(query)
	require.NoError(t, err)

	query = "SELECT id FROM system.statement_diagnostics LIMIT 1"
	idRow, err := db.Query(query)
	require.NoError(t, err)
	var diagnosticRow string
	if idRow.Next() {
		err = idRow.Scan(&diagnosticRow)
		require.NoError(t, err)
	} else {
		t.Fatal("no results")
	}

	client, err := ts.GetAuthenticatedHTTPClient(false, serverutils.SingleTenantSession)
	require.NoError(t, err)
	resp, err := client.Get(ts.AdminURL() + "/_admin/v1/stmtbundle/" + diagnosticRow)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 500, resp.StatusCode)

	adminClient, err := ts.GetAuthenticatedHTTPClient(true, serverutils.SingleTenantSession)
	require.NoError(t, err)
	adminResp, err := adminClient.Get(ts.AdminURL() + "/_admin/v1/stmtbundle/" + diagnosticRow)
	require.NoError(t, err)
	defer adminResp.Body.Close()
	require.Equal(t, 200, adminResp.StatusCode)
}

func TestAdminAPIDatabases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails with
		// it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

	ac := ts.AmbientCtx()
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()

	testDbName := generateRandomName()
	testDbEscaped := tree.NameString(testDbName)
	query := "CREATE DATABASE " + testDbEscaped
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}
	// Test needs to revoke CONNECT on the public database to properly exercise
	// fine-grained permissions logic.
	if _, err := db.Exec(fmt.Sprintf("REVOKE CONNECT ON DATABASE %s FROM public", testDbEscaped)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("REVOKE CONNECT ON DATABASE defaultdb FROM public"); err != nil {
		t.Fatal(err)
	}

	// We have to create the non-admin user before calling
	// "GRANT ... TO authenticatedUserNameNoAdmin".
	// This is done in "GetAuthenticatedHTTPClient".
	if _, err := ts.GetAuthenticatedHTTPClient(false, serverutils.SingleTenantSession); err != nil {
		t.Fatal(err)
	}

	// Grant permissions to view the tables for the given viewing user.
	privileges := []string{"CONNECT"}
	query = fmt.Sprintf(
		"GRANT %s ON DATABASE %s TO %s",
		strings.Join(privileges, ", "),
		testDbEscaped,
		authenticatedUserNameNoAdmin().SQLIdentifier(),
	)
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}
	// Non admins now also require VIEWACTIVITY.
	query = fmt.Sprintf(
		"GRANT SYSTEM %s TO %s",
		"VIEWACTIVITY",
		authenticatedUserNameNoAdmin().SQLIdentifier(),
	)
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		expectedDBs []string
		isAdmin     bool
	}{
		{[]string{"defaultdb", "postgres", "system", testDbName}, true},
		{[]string{"postgres", testDbName}, false},
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

			sort.Strings(tc.expectedDBs)
			sort.Strings(resp.Databases)
			for i, e := range tc.expectedDBs {
				if a := resp.Databases[i]; a != e {
					t.Fatalf("database name %s != expected %s", a, e)
				}
			}

			// Test database details endpoint.
			var details serverpb.DatabaseDetailsResponse
			urlEscapeDbName := url.PathEscape(testDbName)

			if err := getAdminJSONProtoWithAdminOption(
				s,
				"databases/"+urlEscapeDbName,
				&details,
				tc.isAdmin,
			); err != nil {
				t.Fatal(err)
			}

			if a, e := len(details.Grants), 3; a != e {
				t.Fatalf("# of grants %d != expected %d", a, e)
			}

			userGrants := make(map[string][]string)
			for _, grant := range details.Grants {
				switch grant.User {
				case username.AdminRole, username.RootUser, authenticatedUserNoAdmin:
					userGrants[grant.User] = append(userGrants[grant.User], grant.Privileges...)
				default:
					t.Fatalf("unknown grant to user %s", grant.User)
				}
			}
			for u, p := range userGrants {
				switch u {
				case username.AdminRole:
					if !reflect.DeepEqual(p, []string{"ALL"}) {
						t.Fatalf("privileges %v != expected %v", p, privileges)
					}
				case username.RootUser:
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
			databaseID, err := ts.admin.queryDatabaseID(ctx, username.RootUserName(), testDbName)
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails with
		// it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())

	const errPattern = "database.+does not exist"
	if err := getAdminJSONProto(s, "databases/i_do_not_exist", nil); !testutils.IsError(err, errPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPIDatabaseSQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails with
		// it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
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
			RangeCount:   11,
			ReplicaCount: 15,
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
	require.NoError(t, testCluster.WaitForFullReplication())
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
		// Hardcode the single range used by each system sequence, the above
		// request does not return sequences.
		// TODO(richardjcai): Maybe update the request to return
		// sequences as well?
		m[fmt.Sprintf("public.%s", catconstants.DescIDSequenceTableName)] = 1
		m[fmt.Sprintf("public.%s", catconstants.RoleIDSequenceName)] = 1
		m[fmt.Sprintf("public.%s", catconstants.TenantIDSequenceTableName)] = 1
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

	var systemTableRangeCount int64
	sysDBMap := getSystemTableRangeCount()
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
		s := sqlutils.MatrixToStr(runner.QueryStr(t, `SHOW CLUSTER RANGES`))
		t.Logf("actual ranges:\n%s", s)
	}
}

func TestAdminAPITableDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails with
		// it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails with
		// it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
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
		name, dbName, tblName, pkName string
	}{
		{name: "lower", dbName: "test", tblName: "tbl", pkName: "tbl_pkey"},
		{name: "lower other schema", dbName: "test", tblName: `testschema.tbl`, pkName: "tbl_pkey"},
		{name: "lower with space", dbName: "test test", tblName: `"tbl tbl"`, pkName: "tbl tbl_pkey"},
		{name: "upper", dbName: "TEST", tblName: `"TBL"`, pkName: "TBL_pkey"}, // Regression test for issue #14056
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
				// Disable the default test tenant for now as this tests fails
				// with it enabled. Tracked with #81590.
				DefaultTestTenant: base.TestTenantDisabled,
			})
			defer s.Stopper().Stop(context.Background())
			ts := s.(*TestServer)

			escDBName := tree.NameStringP(&tc.dbName)
			tblName := tc.tblName
			schemaName := "testschema"

			ac := ts.AmbientCtx()
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
				fmt.Sprintf("CREATE STATISTICS test_stats FROM %s.%s", escDBName, tblName),
			}
			pgURL, cleanupGoDB := sqlutils.PGUrl(
				t, s.ServingSQLAddr(), "StartServer" /* prefix */, url.User(username.RootUser))
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
				{Name: "nulls_not_allowed", Type: "INT8", Nullable: false, DefaultValue: "1000"},
				{Name: "default2", Type: "INT8", Nullable: true, DefaultValue: "2"},
				{Name: "string_default", Type: "STRING", Nullable: true, DefaultValue: "'default_string'"},
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
				{User: username.AdminRole, Privileges: []string{"ALL"}},
				{User: username.RootUser, Privileges: []string{"ALL"}},
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
				{Name: tc.pkName, Column: "string_default", Direction: "N/A", Unique: true, Seq: 5, Storing: true},
				{Name: tc.pkName, Column: "default2", Direction: "N/A", Unique: true, Seq: 4, Storing: true},
				{Name: tc.pkName, Column: "nulls_not_allowed", Direction: "N/A", Unique: true, Seq: 3, Storing: true},
				{Name: tc.pkName, Column: "nulls_allowed", Direction: "N/A", Unique: true, Seq: 2, Storing: true},
				{Name: tc.pkName, Column: "rowid", Direction: "ASC", Unique: true, Seq: 1},
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

			// Verify statistics last updated.
			{

				showStatisticsForTableQuery := fmt.Sprintf("SELECT max(created) AS created FROM [SHOW STATISTICS FOR TABLE %s.%s]", escDBName, tblName)

				row := db.QueryRow(showStatisticsForTableQuery)
				var createdTs time.Time
				if err := row.Scan(&createdTs); err != nil {
					t.Fatal(err)
				}

				if a, e := resp.StatsLastCreatedAt, createdTs; reflect.DeepEqual(a, e) {
					t.Fatalf("mismatched statistics creation timestamp; expected %s, got %s", e, a)
				}
			}

			// Verify Descriptor ID.
			tableID, err := ts.admin.queryTableID(ctx, username.RootUserName(), tc.dbName, tc.tblName)
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
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

	// Create database and table.
	ac := ts.AmbientCtx()
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

	databaseID, err := ts.admin.queryDatabaseID(ctx, username.RootUserName(), "test")
	if err != nil {
		t.Fatal(err)
	}
	tableID, err := ts.admin.queryTableID(ctx, username.RootUserName(), "test", "tbl")
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
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())

	// Create sample users.
	query := `
INSERT INTO system.users (username, "hashedPassword", user_id)
VALUES ('adminUser', 'abc', 200), ('bob', 'xyz', 201)`
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
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
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
		{"set_cluster_setting", false, 0, false, 2},
		// We use limit=true with no limit here because otherwise the
		// expCount will mess up the expected total count below.
		{"set_cluster_setting", true, 0, true, 2},
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

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())

	// Any bool that defaults to true will work here.
	const settingKey = "sql.metrics.statement_details.enabled"
	st := s.ClusterSettings()
	allKeys := settings.Keys(settings.ForSystemTenant)

	checkSetting := func(t *testing.T, k string, v serverpb.SettingsResponse_Value) {
		ref, ok := settings.LookupForReporting(k, settings.ForSystemTenant)
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
		if v.LastUpdated != nil {
			db := sqlutils.MakeSQLRunner(conn)
			q := makeSQLQuery()
			q.Append(`SELECT name, "lastUpdated" FROM system.settings WHERE name=$`, k)
			rows := db.Query(
				t,
				q.String(),
				q.QueryArguments()...,
			)
			defer rows.Close()
			if rows.Next() == false {
				t.Errorf("missing sql row for %s", k)
			}
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
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
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())

	testutils.RunTrueAndFalse(t, "reportingOn", func(t *testing.T, reportingOn bool) {
		testutils.RunTrueAndFalse(t, "enterpriseOn", func(t *testing.T, enterpriseOn bool) {
			// Override server license check.
			if enterpriseOn {
				old := base.CheckEnterpriseEnabled
				base.CheckEnterpriseEnabled = func(_ *cluster.Settings, _ uuid.UUID, _ string) error {
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
				if a, e := resp.ClusterID, s.RPCContext().StorageClusterID.String(); a != e {
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

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(ctx)
	ts := s.(*TestServer)

	// We need to retry because the node ID isn't set until after
	// bootstrapping.
	testutils.SucceedsSoon(t, func() error {
		var resp serverpb.HealthResponse
		return getAdminJSONProto(s, "health", &resp)
	})

	// Make the SQL listener appear unavailable. Verify that health fails after that.
	ts.sqlServer.isReady.Set(false)
	var resp serverpb.HealthResponse
	err := getAdminJSONProto(s, "health?ready=1", &resp)
	if err == nil {
		t.Error("server appears ready even though SQL listener is not")
	}
	ts.sqlServer.isReady.Set(true)
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

// getSystemJobIDsForNonAutoJobs queries the jobs table for all job IDs that have
// the given status. Sorted by decreasing creation time.
func getSystemJobIDsForNonAutoJobs(
	t testing.TB, db *sqlutils.SQLRunner, status jobs.Status,
) []int64 {
	q := makeSQLQuery()
	q.Append(`SELECT job_id FROM crdb_internal.jobs WHERE status=$`, status)
	q.Append(` AND (`)
	for i, jobType := range jobspb.AutomaticJobTypes {
		q.Append(`job_type != $`, jobType.String())
		if i < len(jobspb.AutomaticJobTypes)-1 {
			q.Append(" AND ")
		}
	}
	q.Append(` OR job_type IS NULL)`)
	q.Append(` ORDER BY created DESC`)
	rows := db.Query(
		t,
		q.String(),
		q.QueryArguments()...,
	)
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

	now := timeutil.Now()
	retentionTime := 336 * time.Hour
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: &jobs.TestingKnobs{
				IntervalOverrides: jobs.TestingIntervalOverrides{
					RetentionTime: &retentionTime,
				},
			},
			Server: &TestingKnobs{
				StubTimeNow: func() time.Time { return now },
			},
		},
	})

	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	testutils.RunTrueAndFalse(t, "isAdmin", func(t *testing.T, isAdmin bool) {
		// Creating this client causes a user to be created, which causes jobs
		// to be created, so we do it up-front rather than inside the test.
		_, err := s.GetAuthenticatedHTTPClient(isAdmin, serverutils.SingleTenantSession)
		if err != nil {
			t.Fatal(err)
		}
	})

	existingSucceededIDs := getSystemJobIDsForNonAutoJobs(t, sqlDB, jobs.StatusSucceeded)
	existingRunningIDs := getSystemJobIDsForNonAutoJobs(t, sqlDB, jobs.StatusRunning)
	existingIDs := append(existingSucceededIDs, existingRunningIDs...)

	runningOnlyIds := []int64{1, 2, 4, 11, 12}
	revertingOnlyIds := []int64{7, 8, 9}
	retryRunningIds := []int64{6}
	retryRevertingIds := []int64{10}
	ef := &jobspb.RetriableExecutionFailure{
		TruncatedError: "foo",
	}
	// Add a regression test for #84139 where a string with a quote in it
	// caused a failure in the admin API.
	efQuote := &jobspb.RetriableExecutionFailure{
		TruncatedError: "foo\"abc\"",
	}

	testJobs := []struct {
		id                int64
		status            jobs.Status
		details           jobspb.Details
		progress          jobspb.ProgressDetails
		username          username.SQLUsername
		numRuns           int64
		lastRun           time.Time
		executionFailures []*jobspb.RetriableExecutionFailure
	}{
		{1, jobs.StatusRunning, jobspb.RestoreDetails{}, jobspb.RestoreProgress{}, username.RootUserName(), 1, time.Time{}, nil},
		{2, jobs.StatusRunning, jobspb.BackupDetails{}, jobspb.BackupProgress{}, username.RootUserName(), 1, timeutil.Now().Add(10 * time.Minute), nil},
		{3, jobs.StatusSucceeded, jobspb.BackupDetails{}, jobspb.BackupProgress{}, username.RootUserName(), 1, time.Time{}, nil},
		{4, jobs.StatusRunning, jobspb.ChangefeedDetails{}, jobspb.ChangefeedProgress{}, username.RootUserName(), 2, time.Time{}, nil},
		{5, jobs.StatusSucceeded, jobspb.BackupDetails{}, jobspb.BackupProgress{}, authenticatedUserNameNoAdmin(), 1, time.Time{}, nil},
		{6, jobs.StatusRunning, jobspb.ImportDetails{}, jobspb.ImportProgress{}, username.RootUserName(), 2, timeutil.Now().Add(10 * time.Minute), nil},
		{7, jobs.StatusReverting, jobspb.ImportDetails{}, jobspb.ImportProgress{}, username.RootUserName(), 1, time.Time{}, nil},
		{8, jobs.StatusReverting, jobspb.ImportDetails{}, jobspb.ImportProgress{}, username.RootUserName(), 1, timeutil.Now().Add(10 * time.Minute), nil},
		{9, jobs.StatusReverting, jobspb.ImportDetails{}, jobspb.ImportProgress{}, username.RootUserName(), 2, time.Time{}, nil},
		{10, jobs.StatusReverting, jobspb.ImportDetails{}, jobspb.ImportProgress{}, username.RootUserName(), 2, timeutil.Now().Add(10 * time.Minute), nil},
		{11, jobs.StatusRunning, jobspb.RestoreDetails{}, jobspb.RestoreProgress{}, username.RootUserName(), 1, time.Time{}, []*jobspb.RetriableExecutionFailure{ef}},
		{12, jobs.StatusRunning, jobspb.RestoreDetails{}, jobspb.RestoreProgress{}, username.RootUserName(), 1, time.Time{}, []*jobspb.RetriableExecutionFailure{efQuote}},
	}
	for _, job := range testJobs {
		payload := jobspb.Payload{
			UsernameProto:                job.username.EncodeProto(),
			Details:                      jobspb.WrapPayloadDetails(job.details),
			RetriableExecutionFailureLog: job.executionFailures,
		}
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
			`INSERT INTO system.jobs (id, status, num_runs, last_run, job_type) VALUES ($1, $2, $3, $4, $5)`,
			job.id, job.status, job.numRuns, job.lastRun, payload.Type().String(),
		)
		sqlDB.Exec(t,
			`INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`,
			job.id, jobs.GetLegacyPayloadKey(), payloadBytes,
		)
		sqlDB.Exec(t,
			`INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`,
			job.id, jobs.GetLegacyProgressKey(), progressBytes,
		)
	}

	const invalidJobType = math.MaxInt32

	testCases := []struct {
		uri                    string
		expectedIDsViaAdmin    []int64
		expectedIDsViaNonAdmin []int64
	}{
		{
			"jobs",
			append([]int64{12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, existingIDs...),
			[]int64{5},
		},
		{
			"jobs?limit=1",
			[]int64{12},
			[]int64{5},
		},
		{
			"jobs?status=succeeded",
			append([]int64{5, 3}, existingSucceededIDs...),
			[]int64{5},
		},
		{
			"jobs?status=running",
			append(append(append([]int64{}, runningOnlyIds...), retryRunningIds...), existingRunningIDs...),
			[]int64{},
		},
		{
			"jobs?status=reverting",
			append(append([]int64{}, revertingOnlyIds...), retryRevertingIds...),
			[]int64{},
		},
		{
			"jobs?status=pending",
			[]int64{},
			[]int64{},
		},
		{
			"jobs?status=garbage",
			[]int64{},
			[]int64{},
		},
		{
			fmt.Sprintf("jobs?type=%d", jobspb.TypeBackup),
			[]int64{5, 3, 2},
			[]int64{5},
		},
		{
			fmt.Sprintf("jobs?type=%d", jobspb.TypeRestore),
			[]int64{1, 11, 12},
			[]int64{},
		},
		{
			fmt.Sprintf("jobs?type=%d", invalidJobType),
			[]int64{},
			[]int64{},
		},
		{
			fmt.Sprintf("jobs?status=running&type=%d", jobspb.TypeBackup),
			[]int64{2},
			[]int64{},
		},
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

			sort.Slice(expected, func(i, j int) bool {
				return expected[i] < expected[j]
			})

			sort.Slice(resIDs, func(i, j int) bool {
				return resIDs[i] < resIDs[j]
			})
			if e, a := expected, resIDs; !reflect.DeepEqual(e, a) {
				t.Errorf("%d - %v: expected job IDs %v, but got %v", i, testCase.uri, e, a)
			}
			// We don't use require.Equal() because timestamps don't necessarily
			// compare == due to only one of them having a monotonic clock reading.
			require.True(t, now.Add(-retentionTime).Equal(res.EarliestRetainedTime))
		}
	})
}

func TestAdminAPIJobsDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	now := timeutil.Now()

	encodedError := func(err error) *errors.EncodedError {
		ee := errors.EncodeError(context.Background(), err)
		return &ee
	}
	testJobs := []struct {
		id           int64
		status       jobs.Status
		details      jobspb.Details
		progress     jobspb.ProgressDetails
		username     username.SQLUsername
		numRuns      int64
		lastRun      time.Time
		executionLog []*jobspb.RetriableExecutionFailure
	}{
		{1, jobs.StatusRunning, jobspb.RestoreDetails{}, jobspb.RestoreProgress{}, username.RootUserName(), 1, time.Time{}, nil},
		{2, jobs.StatusReverting, jobspb.BackupDetails{}, jobspb.BackupProgress{}, username.RootUserName(), 1, time.Time{}, nil},
		{3, jobs.StatusRunning, jobspb.BackupDetails{}, jobspb.BackupProgress{}, username.RootUserName(), 1, now.Add(10 * time.Minute), nil},
		{4, jobs.StatusReverting, jobspb.ChangefeedDetails{}, jobspb.ChangefeedProgress{}, username.RootUserName(), 1, now.Add(10 * time.Minute), nil},
		{5, jobs.StatusRunning, jobspb.BackupDetails{}, jobspb.BackupProgress{}, username.RootUserName(), 2, time.Time{}, nil},
		{6, jobs.StatusReverting, jobspb.ChangefeedDetails{}, jobspb.ChangefeedProgress{}, username.RootUserName(), 2, time.Time{}, nil},
		{7, jobs.StatusRunning, jobspb.BackupDetails{}, jobspb.BackupProgress{}, username.RootUserName(), 2, now.Add(10 * time.Minute), nil},
		{8, jobs.StatusReverting, jobspb.ChangefeedDetails{}, jobspb.ChangefeedProgress{}, username.RootUserName(), 2, now.Add(10 * time.Minute), []*jobspb.RetriableExecutionFailure{
			{
				Status:               string(jobs.StatusRunning),
				ExecutionStartMicros: now.Add(-time.Minute).UnixMicro(),
				ExecutionEndMicros:   now.Add(-30 * time.Second).UnixMicro(),
				InstanceID:           1,
				Error:                encodedError(errors.New("foo")),
			},
			{
				Status:               string(jobs.StatusReverting),
				ExecutionStartMicros: now.Add(-29 * time.Minute).UnixMicro(),
				ExecutionEndMicros:   now.Add(-time.Second).UnixMicro(),
				InstanceID:           1,
				TruncatedError:       "bar",
			},
		}},
	}
	for _, job := range testJobs {
		payload := jobspb.Payload{
			UsernameProto:                job.username.EncodeProto(),
			Details:                      jobspb.WrapPayloadDetails(job.details),
			RetriableExecutionFailureLog: job.executionLog,
		}
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
			`INSERT INTO system.jobs (id, status, num_runs, last_run) VALUES ($1, $2, $3, $4)`,
			job.id, job.status, job.numRuns, job.lastRun,
		)
		sqlDB.Exec(t,
			`INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`,
			job.id, jobs.GetLegacyPayloadKey(), payloadBytes,
		)
		sqlDB.Exec(t,
			`INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`,
			job.id, jobs.GetLegacyProgressKey(), progressBytes,
		)
	}

	var res serverpb.JobsResponse
	if err := getAdminJSONProto(s, "jobs", &res); err != nil {
		t.Fatal(err)
	}

	// Trim down our result set to the jobs we injected.
	resJobs := append([]serverpb.JobResponse(nil), res.Jobs...)
	sort.Slice(resJobs, func(i, j int) bool {
		return resJobs[i].ID < resJobs[j].ID
	})
	resJobs = resJobs[:len(testJobs)]

	for i, job := range resJobs {
		require.Equal(t, testJobs[i].id, job.ID)
		require.Equal(t, len(testJobs[i].executionLog), len(job.ExecutionFailures))
		for j, f := range job.ExecutionFailures {
			tf := testJobs[i].executionLog[j]
			require.Equal(t, tf.Status, f.Status)
			require.Equal(t, tf.ExecutionStartMicros, f.Start.UnixMicro())
			require.Equal(t, tf.ExecutionEndMicros, f.End.UnixMicro())
			var expErr string
			if tf.Error != nil {
				expErr = errors.DecodeError(context.Background(), *tf.Error).Error()
			} else {
				expErr = tf.TruncatedError
			}
			require.Equal(t, expErr, f.Error)
		}
	}
}

func TestAdminAPILocations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
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

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `CREATE DATABASE api_test`)
	sqlDB.Exec(t, `CREATE TABLE api_test.t1 (id int primary key, name string)`)
	sqlDB.Exec(t, `CREATE TABLE api_test.t2 (id int primary key, name string)`)

	testCases := []struct {
		query string
		exp   []string
	}{
		{"SELECT sum(id) FROM api_test.t1", []string{"nodeNames\":[\"1\"]", "Columns: id"}},
		{"SELECT sum(1) FROM api_test.t1 JOIN api_test.t2 on t1.id = t2.id", []string{"nodeNames\":[\"1\"]", "Columns: id"}},
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
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
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
			// Disable the default test tenant for now as this tests fails
			// with it enabled. Tracked with #81590.
			DefaultTestTenant: base.TestTenantDisabled,
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

	// TODO(irfansharif): The data-distribution page and underyling APIs don't
	// know how to deal with coalesced ranges. See #97942.
	sqlDB.Exec(t, `SET CLUSTER SETTING spanconfig.storage_coalesce_adjacent.enabled = false`)

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

	// Make sure secondary tenants don't cause the endpoint to error.
	sqlDB.Exec(t, "CREATE TENANT 'app'")

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
		{0, "mvccGC", realRangeID, allReplicas, leaseholder},
		{0, "split", realRangeID, allReplicas, leaseholder},
		{0, "replicaGC", realRangeID, allReplicas, allReplicas},
		{0, "RaFtLoG", realRangeID, allReplicas, allReplicas},
		{0, "RAFTSNAPSHOT", realRangeID, allReplicas, allReplicas},
		{0, "consistencyChecker", realRangeID, allReplicas, leaseholder},
		{0, "TIMESERIESmaintenance", realRangeID, allReplicas, leaseholder},
		{1, "raftlog", realRangeID, leaseholder, leaseholder},
		{2, "raftlog", realRangeID, leaseholder, 1},
		{3, "raftlog", realRangeID, leaseholder, 1},
		// Compatibility cases.
		// TODO(nvanbenschoten): remove this in v23.1.
		{0, "gc", realRangeID, allReplicas, leaseholder},
		{0, "GC", realRangeID, allReplicas, leaseholder},
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
		{NodeID: -1, Queue: "mvccGC"},
		{Queue: ""},
		{RangeID: -1, Queue: "mvccGC"},
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
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
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

// checkNodeCheckResultReady is a helper function for validating that the
// results of a decommission pre-check on a single node show it is ready.
func checkNodeCheckResultReady(
	t *testing.T,
	nID roachpb.NodeID,
	replicaCount int64,
	checkResult serverpb.DecommissionPreCheckResponse_NodeCheckResult,
) {
	require.Equal(t, serverpb.DecommissionPreCheckResponse_NodeCheckResult{
		NodeID:                nID,
		DecommissionReadiness: serverpb.DecommissionPreCheckResponse_READY,
		LivenessStatus:        livenesspb.NodeLivenessStatus_LIVE,
		ReplicaCount:          replicaCount,
		CheckedRanges:         nil,
	}, checkResult)
}

// checkRangeCheckResult is a helper function for validating a range error
// returned as part of a decommission pre-check.
func checkRangeCheckResult(
	t *testing.T,
	desc roachpb.RangeDescriptor,
	checkResult serverpb.DecommissionPreCheckResponse_RangeCheckResult,
	expectedAction string,
	expectedErrSubstr string,
	expectTraces bool,
) {
	passed := false
	defer func() {
		if !passed {
			t.Logf("failed checking %s", desc)
			if expectTraces {
				var traceBuilder strings.Builder
				for _, event := range checkResult.Events {
					fmt.Fprintf(&traceBuilder, "\n(%s) %s", event.Time, event.Message)
				}
				t.Logf("trace events: %s", traceBuilder.String())
			}
		}
	}()
	require.Equalf(t, desc.RangeID, checkResult.RangeID, "expected r%d, got r%d with error: \"%s\"",
		desc.RangeID, checkResult.RangeID, checkResult.Error)
	require.Equalf(t, expectedAction, checkResult.Action, "r%d expected action %s, got action %s with error: \"%s\"",
		desc.RangeID, expectedAction, checkResult.Action, checkResult.Error)
	require.NotEmptyf(t, checkResult.Error, "r%d expected non-empty error", checkResult.RangeID)
	if len(expectedErrSubstr) > 0 {
		require.Containsf(t, checkResult.Error, expectedErrSubstr, "r%d expected error with \"%s\", got error: \"%s\"",
			desc.RangeID, expectedErrSubstr, checkResult.Error)
	}
	if expectTraces {
		require.NotEmptyf(t, checkResult.Events, "r%d expected traces, got none with error: \"%s\"",
			checkResult.RangeID, checkResult.Error)
	} else {
		require.Emptyf(t, checkResult.Events, "r%d expected no traces with error: \"%s\"",
			checkResult.RangeID, checkResult.Error)
	}
	passed = true
}

// TestDecommissionPreCheckBasicReadiness tests the basic functionality of the
// DecommissionPreCheck endpoint.
func TestDecommissionPreCheckBasicReadiness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t) // can't handle 7-node clusters

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 7, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)

	adminSrv := tc.Server(4)
	conn, err := adminSrv.RPCContext().GRPCDialNode(
		adminSrv.RPCAddr(), adminSrv.NodeID(), rpc.DefaultClass).Connect(ctx)
	require.NoError(t, err)
	adminClient := serverpb.NewAdminClient(conn)

	resp, err := adminClient.DecommissionPreCheck(ctx, &serverpb.DecommissionPreCheckRequest{
		NodeIDs: []roachpb.NodeID{tc.Server(5).NodeID()},
	})
	require.NoError(t, err)
	require.Len(t, resp.CheckedNodes, 1)
	checkNodeCheckResultReady(t, tc.Server(5).NodeID(), 0, resp.CheckedNodes[0])
}

// TestDecommissionPreCheckUnready tests the functionality of the
// DecommissionPreCheck endpoint with some nodes not ready.
func TestDecommissionPreCheckUnready(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t) // can't handle 7-node clusters

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 7, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)

	// Add replicas to a node we will check.
	// Scratch range should have RF=3, liveness range should have RF=5.
	adminSrvIdx := 3
	decommissioningSrvIdx := 5
	scratchKey := tc.ScratchRange(t)
	scratchDesc := tc.AddVotersOrFatal(t, scratchKey, tc.Target(decommissioningSrvIdx))
	livenessDesc := tc.LookupRangeOrFatal(t, keys.NodeLivenessPrefix)
	livenessDesc = tc.AddVotersOrFatal(t, livenessDesc.StartKey.AsRawKey(), tc.Target(decommissioningSrvIdx))

	adminSrv := tc.Server(adminSrvIdx)
	decommissioningSrv := tc.Server(decommissioningSrvIdx)
	conn, err := adminSrv.RPCContext().GRPCDialNode(
		adminSrv.RPCAddr(), adminSrv.NodeID(), rpc.DefaultClass).Connect(ctx)
	require.NoError(t, err)
	adminClient := serverpb.NewAdminClient(conn)

	checkNodeReady := func(nID roachpb.NodeID, replicaCount int64, strict bool) {
		resp, err := adminClient.DecommissionPreCheck(ctx, &serverpb.DecommissionPreCheckRequest{
			NodeIDs:         []roachpb.NodeID{nID},
			StrictReadiness: strict,
		})
		require.NoError(t, err)
		require.Len(t, resp.CheckedNodes, 1)
		checkNodeCheckResultReady(t, nID, replicaCount, resp.CheckedNodes[0])
	}

	awaitDecommissioned := func(nID roachpb.NodeID) {
		testutils.SucceedsSoon(t, func() error {
			livenesses, err := adminSrv.NodeLiveness().(*liveness.NodeLiveness).ScanNodeVitalityFromKV(ctx)
			if err != nil {
				return err
			}
			for nodeID, nodeLiveness := range livenesses {
				if nodeID == nID {
					if nodeLiveness.IsDecommissioned() {
						return nil
					} else {
						return errors.Errorf("n%d has membership: %s", nID, nodeLiveness.Liveness.Membership)
					}
				}
			}
			return errors.Errorf("n%d liveness not found", nID)
		})
	}

	checkAndDecommission := func(srvIdx int, replicaCount int64, strict bool) {
		nID := tc.Server(srvIdx).NodeID()
		checkNodeReady(nID, replicaCount, strict)
		require.NoError(t, adminSrv.Decommission(
			ctx, livenesspb.MembershipStatus_DECOMMISSIONING, []roachpb.NodeID{nID}))
		require.NoError(t, adminSrv.Decommission(
			ctx, livenesspb.MembershipStatus_DECOMMISSIONED, []roachpb.NodeID{nID}))
		awaitDecommissioned(nID)
	}

	// In non-strict mode, this decommission appears "ready". This is because the
	// ranges with replicas on decommissioningSrv have priority action "AddVoter",
	// and they have valid targets.
	checkNodeReady(decommissioningSrv.NodeID(), 2, false)

	// In strict mode, we would expect the readiness check to fail.
	resp, err := adminClient.DecommissionPreCheck(ctx, &serverpb.DecommissionPreCheckRequest{
		NodeIDs:          []roachpb.NodeID{decommissioningSrv.NodeID()},
		NumReplicaReport: 50,
		StrictReadiness:  true,
		CollectTraces:    true,
	})
	require.NoError(t, err)
	nodeCheckResult := resp.CheckedNodes[0]
	require.Equalf(t, serverpb.DecommissionPreCheckResponse_ALLOCATION_ERRORS, nodeCheckResult.DecommissionReadiness,
		"expected n%d to have allocation errors, got %s", nodeCheckResult.NodeID, nodeCheckResult.DecommissionReadiness)
	require.Len(t, nodeCheckResult.CheckedRanges, 2)
	checkRangeCheckResult(t, livenessDesc, nodeCheckResult.CheckedRanges[0],
		"add voter", "needs repair beyond replacing/removing", true,
	)
	checkRangeCheckResult(t, scratchDesc, nodeCheckResult.CheckedRanges[1],
		"add voter", "needs repair beyond replacing/removing", true,
	)

	// Add replicas to ensure we have the correct number of replicas for each range.
	scratchDesc = tc.AddVotersOrFatal(t, scratchKey, tc.Target(adminSrvIdx))
	livenessDesc = tc.AddVotersOrFatal(t, livenessDesc.StartKey.AsRawKey(),
		tc.Target(adminSrvIdx), tc.Target(4), tc.Target(6),
	)
	require.True(t, hasReplicaOnServers(tc, &scratchDesc, 0, adminSrvIdx, decommissioningSrvIdx))
	require.True(t, hasReplicaOnServers(tc, &livenessDesc, 0, adminSrvIdx, decommissioningSrvIdx, 4, 6))
	require.Len(t, scratchDesc.InternalReplicas, 3)
	require.Len(t, livenessDesc.InternalReplicas, 5)

	// Decommissioning pre-check should pass on decommissioningSrv in both strict
	// and non-strict modes, as each range can find valid upreplication targets.
	checkNodeReady(decommissioningSrv.NodeID(), 2, true)

	// Check and decommission empty nodes, decreasing to a 5-node cluster.
	checkAndDecommission(1, 0, true)
	checkAndDecommission(2, 0, true)

	// Check that we can still decommission.
	// Below 5 nodes, system ranges will have an effective RF=3.
	checkNodeReady(decommissioningSrv.NodeID(), 2, true)

	// Check that we can decommission the nodes with liveness replicas only.
	checkAndDecommission(4, 1, true)
	checkAndDecommission(6, 1, true)

	// Check range descriptors are as expected.
	scratchDesc = tc.LookupRangeOrFatal(t, scratchDesc.StartKey.AsRawKey())
	livenessDesc = tc.LookupRangeOrFatal(t, livenessDesc.StartKey.AsRawKey())
	require.True(t, hasReplicaOnServers(tc, &scratchDesc, 0, adminSrvIdx, decommissioningSrvIdx))
	require.True(t, hasReplicaOnServers(tc, &livenessDesc, 0, adminSrvIdx, decommissioningSrvIdx, 4, 6))
	require.Len(t, scratchDesc.InternalReplicas, 3)
	require.Len(t, livenessDesc.InternalReplicas, 5)

	// Cleanup orphaned liveness replicas and check.
	livenessDesc = tc.RemoveVotersOrFatal(t, livenessDesc.StartKey.AsRawKey(), tc.Target(4), tc.Target(6))
	require.True(t, hasReplicaOnServers(tc, &livenessDesc, 0, adminSrvIdx, decommissioningSrvIdx))
	require.Len(t, livenessDesc.InternalReplicas, 3)

	// Validate that the node is not ready to decommission.
	resp, err = adminClient.DecommissionPreCheck(ctx, &serverpb.DecommissionPreCheckRequest{
		NodeIDs:          []roachpb.NodeID{decommissioningSrv.NodeID()},
		NumReplicaReport: 1, // Test that we limit errors.
		StrictReadiness:  true,
	})
	require.NoError(t, err)
	nodeCheckResult = resp.CheckedNodes[0]
	require.Equalf(t, serverpb.DecommissionPreCheckResponse_ALLOCATION_ERRORS, nodeCheckResult.DecommissionReadiness,
		"expected n%d to have allocation errors, got %s", nodeCheckResult.NodeID, nodeCheckResult.DecommissionReadiness)
	require.Equal(t, int64(2), nodeCheckResult.ReplicaCount)
	require.Len(t, nodeCheckResult.CheckedRanges, 1)
	checkRangeCheckResult(t, livenessDesc, nodeCheckResult.CheckedRanges[0],
		"replace decommissioning voter",
		"0 of 2 live stores are able to take a new replica for the range "+
			"(2 already have a voter, 0 already have a non-voter); "+
			"likely not enough nodes in cluster",
		false,
	)
}

// TestDecommissionPreCheckMultiple tests the functionality of the
// DecommissionPreCheck endpoint with multiple nodes.
func TestDecommissionPreCheckMultiple(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 5, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)

	// TODO(sarkesian): Once #95909 is merged, test checks on a 3-node decommission.
	// e.g. Test both server idxs 3,4 and 2,3,4 (which should not pass checks).
	adminSrvIdx := 1
	decommissioningSrvIdxs := []int{3, 4}
	decommissioningSrvNodeIDs := make([]roachpb.NodeID, len(decommissioningSrvIdxs))
	for i, srvIdx := range decommissioningSrvIdxs {
		decommissioningSrvNodeIDs[i] = tc.Server(srvIdx).NodeID()
	}

	// Add replicas to nodes we will check.
	// Scratch range should have RF=3, liveness range should have RF=5.
	rangeDescs := []roachpb.RangeDescriptor{
		tc.LookupRangeOrFatal(t, keys.NodeLivenessPrefix),
		tc.LookupRangeOrFatal(t, tc.ScratchRange(t)),
	}
	rangeDescSrvIdxs := [][]int{
		{0, 1, 2, 3, 4},
		{0, 3, 4},
	}
	rangeDescSrvTargets := make([][]roachpb.ReplicationTarget, len(rangeDescs))
	for i, srvIdxs := range rangeDescSrvIdxs {
		for _, srvIdx := range srvIdxs {
			if srvIdx != 0 {
				rangeDescSrvTargets[i] = append(rangeDescSrvTargets[i], tc.Target(srvIdx))
			}
		}
	}

	for i, rangeDesc := range rangeDescs {
		rangeDescs[i] = tc.AddVotersOrFatal(t, rangeDesc.StartKey.AsRawKey(), rangeDescSrvTargets[i]...)
	}

	for i, rangeDesc := range rangeDescs {
		require.True(t, hasReplicaOnServers(tc, &rangeDesc, rangeDescSrvIdxs[i]...))
		require.Len(t, rangeDesc.InternalReplicas, len(rangeDescSrvIdxs[i]))
	}

	adminSrv := tc.Server(adminSrvIdx)
	conn, err := adminSrv.RPCContext().GRPCDialNode(
		adminSrv.RPCAddr(), adminSrv.NodeID(), rpc.DefaultClass).Connect(ctx)
	require.NoError(t, err)
	adminClient := serverpb.NewAdminClient(conn)

	// We expect to be able to decommission the targeted nodes simultaneously.
	resp, err := adminClient.DecommissionPreCheck(ctx, &serverpb.DecommissionPreCheckRequest{
		NodeIDs:          decommissioningSrvNodeIDs,
		NumReplicaReport: 50,
		StrictReadiness:  true,
		CollectTraces:    true,
	})
	require.NoError(t, err)
	require.Len(t, resp.CheckedNodes, len(decommissioningSrvIdxs))
	for i, nID := range decommissioningSrvNodeIDs {
		checkNodeCheckResultReady(t, nID, int64(len(rangeDescs)), resp.CheckedNodes[i])
	}
}

// TestDecommissionPreCheckInvalidNode tests the functionality of the
// DecommissionPreCheck endpoint where some nodes are invalid.
func TestDecommissionPreCheckInvalidNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 5, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)

	adminSrvIdx := 1
	validDecommissioningNodeID := roachpb.NodeID(5)
	invalidDecommissioningNodeID := roachpb.NodeID(34)
	decommissioningNodeIDs := []roachpb.NodeID{validDecommissioningNodeID, invalidDecommissioningNodeID}

	// Add replicas to nodes we will check.
	// Scratch range should have RF=3, liveness range should have RF=5.
	rangeDescs := []roachpb.RangeDescriptor{
		tc.LookupRangeOrFatal(t, keys.NodeLivenessPrefix),
		tc.LookupRangeOrFatal(t, tc.ScratchRange(t)),
	}
	rangeDescSrvIdxs := [][]int{
		{0, 1, 2, 3, 4},
		{0, 3, 4},
	}
	rangeDescSrvTargets := make([][]roachpb.ReplicationTarget, len(rangeDescs))
	for i, srvIdxs := range rangeDescSrvIdxs {
		for _, srvIdx := range srvIdxs {
			if srvIdx != 0 {
				rangeDescSrvTargets[i] = append(rangeDescSrvTargets[i], tc.Target(srvIdx))
			}
		}
	}

	for i, rangeDesc := range rangeDescs {
		rangeDescs[i] = tc.AddVotersOrFatal(t, rangeDesc.StartKey.AsRawKey(), rangeDescSrvTargets[i]...)
	}

	for i, rangeDesc := range rangeDescs {
		require.True(t, hasReplicaOnServers(tc, &rangeDesc, rangeDescSrvIdxs[i]...))
		require.Len(t, rangeDesc.InternalReplicas, len(rangeDescSrvIdxs[i]))
	}

	adminSrv := tc.Server(adminSrvIdx)
	conn, err := adminSrv.RPCContext().GRPCDialNode(
		adminSrv.RPCAddr(), adminSrv.NodeID(), rpc.DefaultClass).Connect(ctx)
	require.NoError(t, err)
	adminClient := serverpb.NewAdminClient(conn)

	// We expect the pre-check to fail as some node IDs are invalid.
	resp, err := adminClient.DecommissionPreCheck(ctx, &serverpb.DecommissionPreCheckRequest{
		NodeIDs:          decommissioningNodeIDs,
		NumReplicaReport: 50,
		StrictReadiness:  true,
		CollectTraces:    true,
	})
	require.NoError(t, err)
	require.Len(t, resp.CheckedNodes, len(decommissioningNodeIDs))
	checkNodeCheckResultReady(t, validDecommissioningNodeID, int64(len(rangeDescs)), resp.CheckedNodes[0])
	require.Equal(t, serverpb.DecommissionPreCheckResponse_NodeCheckResult{
		NodeID:                invalidDecommissioningNodeID,
		DecommissionReadiness: serverpb.DecommissionPreCheckResponse_UNKNOWN,
		LivenessStatus:        livenesspb.NodeLivenessStatus_UNKNOWN,
		ReplicaCount:          0,
		CheckedRanges:         nil,
	}, resp.CheckedNodes[1])
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

// TestDecommissionEnqueueReplicas tests that a decommissioning node's replicas
// are proactively enqueued into their replicateQueues by the other nodes in the
// system.
func TestDecommissionEnqueueReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t) // can't handle 7-node clusters

	ctx := context.Background()
	enqueuedRangeIDs := make(chan roachpb.RangeID)
	tc := serverutils.StartNewTestCluster(t, 7, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Insecure: true, // allows admin client without setting up certs
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					EnqueueReplicaInterceptor: func(
						queueName string, repl *kvserver.Replica,
					) {
						require.Equal(t, queueName, "replicate")
						enqueuedRangeIDs <- repl.RangeID
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	decommissionAndCheck := func(decommissioningSrvIdx int) {
		t.Logf("decommissioning n%d", tc.Target(decommissioningSrvIdx).NodeID)
		// Add a scratch range's replica to a node we will decommission.
		scratchKey := tc.ScratchRange(t)
		decommissioningSrv := tc.Server(decommissioningSrvIdx)
		tc.AddVotersOrFatal(t, scratchKey, tc.Target(decommissioningSrvIdx))

		conn, err := decommissioningSrv.RPCContext().GRPCDialNode(
			decommissioningSrv.RPCAddr(), decommissioningSrv.NodeID(), rpc.DefaultClass,
		).Connect(ctx)
		require.NoError(t, err)
		adminClient := serverpb.NewAdminClient(conn)
		decomNodeIDs := []roachpb.NodeID{tc.Server(decommissioningSrvIdx).NodeID()}
		_, err = adminClient.Decommission(
			ctx,
			&serverpb.DecommissionRequest{
				NodeIDs:          decomNodeIDs,
				TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
			},
		)
		require.NoError(t, err)

		// Ensure that the scratch range's replica was proactively enqueued.
		require.Equal(t, <-enqueuedRangeIDs, tc.LookupRangeOrFatal(t, scratchKey).RangeID)

		// Check that the node was marked as decommissioning in each of the nodes'
		// decommissioningNodeMap. This needs to be wrapped in a SucceedsSoon to
		// deal with gossip propagation delays.
		testutils.SucceedsSoon(t, func() error {
			for i := 0; i < tc.NumServers(); i++ {
				srv := tc.Server(i)
				if _, exists := srv.DecommissioningNodeMap()[decommissioningSrv.NodeID()]; !exists {
					return errors.Newf("node %d not detected to be decommissioning", decommissioningSrv.NodeID())
				}
			}
			return nil
		})
	}

	decommissionAndCheck(2 /* decommissioningSrvIdx */)
	decommissionAndCheck(3 /* decommissioningSrvIdx */)
	decommissionAndCheck(5 /* decommissioningSrvIdx */)
}

func TestAdminDecommissionedOperations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test uses timeouts, and race builds cause the timeouts to be exceeded")

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
		ServerArgs: base.TestServerArgs{
			// Disable the default test tenant for now as this tests fails
			// with it enabled. Tracked with #81590.
			DefaultTestTenant: base.TestTenantDisabled,
			Insecure:          true, // allows admin client without setting up certs
		},
	})
	defer tc.Stopper().Stop(ctx)

	serverutils.SetClusterSetting(t, tc, "server.shutdown.jobs_wait", 0)

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

	testutils.SucceedsWithin(t, func() error {
		timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_, err := decomSrv.DB().Scan(timeoutCtx, keys.LocalMax, keys.MaxKey, 0)
		if err == nil {
			return errors.New("expected error")
		}
		s, ok := status.FromError(errors.UnwrapAll(err))
		if ok && s.Code() == codes.PermissionDenied {
			return nil
		}
		return err
	}, 10*time.Second)

	// Set up an admin client.
	//lint:ignore SA1019 grpc.WithInsecure is deprecated
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
		op         func(context.Context, serverpb.AdminClient) error
	}{
		{"Cluster", codes.OK, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Cluster(ctx, &serverpb.ClusterRequest{})
			return err
		}},
		{"Databases", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Databases(ctx, &serverpb.DatabasesRequest{})
			return err
		}},
		{"DatabaseDetails", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.DatabaseDetails(ctx, &serverpb.DatabaseDetailsRequest{Database: "foo"})
			return err
		}},
		{"DataDistribution", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.DataDistribution(ctx, &serverpb.DataDistributionRequest{})
			return err
		}},
		{"Decommission", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Decommission(ctx, &serverpb.DecommissionRequest{
				NodeIDs:          []roachpb.NodeID{srv.NodeID(), decomSrv.NodeID()},
				TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONED,
			})
			return err
		}},
		{"DecommissionStatus", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.DecommissionStatus(ctx, &serverpb.DecommissionStatusRequest{
				NodeIDs: []roachpb.NodeID{srv.NodeID(), decomSrv.NodeID()},
			})
			return err
		}},
		{"EnqueueRange", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.EnqueueRange(ctx, &serverpb.EnqueueRangeRequest{
				RangeID: scratchRange.RangeID,
				Queue:   "replicaGC",
			})
			return err
		}},
		{"Events", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Events(ctx, &serverpb.EventsRequest{})
			return err
		}},
		{"Health", codes.OK, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Health(ctx, &serverpb.HealthRequest{})
			return err
		}},
		{"Jobs", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Jobs(ctx, &serverpb.JobsRequest{})
			return err
		}},
		{"Liveness", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Liveness(ctx, &serverpb.LivenessRequest{})
			return err
		}},
		{"Locations", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Locations(ctx, &serverpb.LocationsRequest{})
			return err
		}},
		{"NonTableStats", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.NonTableStats(ctx, &serverpb.NonTableStatsRequest{})
			return err
		}},
		{"QueryPlan", codes.OK, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.QueryPlan(ctx, &serverpb.QueryPlanRequest{Query: "SELECT 1"})
			return err
		}},
		{"RangeLog", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.RangeLog(ctx, &serverpb.RangeLogRequest{})
			return err
		}},
		{"Settings", codes.OK, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Settings(ctx, &serverpb.SettingsRequest{})
			return err
		}},
		{"TableStats", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.TableStats(ctx, &serverpb.TableStatsRequest{Database: "foo", Table: "bar"})
			return err
		}},
		{"TableDetails", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.TableDetails(ctx, &serverpb.TableDetailsRequest{Database: "foo", Table: "bar"})
			return err
		}},
		{"Users", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Users(ctx, &serverpb.UsersRequest{})
			return err
		}},
		// We drain at the end, since it may evict us.
		{"Drain", codes.Unknown, func(ctx context.Context, c serverpb.AdminClient) error {
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
			testutils.SucceedsWithin(t, func() error {
				timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
				err := tc.op(timeoutCtx, adminClient)
				if tc.expectCode == codes.OK {
					require.NoError(t, err)
					return nil
				}
				if err == nil {
					// This will cause SuccessWithin to retry.
					return errors.New("expected error, got no error")
				}
				s, ok := status.FromError(errors.UnwrapAll(err))
				if !ok {
					// Not a gRPC error.
					// This will cause SuccessWithin to retry.
					return err
				}
				require.Equal(t, tc.expectCode, s.Code(), "%+v", err)
				return nil
			}, 10*time.Second)
		})
	}
}

func TestAdminPrivilegeChecker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "CREATE USER withadmin")
	sqlDB.Exec(t, "GRANT admin TO withadmin")
	sqlDB.Exec(t, "CREATE USER withva")
	sqlDB.Exec(t, "ALTER ROLE withva WITH VIEWACTIVITY")
	sqlDB.Exec(t, "CREATE USER withvaredacted")
	sqlDB.Exec(t, "ALTER ROLE withvaredacted WITH VIEWACTIVITYREDACTED")
	sqlDB.Exec(t, "CREATE USER withvaandredacted")
	sqlDB.Exec(t, "ALTER ROLE withvaandredacted WITH VIEWACTIVITY")
	sqlDB.Exec(t, "ALTER ROLE withvaandredacted WITH VIEWACTIVITYREDACTED")
	sqlDB.Exec(t, "CREATE USER withoutprivs")
	sqlDB.Exec(t, "CREATE USER withvaglobalprivilege")
	sqlDB.Exec(t, "GRANT SYSTEM VIEWACTIVITY TO withvaglobalprivilege")
	sqlDB.Exec(t, "CREATE USER withvaredactedglobalprivilege")
	sqlDB.Exec(t, "GRANT SYSTEM VIEWACTIVITYREDACTED TO withvaredactedglobalprivilege")
	sqlDB.Exec(t, "CREATE USER withvaandredactedglobalprivilege")
	sqlDB.Exec(t, "GRANT SYSTEM VIEWACTIVITY TO withvaandredactedglobalprivilege")
	sqlDB.Exec(t, "GRANT SYSTEM VIEWACTIVITYREDACTED TO withvaandredactedglobalprivilege")
	sqlDB.Exec(t, "CREATE USER withviewclustermetadata")
	sqlDB.Exec(t, "GRANT SYSTEM VIEWCLUSTERMETADATA TO withviewclustermetadata")
	sqlDB.Exec(t, "CREATE USER withviewdebug")
	sqlDB.Exec(t, "GRANT SYSTEM VIEWDEBUG TO withviewdebug")

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	plannerFn := func(opName string) (interface{}, func()) {
		// This is a hack to get around a Go package dependency cycle. See comment
		// in sql/jobs/registry.go on planHookMaker.
		txn := kvDB.NewTxn(ctx, "test")
		return sql.NewInternalPlanner(
			opName,
			txn,
			username.RootUserName(),
			&sql.MemoryMetrics{},
			&execCfg,
			sql.NewInternalSessionData(ctx, execCfg.Settings, opName),
		)
	}

	underTest := &adminPrivilegeChecker{
		ie:          s.InternalExecutor().(*sql.InternalExecutor),
		st:          s.ClusterSettings(),
		makePlanner: plannerFn,
	}

	withAdmin, err := username.MakeSQLUsernameFromPreNormalizedStringChecked("withadmin")
	require.NoError(t, err)
	withVa, err := username.MakeSQLUsernameFromPreNormalizedStringChecked("withva")
	require.NoError(t, err)
	withVaRedacted, err := username.MakeSQLUsernameFromPreNormalizedStringChecked("withvaredacted")
	require.NoError(t, err)
	withVaAndRedacted, err := username.MakeSQLUsernameFromPreNormalizedStringChecked("withvaandredacted")
	require.NoError(t, err)
	withoutPrivs, err := username.MakeSQLUsernameFromPreNormalizedStringChecked("withoutprivs")
	require.NoError(t, err)
	withVaGlobalPrivilege := username.MakeSQLUsernameFromPreNormalizedString("withvaglobalprivilege")
	withVaRedactedGlobalPrivilege := username.MakeSQLUsernameFromPreNormalizedString("withvaredactedglobalprivilege")
	withVaAndRedactedGlobalPrivilege := username.MakeSQLUsernameFromPreNormalizedString("withvaandredactedglobalprivilege")
	withviewclustermetadata := username.MakeSQLUsernameFromPreNormalizedString("withviewclustermetadata")
	withViewDebug := username.MakeSQLUsernameFromPreNormalizedString("withviewdebug")

	tests := []struct {
		name            string
		checkerFun      func(context.Context) error
		usernameWantErr map[username.SQLUsername]bool
	}{
		{
			"requireViewActivityPermission",
			underTest.requireViewActivityPermission,
			map[username.SQLUsername]bool{
				withAdmin: false, withVa: false, withVaRedacted: true, withVaAndRedacted: false, withoutPrivs: true,
				withVaGlobalPrivilege: false, withVaRedactedGlobalPrivilege: true, withVaAndRedactedGlobalPrivilege: false,
			},
		},
		{
			"requireViewActivityOrViewActivityRedactedPermission",
			underTest.requireViewActivityOrViewActivityRedactedPermission,
			map[username.SQLUsername]bool{
				withAdmin: false, withVa: false, withVaRedacted: false, withVaAndRedacted: false, withoutPrivs: true,
				withVaGlobalPrivilege: false, withVaRedactedGlobalPrivilege: false, withVaAndRedactedGlobalPrivilege: false,
			},
		},
		{
			"requireViewActivityAndNoViewActivityRedactedPermission",
			underTest.requireViewActivityAndNoViewActivityRedactedPermission,
			map[username.SQLUsername]bool{
				withAdmin: false, withVa: false, withVaRedacted: true, withVaAndRedacted: true, withoutPrivs: true,
				withVaGlobalPrivilege: false, withVaRedactedGlobalPrivilege: true, withVaAndRedactedGlobalPrivilege: true,
			},
		},
		{
			"requireViewClusterMetadataPermission",
			underTest.requireViewClusterMetadataPermission,
			map[username.SQLUsername]bool{
				withAdmin: false, withoutPrivs: true, withviewclustermetadata: false,
			},
		},
		{
			"requireViewDebugPermission",
			underTest.requireViewDebugPermission,
			map[username.SQLUsername]bool{
				withAdmin: false, withoutPrivs: true, withViewDebug: false,
			},
		},
	}

	for _, tt := range tests {
		for userName, wantErr := range tt.usernameWantErr {
			t.Run(fmt.Sprintf("%s-%s", tt.name, userName), func(t *testing.T) {
				ctx := metadata.NewIncomingContext(ctx, metadata.New(map[string]string{"websessionuser": userName.SQLIdentifier()}))
				err := tt.checkerFun(ctx)
				if wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
			})
		}
	}
}

func TestServerError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	pgError := pgerror.New(pgcode.OutOfMemory, "TestServerError.OutOfMemory")
	err := serverError(ctx, pgError)
	require.Equal(t, "rpc error: code = Internal desc = An internal server error has occurred. Please check your CockroachDB logs for more details. Error Code: 53200", err.Error())

	err = serverError(ctx, err)
	require.Equal(t, "rpc error: code = Internal desc = An internal server error has occurred. Please check your CockroachDB logs for more details. Error Code: 53200", err.Error())

	err = fmt.Errorf("random error that is not pgerror or grpcstatus")
	err = serverError(ctx, err)
	require.Equal(t, "rpc error: code = Internal desc = An internal server error has occurred. Please check your CockroachDB logs for more details.", err.Error())
}

func TestDatabaseAndTableIndexRecommendations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stubTime := stubUnusedIndexTime{}
	stubDropUnusedDuration := time.Hour

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TestTenantDisabled,
		Knobs: base.TestingKnobs{
			UnusedIndexRecommendKnobs: &idxusage.UnusedIndexRecommendationTestingKnobs{
				GetCreatedAt:   stubTime.getCreatedAt,
				GetLastRead:    stubTime.getLastRead,
				GetCurrentTime: stubTime.getCurrent,
			},
		},
	})
	idxusage.DropUnusedIndexDuration.Override(context.Background(), &s.ClusterSettings().SV, stubDropUnusedDuration)
	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, "CREATE DATABASE test")
	db.Exec(t, "USE test")
	// Create a table and secondary index.
	db.Exec(t, "CREATE TABLE test.test_table (num INT PRIMARY KEY, letter char)")
	db.Exec(t, "CREATE INDEX test_idx ON test.test_table (letter)")

	// Test when last read does not exist and there is no creation time. Expect
	// an index recommendation (index never used).
	stubTime.setLastRead(time.Time{})
	stubTime.setCreatedAt(nil)

	// Test database details endpoint.
	var dbDetails serverpb.DatabaseDetailsResponse
	if err := getAdminJSONProto(
		s,
		"databases/test?include_stats=true",
		&dbDetails,
	); err != nil {
		t.Fatal(err)
	}
	// Expect 1 index recommendation (no index recommendation on primary index).
	require.Equal(t, int32(1), dbDetails.Stats.NumIndexRecommendations)

	// Test table details endpoint.
	var tableDetails serverpb.TableDetailsResponse
	if err := getAdminJSONProto(s, "databases/test/tables/test_table", &tableDetails); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, true, tableDetails.HasIndexRecommendations)

	// Test when last read does not exist and there is a creation time, and the
	// unused index duration has been exceeded. Expect an index recommendation.
	currentTime := timeutil.Now()
	createdTime := currentTime.Add(-stubDropUnusedDuration)
	stubTime.setCurrent(currentTime)
	stubTime.setLastRead(time.Time{})
	stubTime.setCreatedAt(&createdTime)

	// Test database details endpoint.
	dbDetails = serverpb.DatabaseDetailsResponse{}
	if err := getAdminJSONProto(
		s,
		"databases/test?include_stats=true",
		&dbDetails,
	); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, int32(1), dbDetails.Stats.NumIndexRecommendations)

	// Test table details endpoint.
	tableDetails = serverpb.TableDetailsResponse{}
	if err := getAdminJSONProto(s, "databases/test/tables/test_table", &tableDetails); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, true, tableDetails.HasIndexRecommendations)

	// Test when last read does not exist and there is a creation time, and the
	// unused index duration has not been exceeded. Expect no index
	// recommendation.
	currentTime = timeutil.Now()
	stubTime.setCurrent(currentTime)
	stubTime.setLastRead(time.Time{})
	stubTime.setCreatedAt(&currentTime)

	// Test database details endpoint.
	dbDetails = serverpb.DatabaseDetailsResponse{}
	if err := getAdminJSONProto(
		s,
		"databases/test?include_stats=true",
		&dbDetails,
	); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, int32(0), dbDetails.Stats.NumIndexRecommendations)

	// Test table details endpoint.
	tableDetails = serverpb.TableDetailsResponse{}
	if err := getAdminJSONProto(s, "databases/test/tables/test_table", &tableDetails); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, false, tableDetails.HasIndexRecommendations)

	// Test when last read exists and the unused index duration has been
	// exceeded. Expect an index recommendation.
	currentTime = timeutil.Now()
	lastRead := currentTime.Add(-stubDropUnusedDuration)
	stubTime.setCurrent(currentTime)
	stubTime.setLastRead(lastRead)
	stubTime.setCreatedAt(nil)

	// Test database details endpoint.
	dbDetails = serverpb.DatabaseDetailsResponse{}
	if err := getAdminJSONProto(
		s,
		"databases/test?include_stats=true",
		&dbDetails,
	); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, int32(1), dbDetails.Stats.NumIndexRecommendations)

	// Test table details endpoint.
	tableDetails = serverpb.TableDetailsResponse{}
	if err := getAdminJSONProto(s, "databases/test/tables/test_table", &tableDetails); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, true, tableDetails.HasIndexRecommendations)

	// Test when last read exists and the unused index duration has not been
	// exceeded. Expect no index recommendation.
	currentTime = timeutil.Now()
	stubTime.setCurrent(currentTime)
	stubTime.setLastRead(currentTime)
	stubTime.setCreatedAt(nil)

	// Test database details endpoint.
	dbDetails = serverpb.DatabaseDetailsResponse{}
	if err := getAdminJSONProto(
		s,
		"databases/test?include_stats=true",
		&dbDetails,
	); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, int32(0), dbDetails.Stats.NumIndexRecommendations)

	// Test table details endpoint.
	tableDetails = serverpb.TableDetailsResponse{}
	if err := getAdminJSONProto(s, "databases/test/tables/test_table", &tableDetails); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, false, tableDetails.HasIndexRecommendations)
}
