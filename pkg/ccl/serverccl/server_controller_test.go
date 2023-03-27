// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestServerControllerHTTP(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "test triggers many goroutines, which results in conn timeouts and test failures under deadlock")

	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DisableDefaultTestTenant: true,
	})
	defer s.Stopper().Stop(ctx)

	t.Logf("waking up HTTP server")

	// Retrieve a privileged HTTP client. NB: this also populates
	// system.web_sessions.
	aurl := s.AdminURL()
	client, err := s.GetAdminHTTPClient()
	require.NoError(t, err)

	t.Logf("retrieving web session details")

	// Now retrieve the entry in the system tenant's web sessions.
	row := db.QueryRow(`SELECT id,"hashedSecret",username,"createdAt","expiresAt" FROM system.web_sessions`)
	var id int64
	var secret string
	var username string
	var created, expires time.Time
	require.NoError(t, row.Scan(&id, &secret, &username, &created, &expires))

	t.Logf("waking up a test tenant")

	// Create our own test tenant with a known name.
	_, _, err = s.(*server.TestServer).StartSharedProcessTenant(ctx,
		base.TestSharedProcessTenantArgs{
			TenantName: "hello",
		})
	require.NoError(t, err)

	t.Logf("connecting to the test tenant")

	// Get a SQL connection to the test tenant.
	sqlAddr := s.ServingSQLAddr()
	db2, err := serverutils.OpenDBConnE(sqlAddr, "cluster:hello/defaultdb", false, s.Stopper())
	// Expect no error yet: the connection is opened lazily; an
	// error here means the parameters were incorrect.
	require.NoError(t, err)

	// This actually uses the connection.
	require.NoError(t, db2.Ping())

	t.Logf("creating a test user and session")

	// Instantiate the HTTP test username and privileges into the test tenant.
	_, err = db2.Exec(fmt.Sprintf(`CREATE USER %s`, lexbase.EscapeSQLIdent(username)))
	require.NoError(t, err)
	_, err = db2.Exec(fmt.Sprintf(`GRANT admin TO %s`, lexbase.EscapeSQLIdent(username)))
	require.NoError(t, err)

	// Copy the session entry to the test tenant.
	_, err = db2.Exec(`INSERT INTO system.web_sessions(id, "hashedSecret", username, "createdAt", "expiresAt", user_id)
VALUES($1, $2, $3, $4, $5, (SELECT user_id FROM system.users WHERE username = $3))`,
		id, secret, username, created, expires)
	require.NoError(t, err)

	t.Logf("configuring the test connections")

	// From this point, we are expecting the ability to access both tenants using
	// the same cookie jar.
	// Let's assert this is true by retrieving session lists, asserting
	// they are different and that each of them contains the appropriate entries.

	// Make our session to the system tenant recognizable in session lists.
	_, err = db.Exec("SET application_name = 'hello system'")
	require.NoError(t, err)

	// Ditto for the test tenant.
	_, err = db2.Exec("SET application_name = 'hello hello'")
	require.NoError(t, err)

	get := func(req *http.Request) (*serverpb.ListSessionsResponse, error) {
		req.Header.Set("Content-Type", httputil.ProtoContentType)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.Newf("request failed: %v", resp.StatusCode)
		}
		if resp.StatusCode != http.StatusOK {
			return nil, errors.Newf("request failed: %v / %q", resp.StatusCode, string(body))
		}
		var ls serverpb.ListSessionsResponse
		if err := protoutil.Unmarshal(body, &ls); err != nil {
			return nil, err
		}
		return &ls, err
	}

	newreq := func() *http.Request {
		req, err := http.NewRequest("GET", aurl+"/_status/sessions", nil)
		require.NoError(t, err)
		return req
	}

	t.Logf("retrieving session list from system tenant")

	// Retrieve the session list for the system tenant.
	req := newreq()
	req.Header.Set(server.TenantSelectHeader, catconstants.SystemTenantName)
	body, err := get(req)
	require.NoError(t, err)
	t.Logf("response 1:\n%#v", body)
	require.Equal(t, len(body.Sessions), 1)
	require.Equal(t, body.Sessions[0].ApplicationName, "hello system")

	t.Logf("retrieving session list from test tenant")

	// Ditto for the test tenant.
	req = newreq()
	req.Header.Set(server.TenantSelectHeader, "hello")
	body, err = get(req)
	require.NoError(t, err)
	t.Logf("response 2:\n%#v", body)
	require.Equal(t, len(body.Sessions), 1)
	require.Equal(t, body.Sessions[0].ApplicationName, "hello hello")

	t.Logf("retrieving session list from system tenant via cookie")

	c := &http.Cookie{
		Name:     server.TenantSelectCookieName,
		Value:    catconstants.SystemTenantName,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
	}
	purl, err := url.Parse(aurl)
	require.NoError(t, err)
	client.Jar.SetCookies(purl, []*http.Cookie{c})

	req = newreq()
	body, err = get(req)
	require.NoError(t, err)
	t.Logf("response 3:\n%#v", body)
	require.Equal(t, len(body.Sessions), 1)
	require.Equal(t, body.Sessions[0].ApplicationName, "hello system")

	t.Logf("retrieving session list from test tenant via cookie")

	c.Value = "hello"
	client.Jar.SetCookies(purl, []*http.Cookie{c})
	req = newreq()
	body, err = get(req)
	require.NoError(t, err)
	t.Logf("response 4:\n%#v", body)
	require.Equal(t, len(body.Sessions), 1)
	require.Equal(t, body.Sessions[0].ApplicationName, "hello hello")

	t.Logf("retrieving session list from test tenant via cookie and header")

	// Finally, do it again with both cookie and header. Verify
	// that the header wins.
	req = newreq()
	req.Header.Set(server.TenantSelectHeader, catconstants.SystemTenantName)
	body, err = get(req)
	require.NoError(t, err)
	t.Logf("response 5:\n%#v", body)
	require.Equal(t, len(body.Sessions), 1)
	require.Equal(t, body.Sessions[0].ApplicationName, "hello system")

	t.Logf("end of test")
}

// TestServerControllerBadHTTPCookies tests the controller's proxy
// layer for correct behavior under scenarios where the client has
// stale or invalid cookies. This helps ensure that we continue to
// serve static assets even when the browser is referencing an
// unknown tenant, or bad sessions.
func TestServerControllerBadHTTPCookies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	client, err := s.GetUnauthenticatedHTTPClient()
	require.NoError(t, err)

	c := &http.Cookie{
		Name:     server.TenantSelectCookieName,
		Value:    "some-nonexistent-tenant",
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
	}

	req, err := http.NewRequest("GET", s.AdminURL()+"/", nil)
	require.NoError(t, err)
	req.AddCookie(c)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	req, err = http.NewRequest("GET", s.AdminURL()+"/bundle.js", nil)
	require.NoError(t, err)
	req.AddCookie(c)
	resp, err = client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
}

func TestServerControllerMultiNodeTenantStartup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	numNodes := 3
	tc := serverutils.StartNewTestCluster(t, numNodes, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DisableDefaultTestTenant: true,
		}})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	_, err := db.Exec("CREATE TENANT hello; ALTER TENANT hello START SERVICE SHARED")
	require.NoError(t, err)

	// Pick a random node, try to run some SQL inside that tenant.
	rng, _ := randutil.NewTestRand()
	sqlAddr := tc.Server(int(rng.Int31n(int32(numNodes)))).ServingSQLAddr()
	testutils.SucceedsSoon(t, func() error {
		tenantDB, err := serverutils.OpenDBConnE(sqlAddr, "cluster:hello", false, tc.Stopper())
		if err != nil {
			return err
		}
		defer tenantDB.Close()
		if _, err := tenantDB.Exec("CREATE ROLE foo"); err != nil {
			return err
		}
		if _, err := tenantDB.Exec("GRANT ADMIN TO foo"); err != nil {
			return err
		}
		return nil
	})
}

func TestServerStartStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DisableDefaultTestTenant: true,
	})
	defer s.Stopper().Stop(ctx)

	sqlAddr := s.ServingSQLAddr()

	// Create our own test tenant with a known name.
	_, err := db.Exec("CREATE TENANT hello")
	require.NoError(t, err)

	// Make the service alive.
	_, err = db.Exec("ALTER TENANT hello START SERVICE SHARED")
	require.NoError(t, err)

	// Check the liveness.
	testutils.SucceedsSoon(t, func() error {
		db2, err := serverutils.OpenDBConnE(sqlAddr, "cluster:hello/defaultdb", false, s.Stopper())
		// Expect no error yet: the connection is opened lazily; an
		// error here means the parameters were incorrect.
		require.NoError(t, err)

		defer db2.Close()
		if err := db2.Ping(); err != nil {
			return err
		}
		return nil
	})

	// Stop the service.     .
	_, err = db.Exec("ALTER TENANT hello STOP SERVICE")
	require.NoError(t, err)

	// Verify that the service is indeed stopped.
	testutils.SucceedsSoon(t, func() error {
		db2, err := serverutils.OpenDBConnE(sqlAddr, "cluster:hello/defaultdb", false, s.Stopper())
		// Expect no error yet: the connection is opened lazily; an
		// error here means the parameters were incorrect.
		require.NoError(t, err)
		defer db2.Close()
		if err := db2.Ping(); err != nil {
			// Connection error: success.
			return nil //nolint:returnerrcheck
		}
		return errors.New("server still alive")
	})
}

func TestServerControllerLoginLogout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	client, err := s.GetAuthenticatedHTTPClient(false, serverutils.SingleTenantSession)
	require.NoError(t, err)

	resp, err := client.Post(s.AdminURL()+"/logout", "", nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	cookieNames := make([]string, len(resp.Cookies()))
	cookieValues := make([]string, len(resp.Cookies()))
	for i, c := range resp.Cookies() {
		cookieNames[i] = c.Name
		cookieValues[i] = c.Value
	}
	require.ElementsMatch(t, []string{"session", "tenant"}, cookieNames)
	require.ElementsMatch(t, []string{"", ""}, cookieValues)

	// Need a new server because the HTTP Client is memoized.
	s2, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s2.Stopper().Stop(ctx)

	clientMT, err := s2.GetAuthenticatedHTTPClient(false, serverutils.MultiTenantSession)
	require.NoError(t, err)

	respMT, err := clientMT.Get(s.AdminURL() + "/logout")
	require.NoError(t, err)
	defer respMT.Body.Close()

	require.Equal(t, 200, respMT.StatusCode)
	cookieNames = make([]string, len(resp.Cookies()))
	cookieValues = make([]string, len(resp.Cookies()))
	for i, c := range resp.Cookies() {
		cookieNames[i] = c.Name
		cookieValues[i] = c.Value
	}
	require.ElementsMatch(t, []string{"session", "tenant"}, cookieNames)
	require.ElementsMatch(t, []string{"", ""}, cookieValues)

	// Now using manual clients to simulate states that might be invalid
	url, err := url.Parse(s2.AdminURL())
	require.NoError(t, err)
	cookieJar, err := cookiejar.New(nil)
	require.NoError(t, err)
	cookieJar.SetCookies(url, []*http.Cookie{
		{
			Name:  "multitenant-session",
			Value: "abc-123",
		},
	})
	clientMT.Jar = cookieJar

	respBadCookie, err := clientMT.Get(s.AdminURL() + "/logout")
	require.NoError(t, err)
	defer respBadCookie.Body.Close()

	require.Equal(t, 200, respMT.StatusCode)
	cookieNames = make([]string, len(resp.Cookies()))
	cookieValues = make([]string, len(resp.Cookies()))
	for i, c := range resp.Cookies() {
		cookieNames[i] = c.Name
		cookieValues[i] = c.Value
	}
	require.ElementsMatch(t, []string{"session", "tenant"}, cookieNames)
	require.ElementsMatch(t, []string{"", ""}, cookieValues)
}
